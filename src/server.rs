// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

use hyper::{self, body};
use hyper::{server::conn::http1, service::service_fn};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::tokio::TokioIo;

use http_body_util::Full;

use bytes::Bytes;

use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use std::collections::HashMap;
use std::net::SocketAddr;

use tokio::sync::{mpsc, oneshot};

use serde::Serialize;

use crate::metrics::{self, LastMetricsResult, MessageCounts};

use log::*;

fn parse_query(q: &str) -> HashMap<String, String> {
    form_urlencoded::parse(q.as_bytes())
        .into_owned()
        .collect::<HashMap<String, String>>()
}

#[derive(Serialize)]
pub struct SummaryStats {
    min: f64,
    max: f64,
    median: f64,
    counts_from_start: MessageCounts,
    clients_started: i64,
    clients_stopped: i64,
}

#[derive(Debug, Clone)]
pub struct MetricsService {
    m: mpsc::Sender<metrics::MetricsCommands>,
}

impl MetricsService {
    pub fn new(m: mpsc::Sender<metrics::MetricsCommands>) -> MetricsService {
        MetricsService { m }
    }

    async fn get_summary_stats(
        &mut self,
        from: Option<std::time::Duration>,
    ) -> Option<SummaryStats> {
        let (tx, rx) = oneshot::channel();

        if let Err(e) = self
            .m
            .send(metrics::MetricsCommands::GetLastMetrics {
                window_duration: from,
                resp: tx,
            })
            .await
        {
            error!("Couldn't get summary stats: {}", e);
            return None;
        }

        fn pct(resp: &LastMetricsResult, p: f64) -> f64 {
            // get the pth percentile and convert from msg/Msec to msg/sec
            let b = resp.msg_rate.percentile(p).unwrap().start() as f64 / 1000000.;
            let e = resp.msg_rate.percentile(p).unwrap().end() as f64 / 1000000.;
            // return the bucket midpoint
            b + (e - b) / 2.
        }

        match rx.await {
            Ok(resp) => Some(SummaryStats {
                min: pct(&resp, f64::MIN_POSITIVE), // use 'eps' since 0.0 just returns the first bucket with lower bound 0
                max: pct(&resp, 100.),
                median: pct(&resp, 50.),
                counts_from_start: resp.total_counts,
                clients_started: resp.clients_started,
                clients_stopped: resp.clients_stopped,
            }),
            Err(e) => {
                error!("Couldn't get summary stats: {}", e);
                None
            }
        }
    }

    async fn handle(
        &mut self,
        req: Request<body::Incoming>,
    ) -> Result<Response<Full<Bytes>>, hyper::Error> {
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/metrics/summary") => {
                let params = req.uri().query().map(|q| parse_query(q));
                let seconds = params
                    .and_then(|q| {
                        if let Some(s) = q.get("seconds") {
                            Some(s.clone())
                        } else {
                            None
                        }
                    })
                    .and_then(|s: String| s.parse::<u64>().ok())
                    .map(|v: u64| std::time::Duration::from_secs(v));

                let j = self
                    .get_summary_stats(seconds)
                    .await
                    .and_then(|stats| serde_json::to_string(&stats).ok());

                if let Some(body) = j {
                    Ok(Response::builder()
                        .status(StatusCode::OK)
                        .body(Full::new(Bytes::from(body)))
                        .unwrap())
                } else {
                    Ok(Response::builder()
                        .status(StatusCode::EXPECTATION_FAILED)
                        .body(Full::new(Bytes::from("")))
                        .unwrap())
                }
            }
            _ => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from("")))
                .unwrap()),
        }
    }
}

pub struct ServerConfig {
    pub addr: SocketAddr,
}

pub struct Server {
    config: ServerConfig,
    ms: MetricsService,
    cancel: CancellationToken,
}

impl Server {
    pub fn new(config: ServerConfig, ms: MetricsService, cancel: CancellationToken) -> Server {
        Server { config, ms, cancel }
    }

    pub async fn run(&mut self) {
        let listener = TcpListener::bind(self.config.addr).await.unwrap();

        loop {
            tokio::select! {
                Ok((stream, _)) = listener.accept() => {
                    let io = TokioIo::new(stream);
                    let ms = self.ms.clone();
                    let ms_service = service_fn(move |r| {
                        let mut ms_1 = ms.clone();
                        async move {
                            ms_1.handle(r).await
                        }
                    });

                    tokio::task::spawn(async move {
                        if let Err(err) = http1::Builder::new().serve_connection(io, ms_service).await {
                           error!("Failed to serve connection: {:?}", err);
                        }
                    });
                },
                _ = self.cancel.cancelled() => { return },
                else => { return },
            }
        }
    }
}
