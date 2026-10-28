// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

use async_std::sync::Arc;
use lazy_static::lazy_static;
use rand::seq::SliceRandom;
use std::fs;
use std::time::{Duration, Instant};
use tokio::task::{JoinError, JoinSet};

use governor::{Quota, RateLimiter};

use rand::RngCore;

use tokio;
use tokio::sync::mpsc;

use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::producer::{FutureProducer, FutureRecord};

use log::*;

use crate::metrics::{ClientMessages, MetricsContext};
use crate::utils::{auth_progressed, split_properties, ExitOnAuthFailure};

const MAX_COMPRESSIBLE_PAYLOAD: usize = 128 * 1024 * 1024;
const QUEUE_FULL_BACKOFF: Duration = Duration::from_millis(1);

lazy_static! {
    static ref COMPRESSIBLE_PAYLOAD: [u8; MAX_COMPRESSIBLE_PAYLOAD] =
        [0x0f; MAX_COMPRESSIBLE_PAYLOAD];
}

#[derive(Clone)]
pub struct DirectoryPayloadSource {
    payloads: Arc<Vec<Vec<u8>>>,
}

impl DirectoryPayloadSource {
    pub fn new(payload_dir: String) -> DirectoryPayloadSource {
        let mut data_file_locs = fs::read_dir(payload_dir)
            .expect("invalid payload directory")
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.is_file() && p.extension().and_then(|e| e.to_str()) == Some("data"))
            .peekable();

        assert!(
            data_file_locs.peek().is_some(),
            "no data files in payload directory"
        );

        DirectoryPayloadSource {
            payloads: Arc::new(
                data_file_locs
                    .map(|p| {
                        fs::read(&p).expect(&format!("unable to read payload file at {:#?}", p))
                    })
                    .collect(),
            ),
        }
    }

    fn rand_payload(&self) -> &[u8] {
        self.payloads.choose(&mut rand::thread_rng()).unwrap()
    }
}

pub struct ProducerStats {
    id: usize,
    rate: u64,
    total_size: usize,
    errors: u32,
}

#[derive(Clone)]
pub struct Payload {
    pub key_range: u64,
    pub compressible: bool,
    pub min_size: usize,
    pub max_size: usize,
    pub payload_directory: Option<DirectoryPayloadSource>,
}

type Joined<D, M, C> = Result<(usize, usize, Result<Result<D, (KafkaError, M)>, C>), JoinError>;

struct Outcomes<'a> {
    my_id: usize,
    m: usize,
    compression: &'a str,
    compressible: bool,
    metrics: &'a mpsc::Sender<ClientMessages>,
    errors: u32,
    start_sent: bool,
}

impl<'a> Outcomes<'a> {
    async fn send_metric(&self, msg: ClientMessages) {
        if let Err(e) = self.metrics.send(msg).await {
            error!(
                "Error on producer {}, unable to send metrics message: {}",
                self.my_id, e
            );
        }
    }

    async fn record(&mut self, i: usize, sz: usize, result: Result<(), KafkaError>) {
        match result {
            Ok(()) => {
                auth_progressed();
                if !self.start_sent {
                    self.start_sent = true;
                    self.send_metric(ClientMessages::ClientStart {
                        client_id: self.my_id,
                    })
                    .await;
                }
                self.send_metric(ClientMessages::MessageSuccess {
                    client_id: self.my_id,
                })
                .await;
            }
            Err(e) => {
                warn!(
                    "Error on producer {} {}/{}, producing {} bytes, compression={}, compressible={} : {}",
                    self.my_id, i, self.m, sz, self.compression, self.compressible, e
                );
                self.errors += 1;
                self.send_metric(ClientMessages::MessageFailure {
                    client_id: self.my_id,
                })
                .await;
            }
        }
    }

    async fn delivered<D, M, C>(&mut self, joined: Joined<D, M, C>) {
        match joined {
            Ok((i, sz, Ok(Ok(_)))) => self.record(i, sz, Ok(())).await,
            Ok((i, sz, Ok(Err((e, _))))) => self.record(i, sz, Err(e)).await,
            Ok((i, sz, Err(_))) => self.record(i, sz, Err(KafkaError::Canceled)).await,
            Err(e) => {
                error!("Producer {} delivery task failed: {}", self.my_id, e);
                self.errors += 1;
                self.send_metric(ClientMessages::MessageFailure {
                    client_id: self.my_id,
                })
                .await;
            }
        }
    }
}

async fn produce(
    brokers: String,
    topics: Vec<String>,
    my_id: usize,
    m: usize,
    message_period: Option<Duration>,
    properties: Vec<(String, String)>,
    payload: Payload,
    timeout: Duration,
    max_in_flight: usize,
    metrics: mpsc::Sender<ClientMessages>,
) -> ProducerStats {
    debug!("Producer {} constructing", my_id);
    let mut cfg: ClientConfig = ClientConfig::new();
    cfg.set("bootstrap.servers", &brokers);
    cfg.set("message.max.bytes", "1000000000");

    let mut compression: Option<String> = None;

    for (k, v) in properties {
        if k == "compression.type" {
            compression = Some(v.clone());
        }
        cfg.set(k, v);
    }
    let producer: FutureProducer<ExitOnAuthFailure> =
        cfg.create_with_context(ExitOnAuthFailure).unwrap();

    let mut local_payload: Option<Vec<u8>> = None;
    if !payload.compressible && payload.payload_directory.is_none() {
        local_payload = Some(vec![0x0f; payload.max_size]);
        rand::thread_rng().fill_bytes(local_payload.as_mut().unwrap());
    }

    debug!(
        "Producer {} sending with up to {} messages in flight",
        my_id, max_in_flight
    );

    let start_time = Instant::now();
    let mut total_size: usize = 0;

    let mut rate_limit = {
        if let Some(mp) = message_period {
            Some(RateLimiter::direct(Quota::with_period(mp).unwrap()))
        } else {
            None
        }
    };

    let compression_name = compression.clone().unwrap_or_else(|| "none".to_string());
    let mut outcomes = Outcomes {
        my_id,
        m,
        compression: &compression_name,
        compressible: payload.compressible,
        metrics: &metrics,
        errors: 0,
        start_sent: false,
    };

    let mut in_flight = JoinSet::new();
    let mut current_producer_index = 0;

    for i in 0..m {
        if let Some(rl) = &mut rate_limit {
            rl.until_ready().await;
        }

        let key = format!(
            "{:#016x}",
            rand::thread_rng().next_u64() % payload.key_range
        );
        let sz = if payload.min_size != payload.max_size {
            payload.min_size
                + rand::thread_rng().next_u32() as usize % (payload.max_size - payload.min_size)
        } else {
            payload.max_size
        };

        let payload_slice: &[u8] = if let Some(ref dir_src) = payload.payload_directory {
            dir_src.rand_payload()
        } else if payload.compressible {
            &COMPRESSIBLE_PAYLOAD.as_slice()[0..sz]
        } else {
            &local_payload.as_ref().unwrap()[0..sz]
        };

        total_size += sz;
        let topic = &topics[current_producer_index];
        current_producer_index = (current_producer_index + 1) % topics.len();

        let enqueue_deadline = Instant::now() + timeout;
        let enqueued = loop {
            match producer.send_result(FutureRecord::to(topic).key(&key).payload(payload_slice)) {
                Ok(delivery) => break Ok(delivery),
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), _))
                    if Instant::now() < enqueue_deadline =>
                {
                    tokio::time::sleep(QUEUE_FULL_BACKOFF).await;
                }
                Err((e, _)) => break Err(e),
            }
        };

        match enqueued {
            Ok(delivery) => {
                in_flight.spawn(async move { (i, sz, delivery.await) });
            }
            Err(e) => outcomes.record(i, sz, Err(e)).await,
        }

        debug!("Producer {} waiting", my_id);
        while in_flight.len() >= max_in_flight {
            match in_flight.join_next().await {
                Some(joined) => outcomes.delivered(joined).await,
                None => break,
            }
        }
    }

    while let Some(joined) = in_flight.join_next().await {
        outcomes.delivered(joined).await;
    }

    let total_time = start_time.elapsed();
    let mut rate = total_size as u64;
    if total_time.as_secs() != 0 {
        rate = total_size as u64 / total_time.as_secs();
    }

    info!("Producer {} complete with rate {} bytes/s", my_id, rate);

    if outcomes.start_sent {
        outcomes
            .send_metric(ClientMessages::ClientStop { client_id: my_id })
            .await;
    }

    ProducerStats {
        id: my_id,
        rate,
        total_size,
        errors: outcomes.errors,
    }
}

/// Stress the system for very large numbers of producers
pub async fn producers(
    brokers: String,
    topic: String,
    unique_topics: bool,
    topics_per_producer: usize,
    m: usize,
    message_period: Option<Duration>,
    n: usize,
    properties: Vec<String>,
    compression_type: Option<String>,
    payload: Payload,
    timeout: Duration,
    metrics: MetricsContext,
    client_spawn_wait: Duration,
    max_in_flight: usize,
) {
    let mut tasks = JoinSet::<ProducerStats>::new();
    let kv_pairs = split_properties(properties);
    let start_time = Instant::now();
    info!("Spawning {}", n);
    for i in 0..n {
        let mut cfg_pairs = kv_pairs.clone();
        if let Some(c_type) = &compression_type {
            if c_type == "mixed" {
                let types: Vec<&str> = vec!["gzip", "lz4", "snappy", "zstd"];

                cfg_pairs.push((
                    "compression.type".to_string(),
                    types.choose(&mut rand::thread_rng()).unwrap().to_string(),
                ));
            } else {
                cfg_pairs.push(("compression.type".to_string(), c_type.clone()));
            }
        }

        let topics = if unique_topics {
            (0..topics_per_producer)
                .map(|j| format!("{}-{}", topic, i * topics_per_producer + j))
                .collect()
        } else {
            vec![topic.clone()]
        };

        tasks.spawn(produce(
            brokers.clone(),
            topics,
            i,
            m,
            message_period,
            cfg_pairs,
            payload.clone(),
            timeout,
            max_in_flight,
            metrics.spawn_new_sender(),
        ));

        tokio::time::sleep(client_spawn_wait).await;
    }

    let mut results = vec![];
    let mut total_size: usize = 0;

    let mut remaining = n;
    while let Some(t) = tasks.join_next().await {
        remaining -= 1;
        let produce_stats = t.unwrap();
        let id = produce_stats.id;
        info!("Joined producer ID {}, {}/{} remaining.", id, remaining, n);
        results.push(produce_stats.rate);
        if produce_stats.errors > 0 {
            warn!("Producer {} had {} errors", id, produce_stats.errors);
        }
        total_size += produce_stats.total_size;
    }

    let total_time = start_time.elapsed();

    if !results.is_empty() {
        let min_result = *results.iter().min().unwrap();
        let max_result = *results.iter().max().unwrap();
        let avg_result = results.iter().sum::<u64>() / results.len() as u64;
        info!(
            "Producer rates: [min={}, max={}, avg={}] bytes/s",
            min_result, max_result, avg_result
        );

        let rate = total_size as u64 / total_time.as_secs();
        // Depending on how many producers are running in parallel
        // the global produce rate could be more or less even if the
        // produce rate for individual producers remains the same.
        info!("Global produce rate: {} bytes/s", rate);
    }

    info!("All producers complete");
}
