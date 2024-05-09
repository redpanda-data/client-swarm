// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

use std::num::NonZeroU32;
use std::process;
use std::time::Duration;

use clap::{Parser, Subcommand};

use tokio;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use log::*;

use crate::connections::connections;
use crate::consumers::consumers;
use crate::producers::{producers, Payload};

mod connections;
mod consumers;
mod metrics;
mod producers;
mod server;
mod utils;

use shadow_rs::shadow;

shadow!(build);

#[derive(Parser)]
#[clap(author, version, about, long_about = None, long_version = build::CLAP_LONG_VERSION)]
struct Cli {
    #[clap(subcommand)]
    command: Option<Commands>,
    #[clap(short, long)]
    brokers: String,
    #[clap(short = 'a', long, default_value = "127.0.0.1")]
    metrics_address: String,
    #[clap(short = 'p', long, default_value_t = 8080)]
    metrics_port: u16,
}

#[derive(Subcommand)]
enum Commands {
    /// Stress the tcp_server_listen_backlog setting
    Connections {
        #[clap(short, long)]
        number: i32,
    },
    /// Creates a producer swarm
    Producers {
        #[clap(short, long)]
        topic: String,
        #[clap(short, long, action)]
        unique_topics: bool,
        #[clap(short, long)]
        count: usize,
        #[clap(short, long)]
        messages: usize,
        #[clap(short = 'r', long, default_value_t = 0)]
        messages_per_second: u32,
        // list of librdkafka producer properties to set as `key=value` pairs
        #[clap(short, long)]
        properties: Vec<String>,
        #[clap(short = 'o', long, default_value_t = 1000)]
        timeout_ms: u64,
        #[clap(long)]
        compression_type: Option<String>,
        #[clap(long)]
        min_record_size: Option<usize>,
        #[clap(long, default_value_t = 16384)]
        max_record_size: usize,
        #[clap(long)]
        compressible_payload: bool,
        #[clap(long, default_value_t = 1000)]
        keys: u64,
        #[clap(long, default_value_t = 33)]
        client_spawn_wait_ms: u64,
    },
    /// Creates consumer swarm
    Consumers {
        #[clap(short, long)]
        topic: String,
        #[clap(short, long, action)]
        unique_topics: bool,
        // creates a unique consumer group per consumer
        #[clap(short, long, action)]
        unique_groups: bool,
        #[clap(short, long)]
        group: String,
        /// if set uses static group membership protocol
        #[clap(short, long)]
        static_prefix: Option<String>,
        #[clap(short, long)]
        count: usize,
        // list of librdkafka consumer properties to set as `key=value` pairs
        #[clap(short, long)]
        properties: Vec<String>,
        #[clap(short, long)]
        messages: Option<u64>,
        #[clap(long, default_value_t = 33)]
        client_spawn_wait_ms: u64,
    },
}

fn start_metrics(
    metrics_config: metrics::MetricsConfig,
    window_config: metrics::WindowConfig,
    srv_cfg: server::ServerConfig,
    cancel: CancellationToken,
) -> (metrics::MetricsContext, Vec<tokio::task::JoinHandle<()>>) {
    let (metrics_send, metrics_recv) = mpsc::channel(64);
    let (commands_send, commands_recv) = mpsc::channel(64);

    let mut metrics = metrics::Metrics::new(
        metrics_config,
        window_config.clone(),
        metrics_recv,
        commands_recv,
        cancel.clone(),
    );

    let metrics_service = server::MetricsService::new(commands_send);
    let mut srv = server::Server::new(srv_cfg, metrics_service, cancel.clone());

    let mut join_handles = vec![];

    join_handles.push(tokio::spawn(async move {
        metrics.run().await;
    }));

    join_handles.push(tokio::spawn(async move {
        srv.run().await;
    }));

    let mc = metrics::MetricsContext::new(window_config, metrics_send, cancel.clone());

    (mc, join_handles)
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli = Cli::parse();
    let brokers = cli.brokers;
    let addr = cli.metrics_address.parse::<std::net::Ipv4Addr>().unwrap();

    // setup metrics
    let token = CancellationToken::new();
    let (mc, join_handles) = start_metrics(
        metrics::MetricsConfig { max_samples: 100 },
        metrics::WindowConfig {
            window_start: tokio::time::Instant::now(),
            window_duration: Duration::from_secs(10),
        },
        server::ServerConfig {
            addr: (addr, cli.metrics_port).into(),
        },
        token.clone(),
    );

    match &cli.command {
        Some(Commands::Connections { number }) => {
            connections(brokers, *number).await;
        }
        Some(Commands::Producers {
            topic,
            unique_topics,
            count,
            messages,
            messages_per_second,
            properties,
            compression_type,
            compressible_payload,
            min_record_size,
            max_record_size,
            keys,
            timeout_ms,
            client_spawn_wait_ms,
        }) => {
            let min_size = min_record_size.unwrap_or(*max_record_size);
            if let Some(min) = min_record_size {
                if max_record_size < min {
                    error!("Max record size must be >= min record size");
                    process::exit(-1);
                }
            }

            // Default arg value 0 will return None here (no rate limiting)
            let mps_opt = NonZeroU32::new(*messages_per_second);

            producers(
                brokers,
                topic.clone(),
                unique_topics.clone(),
                *messages,
                mps_opt,
                *count,
                properties.clone(),
                (*compression_type).clone(),
                Payload {
                    key_range: *keys,
                    compressible: *compressible_payload,
                    min_size,
                    max_size: *max_record_size,
                },
                Duration::from_millis(*timeout_ms),
                mc,
                Duration::from_millis(*client_spawn_wait_ms),
            )
            .await;
        }
        Some(Commands::Consumers {
            topic,
            unique_topics,
            unique_groups,
            group,
            static_prefix,
            count,
            properties,
            messages,
            client_spawn_wait_ms,
        }) => {
            consumers(
                brokers,
                topic.clone(),
                unique_topics.clone(),
                unique_groups.clone(),
                group.clone(),
                static_prefix.clone(),
                *count,
                *messages,
                properties.clone(),
                mc,
                Duration::from_millis(*client_spawn_wait_ms),
            )
            .await;
        }
        _ => {
            unimplemented!();
        }
    };

    token.cancel();
    for h in join_handles {
        h.await.unwrap();
    }
}
