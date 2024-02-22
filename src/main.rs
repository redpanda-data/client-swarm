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

use log::*;

use crate::connections::connections;
use crate::consumers::consumers;
use crate::producers::{producers, Payload};

mod connections;
mod consumers;
mod producers;
mod utils;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Option<Commands>,
    #[clap(short, long)]
    brokers: String,
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
    },
    /// Creates consumer swarm
    Consumers {
        #[clap(short, long)]
        topic: String,
        #[clap(short, long, action)]
        unique_topics: bool,
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
    },
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli = Cli::parse();
    let brokers = cli.brokers;
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
            )
            .await;
        }
        Some(Commands::Consumers {
            topic,
            unique_topics,
            group,
            static_prefix,
            count,
            properties,
            messages,
        }) => {
            consumers(
                brokers,
                topic.clone(),
                unique_topics.clone(),
                group.clone(),
                static_prefix.clone(),
                *count,
                *messages,
                properties.clone(),
            )
            .await;
        }
        _ => {
            unimplemented!();
        }
    };
}
