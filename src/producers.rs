// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

use lazy_static::lazy_static;
use rand::seq::SliceRandom;
use std::num::NonZeroU32;
use std::time::{Duration, Instant};

use governor::{Quota, RateLimiter};

use rand::RngCore;

use tokio;
use tokio::sync::mpsc;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use log::*;

use crate::metrics::{ClientMessages, MetricsContext};
use crate::utils::split_properties;

const MAX_COMPRESSIBLE_PAYLOAD: usize = 128 * 1024 * 1024;

lazy_static! {
    static ref COMPRESSIBLE_PAYLOAD: [u8; MAX_COMPRESSIBLE_PAYLOAD] =
        [0x0f; MAX_COMPRESSIBLE_PAYLOAD];
}

pub struct ProducerStats {
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
}

async fn produce(
    brokers: String,
    topic: String,
    my_id: usize,
    m: usize,
    messages_per_second: Option<NonZeroU32>,
    properties: Vec<(String, String)>,
    payload: Payload,
    timeout: Duration,
    metrics: mpsc::Sender<ClientMessages>,
) -> ProducerStats {
    debug!("Producer {} constructing", my_id);
    let mut cfg: ClientConfig = ClientConfig::new();
    cfg.set("bootstrap.servers", &brokers);
    cfg.set("message.max.bytes", "1000000000");

    // Stash compression mode for use in log messages
    let mut compression: Option<String> = None;

    // custom properties
    for (k, v) in properties {
        if k == "compression.type" {
            compression = Some(v.clone());
        }
        cfg.set(k, v);
    }
    let producer: FutureProducer = cfg.create().unwrap();

    let mut local_payload: Option<Vec<u8>> = None;
    if !payload.compressible {
        local_payload = Some(vec![0x0f; payload.max_size]);
        rand::thread_rng().fill_bytes(local_payload.as_mut().unwrap());
    }

    debug!("Producer {} sending", my_id);

    let start_time = Instant::now();
    let mut total_size: usize = 0;
    let mut errors: u32 = 0;

    let mut rate_limit = if messages_per_second.is_some() {
        Option::Some(RateLimiter::direct(Quota::per_second(
            messages_per_second.unwrap(),
        )))
    } else {
        None
    };

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

        let payload_slice: &[u8] = if payload.compressible {
            &COMPRESSIBLE_PAYLOAD.as_slice()[0..sz]
        } else {
            &local_payload.as_ref().unwrap()[0..sz]
        };

        total_size += sz;
        let fut = producer.send(
            FutureRecord::to(&topic).key(&key).payload(payload_slice),
            timeout,
        );
        debug!("Producer {} waiting", my_id);
        match fut.await {
            Err((e, _msg)) => {
                let compression: &str = compression
                    .as_ref()
                    .map(|s: &String| s.as_str())
                    .unwrap_or("none");
                warn!(
                    "Error on producer {} {}/{}, producing {} bytes, compression={}, compressible={} : {}",
                    my_id, i, m, sz, compression, payload.compressible, e
                );
                errors += 1;
            }
            Ok(_) => {
                if let Err(e) = metrics
                    .send(ClientMessages::MessageProcessed { client_id: my_id })
                    .await
                {
                    error!("Error on producer {}, unable to send metrics: {}", my_id, e);
                }
            }
        }
    }

    let total_time = start_time.elapsed();
    let mut rate = total_size as u64;
    if total_time.as_secs() != 0 {
        rate = total_size as u64 / total_time.as_secs();
    }

    info!("Producer {} complete with rate {} bytes/s", my_id, rate);

    ProducerStats {
        rate,
        total_size,
        errors,
    }
}

/// Stress the system for very large numbers of producers
pub async fn producers(
    brokers: String,
    topic: String,
    unique_topics: bool,
    m: usize,
    messages_per_second: Option<NonZeroU32>,
    n: usize,
    properties: Vec<String>,
    compression_type: Option<String>,
    payload: Payload,
    timeout: Duration,
    metrics: MetricsContext,
) {
    let mut tasks = vec![];
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
        let mut topic_prefix = topic.clone();
        if unique_topics {
            topic_prefix = format!("{}-{}", topic, i);
        }
        tasks.push(tokio::spawn(produce(
            brokers.clone(),
            topic_prefix.clone(),
            i,
            m,
            messages_per_second,
            cfg_pairs,
            payload.clone(),
            timeout,
            metrics.spawn_new_sender(),
        )))
    }

    let mut results = vec![];
    let mut total_size: usize = 0;
    for (i, t) in tasks.into_iter().enumerate() {
        info!("Joining producer {}...", i);
        let produce_stats = t.await.unwrap();
        results.push(produce_stats.rate);
        if produce_stats.errors > 0 {
            warn!("Producer {}  had {} errors", i, produce_stats.errors);
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
