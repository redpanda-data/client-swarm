// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

use futures::stream::TryStreamExt;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};

use rand::RngCore;
use tokio;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use log::*;

/// Stress the tcp_server_listen_backlog setting
/// A system with a small backlog will experience errors here: a system with
/// a larger backlog will not.
async fn connections(addr_str: String, n: i32) {
    let error_count = Arc::new(Mutex::new(0 as u32));
    let mut tasks = vec![];
    for _i in 0..n {
        let addr_str_copy = addr_str.clone();
        let error_count_ref = error_count.clone();
        tasks.push(tokio::spawn(async move {
            let con = TcpStream::connect(addr_str_copy).await;
            match con {
                Err(e) => {
                    let mut locked = error_count_ref.lock().unwrap();
                    *locked += 1;
                    println!("Connection error: {}", e);
                }
                Ok(mut sock) => {
                    for _j in 0..1000 {
                        match sock.write_all("ohaihowyoudoing".as_bytes()).await {
                            Ok(_bytes) => {}
                            Err(e) => {
                                println!("write error {}", e);
                                let mut locked = error_count_ref.lock().unwrap();
                                *locked += 1;
                                break;
                            }
                        }
                        sock.flush().await.unwrap();
                    }
                }
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    let final_errs = *(error_count.lock().unwrap());
    if final_errs > 0 {
        error!("{} connection attempts failed", final_errs);
        std::process::exit(-1);
    } else {
        info!("Completed with no connection errors.")
    }
}

fn split_properties(properties: Vec<String>) -> Vec<(String, String)> {
    return properties
        .iter()
        .map(|p| p.split("=").collect::<Vec<&str>>())
        .map(|parts| (parts[0].to_string(), parts[1].to_string()))
        .collect();
}

struct ProducerStats {
    rate: u64,
    total_size: usize,
    errors: u32,
}

async fn produce(
    brokers: String,
    topic: String,
    my_id: usize,
    m: usize,
    properties: Vec<(String, String)>,
    timeout: Duration,
) -> ProducerStats {
    debug!("Producer {} constructing", my_id);
    let mut cfg: ClientConfig = ClientConfig::new();
    cfg.set("bootstrap.servers", &brokers);

    // custom properties
    for (k, v) in properties {
        cfg.set(k, v);
    }
    let producer: FutureProducer = cfg.create().unwrap();

    let mut payload: Vec<u8> = vec![0x0f; 0x7fff];
    rand::thread_rng().fill_bytes(&mut payload);
    debug!("Producer {} sending", my_id);

    let start_time = Instant::now();
    let mut total_size: usize = 0;
    let mut errors: u32 = 0;

    for i in 0..m {
        let key = format!("{:#010x}", rand::thread_rng().next_u32() & 0x6ffff);
        let sz = (rand::thread_rng().next_u32() & 0x7fff) as usize;
        total_size += sz;
        let fut = producer.send(
            FutureRecord::to(&topic).key(&key).payload(&payload[0..sz]),
            timeout,
        );
        debug!("Producer {} waiting", my_id);
        match fut.await {
            Err((e, _msg)) => {
                warn!("Error on producer {} {}/{}: {}", my_id, i, m, e);
                errors += 1;
            }
            Ok(_) => {}
        }
    }

    let total_time = start_time.elapsed();
    let rate = total_size as u64 / total_time.as_secs();

    info!("Producer {} complete with rate {} bytes/s", my_id, rate);

    ProducerStats {
        rate,
        total_size,
        errors,
    }
}

/// Stress the system for very large numbers of producers
async fn producers(
    brokers: String,
    topic: String,
    m: usize,
    n: usize,
    properties: Vec<String>,
    timeout: Duration,
) {
    let mut tasks = vec![];
    let kv_pairs = split_properties(properties);
    let start_time = Instant::now();
    info!("Spawning {}", n);
    for i in 0..n {
        tasks.push(tokio::spawn(produce(
            brokers.clone(),
            topic.clone(),
            i,
            m,
            kv_pairs.clone(),
            timeout,
        )))
    }

    let mut results = vec![];
    let mut total_size: usize = 0;
    for t in tasks {
        let produce_stats = t.await.unwrap();
        results.push(produce_stats.rate);
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

struct ConsumeCounter {
    count: u64,
    target_count: Option<u64>,
}

/**
 * Shared state between the consume tasks, to track a global count + check it
 * against an exit condition.
 */
impl ConsumeCounter {
    pub fn new(target_count: Option<u64>) -> ConsumeCounter {
        ConsumeCounter {
            count: 0,
            target_count,
        }
    }

    pub fn record_borrowed_message_receipt(&mut self, msg: &BorrowedMessage<'_>) -> bool {
        // log every 10000 messages
        if msg.offset() % 10000 == 0 {
            debug!("Message received: {}", msg.offset());
        }
        self.count += 1;
        let c = match self.target_count {
            Some(i) => i,
            None => 0,
        };
        info!("Count {}/{}", self.count, c);
        match self.target_count {
            None => false,
            Some(limit) => self.count >= limit,
        }
    }
}

async fn consume(
    brokers: String,
    topic: String,
    group: String,
    static_prefix: Option<String>,
    properties: Vec<(String, String)>,
    my_id: usize,
    counter: Arc<Mutex<ConsumeCounter>>,
) {
    debug!("Consumer {} constructing", my_id);
    let mut cfg: ClientConfig = ClientConfig::new();
    // basic options
    cfg.set("group.id", &group)
        .set("bootstrap.servers", &brokers)
        .set("enable.partition.eof", "false")
        .set("socket.timeout.ms", "180000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest");

    match static_prefix {
        Some(prefix) => {
            cfg.set(
                "group.instance.id",
                format!("swarm-consumer-{}-{}", prefix, my_id),
            );
        }
        None => {}
    }

    // custom properties
    for (k, v) in properties {
        cfg.set(k, v);
    }

    let consumer: StreamConsumer = cfg.create().unwrap();

    debug!("Consumer {} fetching", my_id);
    consumer
        .subscribe(&[&topic])
        .expect("Can't subscribe to specified topic");

    let mut stream = consumer.stream();
    loop {
        let item = stream.try_next().await;
        let msg = match item.expect("Error reading from stream") {
            Some(msg) => msg,
            None => {
                continue;
            }
        };

        let mut counter_locked = counter.lock().unwrap();
        let complete = (*counter_locked).record_borrowed_message_receipt(&msg);
        if complete {
            break;
        }
    }
}

/// Stress the system for very large numbers of consumers
async fn consumers(
    brokers: String,
    topic: String,
    group: String,
    static_prefix: Option<String>,
    n: usize,
    messages: Option<u64>,
    properties: Vec<String>,
) {
    let kv_pairs = split_properties(properties);
    let mut tasks = vec![];
    info!("Spawning {} consumers", n);

    let counter = Arc::new(Mutex::new(ConsumeCounter::new(messages)));

    for i in 0..n {
        tasks.push(tokio::spawn(consume(
            brokers.clone(),
            topic.clone(),
            group.clone(),
            static_prefix.clone(),
            kv_pairs.clone(),
            i,
            counter.clone(),
        )))
    }

    for t in tasks {
        t.await.unwrap();
    }
}

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
        #[clap(short, long)]
        count: usize,
        #[clap(short, long)]
        messages: usize,
        // list of librdkafka producer properties to set as `key=value` pairs
        #[clap(short, long)]
        properties: Vec<String>,
        #[clap(short, long, default_value_t = 1000)]
        timeout_ms: u64,
    },
    /// Creates consumer swarm
    Consumers {
        #[clap(short, long)]
        topic: String,
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
            count,
            messages,
            properties,
            timeout_ms,
        }) => {
            producers(
                brokers,
                topic.clone(),
                *messages,
                *count,
                properties.clone(),
                Duration::from_millis(*timeout_ms),
            )
            .await;
        }
        Some(Commands::Consumers {
            topic,
            group,
            static_prefix,
            count,
            properties,
            messages,
        }) => {
            consumers(
                brokers,
                topic.clone(),
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
