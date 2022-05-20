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
use std::time::Duration;

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

async fn produce(brokers: String, topic: String, my_id: usize, m: usize) {
    debug!("Producer {} constructing", my_id);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("linger.ms", "100")
        .set("retries", "0")
        .set("batch.size", "16777216")
        .set("acks", "-1")
        .create()
        .unwrap();

    let mut payload: Vec<u8> = vec![0x0f; 0x7fff];
    rand::thread_rng().fill_bytes(&mut payload);
    debug!("Producer {} sending", my_id);

    for i in 0..m {
        let key = format!("{:#010x}", rand::thread_rng().next_u32() & 0x6ffff);
        let sz = (rand::thread_rng().next_u32() & 0x7fff) as usize;
        let fut = producer.send(
            FutureRecord::to(&topic).key(&key).payload(&payload[0..sz]),
            Duration::from_millis(1000),
        );
        debug!("Producer {} waiting", my_id);
        match fut.await {
            Err((e, _msg)) => warn!("Error on producer {} {}/{}: {}", my_id, i, m, e),
            Ok(_) => {}
        }
    }
    info!("Producer {} complete", my_id);
}

/// Stress the system for very large numbers of producers
async fn producers(brokers: String, topic: String, m: usize, n: usize) {
    let mut tasks = vec![];
    info!("Spawning {}", n);
    for i in 0..n {
        tasks.push(tokio::spawn(produce(brokers.clone(), topic.clone(), i, m)))
    }

    for t in tasks {
        t.await.unwrap();
    }
}

// log every 10000 messages
async fn record_borrowed_message_receipt(msg: &BorrowedMessage<'_>) {
    if msg.offset() % 10000 == 0 {
        debug!("Message received: {}", msg.offset());
    }
}

async fn consume(
    brokers: String,
    topic: String,
    group: String,
    static_prefix: Option<String>,
    properties: Vec<(String, String)>,
    my_id: usize,
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

    let stream_processor = consumer
        .stream()
        .try_for_each(|borrowed_message| async move {
            record_borrowed_message_receipt(&borrowed_message).await;
            return Ok(());
        });

    stream_processor.await.expect("stream processing failed");
}

/// Stress the system for very large numbers of consumers
async fn consumers(
    brokers: String,
    topic: String,
    group: String,
    static_prefix: Option<String>,
    n: usize,
    properties: Vec<String>,
) {
    let mut kv_pairs = vec![];
    for p in properties {
        let parts = p.split("=").collect::<Vec<&str>>();
        kv_pairs.push((parts[0].to_string(), parts[1].to_string()))
    }
    let mut tasks = vec![];
    info!("Spawning {} consumers", n);
    for i in 0..n {
        tasks.push(tokio::spawn(consume(
            brokers.clone(),
            topic.clone(),
            group.clone(),
            static_prefix.clone(),
            kv_pairs.clone(),
            i,
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
        }) => {
            producers(brokers, topic.clone(), *messages, *count).await;
        }
        Some(Commands::Consumers {
            topic,
            group,
            static_prefix,
            count,
            properties,
        }) => {
            consumers(
                brokers,
                topic.clone(),
                group.clone(),
                static_prefix.clone(),
                *count,
                properties.clone(),
            )
            .await;
        }
        _ => {
            unimplemented!();
        }
    };
}
