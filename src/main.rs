use std::time::Duration;
use std::sync::Arc;
use std::sync::Mutex;

use rand::RngCore;
use tokio;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{
    FutureProducer, FutureRecord,

};


use log::*;

/// Stress the tcp_server_listen_backlog setting
/// A system with a small backlog will experience errors here: a system with
/// a larger backlog will not.
async fn connections(args: Vec<String>) {
    let addr_str = args.get(0).unwrap().to_string();
    let n_str = args.get(1).unwrap();
    let n: i32 = n_str.parse::<i32>().unwrap();

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
                },
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
    if  final_errs > 0 {
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
async fn producers(args: Vec<String>) {
    let brokers = args.get(0).unwrap();
    let topic = args.get(1).unwrap();
    let n: usize = args.get(2).unwrap().parse().unwrap();
    let m: usize = args.get(3).unwrap().parse().unwrap();

    let mut tasks = vec![];
    info!("Spawning {}", n);
    for i in 0..n {
        tasks.push(tokio::spawn(produce(brokers.clone(), topic.clone(), i, m)))
    }

    for t in tasks {
        t.await.unwrap();
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args: Vec<String> = std::env::args().collect();
    let command = args[1].clone();
    let trailing_args: Vec<String> = args[2..].to_vec();
    match command.as_str() {
        "connections" => connections(trailing_args).await,
        "producers" => {
            producers(trailing_args).await;
        }
        _ => {
            unimplemented!();
        }
    };
}
