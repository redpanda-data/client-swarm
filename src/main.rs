use std::net::SocketAddr;
use std::str::FromStr;
use tokio;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use log::*;
use rand::RngCore;

/// Stress the tcp_server_listen_backlog setting
async fn connections() {
    let mut tasks = vec![];

    let args: Vec<String> = std::env::args().collect();
    let n_str = args.get(1).unwrap();
    let n: i32 = n_str.parse::<i32>().unwrap();

    for _i in 0..n {
        tasks.push(tokio::spawn(async {
            let con = TcpStream::connect(SocketAddr::from_str("192.168.1.100:9092").unwrap()).await;
            match con {
                Err(e) => println!("Connection error {}", e),
                Ok(mut sock) => {
                    for _j in 0..1000 {
                        match sock.write_all("ohaihowyoudoing".as_bytes()).await {
                            Ok(_bytes) => {}
                            Err(e) => {
                                println!("write error {}", e);
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
}

async fn produce(brokers: String, topic: String, my_id: usize, m: usize) {
    debug!("Producer {} constructing", my_id);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("linger.ms", "10")
        .set("retries", "0")
        .set("batch.size", "16777216")
        .create()
        .unwrap();
    let key = format!("{}", rand::thread_rng().next_u32());
    let mut payload: Vec<u8> = vec![0x0f; 0x1ffff];
    rand::thread_rng().fill_bytes(&mut payload);
    debug!("Producer {} sending", my_id);

    for i in 0..m {
        let sz = (rand::thread_rng().next_u32() & 0x7fff) as usize;
        let fut = producer.send(
            FutureRecord::to(&topic).key(&key).payload(&payload[0..sz]),
            Duration::from_millis(10000),
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
async fn producers() {
    let args: Vec<String> = std::env::args().collect();
    let brokers = args.get(1).unwrap();
    let topic = args.get(2).unwrap();
    let n: usize = args.get(3).unwrap().parse().unwrap();
    let m: usize = args.get(4).unwrap().parse().unwrap();

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
    //connections().await;

    producers().await;
}
