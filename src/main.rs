use std::time::Duration;
use std::sync::Arc;
use std::sync::Mutex;

use rand::RngCore;
use serde::{Deserialize, Serialize};
use tokio;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::error::{KafkaResult, RDKafkaErrorCode};
use rdkafka::producer::{
    BaseRecord, DefaultProducerContext, FutureProducer, FutureRecord, Producer,
    ThreadedProducer,
};
use rdkafka::util::Timeout;
use rdkafka::Message;


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

struct TxProducer {
    producer: ThreadedProducer<DefaultProducerContext>,
    notx_producer: ThreadedProducer<DefaultProducerContext>,
    topic1: String,
    topic2: String,
}

impl TxProducer {
    fn new(brokers: String, topic1: String, topic2: String) -> Self {
        let producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
            .set("bootstrap.servers", brokers.clone())
            .set("linger.ms", "0")
            .set("retries", "3")
            .set("transactional.id", "test1")
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .create()
            .unwrap();

        let notx_producer: ThreadedProducer<DefaultProducerContext> = ClientConfig::new()
            .set("bootstrap.servers", brokers.clone())
            .set("retries", "3")
            .set("linger.ms", "0")
            .set("acks", "all")
            .create()
            .unwrap();

        producer.init_transactions(Timeout::Never).unwrap();

        Self {
            producer,
            notx_producer,
            topic1,
            topic2,
        }
    }

    fn send_tx(&self, seq: u64) -> KafkaResult<()> {
        let key1 = format!("key1_tx_{:#010x}", rand::thread_rng().next_u32() & 0xffff);
        let key2 = format!("key1_tx_{:#010x}", rand::thread_rng().next_u32() & 0xffff);
        let payload1 = serde_json::to_string(&TxMsg {
            test_complete: false,
            is_aborted: false,
            seq,
            other_topic: self.topic2.clone(),
        })
        .unwrap();
        let payload2 = serde_json::to_string(&TxMsg {
            test_complete: false,
            is_aborted: false,
            seq,
            other_topic: self.topic2.clone(),
        })
        .unwrap();

        // Intentionally send topic1 before topic1: reader will read topic1 and
        // then go read topic2 to confirm the write landed there.
        self.producer.begin_transaction()?;
        self.producer
            .send(
                BaseRecord::to(&self.topic2)
                    .key(&key2)
                    .payload(payload2.as_bytes()),
            )
            .map_err(|e| e.0)?;

        self.producer
            .send(
                BaseRecord::to(&self.topic1)
                    .key(&key1)
                    .payload(payload1.as_bytes()),
            )
            .map_err(|e| e.0)?;

        info!("Producer committing {}...", seq);
        self.producer.commit_transaction(Timeout::Never)?;
        info!("Producer committed {}.", seq);
        Ok(())
    }

    fn send_notx(&self, seq: u64) -> KafkaResult<()> {
        // Non-transactional write
        let key1 = format!("key1_notx_{:#010x}", rand::thread_rng().next_u32() & 0xffff);
        let key2 = format!("key2_notx_{:#010x}", rand::thread_rng().next_u32() & 0xffff);
        let payload1 = serde_json::to_string(&TxMsg {
            test_complete: false,
            is_aborted: false,
            seq,
            other_topic: self.topic2.clone(),
        })
        .unwrap();
        let payload2 = serde_json::to_string(&TxMsg {
            test_complete: false,
            is_aborted: false,
            seq,
            other_topic: self.topic2.clone(),
        })
        .unwrap();

        self.notx_producer
            .send(
                BaseRecord::to(&self.topic2)
                    .key(&key2)
                    .payload(payload2.as_bytes()),
            )
            .map_err(|e| e.0)?;
        // the reader's expectation that when they see the topic1 message, topic 2
        // message must already be available.
        self.notx_producer.flush(Timeout::Never);

        self.notx_producer
            .send(
                BaseRecord::to(&self.topic1)
                    .key(&key1)
                    .payload(payload1.as_bytes()),
            )
            .map_err(|e| e.0)?;

        // Must flush here to preserve ordering wrt to transactional writes
        self.notx_producer.flush(Timeout::Never);

        info!("Producer wrote non-transactional {}...", seq);
        Ok(())
    }

    fn send_tx_abort(&self, seq: u64) -> KafkaResult<()> {
        let key = format!("key_abrt_{:#010x}", rand::thread_rng().next_u32() & 0xffff);
        let payload = serde_json::to_string(&TxMsg {
            test_complete: false,
            is_aborted: true,
            seq,
            other_topic: "".into(),
        })
        .unwrap();
        self.producer.begin_transaction()?;
        self.producer
            .send(
                BaseRecord::to(&self.topic1)
                    .key(&key)
                    .payload(payload.as_bytes()),
            )
            .map_err(|e| e.0)?;

        self.producer
            .send(
                BaseRecord::to(&self.topic2)
                    .key(&key)
                    .payload(payload.as_bytes()),
            )
            .map_err(|e| e.0)?;

        info!("Producer aborting {}...", seq);
        self.producer.abort_transaction(Timeout::Never)?;
        info!("Producer aborted {}...", seq);
        Ok(())
    }

    fn execute_seq(&self, seq: u64) -> KafkaResult<()> {
        if seq % 3 == 0 {
            self.send_tx(seq)
        } else if seq % 3 == 1 {
            self.send_notx(seq)
        } else if seq % 3 == 2 {
            self.send_tx_abort(seq)
        } else {
            Ok(())
        }
    }

    fn execute(&mut self) {
        for seq in 0..100 {
            loop {
                match self.execute_seq(seq) {
                    Ok(_o) => {
                        break;
                    }
                    Err(e) => {
                        if let Some(ec) = e.rdkafka_error_code() {
                            if ec == RDKafkaErrorCode::InvalidTransactionalState {
                                match self.producer.abort_transaction(Timeout::Never) {
                                    Ok(_) => {
                                        info!("Called abort_transaction for InvalidTransactionalState")
                                    }
                                    Err(e) => {
                                        warn!("InvalidTransactionalState and abort_transaction failed: {}", e)
                                    }
                                }
                            }
                        }

                        warn!("Retrying seq {} for {}", seq, e);
                    }
                }
            }
        }

        self.producer.begin_transaction().unwrap();
        let final_payload = serde_json::to_string(&TxMsg {
            test_complete: true,
            is_aborted: false,
            seq: 0,
            other_topic: "".into(),
        })
        .unwrap();
        self.producer
            .send(
                BaseRecord::to(&self.topic1)
                    .key("final".as_bytes())
                    .payload(final_payload.as_bytes()),
            )
            .unwrap();
        self.producer.commit_transaction(Timeout::Never).unwrap();
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

#[derive(Serialize, Deserialize)]
struct TxMsg {
    // If true, consumers drop out of receive loop
    test_complete: bool,

    // If true, transactional consumers should never see this message
    is_aborted: bool,

    // Transactions consumers should be able to see a message with the same
    // seq on the `other` topic as soon as they can see this message.
    seq: u64,
    other_topic: String,
}

fn transactions_consumer(brokers: String, topic1: String, topic2: String) {
    info!("Consumer starting");
    let consumer1: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers.clone())
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .set("group.id", "test_grp_101_t1")
        .create()
        .unwrap();

    let consumer2: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers.clone())
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .set("group.id", "test_grp_101_t2")
        .create()
        .unwrap();

    match consumer1.subscribe(&vec![topic1.as_str()]) {
        Err(e) => {
            error!("Error subscribing to {}: {}", topic1, e);
            return;
        }
        Ok(_) => {
            info!("Consumer subscribed to {}", topic1)
        }
    }

    match consumer2.subscribe(&vec![topic2.as_str()]) {
        Err(e) => {
            error!("Error subscribing to {}: {}", topic2, e);
            return;
        }
        Ok(_) => {
            info!("Consumer subscribed to {}", topic2)
        }
    }

    assert!(consumer1
        .poll(Timeout::After(std::time::Duration::from_millis(100)))
        .is_none());
    assert!(consumer2
        .poll(Timeout::After(std::time::Duration::from_millis(100)))
        .is_none());

    for msg_res in consumer1.iter() {
        match msg_res {
            Ok(msg) => {
                let bytes = msg.payload().unwrap();
                let tx_msg = serde_json::from_slice::<TxMsg>(bytes).unwrap();
                if tx_msg.test_complete {
                    info!("Test complete, stopping consumer");
                    break;
                } else if tx_msg.is_aborted {
                    error!("Aborted transaction message seen by consumer!");
                } else {
                    info!("TxMsg seq={}", tx_msg.seq);
                    assert_eq!(tx_msg.other_topic, topic2);
                    match consumer2.poll(Timeout::After(std::time::Duration::from_millis(1000))) {
                        Some(t2_tx_record_res) => {
                            let t2_tx_record = t2_tx_record_res.unwrap();
                            let t2_tx_msg =
                                serde_json::from_slice::<TxMsg>(t2_tx_record.payload().unwrap())
                                    .unwrap();
                            if t2_tx_msg.seq == tx_msg.seq {
                                // Valid
                                info!("OK Matching seq on {}", topic2);
                                consumer1.commit_consumer_state(CommitMode::Sync).unwrap();
                                consumer2.commit_consumer_state(CommitMode::Sync).unwrap();
                            } else {
                                error!(
                                    "Unexpected seq {} on {} after receiving seq {} on {}",
                                    t2_tx_msg.seq, topic2, tx_msg.seq, topic1
                                );
                            }
                        }
                        None => {
                            // Invalid!   Message on t2 should be visible because
                            // we saw the message on t1 in read_committed.
                            error!(
                                "No message on {} after receiving seq {} on {}",
                                topic2, tx_msg.seq, topic1
                            );
                        }
                    }
                }
            }
            Err(e) => {
                error!("Consumer error {}", e);
            }
        }
    }

    info!("Consumer complete");
}

// correctness tests for kafka transactions
fn transactions(args: Vec<String>) {
    let brokers = args.get(0).unwrap();
    let topic1 = args.get(1).unwrap();
    let topic2 = args.get(2).unwrap();

    let mut producer = TxProducer::new(brokers.clone(), topic1.clone(), topic2.clone());

    let consumers_brokers = brokers.clone();
    let consumer_topic1 = topic1.clone();
    let consumer_topic2 = topic2.clone();

    let consumer_jh = std::thread::spawn(move || {
        transactions_consumer(consumers_brokers, consumer_topic1, consumer_topic2);
    });

    std::thread::sleep(std::time::Duration::from_millis(1000));

    producer.execute();

    consumer_jh.join().unwrap();
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args: Vec<String> = std::env::args().collect();
    let command = args[1].clone();
    let trailing_args: Vec<String> = args[2..].to_vec();
    match command.as_str() {
        "transactions" => {
            // Transaction code is not async because rust-rdkafka doesn't
            // provide async bindings to transactional functionality.
            tokio::task::spawn_blocking(move || {
                transactions(trailing_args);
            })
            .await.unwrap();
        }
        "connections" => connections(trailing_args).await,
        "producers" => {
            producers(trailing_args).await;
        }
        _ => {
            unimplemented!();
        }
    };
}
