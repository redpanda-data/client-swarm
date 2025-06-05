use futures::stream::TryStreamExt;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use tokio;
use tokio::sync::mpsc;

use rdkafka::config::ClientConfig;

use log::*;

use crate::metrics;
use crate::utils::split_properties;

struct ConsumeCounter {
    count: u64,
    target_count: Option<u64>,
    cancel: CancellationToken,
}

/**
 * Shared state between the consume tasks, to track a global count + check it
 * against an exit condition.
 */
impl ConsumeCounter {
    pub fn new(target_count: Option<u64>, cancel: CancellationToken) -> ConsumeCounter {
        ConsumeCounter {
            count: 0,
            target_count,
            cancel,
        }
    }

    pub fn is_completed(&mut self) -> bool {
        if self.cancel.is_cancelled() {
            return true;
        }

        if let Some(target_count) = self.target_count {
            self.count >= target_count
        } else {
            false
        }
    }

    pub fn record_borrowed_message_receipt(&mut self, msg: &BorrowedMessage<'_>) -> bool {
        self.count += 1;
        let c = match self.target_count {
            Some(i) => i,
            None => 0,
        };
        let done = match self.target_count {
            None => false,
            Some(limit) => self.count >= limit,
        };
        // log every 10000 messages
        if self.count % 10000 == 0 || done {
            debug!("Message received: {}", msg.offset());
            info!("Count {}/{}", self.count, c);
        }

        if done {
            self.cancel.cancel();
            info!("Count {}/{}", self.count, c);
            info!("Cancelling consumers");
            true
        } else {
            false
        }
    }
}

async fn retrying_consume(
    brokers: String,
    topics: Vec<String>,
    group: String,
    static_prefix: Option<String>,
    properties: Vec<(String, String)>,
    my_id: usize,
    counter: Arc<Mutex<ConsumeCounter>>,
    metrics: mpsc::Sender<metrics::ClientMessages>,
    cancel: CancellationToken,
) {
    let topics = topics.iter().map(String::as_ref).collect();
    loop {
        let c = consume(
            brokers.clone(),
            &topics,
            group.clone(),
            static_prefix.clone(),
            properties.clone(),
            my_id,
            counter.clone(),
            metrics.clone(),
            cancel.clone(),
        );

        tokio::select! {
            _ = c => {},
            _ = cancel.cancelled() => return,
        }

        let complete = {
            let mut counter_locked = counter.lock().unwrap();
            (*counter_locked).is_completed()
        };

        if complete {
            break;
        }

        error!("consumer failed; retrying {}", my_id);
    }
}

async fn consume(
    brokers: String,
    topics: &Vec<&str>,
    group: String,
    static_prefix: Option<String>,
    properties: Vec<(String, String)>,
    my_id: usize,
    counter: Arc<Mutex<ConsumeCounter>>,
    metrics: mpsc::Sender<metrics::ClientMessages>,
    cancel: CancellationToken,
) {
    debug!("Consumer {} constructing", my_id);

    let send_metric = |msg| async {
        if let Err(e) = metrics.send(msg).await {
            error!(
                "Error on consumer {}, unable to send metrics message: {}",
                my_id, e
            );
        }
    };

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
        .subscribe(topics)
        .expect("Can't subscribe to topic(s)");

    send_metric(metrics::ClientMessages::ClientStart { client_id: my_id }).await;

    let mut stream = consumer.stream();
    loop {
        let item = tokio::select! {
            item = stream.try_next() => item,
            _ = cancel.cancelled() => {
                debug!("Consumer {} cancelled", my_id);
                break
            }
        };

        if let Err(e) = item {
            debug!("failed to consume a message: {}", e);
            break;
        }

        let msg = match item.expect("Error reading from stream") {
            Some(msg) => msg,
            None => {
                continue;
            }
        };

        send_metric(metrics::ClientMessages::MessageSuccess { client_id: my_id }).await;

        let mut counter_locked = counter.lock().unwrap();
        let complete = (*counter_locked).record_borrowed_message_receipt(&msg);
        if complete {
            break;
        }
    }

    send_metric(metrics::ClientMessages::ClientStop { client_id: my_id }).await;
}

/// Stress the system for very large numbers of consumers
pub async fn consumers(
    brokers: String,
    topic: String,
    unique_topics: bool,
    topics_per_consumer: usize,
    unique_groups: bool,
    group: String,
    static_prefix: Option<String>,
    n: usize,
    messages: Option<u64>,
    properties: Vec<String>,
    metrics: metrics::MetricsContext,
    client_spawn_wait: Duration,
) {
    let kv_pairs = split_properties(properties);
    let mut tasks = vec![];
    info!("Spawning {} consumers", n);

    let cancel = CancellationToken::new();
    let counter = Arc::new(Mutex::new(ConsumeCounter::new(messages, cancel.clone())));

    for i in 0..n {
        let topics = {
            if unique_topics {
                (0..topics_per_consumer)
                    .map(|j| format!("{}-{}", topic, i * topics_per_consumer + j))
                    .collect()
            } else {
                vec![topic.clone()]
            }
        };
        let group = {
            if unique_groups {
                format!("{}-{}-{}", group, topic, i)
            } else {
                group.clone()
            }
        };
        tasks.push(tokio::spawn(retrying_consume(
            brokers.clone(),
            topics,
            group,
            static_prefix.clone(),
            kv_pairs.clone(),
            i,
            counter.clone(),
            metrics.spawn_new_sender(),
            cancel.clone(),
        )));

        tokio::time::sleep(client_spawn_wait).await;
    }

    for t in tasks {
        t.await.unwrap();
    }
}
