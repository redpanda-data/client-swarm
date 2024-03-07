use futures::stream::TryStreamExt;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::message::BorrowedMessage;
use rdkafka::Message;
use std::sync::Arc;
use std::sync::Mutex;

use tokio;
use tokio::sync::mpsc;

use rdkafka::config::ClientConfig;

use log::*;

use crate::metrics;
use crate::utils::split_properties;

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
    metrics: mpsc::Sender<metrics::ClientMessages>,
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

        if let Err(e) = metrics
            .send(metrics::ClientMessages::MessageProcessed { client_id: my_id })
            .await
        {
            error!("Error on consumer {}, unable to send metrics: {}", my_id, e);
        }

        let mut counter_locked = counter.lock().unwrap();
        let complete = (*counter_locked).record_borrowed_message_receipt(&msg);
        if complete {
            break;
        }
    }
}

/// Stress the system for very large numbers of consumers
pub async fn consumers(
    brokers: String,
    topic: String,
    unique_topics: bool,
    unique_groups: bool,
    group: String,
    static_prefix: Option<String>,
    n: usize,
    messages: Option<u64>,
    properties: Vec<String>,
    metrics: metrics::MetricsContext,
) {
    let kv_pairs = split_properties(properties);
    let mut tasks = vec![];
    info!("Spawning {} consumers", n);

    let counter = Arc::new(Mutex::new(ConsumeCounter::new(messages)));

    for i in 0..n {
        let mut topic_prefix = topic.clone();
        let mut group = group.clone();
        if unique_topics {
            topic_prefix = format!("{}-{}", topic, i);
        }
        if unique_groups {
            group = format!("{}-{}-{}", group, topic, i);
        }
        tasks.push(tokio::spawn(consume(
            brokers.clone(),
            topic_prefix.clone(),
            group.clone(),
            static_prefix.clone(),
            kv_pairs.clone(),
            i,
            counter.clone(),
            metrics.spawn_new_sender(),
        )))
    }

    for t in tasks {
        t.await.unwrap();
    }
}
