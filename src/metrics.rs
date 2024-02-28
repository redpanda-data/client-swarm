// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
use histogram::Histogram;

use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio_util::sync::CancellationToken;

use ringbuffer::{AllocRingBuffer, RingBuffer};

use std::time::Duration;

use log::*;

#[derive(Clone, Debug)]
pub enum AggregatedMetricsMessages {
    AggWindow {
        messages_in_window: u64,
        window_epoch: u32,
    },
}

#[derive(Clone, Debug)]
pub enum ClientMessages {
    MessageProcessed { client_id: usize },
    ClientFailed { client_id: usize },
}

#[derive(Clone, Debug)]
pub struct WindowConfig {
    pub window_start: time::Instant,
    pub window_duration: Duration,
}

pub struct MetricsAggregator {
    recv: mpsc::Receiver<ClientMessages>,
    send: mpsc::Sender<AggregatedMetricsMessages>,
    window_interval: time::Interval,
    messages_in_window: u64,
    current_window_epoch: u32,
    cancel: CancellationToken,
}

impl MetricsAggregator {
    pub fn new(
        window_config: WindowConfig,
        recv: mpsc::Receiver<ClientMessages>,
        send: mpsc::Sender<AggregatedMetricsMessages>,
        cancel: CancellationToken,
    ) -> MetricsAggregator {
        MetricsAggregator {
            recv,
            send,
            // Note: MetricsAggregator relies on the default value of
            // `tokio::time::MissedTickBehavior` to be `Burst` in order to
            // function correctly.
            //
            // This behavior means that the interval will rapidly `tick` in
            // order to catch up in the event of a delay. This in turn
            // simplifies determining which `window_epoch` we're collecting
            // data for.
            window_interval: time::interval_at(
                window_config.window_start,
                window_config.window_duration,
            ),
            messages_in_window: 0,
            current_window_epoch: 0,
            cancel,
        }
    }

    fn recv_msg(&mut self, msg: ClientMessages) {
        match msg {
            ClientMessages::MessageProcessed { client_id: _ } => {
                self.messages_in_window += 1;
            }
            ClientMessages::ClientFailed { client_id: _ } => {}
        }
    }

    fn tick(&mut self) {
        let messages_in_window = self.messages_in_window;
        let window_epoch = self.current_window_epoch;

        self.messages_in_window = 0;
        self.current_window_epoch += 1;

        let agg_sender = self.send.clone();

        tokio::spawn(async move {
            let res = agg_sender
                .send(AggregatedMetricsMessages::AggWindow {
                    messages_in_window,
                    window_epoch,
                })
                .await;

            if let Err(msg) = res {
                warn!("failed to send aggregated metrics: {}", msg);
            }
        });
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.recv.recv() => self.recv_msg(msg),
                _ = self.window_interval.tick() => self.tick(),
                _ = self.cancel.cancelled() => { return },
                else => { return },
            }
        }
    }
}

struct AggregatedSample {
    hist: Histogram,
    window_epoch: u32,
}

pub struct MetricsConfig {
    pub max_samples: usize,
}

pub struct LastMetricsResult {
    pub hist: Histogram,
}

pub enum MetricsCommands {
    GetLastMetrics {
        window_duration: Option<Duration>,
        resp: oneshot::Sender<LastMetricsResult>,
    },
}

pub struct Metrics {
    metrics_recv: mpsc::Receiver<AggregatedMetricsMessages>,
    commands_recv: mpsc::Receiver<MetricsCommands>,
    samples: AllocRingBuffer<AggregatedSample>,
    window_config: WindowConfig,
    cancel: CancellationToken,
}

impl Metrics {
    pub fn new(
        metrics_config: MetricsConfig,
        window_config: WindowConfig,
        metrics_recv: mpsc::Receiver<AggregatedMetricsMessages>,
        commands_recv: mpsc::Receiver<MetricsCommands>,
        cancel: CancellationToken,
    ) -> Metrics {
        Metrics {
            metrics_recv,
            commands_recv,
            samples: AllocRingBuffer::new(metrics_config.max_samples),
            window_config,
            cancel,
        }
    }

    fn get_epoch_index(&self, window_epoch: u32) -> Option<usize> {
        for (i, ref sample) in self.samples.iter().enumerate() {
            if sample.window_epoch == window_epoch {
                return Some(i);
            }
        }

        None
    }

    fn get_start_time(&self, window_epoch: u32) -> time::Instant {
        self.window_config.window_start + (self.window_config.window_duration * window_epoch)
    }

    fn add_missing_windows(&mut self, window_epoch: u32) {
        // Check if the window has already been added.
        if !self.samples.is_empty() && self.samples.back().unwrap().window_epoch >= window_epoch {
            return;
        }

        let start_epoch = self.samples.back().map(|w| w.window_epoch + 1).unwrap_or(0);
        let end_epoch = window_epoch + 1;

        for wi in start_epoch..end_epoch {
            self.samples.push(AggregatedSample {
                hist: Histogram::new(7, 64).unwrap(),
                window_epoch: wi,
            });
        }
    }

    fn recv_metrics(&mut self, metrics: AggregatedMetricsMessages) {
        match metrics {
            AggregatedMetricsMessages::AggWindow {
                messages_in_window,
                window_epoch,
            } => {
                self.add_missing_windows(window_epoch);
                // Used to convert messages in window to messages/sec
                let normalization_c = self.window_config.window_duration.as_secs();

                if let Some(i) = self.get_epoch_index(window_epoch) {
                    self.samples[i]
                        .hist
                        .increment(messages_in_window / normalization_c)
                        .expect("shouldn't fail");
                } else {
                    warn!(
                        "Received samples for a window epoch that has been dropped: {}",
                        window_epoch
                    );
                }
            }
        }
    }

    fn get_last_metrics(
        &mut self,
        window_duration_opt: Option<time::Duration>,
    ) -> LastMetricsResult {
        let current_inst = time::Instant::now();
        let mut hist = Histogram::new(7, 64).unwrap();

        for sample in &self.samples {
            let add = if let Some(window_duration) = window_duration_opt {
                let sample_window_start_time = self.get_start_time(sample.window_epoch);
                let requested_start_time = current_inst - window_duration;
                sample_window_start_time >= requested_start_time
            } else {
                true
            };

            if add {
                hist = hist.checked_add(&sample.hist).unwrap();
            }
        }

        LastMetricsResult { hist }
    }

    fn recv_command(&mut self, command: MetricsCommands) {
        match command {
            MetricsCommands::GetLastMetrics {
                window_duration,
                resp,
            } => {
                let res = self.get_last_metrics(window_duration);
                if let Err(_) = resp.send(res) {
                    warn!("receiver dropped");
                }
            }
        }
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                Some(msg) = self.metrics_recv.recv() => self.recv_metrics(msg),
                Some(msg) = self.commands_recv.recv() => self.recv_command(msg),
                _ = self.cancel.cancelled() => { return },
                else => { return }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct MetricsContext {
    window_config: WindowConfig,
    send: mpsc::Sender<AggregatedMetricsMessages>,
    cancel: CancellationToken,
}

impl MetricsContext {
    pub fn new(
        window_config: WindowConfig,
        send: mpsc::Sender<AggregatedMetricsMessages>,
        cancel: CancellationToken,
    ) -> MetricsContext {
        MetricsContext {
            window_config,
            send,
            cancel,
        }
    }

    pub fn spawn_new_sender(&self) -> mpsc::Sender<ClientMessages> {
        let (tx, rx) = mpsc::channel(1);
        let mut aggregator = MetricsAggregator::new(
            self.window_config.clone(),
            rx,
            self.send.clone(),
            self.cancel.clone(),
        );

        tokio::spawn(async move {
            aggregator.run().await;
        });

        tx
    }
}
