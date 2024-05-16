// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
use histogram::Histogram;

use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio_util::sync::CancellationToken;

use ringbuffer::{AllocRingBuffer, RingBuffer};

use std::time::Duration;

use log::*;

#[derive(Clone, Debug)]
pub enum AggregatedMetricsMessages {
    AggWindow {
        window_counts: MessageCounts,
        window_epoch: u32,
    },
    ClientStart,
    ClientStop,
}

#[derive(Clone, Debug)]
pub enum ClientMessages {
    MessageSuccess { client_id: usize },
    MessageFailure { client_id: usize },
    ClientStart { client_id: usize },
    ClientStop { client_id: usize },
}

#[derive(Clone, Debug)]
pub struct WindowConfig {
    pub window_start: time::Instant,
    pub window_duration: Duration,
}

#[derive(Copy, Clone, Debug, Serialize)]
pub struct MessageCounts {
    success_count: u64,
    error_count: u64,
}

impl MessageCounts {
    pub fn new() -> MessageCounts {
        MessageCounts {
            success_count: 0,
            error_count: 0,
        }
    }
}

pub struct MetricsAggregator {
    recv: mpsc::Receiver<ClientMessages>,
    send: mpsc::Sender<AggregatedMetricsMessages>,
    window_interval: time::Interval,
    window_counts: MessageCounts,
    current_window_epoch: u32,
    // true iff we have sent the ClientStart message for this client
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
            window_counts: MessageCounts::new(),
            current_window_epoch: 0,
            cancel,
        }
    }

    fn recv_msg(&mut self, msg: ClientMessages) {
        match msg {
            ClientMessages::MessageSuccess { client_id: _ } => {
                self.window_counts.success_count += 1;
            }
            ClientMessages::MessageFailure { client_id: _ } => {
                self.window_counts.error_count += 1;
            }
            ClientMessages::ClientStart { client_id: _ } => {
                self.send_metrics_message(AggregatedMetricsMessages::ClientStart)
            }
            ClientMessages::ClientStop { client_id: _ } => {
                self.send_metrics_message(AggregatedMetricsMessages::ClientStop)
            }
        }
    }

    // Send a message onwards to the associated Metrics
    // instance.
    fn send_metrics_message(&self, message: AggregatedMetricsMessages) {
        let agg_sender = self.send.clone();
        tokio::spawn(async move {
            let res = agg_sender.send(message).await;

            if let Err(msg) = res {
                warn!("failed to send metrics message ({}): {}", msg, msg);
            }
        });
    }

    fn tick(&mut self) {
        let window_counts = self.window_counts;
        let window_epoch = self.current_window_epoch;

        self.window_counts = MessageCounts::new();
        self.current_window_epoch += 1;

        self.send_metrics_message(AggregatedMetricsMessages::AggWindow {
            window_counts,
            window_epoch,
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
    // The histogram of message rates (msg/million-seconds) over all clients.
    // The weird units are to accommodate the integer nature of histograms.
    msg_rate: Histogram,
    // the number of clients that have reported (so far) into
    // this sample
    report_count: i32,
    // the total number of successful messages this interval (summed over clients)
    total_success: u64,
    // the total number of errored messages this interval (summed over clients)
    total_error: u64,
    window_epoch: u32,
}

pub struct MetricsConfig {
    pub max_samples: usize,
}

pub struct LastMetricsResult {
    pub msg_rate: Histogram,
    pub total_counts: MessageCounts,
    pub clients_started: i64,
    pub clients_stopped: i64,
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
    total_counts: MessageCounts,
    // The number of clients that have ever been started (includes ones that have
    // subsequently stopped). To get the number of _currently_ active clients,
    // subtract stopped from started.
    clients_started: i64,
    // The number of clients that have been stopped.
    clients_stopped: i64,
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
            total_counts: MessageCounts::new(),
            clients_started: 0,
            clients_stopped: 0,
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

    // returns true if at least one new epoch was added
    fn add_missing_windows(&mut self, window_epoch: u32) -> bool {
        // Check if the window has already been added.
        if !self.samples.is_empty() && self.samples.back().unwrap().window_epoch >= window_epoch {
            return false;
        }

        let start_epoch = self.samples.back().map(|w| w.window_epoch + 1).unwrap_or(0);
        let end_epoch = window_epoch + 1;

        for wi in start_epoch..end_epoch {
            self.samples.push(AggregatedSample {
                msg_rate: Histogram::new(7, 64).unwrap(),
                report_count: 0,
                total_success: 0,
                total_error: 0,
                window_epoch: wi,
            });
        }
        start_epoch < end_epoch
    }

    fn recv_metrics(&mut self, metrics: AggregatedMetricsMessages) {
        match metrics {
            AggregatedMetricsMessages::AggWindow {
                window_counts,
                window_epoch,
            } => {
                let added_epoch = self.add_missing_windows(window_epoch);

                self.total_counts.success_count += window_counts.success_count;
                self.total_counts.error_count += window_counts.error_count;

                // we use the addition of a new epoch as a convenient "clock" to emit periodic log
                // messages about the overall progress
                if added_epoch {
                    info!(
                        "Running totals: success={}, errors={}, clients={}/{} (active/total)",
                        self.total_counts.success_count,
                        self.total_counts.error_count,
                        self.clients_started - self.clients_stopped,
                        self.clients_started,
                    );
                }

                // Used to convert messages in window to messages/Msec (million seconds)
                let normalization_c =
                    self.window_config.window_duration.as_secs() as f64 / 1000000.;

                if let Some(i) = self.get_epoch_index(window_epoch) {
                    let sample = &mut self.samples[i];
                    sample.report_count += 1;
                    sample
                        .msg_rate
                        .increment(
                            (window_counts.success_count as f64 / normalization_c).round() as u64,
                        )
                        .expect("shouldn't fail");
                    sample.total_success += window_counts.success_count;
                    sample.total_error += window_counts.error_count;
                } else {
                    warn!(
                        "Received samples for a window epoch that has been dropped: {}",
                        window_epoch
                    );
                }
            }
            AggregatedMetricsMessages::ClientStart => self.clients_started += 1,
            AggregatedMetricsMessages::ClientStop => self.clients_stopped += 1,
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
                hist = hist.checked_add(&sample.msg_rate).unwrap();
            }
        }

        LastMetricsResult {
            msg_rate: hist,
            total_counts: self.total_counts,
            clients_started: self.clients_started,
            clients_stopped: self.clients_stopped,
        }
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
