// Copyright (c) Facebook, Inc. and its affiliates.
use dag_core::types::*;
use dag_core::types::{WorkerMessage, WorkerMessageCommand};
use futures::future::FutureExt;
use futures::select;
use log::*;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::net::SocketAddr;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::interval;

#[cfg(test)]
#[path = "tests/worker_integration_tests.rs"]
pub mod worker_integration_tests;

/// The interval between blocks generated. A block is generated when it reaches the batch_size,
//  or then the WORKER_BLOCK_INTERVAL_TIME has elapsed if it is not empty.
const WORKER_BLOCK_INTERVAL_TIME: u64 = 100; // ms

/// A Sendworker is instatiated in a machine in order to send part of the block in one round
pub struct SendWorker {
    /// The total numer of nodes.
    pub total_workers: u32,
    /// The pool is filled by transactions from clients
    pub transaction_pool: Receiver<(SocketAddr, Transaction)>,
    /// This channel can interface with a TCP connection, a local receiver, or a broadcast primitive
    pub sending_endpoint: Sender<WorkerMessageCommand>,
    /// Control port
    pub control_rx: Option<Receiver<u128>>,

    pub max_batch_size: usize,
}

/// The factory on the local machine has received a start block from the main machine and spawns the worker to stream some batches
impl SendWorker {
    pub fn new(
        transaction_pool: Receiver<(SocketAddr, Transaction)>,
        sending_endpoint: Sender<WorkerMessageCommand>,
        max_batch_size: usize,
    ) -> Self {
        SendWorker {
            total_workers: 1,
            transaction_pool,
            sending_endpoint,
            control_rx: None,
            max_batch_size,
        }
    }

    pub async fn start_sending(&mut self, worker_id: NodeID) {
        let mut buf = Vec::<Transaction>::with_capacity(self.max_batch_size);
        let mut special_list = Vec::new();
        let mut exit = false;
        let mut make_batch = false;

        let const_small_interval = Duration::from_millis(WORKER_BLOCK_INTERVAL_TIME); // ms
        let interval = interval(const_small_interval);
        tokio::pin!(interval);

        // Record timings for controlling latency
        let mut send_times: VecDeque<u128> = VecDeque::new();
        let (_, mut rx) = channel(1);
        let mut active_control = false;
        if let Some(inner_rx) = self.control_rx.take() {
            rx = inner_rx;
            active_control = true;
        }
        let mut weighted_ave_time = 20_u128;

        loop {
            select! {

                finish_time_option = rx.recv().fuse() => {
                    if let Some(finish_time) = finish_time_option {
                        let start_time = send_times.pop_front().unwrap();
                        weighted_ave_time = (finish_time-start_time + 9*weighted_ave_time) / 10;
                        info!("Inner Latency: {}ms, inner queue : {} batches", weighted_ave_time, send_times.len());
                    }
                }

                tx = self.transaction_pool.recv().fuse() => {
                    if let Some((_ip, tx)) = tx {

                        // Increase the counter if we have a special transaction.
                        // TODO: Put this logic behind a perf testing flag
                        if tx.len() >= 16 {

                            let int_bytes = &tx[..8];
                            let tag = u64::from_be_bytes(int_bytes.try_into().unwrap());

                            if tag == 0 {
                                let int_bytes = &tx[8..16];
                                let val = u64::from_be_bytes(int_bytes.try_into().unwrap());
                                special_list.push(val);
                            }
                        }

                        // TODO: do some accounting for this IP / Client.
                        buf.push(tx);
                        if buf.len() >= self.max_batch_size {
                            make_batch = true;
                        }

                    }
                    else {
                        // The channel sending transactions is closed.
                        error!("Transaction stream closed.");
                        exit = true;
                    }
                }
                _ = interval.tick().fuse() => {
                    if !buf.is_empty() {
                        make_batch = true;
                    }
                }
            }

            // Make a batch
            if make_batch {
                let current_batch = WorkerMessage::Batch {
                    node_id: worker_id,
                    transactions: buf,
                };

                // Will ignore the response
                let (mut cmd, _resp) = WorkerMessageCommand::new(current_batch);
                cmd.special_tags = Some(special_list);

                let now_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis();

                send_times.push_back(now_time);

                if let Err(_e) = self.sending_endpoint.send(cmd).await {
                    error!("Error: {}", _e);
                    exit = true;
                }

                // Give a chance to schedule the receiver
                tokio::task::yield_now().await;

                // Toy control logic
                // This controller blocks new batches until there are fewer than 7 batches pending.
                if active_control {
                    while send_times.len() > 7 {
                        if let Some(finish_time) = rx.recv().await {
                            let start_time = send_times.pop_front().unwrap();
                            weighted_ave_time =
                                (finish_time - start_time + 9 * weighted_ave_time) / 10;
                            info!(
                                "Inner Latency: {}ms, inner queue : {} batches",
                                weighted_ave_time,
                                send_times.len()
                            );
                        } else {
                            error!("Exit the sender_worker.");
                            return;
                        }
                    }
                }

                buf = Vec::<Transaction>::with_capacity(self.max_batch_size);

                special_list = Vec::new();
            }

            make_batch = false;
            if exit {
                error!("Exit the sender_worker.");
                return;
            }
        }
    }
}
