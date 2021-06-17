use super::types::*;
use crate::types::{WorkerMessage, WorkerMessageCommand};
use log::*;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::sleep;
use tokio::time::{Duration, Instant};

use futures::future::FutureExt;

// #[cfg(test)]
// #[path = "tests/worker_integration_tests.rs"]
// pub mod worker_integration_tests;

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
    pub batch_size: usize,
}

/// The factory on the local machine has received a start block from the main machine and spawns the worker to stream some batches
impl SendWorker {
    pub fn new(
        transaction_pool: Receiver<(SocketAddr, Transaction)>,
        sending_endpoint: Sender<WorkerMessageCommand>,
        batch_size: usize,
    ) -> Self {
        SendWorker {
            total_workers: 1,
            transaction_pool,
            sending_endpoint,
            batch_size,
        }
    }

    pub async fn start_sending(&mut self, worker_id: NodeID) {
        let mut buf = VecDeque::<Transaction>::with_capacity(self.batch_size);
        let mut special_list = Vec::new();
        let mut exit = false;
        let mut make_batch = false;
        let mut special = 0;

        let const_small_interval = Duration::from_millis(WORKER_BLOCK_INTERVAL_TIME); // ms
        let current_delay_fut = sleep(const_small_interval);
        tokio::pin!(current_delay_fut);
        loop {
            // let fut = self.transaction_pool.recv(); // Do not await yet!

            tokio::select! {
                tx = self.transaction_pool.recv().fuse() => {
                    if let Some((_ip, tx)) = tx {
                        // Increase the counter if we have a special transaction.

                        let int_bytes = &tx[..8];
                        let tag = u64::from_be_bytes(int_bytes.try_into().unwrap());

                        if tag == 0 {
                            special += 1;

                            let int_bytes = &tx[8..16];
                            let val = u64::from_be_bytes(int_bytes.try_into().unwrap());
                            special_list.push(val);
                        }

                        // TODO: do some accounting for this IP / Client.
                        buf.push_back(tx);
                        if buf.len() >= self.batch_size {
                            make_batch = true;
                        }
                    }
                    else {
                        // The channel sending transactions is closed.
                        error!("Transaction stream closed.");
                        exit = true;
                    }
                }
                _ = &mut current_delay_fut => {
                    if !buf.is_empty() {
                        make_batch = true;
                    }
                    else
                    {
                        current_delay_fut.as_mut().reset(Instant::now()+ const_small_interval);
                    }

                }
            }

            // Make a batch
            if make_batch {
                let current_batch = WorkerMessage::Batch {
                    node_id: worker_id,
                    transactions: buf,
                    special,
                };

                // Will ignore the response
                let (mut cmd, _resp) = WorkerMessageCommand::new(current_batch);
                cmd.special_tags = Some(special_list);

                if let Err(_e) = self.sending_endpoint.send(cmd).await {
                    error!("Error: {}", _e);
                    exit = true;
                }
                // Give a chance to schedule the receiver
                tokio::task::yield_now().await;

                special = 0;
                buf = VecDeque::<Transaction>::with_capacity(self.batch_size);
                current_delay_fut
                    .as_mut()
                    .reset(Instant::now() + const_small_interval);

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
