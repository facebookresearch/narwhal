use super::types::*;
use crate::error::DagError;
use crate::store::Store;
use crate::types::WorkerMessage;
use ed25519_dalek::Digest as DalekDigest;
use ed25519_dalek::Sha512;
use log::*;
use tokio::sync::mpsc::{Receiver, Sender};

// #[cfg(test)]
// #[path = "tests/receiver_tests.rs"]
// mod receiver_tests;

/// A ReceiveWorker is instatiated in a machine in order to receive part of the block in one round
pub struct ReceiveWorker {
    // The shard ID for this worker
    pub shard_id: ShardID,
    ///Channel to receive the batches from remote machine
    pub receiving_endpoint: Receiver<WorkerMessageCommand>,
    ///Channel to send the hash to the main machine
    pub hasher_endpoint: Sender<(NodeID, Digest)>,
    /// Persistent storage driver.
    pub store: Store,
    /// The number of transactions processed. Largelly for testing.
    pub tx_processed: usize,
}
/// The factory on the local machine has received a start block from the main machine and spawns the worker to receive some batches
impl ReceiveWorker {
    pub fn new(
        shard_id: ShardID,
        receiving_endpoint: Receiver<WorkerMessageCommand>,
        hasher_endpoint: Sender<(NodeID, Digest)>,
        store: Store,
    ) -> Self {
        ReceiveWorker {
            shard_id,
            receiving_endpoint,
            hasher_endpoint,
            store,
            tx_processed: 0,
        }
    }

    pub async fn store_hash_signal(
        &mut self,
        source: NodeID,
        val: Vec<u8>,
        special: Option<Vec<u64>>,
    ) -> Result<(), DagError> {
        // Final hash
        let mut final_hasher = Sha512::default();
        final_hasher.update(&val);
        let mut final_digest: [u8; 32] = [0u8; 32];
        final_digest.copy_from_slice(&final_hasher.finalize()[..32]);

        if let Some(tag) = special {
            for tx_tag in tag {
                info!(
                    // This log is used to compute performance.
                    "Received a tx digest {:?} computed from {} bytes of txs and with {} special txs",
                    Digest(final_digest),
                    val.len(),
                    tx_tag
                );
            }
        }

        // Store
        //Store does not remember source, not a proble for now but if we enable external sync it should be consistent
        if let Err(e) = self.store.write(final_digest.to_vec(), val).await {
            error!("writing to storage failed; {}", e);
            return Err(DagError::StorageFailure {
                error: format!("{:?}", e),
            });
        };

        // Signal output
        self.hasher_endpoint
            .send((source, Digest(final_digest)))
            .await
            .map_err(|e| DagError::ChannelError {
                error: format!("{}", e),
            })?;

        // Give a chance to schedule the receiver
        tokio::task::yield_now().await;

        Ok(())
    }

    pub async fn start_receiving(&mut self) -> Result<(), DagError> {
        let mut stats: (usize, usize, usize) = (0, 0, 0);
        loop {
            //Listen for a new batch
            if let Some(mut message) = self.receiving_endpoint.recv().await {
                let specials = message.special_tags.take();

                match message.get_message() {
                    batch @ WorkerMessage::Batch { .. } => {
                        stats.0 += 1;

                        if let WorkerMessage::Batch { node_id, .. } = batch {
                            let val = bincode::serialize(&batch).unwrap();
                            self.store_hash_signal(*node_id, val, specials).await?;
                        }

                        if let WorkerMessage::Batch { transactions, .. } = batch {
                            self.tx_processed += transactions.len();
                        }

                        message.respond(None);
                    }
                    WorkerMessage::Flush(node, val) => {
                        stats.1 += 1;

                        self.store_hash_signal(*node, val.clone(), specials).await?;
                        message.respond(None);
                    }
                    WorkerMessage::Query(key) => {
                        stats.2 += 1;

                        if let Ok(Some(data)) = self.store.read(key.clone()).await {
                            let response = bincode::deserialize(&data[..]).unwrap();
                            message.respond(Some(response));
                        } else {
                            message.respond(None);
                        }
                    }
                    WorkerMessage::Synchronize(_node, _key) => {
                        error!("Sync messages must be sent to sync-subsystem.");
                    }
                };

                debug!(
                    "Stats: Batch {} Flush {} Query {}",
                    stats.0, stats.1, stats.2
                );
            }
        }
    }
}
