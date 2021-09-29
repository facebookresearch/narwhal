use crate::consensus::{ConsensusMessage, CHANNEL_CAPACITY};
use crate::error::ConsensusResult;
use crate::messages::{Block, QC};
use bytes::Bytes;
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use network::SimpleSender;
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

const TIMER_ACCURACY: u64 = 5_000;

pub struct Synchronizer {
    store: Store,
    inner_channel: Sender<Block>,
}

impl Synchronizer {
    pub fn new(
        name: PublicKey,
        committee: Committee,
        store: Store,
        tx_loopback: Sender<Block>,
        sync_retry_delay: u64,
    ) -> Self {
        let mut network = SimpleSender::new();
        let (tx_inner, mut rx_inner): (_, Receiver<Block>) = channel(CHANNEL_CAPACITY);

        let store_copy = store.clone();
        tokio::spawn(async move {
            let mut waiting = FuturesUnordered::new();
            let mut pending = HashSet::new();
            let mut requests = HashMap::new();

            let timer = sleep(Duration::from_millis(TIMER_ACCURACY));
            tokio::pin!(timer);
            loop {
                tokio::select! {
                    Some(block) = rx_inner.recv() => {
                        if pending.insert(block.digest()) {
                            let parent = block.parent().clone();
                            let author = block.author;
                            let fut = Self::waiter(store_copy.clone(), parent.clone(), block);
                            waiting.push(fut);

                            if !requests.contains_key(&parent){
                                debug!("Requesting sync for block {}", parent);
                                let now = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .expect("Failed to measure time")
                                    .as_millis();
                                requests.insert(parent.clone(), now);
                                let address = committee
                                    .consensus(&author)
                                    .expect("Author of valid block is not in the committee")
                                    .consensus_to_consensus;
                                let message = ConsensusMessage::SyncRequest(parent, name);
                                let message = bincode::serialize(&message)
                                    .expect("Failed to serialize sync request");
                                network.send(address, Bytes::from(message)).await;
                            }
                        }
                    },
                    Some(result) = waiting.next() => match result {
                        Ok(block) => {
                            let _ = pending.remove(&block.digest());
                            let _ = requests.remove(&block.parent());
                            if let Err(e) = tx_loopback.send(block).await {
                                panic!("Failed to send message through core channel: {}", e);
                            }
                        },
                        Err(e) => error!("{}", e)
                    },
                    () = &mut timer => {
                        // This implements the 'perfect point to point link' abstraction.
                        for (digest, timestamp) in &requests {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();
                            if timestamp + (sync_retry_delay as u128) < now {
                                debug!("Requesting sync for block {} (retry)", digest);
                                let addresses = committee
                                    .others_consensus(&name)
                                    .into_iter()
                                    .map(|(_, x)| x.consensus_to_consensus)
                                    .collect();
                                let message = ConsensusMessage::SyncRequest(digest.clone(), name);
                                let message = bincode::serialize(&message)
                                    .expect("Failed to serialize sync request");
                                network.broadcast(addresses, Bytes::from(message)).await;
                            }
                        }
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_ACCURACY));
                    }
                }
            }
        });
        Self {
            store,
            inner_channel: tx_inner,
        }
    }

    async fn waiter(mut store: Store, wait_on: Digest, deliver: Block) -> ConsensusResult<Block> {
        let _ = store.notify_read(wait_on.to_vec()).await?;
        Ok(deliver)
    }

    pub async fn get_parent_block(&mut self, block: &Block) -> ConsensusResult<Option<Block>> {
        if block.qc == QC::genesis() {
            return Ok(Some(Block::genesis()));
        }
        let parent = block.parent();
        match self.store.read(parent.to_vec()).await? {
            Some(bytes) => Ok(Some(bincode::deserialize(&bytes)?)),
            None => {
                if let Err(e) = self.inner_channel.send(block.clone()).await {
                    panic!("Failed to send request to synchronizer: {}", e);
                }
                Ok(None)
            }
        }
    }

    pub async fn get_ancestors(
        &mut self,
        block: &Block,
    ) -> ConsensusResult<Option<(Block, Block)>> {
        let b1 = match self.get_parent_block(block).await? {
            Some(b) => b,
            None => return Ok(None),
        };
        let b0 = self
            .get_parent_block(&b1)
            .await?
            .expect("We should have all ancestors of delivered blocks");
        Ok(Some((b0, b1)))
    }
}
