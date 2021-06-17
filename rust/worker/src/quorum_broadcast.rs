use bytes::Bytes;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use log::*;
use primary::committee::Committee;
use primary::error::DagError;
use primary::messages::WorkerChannelType;
use primary::primary_net::{PrimaryMessageResponseHandle, PrimaryNet};
use primary::types::NodeID;
use primary::types::{WorkerMessage, WorkerMessageCommand};
use std::collections::BTreeMap;
use tokio::sync::mpsc::{channel, Receiver, Sender};

const BROADCAST_RESPONSE_CHANNEL_SIZE: usize = 100;

type BroadcastChannelMessage = (
    NodeID,
    Bytes,
    Vec<Result<PrimaryMessageResponseHandle, DagError>>,
    Option<Vec<u64>>,
);

pub struct Broadcaster {
    // The node ID of this broadcaster
    worker_id: NodeID,

    // The URLs of all peers (should be 2f + 1).
    host_urls: BTreeMap<NodeID, String>,
    committee: Committee,

    // The input channel that receives worker messages to send. Messages come from a single worker,
    // and must be sent to other workers on a single TCP connection to protect the session integrity.
    input_channel: Receiver<WorkerMessageCommand>,

    // The delivered channel that has worker messages that 2f nodes have acked.
    // It is given as input to the local Receiver who will make the sub-block available for tha main machine to build blocks
    delivered_channel: Sender<WorkerMessageCommand>,
}

impl Broadcaster {
    pub fn new(
        worker_id: NodeID,
        host_urls: BTreeMap<NodeID, String>,
        committee: Committee,
        input_channel: Receiver<WorkerMessageCommand>,
        delivered_channel: Sender<WorkerMessageCommand>,
    ) -> Broadcaster {
        Broadcaster {
            worker_id,
            host_urls,
            committee,
            input_channel,
            delivered_channel,
        }
    }

    pub async fn start_worker_broadcast_task(&mut self) {
        // Create a network to all other nodes (not ourself);
        let mut addr = self.host_urls.clone();
        addr.remove(&self.worker_id);

        // Create the primary network
        let banner =
            Bytes::from(bincode::serialize(&WorkerChannelType::Worker).expect("Bad serialization"));
        let mut net = PrimaryNet::new(self.worker_id, addr, Some(banner)).await;

        let (round_responses_tx, round_responses_rx) =
            channel::<BroadcastChannelMessage>(BROADCAST_RESPONSE_CHANNEL_SIZE);

        let committee = self.committee.clone();
        let delivered_channel = self.delivered_channel.clone();
        let myself = self.worker_id;
        tokio::spawn(async move {
            quorum_waiter_service(myself, committee, round_responses_rx, delivered_channel).await;
        });

        // Give a chance to schedule the new task
        tokio::task::yield_now().await;

        /* Handle sending end */

        // First get messages ... for current round.
        while let Some(msg) = self.input_channel.recv().await {
            // Serialize and send to broadcast workers.
            let data = Bytes::from(bincode::serialize(&msg.message).unwrap());
            let responses = net.broadcast_message(data.clone()).await;

            if round_responses_tx
                .send((self.worker_id, data, responses, msg.special_tags.clone()))
                .await
                .is_err()
            {
                warn!("Unable to connect to worker broadcast delivery task?");
                return;
            }
        }
    }
}

/// Task that waits for 2f+1 response for each batch broadcast, then sends it to the
/// next stage of processing.
async fn quorum_waiter_service(
    myself: NodeID,
    committee: Committee,
    mut round_responses_rx: Receiver<BroadcastChannelMessage>,
    delivered_channel: Sender<WorkerMessageCommand>,
) {
    let start_stake = committee.stake_worker(&myself);
    // Each of these is a vector of responses for a round
    'mainloop: while let Some((node_id, data, responses, special_tags)) =
        round_responses_rx.recv().await
    {
        // We get responses in any order

        let mut fut_stream: FuturesUnordered<_> = responses
            .into_iter()
            .filter(|x| !x.is_err())
            .map(|x| x.unwrap().recv())
            .collect();

        let mut stake = start_stake;
        while let Some(res) = fut_stream.next().await {
            if let Ok((nodex, _msg_resp)) = res {
                stake += committee.stake_worker(&nodex);
                debug!("Broadcast: response stake {} from {:?}.", stake, nodex);
            }

            if stake >= committee.quorum_threshold() {
                let data: Vec<u8> = data.iter().copied().collect();
                let tx_flush = WorkerMessage::Flush(node_id, data);
                let cmd = WorkerMessageCommand {
                    message: tx_flush,
                    response_channel: None,
                    special_tags,
                };

                // Now we are sending the batch to primary
                if let Err(e) = delivered_channel.send(cmd).await {
                    error!("Digest delivery channel closed?: {:?}", e);
                }

                // Drop remaining response handles, and cancel messages
                continue 'mainloop;
            }
        }
    }
}
