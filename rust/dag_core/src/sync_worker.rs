use super::types::*;
use crate::messages::WorkerChannelType;
use crate::primary_net::{PrimaryMessageResponseHandle, PrimaryNet};
use crate::store::Store;
use crate::types::NodeID;
use crate::types::{WorkerMessage, WorkerMessageCommand};
use bytes::Bytes;
use futures::future::FutureExt;
use futures::select;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use log::*;
use std::collections::BTreeMap;
use tokio::sync::mpsc::{Receiver, Sender};

// #[cfg(test)]
// #[path = "tests/sync_worker_tests.rs"]
// mod sync_worker_tests;

/// Create a future that handles message response and cancel handles -- either get a
/// response or drop when the store tell us the blob can be read.
pub async fn handle_sync_response(
    other_key: Vec<u8>,
    resp_handle: PrimaryMessageResponseHandle,
    mut inner_store: Store,
    mut delivered_channel: Sender<WorkerMessageCommand>,
    mut digest_channel: Sender<(NodeID, Digest)>,
    other_worker_id: NodeID,
) {
    select! {
        result = resp_handle.recv().fuse() => {
            if let Ok((node, response)) = result {
                let (cmd, _resp) = WorkerMessageCommand::new(WorkerMessage::Flush(node, response.to_vec()));
                // Now we have received the ack, notify delivery.
                let _ = delivered_channel.send(cmd).await;
                return;
            }
        },
        _value = inner_store.notify_read(other_key.clone()).fuse() => {
                // We found this  key in our store!
                // No need to send again, it is being processed.
                let mut final_digest: [u8; 32] = [0u8; 32];
                final_digest.copy_from_slice(&other_key[..32]);
                let _ = digest_channel.send((other_worker_id, Digest(final_digest))).await;
        }
    };
}

pub async fn sync_pool_start(
    worker_id: NodeID,
    host_urls: BTreeMap<NodeID, String>,
    mut input_channel: Receiver<WorkerMessageCommand>,
    delivered_channel: Sender<WorkerMessageCommand>,
    mut digest_channel: Sender<(NodeID, Digest)>,
    mut store: Store,
) {
    // Create a network to all other nodes (not ourself);
    let mut addr = host_urls.clone();
    addr.remove(&worker_id);

    // Create the primary network
    let banner =
        Bytes::from(bincode::serialize(&WorkerChannelType::Worker).expect("Bad serialization"));
    let mut net = PrimaryNet::new(worker_id, addr, Some(banner)).await;
    let mut responses_futures = FuturesUnordered::new();

    debug!("Running sync subsystem.");

    loop {
        select! {
            _ug = responses_futures.select_next_some() => {
                // No need to do anything here.
                continue;
            }
            msg = input_channel.recv().fuse() => {

                if msg.is_none() {
                    break;
                }
                let mut msg = msg.unwrap();

                // Now serve all the requests
                if let WorkerMessage::Synchronize(other_worker_id, other_key) = msg.get_message() // .get_message()
                {
                    debug!("Got sync for {:?}", other_key);

                    if let Ok(Some(_response)) = store.read(other_key.clone()).await {
                        // We do not need to get this value remotely any more.
                        // Make sure we re-deliver the block to re-notify the primary.
                        //let (cmd, _resp) = WorkerMessageCommand::new(WorkerMessage::Flush(other_worker_id.clone(), response.to_vec()));
                        //let _ = delivered_channel.send(cmd).await;

                        let mut final_digest: [u8; 32] = [0u8; 32];
                        final_digest.copy_from_slice(&other_key[..32]);
                        let _ = digest_channel.send((other_worker_id.clone(), Digest(final_digest))).await;
                        continue;
                    }

                    let msg_bytes =
                        Bytes::from(bincode::serialize(&WorkerMessage::Query(other_key.clone())).expect("Bad Query Serialization?"));
                    if let Ok(resp_handle) = net.send_message(other_worker_id.clone(), msg_bytes).await {
                        let fut = handle_sync_response(other_key.clone(), resp_handle, store.clone(), delivered_channel.clone(), digest_channel.clone(), *other_worker_id);
                        responses_futures.push(fut.fuse());
                    }
                    else
                    {
                        // The address is given to us by our own node. So it should be correct
                        // otherwise fail early.

                        error!("Given address: {:?}", other_worker_id);
                        error!("Our address: {:?}", worker_id);
                        panic!("Unknown address?")
                    }

                    msg.respond(None);
                }
                else
                {
                    info!("Exit sync subsystem.");
                    break;
                }

            }
        };
    }
}
