use super::types::*;
use crate::committee::Committee;
use crate::error::DagError;
use crate::net::*;
use crate::receive_worker::*;
use crate::send_worker::*;
use crate::store::*;
use crate::sync_worker::*;
use bytes::Bytes;
use core::cmp::min;
use futures::sink::SinkExt;
use log::*;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;
use tokio::time::sleep;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

// #[cfg(test)]
// #[path = "tests/primary_worker_intergration_tests.rs"]
// pub mod primary_worker_intergration_tests;

/// A ManageWorker is instatiated in a physical machine to coordinate with external machines (primary, remote workers)
pub struct ManageWorker {
    pub worker_id: NodeID,
}

/// The first thing called when booting
impl ManageWorker {
    pub async fn new(
        worker_id: NodeID,
        committee: Committee,
        max_batch_size: usize,
        given_store: Option<Store>,
    ) -> Self {
        // Sender pipeline
        let inner_channel_size = 1000;

        let my_authority = committee
            .authorities
            .iter()
            .find(|auth| {
                auth.workers
                    .iter()
                    .any(|(_, machine)| machine.name == worker_id)
            })
            .unwrap();
        let (shard_id, _) = my_authority
            .workers
            .iter()
            .find(|(_, machine)| machine.name == worker_id)
            .unwrap()
            .clone();
        let my_primary_url = committee.get_url(&my_authority.primary.name).unwrap();
        let mut addresses = BTreeMap::new();
        for auth in &committee.authorities {
            for (shard, machine) in &auth.workers {
                if &shard_id == shard {
                    let address = format!("{}:{}", machine.host, machine.port);
                    addresses.insert(machine.name, address);
                }
            }
        }
        info!(
            "{:?} is a worker for {:?} at port {:?}",
            worker_id, my_authority.primary.name, my_primary_url
        );

        assert! {
            addresses.contains_key(&worker_id),
            "The addresses provided must be a map of worker_id -> address_url."
        };

        let (tx_digest, rx_digest) = channel(inner_channel_size);

        let store = if let Some(store) = given_store {
            store
        } else {
            Store::new(format!(".storage_integrated_{:?}_{:?}", worker_id, shard_id).to_string())
                .await
        };

        // Stage 1 : listen for transactions
        let (tx_transactions, rx_tansactions) = channel(inner_channel_size * 10);

        // Stage 2 : Batch into Worker Messages
        let (tx_worker_msg, rx_worker_msg) = channel(inner_channel_size);
        tokio::spawn(async move {
            let mut sw = SendWorker::new(rx_tansactions, tx_worker_msg, max_batch_size);
            sw.start_sending(worker_id).await;
        });

        let (tx_worker_msg_v0, rx_worker_msg_v0) = channel(inner_channel_size);

        // Stage 3: Spawn local receiver for hashing
        let mut rw =
            ReceiveWorker::new(shard_id, rx_worker_msg_v0, tx_digest.clone(), store.clone());

        tokio::spawn(async move {
            rw.start_receiving().await.unwrap();
        });

        //sender should be given at the network

        // Dedicated worker to handle block sync
        let (tx_worker_msg_v0_sync, rx_worker_msg_v0_sync) = channel(inner_channel_size);
        let mut rw_sync = ReceiveWorker::new(
            shard_id,
            rx_worker_msg_v0_sync,
            tx_digest.clone(),
            store.clone(),
        );

        tokio::spawn(async move {
            rw_sync.start_receiving().await.unwrap();
        });

        let (tx_sync, rx_sync) = channel(inner_channel_size);

        let tx_worker_sync = tx_worker_msg_v0_sync.clone();
        let addresses_clone = addresses.clone();

        let store_copy = store.clone();
        let tx_digest_clone = tx_digest.clone();
        tokio::spawn(async move {
            sync_pool_start(
                worker_id,
                addresses_clone,
                rx_sync,
                tx_worker_sync,
                tx_digest_clone,
                store_copy,
            )
            .await;
        });

        // Stage 5: broadcast out
        let mut own_url = addresses.get(&worker_id).unwrap().clone();
        let own_port = own_url.split(':').collect::<Vec<_>>()[1];
        own_url = format!("0.0.0.0:{}", own_port);

        let mut broadcaster = Broadcaster::new(
            worker_id,
            addresses,
            committee,
            rx_worker_msg,
            tx_worker_msg_v0.clone(),
        );
        tokio::spawn(async move {
            broadcaster.start_watchdog_tasks().await;
        });

        // Stage 6 : start my own worker listeners that spin receive workers

        // Dedicated worker to handle other people's blocks
        let (tx_worker_msg_v0_peer, rx_worker_msg_v0_peer) = channel(inner_channel_size);
        let mut rw_pool =
            ReceiveWorker::new(shard_id, rx_worker_msg_v0_peer, tx_digest.clone(), store);

        tokio::spawn(async move {
            rw_pool.start_receiving().await.unwrap();
        });

        tokio::spawn(async move {
            let _ =
                worker_server_start(own_url, tx_worker_msg_v0_peer, tx_sync, tx_transactions).await;
        });

        tokio::spawn(async move {
            if let Err(_e) = handle_digests(rx_digest, my_primary_url, shard_id).await {
                error!("Handle Digest error: {:?}", _e);
            }
        });

        debug!("Worker {:?} is alive", worker_id);
        ManageWorker { worker_id }
    }
}

pub async fn handle_digests(
    mut rx_digest: Receiver<(NodeID, Digest)>,
    url: String,
    shard_id: ShardID,
) -> Result<(), DagError> {
    //connect with primary

    debug!("Connecting to Primary for Digests...");
    let mut delay = 200_u64; // milliseconds
    let mut buffer: VecDeque<PrimaryMessage> = VecDeque::new();
    loop {
        // Outer loop tries to re-connect.
        // Connect to server using TCP.
        match TcpStream::connect(url.clone()).await {
            Ok(socket) => {
                debug!("Connect...");
                delay = 200; // Reset the delay.
                let mut transport = Framed::new(socket, LengthDelimitedCodec::new());

                'inner: loop {
                    //empty buffer
                    if let Some(msg) = buffer.pop_front() {
                        let data = Bytes::from(bincode::serialize(&msg).unwrap());
                        if let Err(_e) = transport.send(data).await {
                            error!("Error: {}", _e);
                            buffer.push_front(msg);
                            break 'inner;
                        }
                        debug!("Sent tx digest to our primary.");
                    }
                    if let Some((node_id, msg)) = rx_digest.recv().await {
                        let msg = PrimaryMessage::TxDigest(msg, node_id, shard_id);
                        buffer.push_back(msg);
                    } else {
                        warn!("Digest channel is closing");
                        return Ok(());
                    }
                }
            }
            Err(e) => {
                warn!("Connection Error to {:?}: {:?}", url.clone(), e);
                // Wait for 5 sec -- then try to reconnect.
                sleep(Duration::from_millis(delay)).await;
                delay = min(delay + delay / 10, 1000 * 60); // Increase the delay.
            }
        }
    }
}
