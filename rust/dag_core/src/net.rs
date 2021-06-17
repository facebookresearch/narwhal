use super::messages::WorkerChannelType;
use super::primary_net::{PrimaryMessageResponseHandle, PrimaryNet};
use super::types::{Transaction, WorkerMessage, WorkerMessageCommand};
use crate::committee::Committee;
use crate::error::DagError;
use crate::types::NodeID;
use bytes::Bytes;
use futures::select;
use futures::sink::SinkExt;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::FuturesOrdered;
use futures::stream::StreamExt;
use futures::FutureExt;

use log::*;
use std::collections::BTreeMap;
use std::error;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const BROADCAST_RESPONSE_CHANNEL_SIZE: usize = 100;
// const WAIT_FOR_REMAINING_F_MS : u64= 107;

// #[cfg(test)]
// #[path = "tests/net_tests.rs"]
// mod net_tests;

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

    pub async fn start_watchdog_tasks(&mut self) {
        // Create a network to all other nodes (not ourself);
        let mut addr = self.host_urls.clone();
        addr.remove(&self.worker_id);

        // Create the primary network
        let banner =
            Bytes::from(bincode::serialize(&WorkerChannelType::Worker).expect("Bad serialization"));
        let mut net = PrimaryNet::new(self.worker_id, addr, Some(banner)).await;

        let (round_responses_tx, mut round_responses_rx) =
            channel::<(
                usize,
                NodeID,
                Bytes,
                Vec<Result<PrimaryMessageResponseHandle, DagError>>,
                Option<Vec<u64>>,
            )>(BROADCAST_RESPONSE_CHANNEL_SIZE);

        let cmt = self.committee.clone();
        // let node = self.node;
        let delivered_channel = self.delivered_channel.clone();
        let myself = self.worker_id;
        tokio::spawn(async move {
            let start_stake = cmt.stake_worker(&myself);
            // Each of these is a vector of responses for a round

            'mainloop: while let Some((_subblock_id, node_id, data, responses, special_tags)) =
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
                        stake += cmt.stake_worker(&nodex);
                        debug!(
                            "Broadcast {}: response stake {} from {:?}.",
                            _subblock_id, stake, nodex
                        );
                    }

                    if stake >= cmt.quorum_threshold() {
                        let data: Vec<u8> = data.iter().copied().collect();
                        let tx_flush = WorkerMessage::Flush(node_id, data);
                        let cmd = WorkerMessageCommand {
                            message: tx_flush,
                            response_channel: None,
                            special_tags: special_tags,
                        };

                        /*
                        // We are going to wait for a bit to give messages a change to arrive
                        let mut total_delay = delay_for(Duration::from_millis(WAIT_FOR_REMAINING_F_MS)).fuse();
                        loop {
                            select! {
                                _ = total_delay => break,
                                _ = fut_stream.next().fuse() => {
                                    if fut_stream.len() == 0 {
                                        break
                                    }
                                },
                            }
                        }
                        */

                        // Now we are sending the batch to primary
                        if let Err(e) = delivered_channel.send(cmd).await {
                            error!("Digest delivery channel closed?: {:?}", e);
                        }

                        // Give a chance to schedule the receiver
                        // tokio::task::yield_now().await;

                        // Drop remaining response handles, and cancel messages
                        continue 'mainloop;
                    }
                }
            }
        });

        // Give a chance to schedule the new task
        tokio::task::yield_now().await;

        /* Handle sending end */

        // first block we get on channel is number zero.
        let mut subblock_id: usize = 0;

        // First get messages ... for current round.
        while let Some(msg) = self.input_channel.recv().await {
            // Serialize and send to broadcast workers.
            let data = Bytes::from(bincode::serialize(&msg.message).unwrap());
            let responses = net.broadcast_message(data.clone()).await;

            if round_responses_tx
                .send((
                    subblock_id,
                    self.worker_id,
                    data,
                    responses,
                    msg.special_tags.clone(),
                ))
                .await
                .is_err()
            {
                warn!("Unable to connect to worker broadcast delivery task?");
                return;
            }

            subblock_id += 1;
        }
    }
}

pub async fn worker_server_start(
    url: String,
    worker_message_output: Sender<WorkerMessageCommand>,
    synchronize_message_output: Sender<WorkerMessageCommand>,
    transaction_output: Sender<(SocketAddr, Transaction)>,
) -> Result<(), Box<dyn error::Error>> {
    let listener = TcpListener::bind(url).await?;

    loop {
        // Listen for new connections.
        let (socket, _) = listener.accept().await?;
        let worker_out = worker_message_output.clone();
        let sync_out = synchronize_message_output.clone();
        let transact_out = transaction_output.clone();

        tokio::spawn(async move {
            let ip = socket.peer_addr().unwrap(); // TODO: check error here.
            let mut transport = Framed::new(socket, LengthDelimitedCodec::new());

            // TODO: Do some authentication here
            if let Some(Ok(channel_type_data)) = transport.next().await {
                let channel_type: WorkerChannelType =
                    match bincode::deserialize(&channel_type_data[..]) {
                        Err(e) => {
                            warn!("Cannot parse banner; err = {:?}", e);
                            return;
                        }
                        Ok(channel_type) => channel_type,
                    };

                let ok = Bytes::from("OK");
                if let Err(e) = transport.send(ok).await {
                    warn!("failed to write to socket; err = {:?}", e);
                    return;
                }

                match channel_type {
                    WorkerChannelType::Worker => {
                        debug!("handling worker messages");
                        handle_worker_channel(transport, worker_out, sync_out).await;
                    }
                    WorkerChannelType::Transaction => {
                        debug!("handling transactions");
                        handle_transaction_channel(transport, transact_out, ip).await;
                    }
                }
            } else {
                warn!("Channel is broken on channel type.");
            }
        });
    }
}

pub async fn set_channel_type(
    transport: &mut Framed<TcpStream, LengthDelimitedCodec>,
    label: WorkerChannelType,
) {
    let header = Bytes::from(bincode::serialize(&label).expect("Bad serialization"));
    transport.send(header).await.expect("Error sending");
    let _n = transport.next().await.expect("Error on test receive");
}

async fn handle_worker_channel(
    mut transport: Framed<TcpStream, LengthDelimitedCodec>,
    mut worker_out: Sender<WorkerMessageCommand>,
    mut sync_out: Sender<WorkerMessageCommand>,
) {
    let ok = Bytes::from("OK");
    let notfound = Bytes::from("NOTFOUND");
    let mut responses_ordered = FuturesOrdered::new();

    // In a loop, read data from the socket and write the data back.
    loop {
        select! {
            worker_message_data = transport.next().fuse() => {

                if worker_message_data.is_none() {
                    // Channel is closed, nothing to do any more.
                    return;
                }
                let worker_message_data = worker_message_data.unwrap();

                match worker_message_data {
                    Ok(data) => {
                        // Send the transaction on the channel.
                        // Decode the data.
                        let msg: WorkerMessage = match bincode::deserialize(&data[..]) {
                            Err(e) => {
                                warn!("parsing Error, closing channel; err = {:?}", e);
                                return;
                            }
                            Ok(msg) => msg,
                        };

                        // Determine what we send back on None.
                        let (must_wait, on_none, output_channel) = match &msg {
                            WorkerMessage::Query(..) => (true, &notfound, &mut worker_out),
                            /*
                                For Batch and Sync WorkerMessages we schedule
                                the command for processing and we respond immediately.
                            */
                            WorkerMessage::Synchronize(..) => (false, &ok, &mut sync_out),
                            _ => (false, &ok, &mut worker_out),
                        };

                        let (cmd, resp) = WorkerMessageCommand::new(msg);

                        if let Err(e) = (*output_channel).send(cmd).await {
                            error!("channel has closed; err = {:?}", e);
                            return;
                        }

                        responses_ordered.push(async move {
                            if must_wait {
                                match resp.get().await {
                                    None => {
                                        on_none.clone()
                                    },
                                    Some(response_message) => {
                                        let data = bincode::serialize(&response_message).unwrap();
                                        Bytes::from(data)
                                    }
                                }
                            } else {
                                on_none.clone()
                            }
                        });
                    },
                    Err(_e) => {
                        return;
                    }
                }
            },
            response = responses_ordered.select_next_some() => {
                if let Err(e) = transport.send(response).await {
                    error!("failed to write to socket; err = {:?}", e);
                    return;
                }
            }

        }
    }
}

async fn handle_transaction_channel(
    mut transport: Framed<TcpStream, LengthDelimitedCodec>,
    transaction_out: Sender<(SocketAddr, Transaction)>,
    ip: SocketAddr,
) {
    // In a loop, read data from the socket and write the data back.
    while let Some(transaction_data) = transport.next().await {
        match transaction_data {
            Ok(data) => {
                // Send the transaction on the channel.
                let output = (ip, data.to_vec());
                if let Err(e) = transaction_out.send(output).await {
                    error!("channel has closed; err = {:?}", e);
                    return;
                }

                // Write the data back.
                if let Err(e) = transport.send(Bytes::from("OK")).await {
                    error!("failed to write to socket; err = {:?}", e);
                    return;
                }
            }
            Err(e) => {
                error!("Socket data ended; err = {:?}", e);
                return;
            }
        }
    }
}
