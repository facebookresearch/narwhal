use super::error::*;
use super::types::*;
use crate::committee::*;
use crate::old_messages::*;
use crate::primary::PrimaryWorkerMessage;
use bytes::Bytes;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use log::*;
#[allow(deprecated)]
use rand::distributions::{Distribution, Exp};
use std::cmp::min;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const MESSAGE_HANDLE_DROP_AFTER_N_ROUNDS: u64 = 100;

// #[cfg(test)]
// #[path = "tests/primary_net_tests.rs"]
// pub mod primary_net_tests;
const MIN_MSG_DELAY: f64 = 5.0; // ms
const ADD_MSG_DELAY_LAMBDA: f64 = 1.0 / 20.0; // ms
const MSG_DELAY: bool = false; // ms
                               // const BYTE_PER_MS : u64 = 1_000;

pub struct PrimaryNetReceiver {
    id: NodeID,
    address: String,
    pub deliver_channel: Sender<PrimaryMessage>,
}

impl PrimaryNetReceiver {
    pub fn new(id: NodeID, address: String, deliver_channel: Sender<PrimaryMessage>) -> Self {
        Self {
            id,
            address,
            deliver_channel,
        }
    }

    pub async fn start_receiving(&mut self) -> Result<(), DagError> {
        let socket =
            TcpListener::bind(&self.address)
                .await
                .map_err(|error| DagError::NetworkError {
                    error: format!("{}", error),
                })?;
        info!("Bound TCP address: {:?}", self.address);
        info!("Primary Listener {:?} is alive", self.id);

        loop {
            let (stream, peer) = socket
                .accept()
                .await
                .map_err(|error| DagError::NetworkError {
                    error: format!("{}", error),
                })?;
            info!("Incoming connection from: {:?}", peer.to_string());
            let deliver_channel = self.deliver_channel.clone();
            tokio::spawn(async move {
                let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
                while let Some(frame) = transport.next().await {
                    match frame {
                        Ok(data) => match bincode::deserialize(&data[..]) {
                            Ok(message) => {
                                debug!("Receive message {:?}", message);
                                if let Err(e) = deliver_channel.send(message).await {
                                    error!("Error while delivering: {:?}", e);
                                }
                                tokio::task::yield_now().await;
                            }
                            Err(e) => {
                                warn!("Error while deserializing: {:?}", e);
                            }
                        },
                        Err(e) => {
                            warn!("Error while receiving: {:?}", e);
                        }
                    }
                }
            });
        }
    }
}

pub struct PrimaryNetSender {
    myself: NodeID,
    committee: Committee,
    pub message_channel: Receiver<(RoundNumber, PrimaryMessage)>,
}

impl PrimaryNetSender {
    pub fn new(
        myself: NodeID,
        committee: Committee,
        message_channel: Receiver<(RoundNumber, PrimaryMessage)>,
    ) -> Self {
        Self {
            myself,
            committee,
            message_channel,
        }
    }

    pub async fn start_sending(&mut self) -> Result<(), DagError> {
        // TODO: total hack to not drop messages
        let mut current_round = 0;
        let mut temp_response_handles =
            VecDeque::<Result<PrimaryMessageResponseHandle, DagError>>::new();

        // Spin primary and worker networks
        let primary_addresses: BTreeMap<NodeID, String> = self
            .committee
            .authorities
            .iter()
            .map(|authority| {
                (
                    authority.primary.name,
                    format!("{}:{}", authority.primary.host, authority.primary.port),
                )
            })
            .collect();

        let mut primary_net = PrimaryNet::new(self.myself, primary_addresses, None).await;

        // TODO: Workers do expect a Banner =/= None.

        let our_workers: HashMap<ShardID, Machine> = self
            .committee
            .authorities
            .iter()
            .find(|authority| authority.primary.name == self.myself)
            .unwrap()
            .workers
            .clone()
            .into_iter()
            .collect();

        let worker_addresses: BTreeMap<NodeID, String> = our_workers
            .iter()
            .map(|(_, machine)| (machine.name, format!("{}:{}", machine.host, machine.port)))
            .collect();

        let head = WorkerChannelType::Worker;
        let header = Bytes::from(bincode::serialize(&head).expect("Error serializing"));
        let mut worker_net = PrimaryNet::new(self.myself, worker_addresses, Some(header)).await;

        while let Some((round, message)) = self.message_channel.recv().await {
            debug!("Send message {:?}", message);

            // Clean up handles from previous rounds.
            if round > current_round + MESSAGE_HANDLE_DROP_AFTER_N_ROUNDS {
                temp_response_handles.clear();
                current_round = round;
            }

            // Handle messages
            match message {
                PrimaryMessage::Header(_) | PrimaryMessage::Cert(_) => {
                    // Send the message to the network.
                    let bytes: Vec<u8> = bincode::serialize(&message)?;
                    let handles = primary_net.broadcast_message(Bytes::from(bytes)).await;

                    let mut temp = handles.into_iter().collect();
                    temp_response_handles.append(&mut temp);
                }
                PrimaryMessage::SyncTxSend(SyncTxInfo { digest, shard_id }, other_worker_id) => {
                    if let Some(our_worker_machine) = our_workers.get(&shard_id) {
                        //let message = WorkerMessage::Synchronize(other_worker_id, digest.0.to_vec());
                        let authority = self.committee.find_authority(&other_worker_id).unwrap();
                        let target = authority.primary.name;
                        let message = PrimaryWorkerMessage::Synchronize(vec![digest], target);

                        let data: Vec<u8> = bincode::serialize(&message)?;

                        let handle = worker_net
                            .send_message(our_worker_machine.name, Bytes::from(data))
                            .await;
                        temp_response_handles.push_back(handle);
                    } else {
                        panic!("We seem to have mis-addressed our own worker node.");
                    }
                }

                message @ PrimaryMessage::PartialCert(_) => {
                    let data: Vec<u8> = bincode::serialize(&message)?;

                    let origin_node = if let PrimaryMessage::PartialCert(cert) = message {
                        cert.origin_node
                    } else {
                        unreachable!()
                    };

                    let handle = primary_net
                        .send_message(origin_node, Bytes::from(data))
                        .await;
                    temp_response_handles.push_back(handle);
                }

                PrimaryMessage::SyncHeaderRequest(_, _, responder) => {
                    let data: Vec<u8> = bincode::serialize(&message)?;
                    let handle = primary_net.send_message(responder, Bytes::from(data)).await;
                    temp_response_handles.push_back(handle);
                }

                PrimaryMessage::SyncHeaderReply(_, requester) => {
                    let data: Vec<u8> = bincode::serialize(&message)?;
                    let handle = primary_net.send_message(requester, Bytes::from(data)).await;
                    temp_response_handles.push_back(handle);
                }

                _ => warn!("Message not supported"),
            }
        }
        Ok(())
    }
}

/* Primary net NG */

use futures::FutureExt;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::error;
use tokio::sync::mpsc::channel;
use tokio::sync::oneshot;

pub fn make_message(
    destination: NodeID,
    serialized_message: Bytes,
) -> (PrimaryMessageHandle, PrimaryMessageResponseHandle) {
    let (tx, rx) = oneshot::channel();
    let pmh = PrimaryMessageHandle {
        destination,
        serialized_message,
        response_send: Some(tx),
    };
    let pmrh = PrimaryMessageResponseHandle {
        response_recv: Some(rx),
    };
    (pmh, pmrh)
}

#[derive(Debug)]
pub struct PrimaryMessageHandle {
    /// The destination for this message
    pub destination: NodeID,
    /// A (shared) Byte representation of this message
    pub serialized_message: Bytes,
    /// A channel to signal the response to this message
    pub response_send: Option<oneshot::Sender<Result<(NodeID, Bytes), DagError>>>,
}

impl PrimaryMessageHandle {
    pub fn set_response(&mut self, data: Bytes) {
        if let Some(chan) = self.response_send.take() {
            let resp: Result<(NodeID, Bytes), DagError> = Ok((self.destination, data));
            let _ = chan.send(resp);
        }
    }

    pub fn set_error(&mut self, err: DagError) {
        if let Some(chan) = self.response_send.take() {
            let resp: Result<(NodeID, Bytes), DagError> = Err(err);
            let _ = chan.send(resp);
        }
    }

    pub fn is_cancelled(&mut self) -> bool {
        if let Some(chan) = self.response_send.as_ref() {
            chan.is_closed()
        } else {
            true
        }
    }
}

impl Drop for PrimaryMessageHandle {
    fn drop(&mut self) {
        self.set_error(DagError::MessageHandleDropped);
    }
}

pub struct PrimaryMessageResponseHandle {
    /// A channel to receive a response to a message.
    pub response_recv: Option<oneshot::Receiver<Result<(NodeID, Bytes), DagError>>>,
}

impl PrimaryMessageResponseHandle {
    // Drop the receiver to cancel the sending of this message
    pub fn cancel(mut self) {
        // Drop the channel
        let _ = self.response_recv.take();
    }

    // Block until a response is received.
    pub async fn recv(mut self) -> Result<(NodeID, Bytes), DagError> {
        if let Some(chan) = &mut self.response_recv.take() {
            match chan.await {
                Ok(v) => v,
                Err(e) => Err(DagError::ChannelError {
                    error: format!("{}", e),
                }),
            }
        } else {
            Err(DagError::ChannelClosed)
        }
    }
}

/// Structure representing the interface to a broadcast worker.
#[derive(Clone, Debug)]
pub struct PrimaryChannelProxy {
    /// The channel user to send chunks to the worker to relay to the other host.
    input: Sender<PrimaryMessageHandle>,
}

pub struct PrimaryChannelState {
    /// The URL of the host to which the work should connect
    host: String,
    /// The receiver channel of the chunks to broadcast
    crecv: Receiver<PrimaryMessageHandle>,
}

impl PrimaryChannelState {
    // Runs the watchdog of a single broadcast worker. The watchdog keeps a TCP connection
    // open and send chunks onto it.
    pub async fn run_watchdog_loop(
        &mut self,
        banner: Option<Bytes>,
    ) -> Result<(), Box<dyn error::Error>> {
        let xhost = &self.host;
        let crecv = &mut self.crecv;

        let mut buffer: VecDeque<(u128, PrimaryMessageHandle)> = VecDeque::new();
        info!("Channel to {} started.", xhost);
        let mut delay = 200; // milliseconds
        let mut retry = 0_usize;

        loop {
            retry += 1;

            // Outer loop tries to re-connect.
            // Connect to server using TCP.
            match TcpStream::connect(xhost).await {
                Ok(socket) => {
                    info!("Connect... {:?} (broadcast, retry={})", *xhost, retry);
                    delay = 200; // Reset the delay.

                    let transport = Framed::new(socket, LengthDelimitedCodec::new());
                    let (mut write_t, mut read_t) = transport.split();

                    if let Some(banner_bytes) = &banner {
                        // Send a fixed string first then wait for a response
                        if let Err(_e) = write_t.send(banner_bytes.clone()).await {
                            info!("Connection error: {:?}", _e);
                            continue; // start loop from begining
                        }

                        if let Some(msg) = read_t.next().await {
                            if let Err(e) = msg {
                                info!("Connection error: {:?}", e);
                                continue;
                            }
                        } else {
                            info!("Receiving stream closed");
                            continue;
                        }
                    }

                    /* Handle the sending end of the connections */
                    let mut responses_buffer: VecDeque<PrimaryMessageHandle> = VecDeque::new();
                    let timer = sleep(Duration::from_millis(10));
                    tokio::pin!(timer);

                    #[allow(deprecated)]
                    let exp = Exp::new(ADD_MSG_DELAY_LAMBDA);

                    'inner: loop {
                        // inner loop sends messages into an active TCP connection

                        if !MSG_DELAY {
                            while let Some((send_time, mut msg)) = buffer.pop_front() {
                                // Clear up the channel to accomodate newer messages.

                                // Ignore if cancelled
                                if msg.is_cancelled() {
                                    info!("Drop Message.");
                                    continue;
                                };

                                // Always send the message to the other side.
                                if let Err(_e) = write_t.send(msg.serialized_message.clone()).await
                                {
                                    error!("Connection error: {:?}", _e);
                                    buffer.push_front((send_time, msg));
                                    break 'inner;
                                }

                                responses_buffer.push_back(msg);
                            }
                        }

                        // Process next message or response
                        tokio::select! {
                            // Only send in steps after a random delay
                            _ = &mut timer => {
                                timer.as_mut().reset(Instant::now()+ Duration::from_millis(10));

                                // Check if it is time to send stuff out.
                                let now_time = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis();

                                while let Some((send_time, mut msg)) = buffer.pop_front() {
                                    // Clear up the channel to accomodate newer messages.

                                    if !(send_time < now_time) {
                                        buffer.push_front((send_time, msg));
                                        break;
                                    }

                                    // Ignore if cancelled
                                    if msg.is_cancelled() {
                                        info!("Drop Message.");
                                        continue;
                                    };

                                    // Always send the message to the other side.
                                    if let Err(_e) = write_t.send(msg.serialized_message.clone()).await {
                                        error!("Connection write error: {:?}", _e);
                                        buffer.push_front((send_time, msg));
                                        break 'inner;
                                    }

                                    responses_buffer.push_back(msg);

                                }


                            },

                            /* The branch that checks for messages received */
                            response = read_t.next().fuse() => {

                                if response.is_none(){
                                    error!("Connection receiveing end closed.");
                                    break 'inner;
                                }

                                match response.unwrap(){
                                    Ok(response_data) => {
                                        if let Some(mut msg) = responses_buffer.pop_front() {
                                            // Now we have received the ack, notify delivery.
                                            let data = response_data.freeze();
                                            msg.set_response(data);
                                        } else {
                                            error!("Received response not expected.");
                                            break 'inner;
                                        }
                                    }
                                    Err(e) => {
                                        error!("Connection read error: {:?}", e);
                                        break 'inner;
                                    }
                                }

                            },
                            /* The branch that checks for new message to send */
                            msg = crecv.recv().fuse() => {
                                if let Some(msg) = msg {

                                    let now_time = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis();
                                    let v = exp.sample(&mut rand::thread_rng());
                                    let time_window = (MIN_MSG_DELAY + v) as u128;

                                    buffer.push_back((now_time + time_window, msg));
                                } else {
                                    info!("Worker channel closed");
                                    return Ok(());
                                }
                            },
                        };
                    }

                    // Re-tranmit all messages we did not get an ack for
                    while let Some(v) = responses_buffer.pop_back() {
                        buffer.push_front((0, v));
                    }
                }
                Err(e) => {
                    {
                        info!("Connect Error {:?}...{:?}", e, xhost);

                        // Wait for 5 sec -- then try to reconnect.
                        sleep(Duration::from_millis(delay)).await;
                        info!("waking up to try again for {:?}", xhost);
                        delay = min(delay + delay / 10, 1000 * 60); // Increase the delay.

                        while !buffer.is_empty() {
                            let cont = {
                                let (_, first_m) = buffer.get_mut(0).unwrap();
                                first_m.is_cancelled()
                            };
                            if cont {
                                let _ = buffer.pop_front();
                                continue;
                            }
                            break;
                        }

                        // Read remaining channel -- delete old messages
                        // add current messages to buffer.
                        // Read remaining channel -- delete old messages
                        // add current messages to buffer.

                        let current_delay_fut = sleep(Duration::from_millis(2));
                        tokio::pin!(current_delay_fut);

                        loop {
                            // <<<<<<< HEAD
                            // let fut = self.transaction_pool.recv(); // Do not await yet!

                            tokio::select! {
                                                            ret_val = crecv.recv().fuse()=> {

                                                                                            match ret_val {
                                                                            Some(msg) => {
                                                                                buffer.push_back((0, msg));
                                                                            current_delay_fut.as_mut().reset(Instant::now()+ Duration::from_millis(2));

                                                                            }
                                                                            None => {
                                                                                info!("Worker channel closed");
                                                                                return Ok(());
                                                                            }
                                                                        }
                                                                    }

                                                            _ = &mut current_delay_fut => {
                                                                break;
                            // =======
                            //                             match crecv.try_recv() {
                            //                                 Ok(msg) => {
                            //                                     buffer.push_back((0, msg));
                            //                                 }
                            //                                 Err(TryRecvError::Empty) => {
                            //                                     break; // continue normal execution.
                            //                                 }
                            //                                 Err(_) => {
                            //                                     info!("Worker channel closed");
                            //                                     return Ok(());
                            // >>>>>>> origin/block_explorer
                                                            }
                                                        }
                        }

                        // TODO: Here change logic so that if there are no messages in the buffer
                        // there is no attempt to reconnect until new messages are sent on the channel.
                        // This should aim to prevent long lived TCP connections being open but empty.
                    };
                }
            }
        }
    }
}

use rand::seq::SliceRandom;

#[derive(Clone, Debug)]
pub struct PrimaryNet {
    pub node_channels: HashMap<NodeID, PrimaryChannelProxy>,
}

impl PrimaryNet {
    pub async fn new(
        myself: NodeID,
        addresses: BTreeMap<NodeID, String>,
        banner: Option<Bytes>,
    ) -> PrimaryNet {
        let mut hm = HashMap::new();

        // Start all processes for senders
        for (nid, addr) in &addresses {
            if *nid == myself {
                continue;
            };

            let (tx, rx) = channel(1000);
            let mut pcs = PrimaryChannelState {
                host: addr.clone(),
                crecv: rx,
            };

            let banner = banner.clone();
            tokio::spawn(async move {
                pcs.run_watchdog_loop(banner).await.unwrap();
            });

            hm.insert(*nid, PrimaryChannelProxy { input: tx });
        }

        // Package primary network
        PrimaryNet { node_channels: hm }
    }

    pub async fn send_message(
        &mut self,
        node: NodeID,
        message: Bytes,
    ) -> Result<PrimaryMessageResponseHandle, DagError> {
        match self.node_channels.get_mut(&node) {
            None => Err(DagError::UnknownNode),
            Some(value) => {
                let (msg_handle, msg_resp_handle) = make_message(node, message);
                value
                    .input
                    .send(msg_handle)
                    .await
                    .map_err(|e| DagError::ChannelError {
                        error: format!("{}", e),
                    })?;
                Ok(msg_resp_handle)
            }
        }
    }

    pub async fn broadcast_message(
        &mut self,
        message: Bytes,
    ) -> Vec<Result<PrimaryMessageResponseHandle, DagError>> {
        let mut resps = Vec::new();

        // Make a random permutation of nodes
        let mut keys: Vec<NodeID> = self.node_channels.keys().cloned().collect();
        let keys_slice = &mut keys[..];
        {
            let mut rng = rand::thread_rng();
            keys_slice.shuffle(&mut rng);
        }

        // Broadcast in the random order
        for node in keys_slice {
            let value = self.node_channels.get_mut(node).unwrap();

            let (msg_handle, msg_resp_handle) = make_message(*node, message.clone());
            if value.input.send(msg_handle).await.is_err() {
                info!("Channel to {:?} closed?", node);
                resps.push(Err(DagError::ChannelClosed));
            } else {
                resps.push(Ok(msg_resp_handle));
            }
        }
        resps
    }
}
