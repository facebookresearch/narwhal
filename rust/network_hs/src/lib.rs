use bytes::Bytes;
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use log::{debug, info, warn};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[cfg(test)]
#[path = "tests/network_tests.rs"]
pub mod network_tests;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Network error: {0}")]
    NetworkError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),
}

pub struct NetMessage(pub Bytes, pub Vec<SocketAddr>);

pub struct NetSender {
    transmit: Receiver<NetMessage>,
}

impl NetSender {
    pub fn new(transmit: Receiver<NetMessage>) -> Self {
        Self { transmit }
    }

    // We keep alive one TCP connection per peer, each of which is handled
    // by a separate thread (called worker). We communicate with our workers
    // with a dedicated channel kept by the HashMap called `senders`. If the
    // a connection die, we make a new one.
    pub async fn run(&mut self) {
        let mut senders = HashMap::<_, Sender<_>>::new();
        while let Some(NetMessage(bytes, addresses)) = self.transmit.recv().await {
            for address in addresses {
                let spawn = match senders.get(&address) {
                    Some(tx) => tx.send(bytes.clone()).await.is_err(),
                    None => true,
                };
                if spawn {
                    let tx = Self::spawn_worker(address).await;
                    if let Ok(()) = tx.send(bytes.clone()).await {
                        senders.insert(address, tx);
                    }
                }
            }
        }
    }

    async fn spawn_worker(address: SocketAddr) -> Sender<Bytes> {
        // Each worker handle a TCP connection with on address.
        let (tx, mut rx) = channel(1000);
        tokio::spawn(async move {
            let stream = match TcpStream::connect(address).await {
                Ok(stream) => {
                    info!("Outgoing connection established with {}", address);
                    stream
                }
                Err(e) => {
                    warn!("Failed to connect to {}: {}", address, e);
                    return;
                }
            };
            let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
            while let Some(message) = rx.recv().await {
                match transport.send(message).await {
                    Ok(_) => debug!("Successfully sent message to {}", address),
                    Err(e) => {
                        warn!("Failed to send message to {}: {}", address, e);
                        return;
                    }
                }
            }
        });
        tx
    }
}

pub struct NetReceiver<Message> {
    address: SocketAddr,
    deliver: Sender<Message>,
}

impl<Message: 'static + Send + DeserializeOwned + Debug> NetReceiver<Message> {
    pub fn new(address: SocketAddr, deliver: Sender<Message>) -> Self {
        Self { address, deliver }
    }

    // For each incoming request, we spawn a new worker responsible to receive
    // messages and replay them through the provided deliver channel.
    pub async fn run(&self) {
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind to TCP port");

        debug!("Listening on {}", self.address);
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    warn!("{}", NetworkError::from(e));
                    continue;
                }
            };
            info!("Incoming connection established with {}", peer);
            Self::spawn_worker(socket, peer, self.deliver.clone()).await;
        }
    }

    async fn spawn_worker(socket: TcpStream, peer: SocketAddr, deliver: Sender<Message>) {
        tokio::spawn(async move {
            let mut transport = Framed::new(socket, LengthDelimitedCodec::new());
            while let Some(frame) = transport.next().await {
                match frame
                    .map_err(NetworkError::from)
                    .and_then(|x| bincode::deserialize(&x).map_err(NetworkError::from))
                {
                    Ok(message) => {
                        debug!("Received {:?}", message);
                        deliver.send(message).await.expect("Core channel closed");
                    }
                    Err(e) => {
                        warn!("{}", e);
                        return;
                    }
                }
            }
            warn!("Connection closed by peer {}", peer);
        });
    }
}
