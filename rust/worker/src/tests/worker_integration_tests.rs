// Copyright (c) Facebook, Inc. and its affiliates.
use crate::common::committee;
use crate::manage_worker::*;
use crate::send_worker::*;
use bytes::Bytes;
use dag_core::committee::Committee;
use futures::sink::SinkExt;
use futures::stream::StreamExt as _;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::channel;
use tokio::time::Instant;
use tokio_util::codec::{Framed, FramedRead, FramedWrite, LengthDelimitedCodec};

#[tokio::test]
async fn test_full_worker() {
    let total_transactions = 2000;
    let max_blocks_to_process = 2;
    let batch_size = 1000;
    full_worker(
        committee(),
        total_transactions,
        max_blocks_to_process,
        batch_size,
    )
    .await;
}

pub async fn full_worker(
    committee: Committee,
    total_transactions: usize,
    max_blocks_to_process: usize,
    batch_size: usize,
) {
    let mut workers = Vec::new();
    for auth in &committee.authorities {
        for (_, machine) in &auth.workers {
            let address = format!("{}:{}", machine.host, machine.port);
            workers.push((machine.name, address));
        }
    }
    let primaries: Vec<(NodeID, String)> = committee
        .authorities
        .iter()
        .map(|authority| {
            (
                authority.primary.name.clone(),
                format!("{}:{}", authority.primary.host, authority.primary.port),
            )
        })
        .collect();

    println!(
        "Nodes {:?} {:?} {:?} {:?}",
        primaries[0].0, primaries[1].0, primaries[2].0, primaries[3].0
    );

    let _worker_0 = ManageWorker::new(workers[0].0, committee.clone(), batch_size, None).await;

    //Each worker has a PrimaryListener

    let (tx_primary0, mut rx_primary0) = channel(100);
    let add = primaries[0].1.clone();
    tokio::spawn(async move {
        let mut pl = PrimaryDigestListener::new(tx_primary0);
        if let Err(_e) = pl.start(add).await {
            println!("Digest Listener Boot error: {:?}", _e);
        }
    });

    let _worker_1 = ManageWorker::new(workers[1].0, committee.clone(), batch_size, None).await;

    let (tx_primary1, mut rx_primary1) = channel(100);
    let add = primaries[1].1.clone();
    tokio::spawn(async move {
        let mut pl = PrimaryDigestListener::new(tx_primary1);
        if let Err(_e) = pl.start(add).await {
            println!("Digest Listener Boot error: {:?}", _e);
        }
    });
    let _worker_2 = ManageWorker::new(workers[2].0, committee.clone(), batch_size, None).await;

    let (tx_primary2, mut rx_primary2) = channel(100);
    let add = primaries[2].1.clone();
    tokio::spawn(async move {
        let mut pl = PrimaryDigestListener::new(tx_primary2);
        if let Err(_e) = pl.start(add).await {
            println!("Digest Listener Boot error: {:?}", _e);
        }
    });

    let _worker_3 = ManageWorker::new(workers[3].0, committee.clone(), batch_size, None).await;

    let (tx_primary3, mut rx_primary3) = channel(100);
    let add = primaries[3].1.clone();
    tokio::spawn(async move {
        let mut pl = PrimaryDigestListener::new(tx_primary3);
        if let Err(_e) = pl.start(add).await {
            println!("Digest Listener Boot error: {:?}", _e);
        }
    });

    // Send a few transactions to one of them
    tokio::task::yield_now().await;

    // Connect to server using TCP
    let addr = workers[0].1.clone();
    let stream = TcpStream::connect(addr).await.unwrap();

    let (read_sock, write_sock) = stream.into_split();
    let mut transport_write = FramedWrite::new(write_sock, LengthDelimitedCodec::new());
    let mut transport_read = FramedRead::new(read_sock, LengthDelimitedCodec::new());

    tokio::spawn(async move {
        loop {
            let _ = transport_read.next().await;
        }
    });

    let txs = Bytes::from(vec![0; 520]);

    // Drain all channels
    let join_handle = tokio::spawn(async move {
        let mut iterations = 0_usize;
        loop {
            if let Some(val) = rx_primary0.recv().await {
                // println!("Returned hash: {:?}", val);

                iterations += 1;

                if iterations >= max_blocks_to_process {
                    return val;
                }
            }
        }
    });

    // Drain the digest channels to free channel buffer space
    tokio::spawn(async move {
        loop {
            let _val = rx_primary1.recv().await;
        }
    });

    tokio::spawn(async move {
        loop {
            let _val = rx_primary2.recv().await;
        }
    });

    tokio::spawn(async move {
        loop {
            let _val = rx_primary3.recv().await;
        }
    });

    let now = Instant::now();

    for t in 0..total_transactions {
        // Write some data & wait for processing.
        let txs_copy = txs.clone();

        if t % 10000 == 0 {
            println!("Send {}", t);
        }

        //println!("Before sending tx");
        transport_write.send(txs_copy).await.unwrap();
    }

    if let Ok(_val) = join_handle.await {
        let bytes_per_ms =
            (txs.len() * total_transactions) as f64 / now.elapsed().as_millis() as f64;

        println!(
            "Tx size: {} bytes     Block size: {} tx",
            txs.len(),
            batch_size
        );
        println!(
            "Bytes per ms: {:.0}     MB/sec: {:.2}",
            bytes_per_ms,
            bytes_per_ms / 1000.0
        );
    }
}

//To be added/merged with  the Primary

pub struct PrimaryDigestListener {
    /// A channel receiver that gets request from the primary (to sync)
    message_output: Sender<PrimaryMessage>,
    //TODO Rename PrimaryMessage to ControlMessage
}

impl PrimaryDigestListener {
    pub fn new(message_sink: Sender<PrimaryMessage>) -> PrimaryDigestListener {
        PrimaryDigestListener {
            message_output: message_sink,
        }
    }

    pub async fn start(&mut self, url: String) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(url.clone()).await?;

        loop {
            // Listen for new connections.
            let (socket, _) = listener.accept().await?;
            let out_copy = self.message_output.clone();

            tokio::spawn(async move {
                let mut transport = Framed::new(socket, LengthDelimitedCodec::new());

                // In a loop, read msg from the socket and forward to channel
                while let Some(msg) = transport.next().await {
                    match msg {
                        Ok(msg) => {
                            // Send the transaction on the channel.
                            let des_msg: PrimaryMessage = bincode::deserialize(&msg[..])
                                .expect("Error de-serializing message.");

                            let output = des_msg;
                            if let Err(e) = out_copy.send(output).await {
                                eprintln!("channel has closed; {:?}", e);
                                return;
                            }

                            // Acks
                            if let Err(e) = transport.send(Bytes::from("OK")).await {
                                eprintln!("failed to write to socket; err = {:?}", e);
                                return;
                            }
                        }
                        Err(e) => {
                            eprintln!("Socket data ended; err = {:?}", e);
                            return;
                        }
                    }
                }
            });
        }
    }
}
