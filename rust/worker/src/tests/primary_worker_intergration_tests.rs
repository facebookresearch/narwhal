// Copyright (c) Facebook, Inc. and its affiliates.
use crate::common::committee;
use crate::manage_worker::*;
use bytes::Bytes;
use dag_core::messages::WorkerChannelType;
use dag_core::primary_net::*;
use futures::stream::StreamExt as _;
use tokio::net::TcpStream;
use tokio::sync::mpsc::channel;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

#[tokio::test]
async fn test_primary_worker_digest() {
    let committee = committee();
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
    println!(
        "Workers {:?} {:?} {:?} {:?}",
        workers[0].0, workers[1].0, workers[2].0, workers[3].0
    );

    let (tx_deliver_channel0, mut rx_deliver_channel0) = channel(100);
    let mut primary_receiver0 =
        PrimaryNetReceiver::new(primaries[0].0, primaries[0].1.clone(), tx_deliver_channel0);

    tokio::spawn(async move {
        if let Err(e) = primary_receiver0.start_receiving().await {
            assert!(false, format!("Receiver error: {:?}", e))
        }
    });

    let _worker_0 = ManageWorker::new(workers[0].0, committee.clone(), 1000, None).await;

    //Boot Primary and Worker for n1

    let (tx_deliver_channel1, mut rx_deliver_channel1) = channel(100);
    let mut primary_receiver1 =
        PrimaryNetReceiver::new(primaries[1].0, primaries[1].1.clone(), tx_deliver_channel1);

    tokio::spawn(async move {
        if let Err(e) = primary_receiver1.start_receiving().await {
            assert!(false, format!("Receiver error: {:?}", e))
        }
    });

    let _worker_1 = ManageWorker::new(workers[1].0, committee.clone(), 1000, None).await;

    //Boot Primary and Worker for n2

    let (tx_deliver_channel2, mut rx_deliver_channel2) = channel(100);
    let mut primary_receiver2 =
        PrimaryNetReceiver::new(primaries[2].0, primaries[2].1.clone(), tx_deliver_channel2);

    tokio::spawn(async move {
        if let Err(e) = primary_receiver2.start_receiving().await {
            assert!(false, format!("Receiver error: {:?}", e))
        }
    });

    let _worker_2 = ManageWorker::new(workers[2].0, committee.clone(), 1000, None).await;

    //Boot Primary and Worker for n3

    let (tx_deliver_channel3, mut rx_deliver_channel3) = channel(100);
    let mut primary_receiver3 =
        PrimaryNetReceiver::new(primaries[3].0, primaries[3].1.clone(), tx_deliver_channel3);

    tokio::spawn(async move {
        if let Err(e) = primary_receiver3.start_receiving().await {
            assert!(false, format!("Receiver error: {:?}", e))
        }
    });

    let _worker_3 = ManageWorker::new(workers[3].0, committee.clone(), 1000, None).await;

    //Step 2: Send a few transactions to worker of n0
    tokio::task::yield_now().await;

    // Connect to server using TCP
    let stream = TcpStream::connect(workers[0].1.clone()).await.unwrap();

    let (read_sock, write_sock) = stream.into_split();
    let mut transport_write = FramedWrite::new(write_sock, LengthDelimitedCodec::new());
    let mut transport_read = FramedRead::new(read_sock, LengthDelimitedCodec::new());

    // Set the type of the channel
    let head = WorkerChannelType::Transaction;
    let header = Bytes::from(bincode::serialize(&head).expect("Error deserializing"));
    transport_write.send(header).await.expect("Error Sending");
    let _n = transport_read.next().await.expect("Error on test receive");

    tokio::spawn(async move {
        loop {
            let _ = transport_read.next().await;
        }
    });

    let total_transactions = 10000;

    // Check that n0 and n1 get the same hashes
    let join_handle = tokio::spawn(async move {
        let mut iterations = 0_usize;
        loop {
            if let Some(msg0) = rx_deliver_channel0.recv().await {
                println!("Returned message: {:?}", msg0);
                if let Some(msg1) = rx_deliver_channel1.recv().await {
                    match msg0 {
                        PrimaryMessage::TxDigest(val0, _, _) => match msg1 {
                            PrimaryMessage::TxDigest(val1, _, _) => {
                                // println!("primary 0 got :{:?} , primary 1 got :{:?}", val0, val1);
                                assert_eq!(val0, val1);
                            }
                            _ => assert!(false, "wrong message {:?}", msg1),
                        },
                        _ => assert!(false, "wrong message {:?}", msg0),
                    }
                    iterations += 1;
                }

                if iterations >= total_transactions / 1000 {
                    return;
                }
            }
        }
    });

    // Drain the channels of n2 and n3 to free space
    tokio::spawn(async move {
        loop {
            let _val = rx_deliver_channel2.recv().await;
        }
    });

    tokio::spawn(async move {
        loop {
            let _val = rx_deliver_channel3.recv().await;
        }
    });

    for t in 0..total_transactions {
        let txs = Bytes::from(t.to_string());

        // Write some data & wait for processing.
        let txs_copy = txs.clone();

        //println!("Before sending tx");
        transport_write.send(txs_copy).await.unwrap();
    }
    let _ = join_handle.await;
}
