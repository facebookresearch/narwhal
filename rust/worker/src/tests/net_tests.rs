// Copyright (c) Facebook, Inc. and its affiliates.
use super::*;
use crate::quorum_broadcast::*;

use dag_core::committee::Committee;

use bytes::Bytes;
use crypto::generate_keypair;
use dag_core::types::NodeID;
use rand::{rngs::StdRng, SeedableRng};
use rstest::*;
use std::error::Error;
use tokio::net::TcpStream;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[rstest]
fn it_works() {
    assert_eq!(2 + 2, 4);
}

// Tokio testing reactor loop (single thread)
#[tokio::test]
async fn test_tx_server() -> Result<(), Box<dyn Error>> {
    // Test second structure
    let (tx, mut rx) = channel(100);

    let _join_hanle_2 = tokio::spawn(async move {
        let (tx_wmsg, _) = channel(1);
        let (tx_sync, _) = channel(100);
        let tx_url = "127.0.0.1:9981".to_string();
        let _ = worker_server_start(tx_url, tx_wmsg, tx_sync, tx).await;
    });
    tokio::task::yield_now().await;

    // Connect to server using TCP
    let stream = (TcpStream::connect("127.0.0.1:9981").await)?;
    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
    set_channel_type(&mut transport, WorkerChannelType::Transaction).await;

    // Write some data & wait for processing.
    transport.send(Bytes::from("hello world!")).await?;
    tokio::task::yield_now().await;

    // Read the data refected back from echo server
    if let Some(Ok(n)) = transport.next().await {
        assert_eq!(n, Bytes::from("OK")); // 4: len + 2 for 'OK'
    }

    // Read data from the output channel.
    tokio::task::yield_now().await;
    println!("Read channel");
    let (_ip, data) = rx.recv().await.unwrap();
    println!("{}", data.len());

    Ok(())
}

#[tokio::test]
async fn test_block_server() -> Result<(), Box<dyn Error>> {
    let (tx, mut rx) = channel(100);
    let (tx2, _) = channel(100);
    let (tx3, _) = channel(100);
    let mut rng = StdRng::from_seed([0; 32]);
    let (node_key, _) = generate_keypair(&mut rng);

    let _join_hanle_2 = tokio::spawn(async move {
        worker_server_start("127.0.0.1:9982".to_string(), tx, tx2, tx3)
            .await
            .expect("Server failed to start.");
    });

    tokio::spawn(async move {
        loop {
            let mut val = rx.recv().await.unwrap();
            val.respond(None);
        }
    });

    tokio::task::yield_now().await;

    // Send down the channel

    // Connect to server using TCP
    let stream = TcpStream::connect("127.0.0.1:9982".to_string()).await?;

    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
    set_channel_type(&mut transport, WorkerChannelType::Worker).await;

    // Write some data & wait for processing.
    let msg = WorkerMessage::Batch {
        node_id: node_key,
        transactions: Vec::new(),
    };

    let data = Bytes::from(bincode::serialize(&msg)?);
    transport.send(data).await?;
    tokio::task::yield_now().await;

    // Read the data refected back from echo server
    let n = transport.next().await.expect("Error on test receive");
    assert_eq!(n.unwrap().len(), 2); // "OK"

    Ok(())
}

fn fixture_get_4nodes() -> Vec<NodeID> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng).0).collect()
}

pub fn fixture_bradcaster(
    port: u32,
) -> (
    Sender<WorkerMessageCommand>,
    Receiver<WorkerMessageCommand>,
    Broadcaster,
) {
    let mut nodes = fixture_get_4nodes();
    let n0 = nodes.pop().unwrap();
    let n1 = nodes.pop().unwrap();
    let n2 = nodes.pop().unwrap();
    let n3 = nodes.pop().unwrap();

    let base_port = 3000u16; // Must be different for each test.
    let cmt = Committee::debug_new(vec![n0, n1, n2, n3], base_port);

    let (input_tx, input_rx) = channel(100);
    let (local_tx, local_rx) = channel(100);
    let b = Broadcaster::new(
        n0,
        (vec![
            (n1, format!("127.0.0.1:{}", port).to_string()),
            (n2, format!("127.0.0.1:{}", port).to_string()),
            (n3, format!("127.0.0.1:{}", port).to_string()),
        ])
        .iter()
        .cloned()
        .collect(),
        cmt,
        input_rx,
        local_tx,
    );

    (input_tx, local_rx, b)
}

fn make_test_batch(node_key: NodeID) -> WorkerMessageCommand {
    let (cmd1, _) = WorkerMessageCommand::new(WorkerMessage::Batch {
        node_id: node_key,
        transactions: Vec::new(),
    });
    cmd1
}

#[tokio::test]
async fn test_broadcast_client_only() -> Result<(), Box<dyn Error>> {
    let (tx, mut _rx) = channel(100);
    let (tx2, mut _rx2) = channel(100);
    let (tx3, _) = channel(100);

    let _join_hanle_2 = tokio::spawn(async move {
        worker_server_start("127.0.0.1:8983".to_string(), tx, tx2, tx3)
            .await
            .expect("Server failed to start.");
    });

    tokio::spawn(async move {
        loop {
            let mut x = _rx.recv().await.unwrap();
            x.respond(None);
        }
    });

    tokio::task::yield_now().await;

    let (input_tx, mut local_rx, mut b) = fixture_bradcaster(8983);

    // Now start the broadcaster
    tokio::spawn(async move {
        println!("Start broadcast service...");
        b.start_worker_broadcast_task().await;
    });

    tokio::task::yield_now().await;
    let mut rng = StdRng::from_seed([0; 32]);
    let (node_key1, _) = generate_keypair(&mut rng);
    let (node_key2, _) = generate_keypair(&mut rng);
    let (node_key3, _) = generate_keypair(&mut rng);

    let cmd1 = make_test_batch(node_key1);
    let cmd2 = make_test_batch(node_key2);
    let cmd3 = make_test_batch(node_key3);

    input_tx.send(cmd1).await?;
    input_tx.send(cmd2).await?;
    input_tx.send(cmd3).await?;

    for _ in 0..2 {
        let x = local_rx.recv().await.unwrap();
        println!("Got: {:?}", x);
    }

    Ok(())
}

#[tokio::test]
async fn test_broadcast_client_one_faulty() -> Result<(), Box<dyn Error>> {
    let (tx, mut _rx) = channel(100);
    let (tx2, mut _rx2) = channel(100);
    let (tx3, _) = channel(100);

    let _join_hanle_2 = tokio::spawn(async move {
        worker_server_start("127.0.0.1:8984".to_string(), tx, tx2, tx3)
            .await
            .expect("Server failed to start.");
    });
    tokio::spawn(async move {
        loop {
            let mut x = _rx.recv().await.unwrap();
            x.respond(None);
        }
    });
    tokio::task::yield_now().await;

    let (input_tx, mut local_rx, mut b) = fixture_bradcaster(8984);

    // Now start the broadcaster
    tokio::spawn(async move {
        println!("Start broadcast service...");
        b.start_worker_broadcast_task().await;
    });
    tokio::task::yield_now().await;

    let mut rng = StdRng::from_seed([0; 32]);
    let (node_key1, _) = generate_keypair(&mut rng);
    let (node_key2, _) = generate_keypair(&mut rng);
    let (node_key3, _) = generate_keypair(&mut rng);

    let cmd1 = make_test_batch(node_key1);
    let cmd2 = make_test_batch(node_key2);
    let cmd3 = make_test_batch(node_key3);

    input_tx.send(cmd1).await?;
    input_tx.send(cmd2).await?;
    input_tx.send(cmd3).await?;

    let x = local_rx.recv().await.unwrap();
    println!("Got: {:?}", x);

    Ok(())
}

#[tokio::test]
async fn test_broadcast_client_late_start() -> Result<(), Box<dyn Error>> {
    let (tx, mut _rx) = channel(100);

    let (input_tx, mut local_rx, mut b) = fixture_bradcaster(8985);

    // Now start the broadcaster
    tokio::spawn(async move {
        println!("Start broadcast service...");
        b.start_worker_broadcast_task().await;
    });
    tokio::task::yield_now().await;
    let mut rng = StdRng::from_seed([0; 32]);
    let (node_key1, _) = generate_keypair(&mut rng);
    let (node_key2, _) = generate_keypair(&mut rng);
    let (node_key3, _) = generate_keypair(&mut rng);

    let cmd1 = make_test_batch(node_key1);
    let cmd2 = make_test_batch(node_key2);
    let cmd3 = make_test_batch(node_key3);

    input_tx.send(cmd1).await?;
    input_tx.send(cmd2).await?;
    input_tx.send(cmd3).await?;

    tokio::task::yield_now().await;

    // Start the server on the other side much much later.
    let (tx2, mut _rx2) = channel(100);
    let (tx3, _) = channel(100);

    let _join_hanle_2 = tokio::spawn(async move {
        worker_server_start("127.0.0.1:8985".to_string(), tx, tx2, tx3)
            .await
            .expect("Server failed to start.");
    });
    tokio::spawn(async move {
        loop {
            let mut x = _rx.recv().await.unwrap();
            x.respond(None);
        }
    });
    tokio::task::yield_now().await;

    let x = local_rx.recv().await.unwrap();
    println!("Got: {:?}", x);

    Ok(())
}
