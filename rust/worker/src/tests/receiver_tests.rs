// Copyright (c) Facebook, Inc. and its affiliates.
use super::*;
use crate::net::*;
use crate::send_worker::SendWorker;
use bytes::Bytes;
use crypto::generate_keypair;
use dag_core::messages::*;
use futures::sink::SinkExt as _;
use rand::{rngs::StdRng, SeedableRng};
use std::collections::VecDeque;
use tokio::net::TcpStream;
use tokio::sync::mpsc::channel;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub fn pool(input: u8) -> TransactionList {
    let mut pool = VecDeque::new();
    for i in 0..input as u8 {
        let mut transaction = Vec::new();
        transaction.push(i);
        pool.push_back(transaction);
    }
    pool
}

#[tokio::test]
pub async fn test_query_response() {
    let mut rng = StdRng::from_seed([0; 32]);
    let (node_key, _) = generate_keypair(&mut rng);
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let (tx_digest, mut rx_digest) = tokio::sync::mpsc::channel(100);

    let store = Store::new(&format!(".storage_integrated_{:?}", "test_query_response").to_string())
        .unwrap();

    // Start a Receive worker
    let mut rw = ReceiveWorker::new(0, rx, tx_digest, store);
    tokio::spawn(async move {
        rw.start_receiving().await.unwrap();
    });

    // Step 1 -- store something
    let (cmd1, resp1) = WorkerMessageCommand::new(WorkerMessage::Batch {
        node_id: node_key,
        transactions: Vec::new(),
    });

    tx.send(cmd1).await.expect("Failed to store Batch?");
    assert!(resp1.get().await.is_none());

    // Step 2. Read the digest
    let (_shard_id, digst) = rx_digest.recv().await.unwrap();
    let digest: Vec<u8> = digst.0.to_vec();
    assert!(digest.len() == 32);

    // Step 3 - now Query for it
    let (cmd_query, resp_query) = WorkerMessageCommand::new(WorkerMessage::Query(digest));

    tx.send(cmd_query).await.expect("Failed to store Batch?");
    let resp = resp_query.get().await;

    assert!(resp.is_some());
    if let WorkerMessage::Batch { node_id, .. } = resp.unwrap() {
        assert_eq!(node_id, node_key);
    } else {
        panic!("Wrong structure");
    }

    // Step 4 -- get something that does not exist
    let bad_key = vec![0_u8; 32];
    let (cmd_query, resp_query) = WorkerMessageCommand::new(WorkerMessage::Query(bad_key));
    tx.send(cmd_query).await.expect("Failed to store Batch?");
    assert!(resp_query.get().await.is_none());
}

#[tokio::test]
async fn test_send_batch() {
    let mut rng = StdRng::from_seed([0; 32]);
    let (node_key, _) = generate_keypair(&mut rng);
    let pool = pool(15);
    let mut send_pool = pool.clone();
    let ip = "127.0.0.1:8080".parse().unwrap();

    let (tx_send, tx_recv) = channel(100);
    for tx in pool {
        let _ = tx_send.send((ip, tx)).await;
    }

    let (send, mut recv) = channel(100);
    let _future_1 = tokio::spawn(async move {
        let mut send_worker = SendWorker::new(tx_recv, send, BATCH_SIZE);

        send_worker.start_sending(node_key).await
    });
    tokio::task::yield_now().await;
    'outer: while let Some(message) = recv.recv().await {
        match message.get_message() {
            WorkerMessage::Batch { transactions, .. } => {
                let mut temp: VecDeque<Transaction> = transactions.into_iter().cloned().collect();
                // let mut transactions = transactions.clone();
                while let Some(send) = temp.pop_front() {
                    if let Some(submitted) = send_pool.pop_front() {
                        assert_eq!(submitted, send);
                        if send_pool.len() == 0 {
                            break 'outer;
                        }
                    } else {
                        assert!(
                            false,
                            "My local pool is empty and there are transactions in the buffer"
                        )
                    }
                }
            }
            _ => assert!(false, "Unexpected other Worker Message"),
        }
    }
    assert!(send_pool.is_empty(), "My local pool still has transactions")
}

const BATCH_SIZE: usize = 15;

#[tokio::test]
async fn test_send_receive_batch() {
    let pool = pool(15);
    let mut rng = StdRng::from_seed([0; 32]);
    let (node_key, _) = generate_keypair(&mut rng);

    let (send, recv) = channel(100);
    let (send_hash, mut recv_hash) = channel(100);

    let ip = "127.0.0.1:8080".parse().unwrap();

    let (tx_send, tx_recv) = channel(100);
    for tx in pool.clone() {
        let _ = tx_send.send((ip, tx)).await;
    }

    //launch one or more senders
    let _future_1 = tokio::spawn(async move {
        let mut send_worker = SendWorker::new(tx_recv, send, BATCH_SIZE);

        send_worker.start_sending(node_key).await
    });
    //launch one or more receivers
    let mut my_store = Store::new(".storage_receiver_storage").unwrap();

    let store2 = my_store.clone();
    let _future_2 = tokio::spawn(async move {
        let mut recv_worker = ReceiveWorker::new(0, recv, send_hash, store2);

        recv_worker.start_receiving().await.unwrap();
        return recv_worker.hasher_endpoint;
    });

    if let Some((_shard_id, recv_hash)) = recv_hash.recv().await {
        let data = my_store.read(recv_hash.0.to_vec()).await.unwrap().unwrap();
        let wm: WorkerMessage = bincode::deserialize(&data[..]).unwrap();

        if let WorkerMessage::Batch { transactions, .. } = wm {
            assert!(transactions.len() == 15);
        } else {
            panic!("Did not get the right structure.")
        }
    }
}

#[tokio::test]
async fn test_receive_network() {
    let pool = pool(15);
    let mut rng = StdRng::from_seed([0; 32]);
    let (node_key, _) = generate_keypair(&mut rng);
    let (send, recv) = channel(100);
    let (send2, _) = channel(100);
    let (send3, _) = channel(100);

    let _future_1 = tokio::spawn(async move {
        worker_server_start("127.0.0.1:8084".to_string(), send, send2, send3)
            .await
            .expect("Server failed to start.");
    });
    tokio::task::yield_now().await;

    // Send down the channel

    // Connect to server using TCP
    let stream = TcpStream::connect("127.0.0.1:8084").await.unwrap();

    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
    set_channel_type(&mut transport, WorkerChannelType::Worker).await;

    let mut buf = Vec::<Transaction>::with_capacity(1);

    for transaction in pool {
        buf.push(transaction);
    }

    let msg = WorkerMessage::Batch {
        node_id: node_key,
        // Drain from the front of the pool either all transactions or the batch_size, whichever is less
        transactions: buf,
    };

    let data = Bytes::from(bincode::serialize(&msg).unwrap());

    if let Err(e) = transport.send(data).await {
        assert!(false, "Sending TCP Message failed {:}", e);
    };
    tokio::task::yield_now().await;

    let (send_hash, mut recv_hash) = channel(100);

    //launch one or more receivers
    let mut my_store = Store::new(".receiver_storage").unwrap();

    let store2 = my_store.clone();
    let _future_2 = tokio::spawn(async move {
        let mut recv_worker = ReceiveWorker::new(0, recv, send_hash, store2);

        recv_worker.start_receiving().await.unwrap();
        return recv_worker.hasher_endpoint;
    });

    if let Some((_shard_id, recv_hash)) = recv_hash.recv().await {
        let data = my_store.read(recv_hash.0.to_vec()).await.unwrap().unwrap();
        let wm: WorkerMessage = bincode::deserialize(&data[..]).unwrap();

        if let WorkerMessage::Batch { transactions, .. } = wm {
            assert!(transactions.len() == 15);
        } else {
            panic!("Did not get the right structure.")
        }
    }
}
