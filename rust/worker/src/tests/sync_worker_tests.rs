// Copyright (c) Facebook, Inc. and its affiliates.
use super::*;
use crate::net::*;
use crate::receive_worker::*;
use crypto::generate_keypair;
use rand::{rngs::StdRng, SeedableRng};
use tokio::sync::mpsc::channel;

#[tokio::test]
async fn test_sync_single_channel() {
    let mut rng = StdRng::from_seed([133; 32]);
    let (my_node_key, _) = generate_keypair(&mut rng);
    let mut rng = StdRng::from_seed([0; 32]);
    let (node_key, _) = generate_keypair(&mut rng);
    // Make a listener with a block.
    let (tx, rx) = tokio::sync::mpsc::channel(100);
    let (tx_digest, mut rx_digest) = tokio::sync::mpsc::channel(100);

    let store =
        Store::new(&format!(".storage_integrated_{:?}", "test_sync_single").to_string()).unwrap();

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

    let (_shard_id, digst) = rx_digest.recv().await.unwrap();
    let digest: Vec<u8> = digst.0.to_vec();
    assert!(digest.len() == 32);

    // Step 2 - Bring up the network interface
    let (tx2, mut _rx2) = channel(100);
    let (tx3, mut _rx3) = channel(100);

    let _join_hanle_2 = tokio::spawn(async move {
        worker_server_start("127.0.0.1:6585".to_string(), tx, tx2, tx3)
            .await
            .expect("Server failed to start.");
    });

    // Step 3 create a sync channel to this address
    let (sync_req_tx, sync_req_rx) = channel(100);
    let (sync_resp_tx, sync_resp_rx) = channel(100);

    // channel for digests to primary
    // let (prim_tx, mut _prim_rx) = channel(1000);
    let (tx_digest_sync, mut rx_digest_sync) = tokio::sync::mpsc::channel(100);

    let inner_tx = tx_digest_sync.clone();
    let sync_store =
        Store::new(&format!(".storage_integrated_{:?}", "test_sync_single_sync2").to_string())
            .unwrap();
    let inner_sync_store = sync_store.clone();
    tokio::spawn(async move {
        let addr: BTreeMap<NodeID, String> = vec![(node_key, "127.0.0.1:6585".to_string())]
            .into_iter()
            .collect();
        sync_pool_start(
            my_node_key,
            addr,
            sync_req_rx,
            sync_resp_tx,
            inner_tx,
            inner_sync_store,
        )
        .await
    });

    let mut rw = ReceiveWorker::new(0, sync_resp_rx, tx_digest_sync, sync_store);
    tokio::spawn(async move {
        rw.start_receiving().await.unwrap();
    });

    // ----------------------------
    // Now send a Sync.
    let sync_msg = WorkerMessage::Synchronize(node_key, digest.clone());
    let (cmd, resp) = WorkerMessageCommand::new(sync_msg);
    sync_req_tx.send(cmd).await.expect("All good?");

    // first test that eventually we get a response.
    let resp_msg = resp.get().await;
    assert!(resp_msg.is_none());
    println!("RESP: {:?}", resp_msg);

    // second test that the digest has been processed
    let (_shard_sycn, digest_from_sync) = rx_digest_sync.recv().await.unwrap();
    assert_eq!(digest_from_sync.0.to_vec(), digest);
    println!("GOT DIGEST");

    // ----------------------------
    // Now test with a unknown key
    let sync_msg = WorkerMessage::Synchronize(node_key, vec![200_u8, 32]);
    let (cmd, resp) = WorkerMessageCommand::new(sync_msg);
    sync_req_tx.send(cmd).await.expect("All good?");

    // first test that eventually we get a response.
    let resp_msg = resp.get().await;
    assert!(resp_msg.is_none());
    println!("RESP: {:?}", resp_msg);
}

#[tokio::test]
async fn test_sync_pool() {}
