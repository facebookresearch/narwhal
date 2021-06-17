use super::*;
use rand::{rngs::StdRng, SeedableRng};

use std::collections::VecDeque;

#[tokio::test]
pub async fn test_query_response() {
    let mut rng = StdRng::from_seed([0; 32]);
    let (node_key, _) = get_keypair(&mut rng);
    let (mut tx, rx) = tokio::sync::mpsc::channel(100);
    let (tx_digest, mut rx_digest) = tokio::sync::mpsc::channel(100);

    let store =
        Store::new(format!(".storage_integrated_{:?}", "test_query_response").to_string()).await;

    // Start a Receive worker
    let mut rw = ReceiveWorker::new(0, rx, tx_digest, store);
    tokio::spawn(async move {
        rw.start_receiving().await.unwrap();
    });

    // Step 1 -- store something
    let (cmd1, resp1) = WorkerMessageCommand::new(WorkerMessage::Batch {
        node_id: node_key,
        special: 0,
        transactions: VecDeque::new(),
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
