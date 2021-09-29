use super::*;
use crate::common::{block, chain, committee, committee_with_base_port, keys, listener};
use std::fs;

#[tokio::test]
async fn get_existing_parent_block() {
    let mut chain = chain(keys());
    let block = chain.pop().unwrap();
    let b2 = chain.pop().unwrap();

    // Add the block b2 to the store.
    let path = ".db_test_get_existing_parent_block";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();
    let key = b2.digest().to_vec();
    let value = bincode::serialize(&b2).unwrap();
    let _ = store.write(key, value).await;

    // Make a new synchronizer.
    let (name, _) = keys().pop().unwrap();
    let (tx_loopback, _) = channel(10);
    let mut synchronizer = Synchronizer::new(
        name,
        committee(),
        store,
        tx_loopback,
        /* sync_retry_delay */ 10_000,
    );

    // Ask the predecessor of 'block' to the synchronizer.
    match synchronizer.get_parent_block(&block).await {
        Ok(Some(b)) => assert_eq!(b, b2),
        _ => assert!(false),
    }
}

#[tokio::test]
async fn get_genesis_parent_block() {
    // Make a new synchronizer.
    let path = ".db_test_get_genesis_parent_block";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path).unwrap();
    let (name, _) = keys().pop().unwrap();
    let (tx_loopback, _) = channel(1);
    let mut synchronizer = Synchronizer::new(
        name,
        committee(),
        store,
        tx_loopback,
        /* sync_retry_delay */ 10_000,
    );

    // Ask the predecessor of 'block' to the synchronizer.
    match synchronizer.get_parent_block(&block()).await {
        Ok(Some(b)) => assert_eq!(b, Block::genesis()),
        _ => assert!(false),
    }
}

#[tokio::test]
async fn get_missing_parent_block() {
    let committee = committee_with_base_port(12_000);
    let mut chain = chain(keys());
    let block = chain.pop().unwrap();
    let parent_block = chain.pop().unwrap();

    // Make a new synchronizer.
    let path = ".db_test_get_missing_parent_block";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();
    let (name, _) = keys().pop().unwrap();
    let (tx_loopback, mut rx_loopback) = channel(1);
    let mut synchronizer = Synchronizer::new(
        name,
        committee.clone(),
        store.clone(),
        tx_loopback,
        /* sync_retry_delay */ 10_000,
    );

    // Spawn a listener to receive our sync request.
    let address = committee.address(&block.author).unwrap();
    let message = ConsensusMessage::SyncRequest(parent_block.digest(), name);
    let expected = Bytes::from(bincode::serialize(&message).unwrap());
    let listener_handle = listener(address, Some(expected.clone()));

    // Ask for the parent of a block to the synchronizer. The store does not have the parent yet.
    let copy = block.clone();
    let handle = tokio::spawn(async move {
        let ret = synchronizer.get_parent_block(&copy).await;
        assert!(ret.is_ok());
        assert!(ret.unwrap().is_none());
    });

    // Ensure the other listeners correctly received the sync request.
    assert!(listener_handle.await.is_ok());

    // Ensure the synchronizer returns None, thus suspending the processing of the block.
    assert!(handle.await.is_ok());

    // Add the parent to the store.
    let key = parent_block.digest().to_vec();
    let value = bincode::serialize(&parent_block).unwrap();
    let _ = store.write(key, value).await;

    // Now that we have the parent, ensure the synchronizer loops back the block to the core
    // to resume processing.
    let delivered = rx_loopback.recv().await.unwrap();
    assert_eq!(delivered, block.clone());
}
