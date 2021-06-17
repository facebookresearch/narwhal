// use super::*;
// use crate::common::{block, keys, MockMempool};
// use crate::messages::{Block, QC};
// use async_trait::async_trait;
// use rand::rngs::StdRng;
// use rand::RngCore as _;
// use rand::SeedableRng as _;
// use std::fs;

// pub struct MockMempoolWait;

// #[async_trait]
// impl NodeMempool for MockMempoolWait {
//     async fn get(&mut self, _max: usize) -> Vec<Vec<u8>> {
//         Vec::default()
//     }

//     async fn verify(&mut self, payload: &[Vec<u8>]) -> PayloadStatus {
//         PayloadStatus::Wait(payload.to_vec())
//     }

//     async fn garbage_collect(&mut self, _payload: &[Vec<u8>]) {}
// }

// #[tokio::test]
// async fn verify_accept() {
//     let (tx_core, _rx_core) = channel(1);
//     let store_path = ".db_test_verify_accept";
//     let _ = fs::remove_dir_all(store_path);
//     let store = Store::new(store_path).unwrap();
//     let mut mempool_driver = MempoolDriver::new(MockMempool, tx_core, store);
//     assert!(mempool_driver.verify(&block()).await.unwrap());
// }

// #[tokio::test]
// async fn verify_wait() {
//     let (tx_core, mut rx_core) = channel(1);
//     let store_path = ".db_test_verify_wait";
//     let _ = fs::remove_dir_all(store_path);
//     let mut store = Store::new(store_path).unwrap();
//     let mut mempool_driver = MempoolDriver::new(MockMempoolWait, tx_core, store.clone());

//     // Make a block with two payloads.
//     let mut rng = StdRng::from_seed([0; 32]);
//     let mut payload_1 = [0u8; 32];
//     rng.fill_bytes(&mut payload_1);
//     let mut payload_2 = [0u8; 32];
//     rng.fill_bytes(&mut payload_2);
//     let payload = vec![payload_1.to_vec(), payload_2.to_vec()];

//     let (public_key, secret_key) = keys().pop().unwrap();
//     let block = Block::new_from_key(QC::genesis(), public_key, 1, payload, &secret_key);

//     // Ensure the driver replies with WAIT.
//     match mempool_driver.verify(&block).await {
//         Ok(false) => assert!(true),
//         _ => assert!(false),
//     }

//     // Add the payload to the store and ensure the driver trigger re-processing.
//     let _ = store.write(payload_1.to_vec(), Vec::new()).await;
//     let _ = store.write(payload_2.to_vec(), Vec::new()).await;
//     match rx_core.recv().await {
//         Some(CoreMessage::LoopBack(b)) => assert_eq!(b, block),
//         _ => assert!(false),
//     }
// }
