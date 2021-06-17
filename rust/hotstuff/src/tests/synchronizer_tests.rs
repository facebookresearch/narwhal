// use super::*;
// use crate::common::{block, chain, committee, keys};
// use std::fs;

// #[tokio::test]
// async fn get_existing_previous_block() {
//     let mut chain = chain(keys());
//     let block = chain.pop().unwrap();
//     let b2 = chain.pop().unwrap();

//     // Add the block b2 to the store.
//     let path = ".db_test_get_existing_previous_block";
//     let _ = fs::remove_dir_all(path);
//     let mut store = Store::new(path).unwrap();
//     let key = b2.digest().to_vec();
//     let value = bincode::serialize(&b2).unwrap();
//     let _ = store.write(key, value).await;

//     // Make a new synchronizer.
//     let (name, _) = keys().pop().unwrap();
//     let (tx_network, _) = channel(10);
//     let (tx_core, _) = channel(10);
//     let mut synchronizer = Synchronizer::new(
//         name,
//         committee(),
//         store,
//         tx_network,
//         tx_core,
//         /* sync_retry_delay */ 10_000,
//     )
//     .await;

//     // Ask the predecessor of 'block' to the synchronizer.
//     match synchronizer.get_previous_block(&block).await {
//         Ok(Some(b)) => assert_eq!(b, b2),
//         _ => assert!(false),
//     }
// }

// #[tokio::test]
// async fn get_genesis_previous_block() {
//     // Make a new synchronizer.
//     let path = ".db_test_get_genesis_previous_block";
//     let _ = fs::remove_dir_all(path);
//     let store = Store::new(path).unwrap();
//     let (name, _) = keys().pop().unwrap();
//     let (tx_network, _) = channel(1);
//     let (tx_core, _) = channel(1);
//     let mut synchronizer = Synchronizer::new(
//         name,
//         committee(),
//         store,
//         tx_network,
//         tx_core,
//         /* sync_retry_delay */ 10_000,
//     )
//     .await;

//     // Ask the predecessor of 'block' to the synchronizer.
//     match synchronizer.get_previous_block(&block()).await {
//         Ok(Some(b)) => assert_eq!(b, Block::genesis()),
//         _ => assert!(false),
//     }
// }

// #[tokio::test]
// async fn get_missing_previous_block() {
//     let mut chain = chain(keys());
//     let block = chain.pop().unwrap();
//     let previous_block = chain.pop().unwrap();

//     // Make a new synchronizer.
//     let path = ".db_test_get_missing_previous_block";
//     let _ = fs::remove_dir_all(path);
//     let mut store = Store::new(path).unwrap();
//     let (name, _) = keys().pop().unwrap();
//     let (tx_network, mut rx_network) = channel(1);
//     let (tx_core, mut rx_core) = channel(1);
//     let mut synchronizer = Synchronizer::new(
//         name,
//         committee(),
//         store.clone(),
//         tx_network,
//         tx_core,
//         /* sync_retry_delay */ 10_000,
//     )
//     .await;

//     // Ask for the parent of a block to the synchronizer.
//     // The store does not have the parent yet.
//     let copy = block.clone();
//     let handle = tokio::spawn(async move {
//         match synchronizer.get_previous_block(&copy).await {
//             Ok(None) => assert!(true),
//             _ => assert!(false),
//         }
//     });

//     // Ensure the synchronizer sends a sync request
//     // asking for the parent block.
//     match rx_network.recv().await {
//         Some(NetMessage(bytes, mut recipients)) => {
//             match bincode::deserialize(&bytes).unwrap() {
//                 CoreMessage::SyncRequest(b, s) => {
//                     assert_eq!(b, previous_block.digest());
//                     assert_eq!(s, name);
//                 }
//                 _ => assert!(false),
//             }
//             let mut addresses = committee().broadcast_addresses(&name);
//             addresses.sort();
//             recipients.sort();
//             assert_eq!(recipients, addresses);
//         }
//         _ => assert!(false),
//     }

//     // Ensure the synchronizer returns None, thus suspending
//     // the processing of the block.
//     assert!(handle.await.is_ok());

//     // Add the parent to the store.
//     let key = previous_block.digest().to_vec();
//     let value = bincode::serialize(&previous_block).unwrap();
//     let _ = store.write(key, value).await;

//     // Now that we have the parent, ensure the synchronizer
//     // loops back the block to the core to resume processing.
//     match rx_core.recv().await {
//         Some(CoreMessage::LoopBack(b)) => assert_eq!(b, block.clone()),
//         _ => assert!(false),
//     }
// }
