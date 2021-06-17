// use super::*;
// use crate::common::{committee, keys, MockMempool};
// use crate::config::Parameters;
// use crypto::SecretKey;
// use futures::future::try_join_all;
// use std::fs;
// use tokio::sync::mpsc::channel;
// use tokio::task::JoinHandle;

// fn spawn_nodes(
//     keys: Vec<(PublicKey, SecretKey)>,
//     committee: Committee,
//     store_path: &str,
// ) -> Vec<JoinHandle<()>> {
//     keys.into_iter()
//         .enumerate()
//         .map(|(i, (name, secret))| {
//             let committee = committee.clone();
//             let parameters = Parameters::default();
//             let store_path = format!("{}_{}", store_path, i);
//             let _ = fs::remove_dir_all(&store_path);
//             let store = Store::new(&store_path).unwrap();
//             let signature_service = SignatureService::new(secret);
//             let mempool = MockMempool;
//             let (tx_commit, mut rx_commit) = channel(1);
//             tokio::spawn(async move {
//                 Consensus::run(
//                     name,
//                     committee,
//                     parameters,
//                     signature_service,
//                     store,
//                     mempool,
//                     tx_commit,
//                 )
//                 .await
//                 .unwrap();

//                 match rx_commit.recv().await {
//                     Some(block) => assert_eq!(block, Block::genesis()),
//                     _ => assert!(false),
//                 }
//             })
//         })
//         .collect()
// }

// #[tokio::test]
// async fn end_to_end() {
//     let mut committee = committee();
//     committee.increment_base_port(6000);

//     // Run all nodes.
//     let store_path = ".db_test_end_to_end";
//     let handles = spawn_nodes(keys(), committee, store_path);

//     // Ensure all threads terminated correctly.
//     assert!(try_join_all(handles).await.is_ok());
// }

// #[tokio::test]
// async fn dead_node() {
//     let mut committee = committee();
//     committee.increment_base_port(6100);

//     // Run all nodes but the last.
//     let leader_elector = LeaderElector::new(committee.clone());
//     let dead = leader_elector.get_leader(0);
//     let keys = keys().into_iter().filter(|(x, _)| *x != dead).collect();
//     let store_path = ".db_test_dead_node";
//     let handles = spawn_nodes(keys, committee, store_path);

//     // Ensure all threads terminated correctly.
//     assert!(try_join_all(handles).await.is_ok());
// }
