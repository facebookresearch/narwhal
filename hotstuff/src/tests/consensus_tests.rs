use super::*;
use crate::common::{committee_with_base_port, keys};
use crate::config::Parameters;
use crypto::SecretKey;
use futures::future::try_join_all;
use std::fs;
use tokio::sync::mpsc::channel;
use tokio::task::JoinHandle;

fn spawn_nodes(
    keys: Vec<(PublicKey, SecretKey)>,
    committee: Committee,
    store_path: &str,
) -> Vec<JoinHandle<Block>> {
    keys.into_iter()
        .enumerate()
        .map(|(i, (name, secret))| {
            let committee = committee.clone();
            let parameters = Parameters {
                timeout_delay: 100,
                ..Parameters::default()
            };
            let store_path = format!("{}_{}", store_path, i);
            let _ = fs::remove_dir_all(&store_path);
            let store = Store::new(&store_path).unwrap();
            let signature_service = SignatureService::new(secret);
            let (tx_consensus_to_mempool, mut rx_consensus_to_mempool) = channel(10);
            let (_tx_mempool_to_consensus, rx_mempool_to_consensus) = channel(1);
            let (tx_commit, mut rx_commit) = channel(1);

            // Sink the mempool channel.
            tokio::spawn(async move {
                loop {
                    rx_consensus_to_mempool.recv().await;
                }
            });

            // Spawn the consensus engine.
            tokio::spawn(async move {
                Consensus::spawn(
                    name,
                    committee,
                    parameters,
                    signature_service,
                    store,
                    rx_mempool_to_consensus,
                    tx_consensus_to_mempool,
                    tx_commit,
                );

                rx_commit.recv().await.unwrap()
            })
        })
        .collect()
}

#[tokio::test]
async fn end_to_end() {
    let committee = committee_with_base_port(15_000);

    // Run all nodes.
    let store_path = ".db_test_end_to_end";
    let handles = spawn_nodes(keys(), committee, store_path);

    // Ensure all threads terminated correctly.
    let blocks = try_join_all(handles).await.unwrap();
    assert!(blocks.windows(2).all(|w| w[0] == w[1]));
}
