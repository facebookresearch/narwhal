use super::*;
use crate::committee::Committee;
use crate::messages::messages_tests::*;
use crate::processor::processor_tests::*;
use std::collections::VecDeque;
use tokio::sync::mpsc::channel;

use rstest::*;

#[rstest]
async fn test_consensus(
    committee: Committee,
    mut many_signed_headers_rounds: VecDeque<Vec<(Certificate, BlockHeader)>>,
) {
    let (_tx_consensus_receiving, rx_consensus_receiving) = channel(1000);
    let (tx_consensus_sending, _rx_consensus_sending) = channel(1000);
    let mut consensus = Consensus::new(
        tx_consensus_sending,
        rx_consensus_receiving,
        committee.clone(),
    );

    let (_tx_consensus_receiving2, rx_consensus_receiving2) = channel(1000);
    let (tx_consensus_sending2, _rx_consensus_sending2) = channel(1000);
    let mut consensus2 = Consensus::new(
        tx_consensus_sending2,
        rx_consensus_receiving2,
        committee.clone(),
    );

    while let Some(round_blocks) = many_signed_headers_rounds.pop_front() {
        for (cert, header) in round_blocks {
            consensus.add_header(header.clone(), cert.digest);
            consensus2.add_header(header, cert.digest);
        }
    }

    consensus.order_dag(11).await;

    //no let's do the same but order every 4 rounds
    //  consensus2.order_dag(5).await;

    consensus2.order_dag(7).await;

    consensus2.order_dag(11).await;

    while !consensus2.header_ordered.is_empty() && !consensus.header_ordered.is_empty() {

        if let Some((digest, _)) = consensus.header_ordered.pop_front() {
            println!("digest equal is {:?}", digest);

            if let Some((digest2, _)) = consensus2.header_ordered.pop_front() {
                println!("digest2 equal is {:?}", digest2);

                assert_eq!(block.0, block2.0);
            }
        }
    }
    if !consensus.header_ordered.is_empty() {
        assert!(false)
    }

    if !consensus2.header_ordered.is_empty() {
        assert!(false)
    }
}
