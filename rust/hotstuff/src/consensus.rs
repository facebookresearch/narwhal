use crate::config::Parameters;
use crate::core::Core;
use crate::error::ConsensusResult;
use crate::leader::LeaderElector;
use crate::messages::Block;
use crate::synchronizer::Synchronizer;
use dag_core::committee::Committee;
use dag_core::store::Store;
use dag_core::types::*;
use log::info;
use network::{NetReceiver, NetSender};
use tokio::sync::mpsc::{channel, Sender};
use tokio::sync::oneshot;

// #[cfg(test)]
// #[path = "tests/consensus_tests.rs"]
// pub mod consensus_tests;

pub struct Consensus;

impl Consensus {
    pub async fn run(
        name: NodeID,
        committee: Committee,
        parameters: Parameters,
        signing_channel: Sender<(Digest, oneshot::Sender<Signature>)>,
        store: Store,
        send_to_mempool: Sender<MempoolMessage>,
        commit_channel: Sender<Block>,
    ) -> ConsensusResult<()> {
        info!(
            "Consensus timeout delay set to {} ms",
            parameters.timeout_delay
        );
        info!(
            "Consensus synchronizer retry delay set to {} ms",
            parameters.sync_retry_delay
        );
        info!(
            "Consensus max payload size set to {} B",
            parameters.max_payload_size
        );
        info!(
            "Consensus min block delay set to {} ms",
            parameters.min_block_delay
        );

        let (tx_core, rx_core) = channel(1000);
        let (tx_network, rx_network) = channel(1000);

        // Make the network sender and receiver.
        let address = committee.address(&name).map(|mut x| {
            x.set_ip("0.0.0.0".parse().unwrap());
            x
        })?;
        let network_receiver = NetReceiver::new(address, tx_core.clone());
        tokio::spawn(async move {
            network_receiver.run().await;
        });

        let mut network_sender = NetSender::new(rx_network);
        tokio::spawn(async move {
            network_sender.run().await;
        });

        // The leader elector algorithm.
        let leader_elector = LeaderElector::new(committee.clone());

        // Make the synchronizer. This instance runs in a background thread
        // and asks other nodes for any block that we may be missing.
        let synchronizer = Synchronizer::new(
            name,
            committee.clone(),
            store.clone(),
            /* network_channel */ tx_network.clone(),
            /* core_channel */ tx_core,
            parameters.sync_retry_delay,
        )
        .await;

        let mut core = Core::new(
            name,
            committee,
            parameters,
            signing_channel,
            store,
            leader_elector,
            send_to_mempool,
            synchronizer,
            /* core_channel */ rx_core,
            /* network_channel */ tx_network,
            commit_channel,
        );
        tokio::spawn(async move {
            core.run().await;
        });

        Ok(())
    }
}
