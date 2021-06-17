use super::types::*;
use crate::committee::Committee;
use crate::primary::*;
use crate::primary_net::*;
use crate::store::*;
use futures::executor::block_on;
use log::*;
use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::Receiver;

// #[cfg(test)]
// #[path = "tests/primary_intergration_tests.rs"]
// pub mod primary_intergration_tests;

/// A ManageWorker is instatiated in a physical machine to coordinate with external machines (primary, remote workers)
pub struct ManagePrimary {
    //Channel to Receive Digests and push to primary
    pub primary_core: PrimaryCore,
}
/// The first thing called when booting
impl ManagePrimary {
    pub fn new(
        primary_id: NodeID,
        secret_key: SecretKey,
        committee: Committee,
        //external_consensus: bool,
        get_from_hotstuff: Receiver<MempoolMessage>,
    ) -> Self {
        let mut receive_url = committee.get_url(&primary_id).unwrap();
        let receive_port = receive_url.split(':').collect::<Vec<_>>()[1];
        receive_url = format!("0.0.0.0:{}", receive_port);

        let channel_capacity = 100;

        let (tx_primary_sending, rx_primary_sending) = channel(channel_capacity);
        let (tx_primary_receiving, rx_primary_receiving) = channel(channel_capacity);
        let (tx_signature_channel, rx_signature_channel) = channel(channel_capacity);
        let store = block_on(Store::new(format!(".storage_integrated_{:?}", primary_id)));

        // 1. Run the signing service.
        let mut factory = SignatureFactory::new(secret_key.duplicate(), rx_signature_channel);
        tokio::spawn(async move {
            factory.start().await;
        });

        let primary = PrimaryCore::new(
            /* id */ primary_id,
            committee.clone(),
            /* signature_channel */ tx_signature_channel,
            /* sending_endpoint */ tx_primary_sending,
            /* receiving_endpoint */ rx_primary_receiving,
            tx_primary_receiving.clone(),
            store,
            //external_consensus,
            get_from_hotstuff,
        );

        //boot primary network

        //cocatenate map of hosts with map of workers into sender_addresses
        let mut primary_net_sender =
            PrimaryNetSender::new(primary_id, committee, rx_primary_sending);

        tokio::spawn(async move {
            if let Err(e) = primary_net_sender.start_sending().await {
                error!("Receiver error: {:?}", e)
            }
        });

        let mut primary_net_receiver =
            PrimaryNetReceiver::new(primary_id, receive_url, tx_primary_receiving);

        tokio::spawn(async move {
            if let Err(e) = primary_net_receiver.start_receiving().await {
                error!("Receiver error: {:?}", e)
            }
        });

        ManagePrimary {
            primary_core: primary,
        }
    }
}
