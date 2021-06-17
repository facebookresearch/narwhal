use super::types::*;
use crate::committee::Committee;
use crate::primary::*;
use crate::primary_net::*;
use crypto::SecretKey;
use log::*;
use store::*;
use tokio::sync::mpsc::{channel, Receiver, Sender};

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
        tx_consensus_receiving: Sender<ConsensusMessage>,
        rx_consensus_sending: Receiver<ConsensusMessage>,
    ) -> Self {
        let mut receive_url = committee.get_url(&primary_id).unwrap();
        let receive_port = receive_url.split(':').collect::<Vec<_>>()[1];
        receive_url = format!("0.0.0.0:{}", receive_port);

        let channel_capacity = 100;

        let (tx_primary_sending, rx_primary_sending) = channel(channel_capacity);
        let (tx_primary_receiving, rx_primary_receiving) = channel(channel_capacity);
        let (tx_signature_channel, rx_signature_channel) = channel(channel_capacity);
        let store = Store::new(&format!(".storage_integrated_{:?}", primary_id))
            .expect("Failed to create store");

        // 1. Run the signing service.
        let mut factory = SignatureFactory::new(secret_key, rx_signature_channel);
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
            tx_consensus_receiving,
            rx_consensus_sending,
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
