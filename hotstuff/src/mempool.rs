use crate::error::ConsensusResult;
use crate::messages::Block;
use config::Committee;
use primary::Certificate;
use tokio::sync::mpsc::Sender;

pub struct MempoolDriver {
    committee: Committee,
    tx_mempool: Sender<Certificate>,
}

impl MempoolDriver {
    pub fn new(committee: Committee, tx_mempool: Sender<Certificate>) -> Self {
        Self {
            committee,
            tx_mempool,
        }
    }

    /// Verify the payload certificates.
    pub async fn verify(&mut self, block: &Block) -> ConsensusResult<()> {
        for certificate in &block.payload {
            certificate.verify(&self.committee)?;
        }
        Ok(())
    }

    /// Cleanup the mempool.
    pub async fn cleanup(&mut self, payload: Vec<Certificate>) {
        for certificate in payload {
            self.tx_mempool
                .send(certificate)
                .await
                .expect("Failed to send cleanup message");
        }
    }
}
