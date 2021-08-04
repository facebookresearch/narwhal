use crate::consensus::Round;
use crate::error::{ConsensusError, ConsensusResult};
use crate::messages::Block;
use config::Committee as MempoolCommittee;
use crypto::Hash as _;
use primary::Certificate;
use tokio::sync::mpsc::Sender;

pub struct MempoolDriver {
    mempool_committee: MempoolCommittee,
    gc_depth: Round,
    tx_mempool: Sender<Certificate>,
}

impl MempoolDriver {
    pub fn new(
        mempool_committee: MempoolCommittee,
        gc_depth: Round,
        tx_mempool: Sender<Certificate>,
    ) -> Self {
        Self {
            mempool_committee,
            gc_depth,
            tx_mempool,
        }
    }

    /// Verify the payload certificates.
    pub async fn verify(&mut self, block: &Block) -> ConsensusResult<()> {
        let gc_round = match block.round > self.gc_depth {
            true => block.round - self.gc_depth,
            false => 0,
        };
        for certificate in &block.payload {
            ensure!(
                gc_round <= certificate.round(),
                ConsensusError::TooOld(certificate.digest(), certificate.round())
            );

            certificate.verify(&self.mempool_committee)?;
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
