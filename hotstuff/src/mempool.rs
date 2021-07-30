use crate::consensus::{Round};
use crate::error::{ConsensusError, ConsensusResult};
use tokio::sync::mpsc::{Sender};
use primary::{Certificate};
use config::Committee as MempoolCommittee;
use crypto::Hash as  _;

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
    pub async fn verify(&mut self, payload: &Vec<Certificate>) -> ConsensusResult<()> {
        for certificate in payload {
            ensure!(
                self.gc_round <= certificate.round(),
                ConsensusError::TooOld(certificate.digest(), certificate.round())
            );

            certificate.verify(&self.mempool_committee).map_err(ConsensusError::from);
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
