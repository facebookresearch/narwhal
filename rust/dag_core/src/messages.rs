use super::committee::*;
use super::error::*;
use super::types::*;

use ed25519_dalek::Digest as DalekDigest;
use ed25519_dalek::Sha512;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;

// #[cfg(test)]
// #[path = "tests/messages_tests.rs"]
// pub mod messages_tests;

/// This is the data structure the receive re-computes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHeader {
    /// The author of the block.
    pub author: NodeID,
    /// The round number of the block.
    pub round: RoundNumber,
    /// The digests of all transactions in that block.
    pub transactions_digest: WorkersDigests,
    /// Links to the parents of the block.
    pub parents: BTreeSet<(NodeID, Digest)>,
    /// User-defined metadata.
    pub metadata: Metadata,
    /// The current instance of the protocol.
    pub instance_id: SequenceNumber,
}
pub const GC_DEPTH: u64 = 10;

impl BlockHeader {
    /// Verify that the block header is well formed.
    pub fn check(&self, committee: &Committee) -> Result<(), DagError> {
        dag_ensure!(
            self.instance_id == committee.instance_id,
            DagError::BadInstanceId
        );
        Ok(())
    }

    // Checks if a block has some digest as parent
    pub fn has_parent(&self, digest: &Digest) -> bool {
        for (_, parent) in &self.parents {
            if *parent == *digest {
                return true;
            }
        }
        false
    }

    // Checks if a block has at least one parent from a set of digests
    pub fn has_at_least_one_parent(&self, digests: &[Digest]) -> bool {
        for digest in digests {
            if self.has_parent(digest) {
                return true;
            }
        }
        false
    }

    // Checks that a set of digests corresponds to all parents of the block
    pub fn has_all_parents(&self, cert_digests: &[Digest]) -> bool {
        for (_, parent_digest) in &self.parents {
            if !(*cert_digests).contains(parent_digest) {
                return false;
            }
        }
        //check that there are not additional certificates trailing
        if self.parents.len() == cert_digests.len() {
            return true;
        }
        false
    }
}

impl Digestible for Vec<u8> {
    fn digest(&self) -> Digest {
        let mut h: Sha512 = Sha512::new();
        let mut hash: [u8; 64] = [0u8; 64];
        let mut digest: [u8; 32] = [0u8; 32];
        h.update(&self);
        hash.copy_from_slice(h.finalize().as_slice());
        digest.copy_from_slice(&hash[..32]);
        Digest(digest)
    }
}

/// This is the data structure the receive re-computes and stores.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedBlockHeader {
    /// The blockheader data.
    pub data: Vec<u8>,
    /// The digest of the blockheader.
    pub digest: Digest,
    /// A signature on the digest of the blockheader.
    signature: Signature,
}

impl SignedBlockHeader {
    pub async fn new(
        header: BlockHeader,
        signing_channel: &mut Sender<(Digest, oneshot::Sender<Signature>)>,
    ) -> Result<Self, DagError> {
        let data: Vec<u8> = bincode::serialize(&header)?;
        let digest = data.digest();
        let (sender, receiver) = oneshot::channel();
        signing_channel
            .send((digest, sender))
            .await
            .map_err(|e| DagError::ChannelError {
                error: format!("{}", e),
            })?;
        match receiver.await {
            Ok(signature) => Ok(Self {
                data,
                digest,
                signature,
            }),
            Err(e) => dag_bail!(DagError::ChannelError {
                error: format!("{}", e)
            }),
        }
    }

    // Make a signed blockheader directly with a secret key.
    // For debug / testing only.
    pub fn debug_new(header: BlockHeader, secret_key: &SecretKey) -> Result<Self, DagError> {
        let data: Vec<u8> = bincode::serialize(&header)?;
        let digest = data.digest();
        let signature = Signature::new(&digest, secret_key);
        Ok(Self {
            data,
            digest,
            signature,
        })
    }

    /// Verify the signature and return the deserialized block header.
    pub fn check(&self, committee: &Committee) -> Result<BlockHeader, DagError> {
        // Check the digest is good.
        dag_ensure!(self.data.digest() == self.digest, DagError::BadDigest);

        // Desirialize & Check instance ID
        let header: BlockHeader = bincode::deserialize(&self.data[..])?;
        header.check(committee)?;

        // Chech author has some stake & signature is correct.
        let weight = committee.stake(&header.author);
        dag_ensure!(weight > 0, DagError::UnknownSigner);
        self.signature.check(&self.digest, header.author)?;

        Ok(header)
    }
}

impl PartialEq for SignedBlockHeader {
    fn eq(&self, other: &SignedBlockHeader) -> bool {
        self.digest == other.digest
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncTxInfo {
    pub digest: Digest,
    pub shard_id: ShardID,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WorkerChannelType {
    Transaction,
    Worker,
}
