use super::committee::*;
use super::error::*;
use super::messages::*;
use crypto::{Digest, PublicKey, SecretKey, Signature};
use ed25519_dalek::Digest as DalekDigest;
use ed25519_dalek::Sha512;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet, VecDeque};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

#[cfg(test)]
#[path = "tests/types_tests.rs"]
pub mod types_tests;

pub type NodeID = PublicKey;
pub type RoundNumber = u64;
pub type WaveNumber = u64;
pub type Transaction = Vec<u8>;
pub type SequenceNumber = u64;
pub type SyncNumber = u64;
pub type Stake = usize;
pub type TransactionList = VecDeque<Transaction>;
pub type WorkersDigests = BTreeSet<(Digest, ShardID)>;
pub type ShardID = u32;

#[derive(Hash, Default, Debug, Clone, Serialize, Deserialize)]
pub struct Metadata(pub Option<[u8; 32]>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Certificate {
    pub digest: Digest,
    pub signatures: Vec<(NodeID, Signature)>,
    pub round: RoundNumber,
    pub primary_id: NodeID,
    pub header: BlockHeader,
}

impl Certificate {
    pub fn genesis(committee: &Committee) -> Vec<Self> {
        let mut weight = 0;
        committee
            .authorities
            .iter()
            .take_while(|authority| {
                let ret = weight < committee.quorum_threshold();
                weight += authority.stake;
                ret
            })
            .map(|authority| {
                let primary_id = authority.primary.name;
                let header = BlockHeader {
                    author: primary_id,
                    round: 0,
                    parents: BTreeSet::new(),
                    metadata: Metadata::default(),
                    transactions_digest: WorkersDigests::new(),
                    instance_id: committee.instance_id,
                };
                let data: Vec<u8> = bincode::serialize(&header).unwrap_or_else(|e| {
                    panic!(
                        "Serialization error: failed to create genesis block: {:?}",
                        e
                    )
                });
                Self {
                    digest: hash_vec(&data),
                    signatures: Vec::new(),
                    round: 0,
                    primary_id,
                    header,
                }
            })
            .collect()
    }

    pub fn debug_new(header: &BlockHeader, key_pairs: &[(NodeID, SecretKey)]) -> Self {
        let header_clone = header.clone();
        if let Ok(data) = bincode::serialize(&header) {
            let digest = hash_vec(&data);

            let signatures = key_pairs
                .iter()
                .map(|(id, secret_key)| (*id, Signature::new(&digest, secret_key)))
                .collect();
            Self {
                digest,
                signatures,
                round: header.round,
                primary_id: header.author,
                header: header_clone,
            }
        } else {
            panic!("Serilization failed for block {:?}", header);
        }
    }

    /// Verifies the certificate.
    pub fn check(&self, committee: &Committee) -> Result<(), DagError> {
        // Check the quorum.
        let mut weight = 0;
        let mut used_nodes = HashSet::new();
        for (id, _) in self.signatures.iter() {
            // Check that each node only appears once.
            dag_ensure!(
                !used_nodes.contains(id),
                DagError::CertificateAuthorityReuse
            );
            used_nodes.insert(*id);
            // Update weight.
            let voting_rights = committee.stake(id);
            dag_ensure!(voting_rights > 0, DagError::UnknownSigner);
            weight += voting_rights;
        }
        dag_ensure!(
            weight >= committee.quorum_threshold(),
            DagError::CertificateRequiresQuorum
        );
        // Check the signatures
        let digest = PartialCertificate::make_digest(&self.digest, self.round, self.primary_id);
        Signature::verify_batch(&digest, &self.signatures).map_err(DagError::from)
    }

    pub fn get_random_signer(&self) -> Option<NodeID> {
        if let Some(signature) = self.signatures.choose(&mut rand::thread_rng()) {
            Some(signature.0)
        } else {
            None
        }
    }
}
#[derive(Clone)]
pub struct PartialCertificatedHeaders {
    pub header: SignedBlockHeader,
    pub partial_certificate: Option<PartialCertificate>,
}

impl PartialCertificatedHeaders {
    pub fn new(header: SignedBlockHeader) -> Self {
        PartialCertificatedHeaders {
            header,
            partial_certificate: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialCertificate {
    pub author: NodeID,
    pub origin_node: NodeID,
    pub digest: Digest,
    pub round: RoundNumber,
    pub signature: Signature,
}

impl PartialEq for PartialCertificate {
    fn eq(&self, other: &PartialCertificate) -> bool {
        let mut ret = self.author == other.author;
        ret &= self.origin_node == other.origin_node;
        ret &= self.digest == other.digest;
        ret
    }
}

impl PartialCertificate {
    pub async fn new(
        author: NodeID,
        origin_node: NodeID,
        digest: Digest,
        round: RoundNumber,
        signing_channel: Sender<(Digest, oneshot::Sender<Signature>)>,
    ) -> Result<Self, DagError> {
        let (sender, receiver) = oneshot::channel();
        let tmp_digest = Self::make_digest(&digest, round, origin_node);
        signing_channel
            .send((tmp_digest, sender))
            .await
            .map_err(|e| DagError::ChannelError {
                error: format!("{}", e),
            })?;
        match receiver.await {
            Ok(signature) => Ok(Self {
                author,
                origin_node,
                digest,
                round,
                signature,
            }),
            Err(e) => dag_bail!(DagError::ChannelError {
                error: format!("{}", e)
            }),
        }
    }

    // Make a signed certificate directly with a secret key.
    // For debug / testing only.
    pub fn debug_new(
        author: NodeID,
        origin_node: NodeID,
        digest: Digest,
        round: RoundNumber,
        secret_key: &SecretKey,
    ) -> Self {
        let tmp_digest = Self::make_digest(&digest, round, origin_node);
        let signature = Signature::new(&tmp_digest, secret_key);
        Self {
            author,
            origin_node,
            digest,
            round,
            signature,
        }
    }

    pub fn make_digest(header_digest: &Digest, round: RoundNumber, id: NodeID) -> Digest {
        let mut h: Sha512 = Sha512::new();
        let mut hash: [u8; 64] = [0u8; 64];
        let mut digest: [u8; 32] = [0u8; 32];
        h.update(&header_digest.0);
        h.update(round.to_be_bytes());
        h.update(id.0);
        hash.copy_from_slice(h.finalize().as_slice());
        digest.copy_from_slice(&hash[..32]);
        Digest(digest)
    }

    // For debug / testing only.
    pub fn debug_check(&self) -> Result<(), DagError> {
        let digest = PartialCertificate::make_digest(&self.digest, self.round, self.origin_node);
        self.signature.verify(&digest, &self.author)?;
        Ok(())
    }
}

#[derive(Default)]
pub struct SignatureAggregator {
    weight: Stake,
    used_nodes: HashSet<NodeID>,
    pub digest: Digest,
    pub partial: Option<Certificate>,
}

impl SignatureAggregator {
    /// Start aggregating signatures for the given value into a certificate.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn init(
        &mut self,
        header: BlockHeader,
        header_digest: Digest,
        round: RoundNumber,
        primary_id: NodeID,
    ) {
        self.clear();
        self.digest = PartialCertificate::make_digest(&header_digest, round, primary_id);
        self.partial = Some(Certificate {
            digest: header_digest,
            signatures: Vec::new(),
            round,
            primary_id,
            header,
        });
    }

    pub fn clear(&mut self) {
        self.weight = 0;
        self.used_nodes.clear();
        self.digest = Digest::default();
        self.partial = None;
    }

    /// Try to append a signature to a (partial) certificate.
    pub fn append(
        &mut self,
        author: NodeID,
        signature: Signature,
        committee: &Committee,
    ) -> Result<Option<Certificate>, DagError> {
        dag_ensure!(
            self.partial.is_some(),
            DagError::UnexpectedOrLatePartialCertificate
        );

        // Check that each authority only appears once.
        dag_ensure!(
            !self.used_nodes.contains(&author),
            DagError::CertificateAuthorityReuse
        );

        signature.verify(&self.digest, &author)?;
        self.used_nodes.insert(author);

        // Update weight.
        let voting_rights = committee.stake(&author);
        dag_ensure!(voting_rights > 0, DagError::UnknownSigner);
        self.weight += voting_rights;

        // Update certificate.
        let partial = self.partial.as_mut().unwrap();
        partial.signatures.push((author, signature));
        if self.weight >= committee.quorum_threshold() {
            Ok(Some(partial.clone()))
        } else {
            Ok(None)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WorkerMessage {
    Batch {
        node_id: NodeID,
        transactions: Vec<Transaction>,
    },
    Flush(NodeID, Vec<u8>),
    Query(Vec<u8>),
    Synchronize(NodeID, Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum PrimaryMessage {
    Header(SignedBlockHeader),
    TxDigest(Digest, NodeID, ShardID),
    Cert(Certificate),
    PartialCert(PartialCertificate),
    SyncTxSend(SyncTxInfo, NodeID),
    CertifiedHeader(Vec<Certificate>, BlockHeader),
    SyncHeaderRequest(Digest, NodeID, NodeID),
    SyncHeaderReply((Certificate, BlockHeader), NodeID),
}

pub enum ConsensusMessage {
    Header(BlockHeader, Digest),
    SyncDone(RoundNumber),
    OrderedHeaders(Vec<(BlockHeader, Digest)>, RoundNumber),
}

pub enum SyncMessage {
    SyncUpToRound(RoundNumber, Vec<Digest>, RoundNumber),
}

pub struct SignatureFactory {
    secret_key: SecretKey,
    receiving_endpoint: Receiver<(Digest, oneshot::Sender<Signature>)>,
}

impl SignatureFactory {
    pub fn new(
        secret_key: SecretKey,
        receiving_endpoint: Receiver<(Digest, oneshot::Sender<Signature>)>,
    ) -> Self {
        Self {
            secret_key,
            receiving_endpoint,
        }
    }

    pub async fn start(&mut self) {
        while let Some(data) = self.receiving_endpoint.recv().await {
            let (digest, sender) = data;
            let signature = Signature::new(&digest, &self.secret_key);
            let _ = sender.send(signature);
        }
    }
}

#[derive(Debug)]
pub struct WorkerMessageCommand {
    pub message: WorkerMessage,
    pub response_channel: Option<oneshot::Sender<Option<WorkerMessage>>>,
    pub special_tags: Option<Vec<u64>>,
}

pub struct WorkerMessageResponse {
    pub response: oneshot::Receiver<Option<WorkerMessage>>,
}

impl WorkerMessageCommand {
    pub fn new(message: WorkerMessage) -> (WorkerMessageCommand, WorkerMessageResponse) {
        let (tx, rx) = oneshot::channel();
        let wc = WorkerMessageCommand {
            message,
            response_channel: Some(tx),
            special_tags: None,
        };
        let wr = WorkerMessageResponse { response: rx };
        (wc, wr)
    }

    pub fn get_message(&self) -> &WorkerMessage {
        &self.message
    }

    pub fn respond(&mut self, response: Option<WorkerMessage>) {
        if let Some(response_channel) = self.response_channel.take() {
            let _ = response_channel.send(response);
        }
    }
}

// Enssure that a worker message answer is always provided
impl Drop for WorkerMessageCommand {
    fn drop(&mut self) {
        self.respond(None);
    }
}

impl WorkerMessageResponse {
    // Waits for a response to a worker message to send back to the
    // requester.  This will always be triggered, possibly with a None,
    // when the corresponding worker message is droppepd.
    pub async fn get(self) -> Option<WorkerMessage> {
        match self.response.await {
            Ok(resp) => resp,
            Err(_e) => {
                //warn!("Error getting Worker Command response: {}", e);
                None
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockPrimaryRecord {
    pub digest: Digest,
    pub worker_id: NodeID,
    pub shard_id: ShardID,
}

impl BlockPrimaryRecord {
    pub fn new(digest: Digest, worker_id: NodeID, shard_id: ShardID) -> Self {
        BlockPrimaryRecord {
            digest,
            worker_id,
            shard_id,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self).expect("Error serializing")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeaderPrimaryRecord {
    pub header: BlockHeader,
    pub digest: Digest,
    pub passed_to_consensus: bool,
}

impl HeaderPrimaryRecord {
    pub fn new(header: BlockHeader, digest: Digest) -> Self {
        HeaderPrimaryRecord {
            header,
            digest,
            passed_to_consensus: false,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(&self).expect("Error serializing")
    }
}

pub fn hash_vec(data: &[u8]) -> Digest {
    let mut h: Sha512 = Sha512::new();
    let mut hash: [u8; 64] = [0u8; 64];
    let mut digest: [u8; 32] = [0u8; 32];
    h.update(data);
    hash.copy_from_slice(h.finalize().as_slice());
    digest.copy_from_slice(&hash[..32]);
    Digest(digest)
}
