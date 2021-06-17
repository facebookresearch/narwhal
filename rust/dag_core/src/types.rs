use super::committee::*;
use super::error::*;
use super::messages::*;
use ed25519_dalek as dalek;
use ed25519_dalek::ed25519::signature::Signature as DalekSignature;
use ed25519_dalek::Digest as DalekDigest;
use ed25519_dalek::Sha512;
use ed25519_dalek::Signer;
use rand::rngs::OsRng;
use rand::seq::SliceRandom;
use rand::{CryptoRng, Rng};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::convert::TryInto;
use std::fmt;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::oneshot;

// #[cfg(test)]
// #[path = "tests/types_tests.rs"]
// pub mod types_tests;

pub type NodeID = EdPublicKeyBytes;
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

#[derive(Default, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Serialize, Deserialize)]
pub struct Digest(pub [u8; 32]);

pub trait Digestible {
    fn digest(&self) -> Digest;
}
impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let s = base64::encode(&self.0);
        write!(f, "{}", s)?;
        Ok(())
    }
}

impl std::fmt::Debug for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        let s = base64::encode(&self.0);
        write!(f, "{}", s)?;
        Ok(())
    }
}

//Does not work, throws an error
pub fn make_u64_digest(round: RoundNumber) -> usize {
    let mut h: Sha512 = Sha512::new();
    h.update(round.to_be_bytes());
    let output = u32::from_be_bytes(
        h.finalize()
            .as_slice()
            .try_into()
            .expect("slice with incorrect length"),
    );
    output as usize
}

// Ensure Secrets are not copyable and movable to control where they are in memory
pub struct SecretKey([u8; dalek::KEYPAIR_LENGTH]);

impl SecretKey {
    pub fn from_base64(s: &str) -> Result<Self, failure::Error> {
        let value = base64::decode(s)?;
        let mut key = [0u8; dalek::KEYPAIR_LENGTH];
        key.copy_from_slice(&value[..dalek::KEYPAIR_LENGTH]);
        Ok(Self(key))
    }

    pub fn encode_base64(&self) -> String {
        base64::encode(&self.0[..])
    }

    // Only for debug / testing.
    pub fn duplicate(&self) -> Self {
        SecretKey(self.0)
    }
}

// Zero the secret key when unallocating.
impl Drop for SecretKey {
    fn drop(&mut self) {
        for i in 0..dalek::KEYPAIR_LENGTH {
            self.0[i] = 0;
        }
    }
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Hash, Default)]
pub struct EdPublicKeyBytes(pub [u8; dalek::PUBLIC_KEY_LENGTH]);

impl fmt::Display for EdPublicKeyBytes {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let s = base64::encode(&self.0[..]);
        write!(f, "{}", s)?;
        Ok(())
    }
}

impl EdPublicKeyBytes {
    pub fn from_base64(s: &str) -> Result<Self, failure::Error> {
        let value = base64::decode(s)?;
        let mut key = [0u8; dalek::PUBLIC_KEY_LENGTH];
        key.copy_from_slice(&value[..dalek::PUBLIC_KEY_LENGTH]);
        Ok(Self(key))
    }

    pub fn encode_base64(&self) -> String {
        base64::encode(&self.0[..])
    }
}

impl std::fmt::Debug for EdPublicKeyBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "{}", self.encode_base64())?;
        Ok(())
    }
}

impl Serialize for EdPublicKeyBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(&self.encode_base64())
    }
}

impl<'de> Deserialize<'de> for EdPublicKeyBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let value =
            Self::from_base64(&s).map_err(|err| serde::de::Error::custom(err.to_string()))?;
        Ok(value)
    }
}

/// The key generator to use in production.
pub fn get_production_keypair() -> (NodeID, SecretKey) {
    get_keypair(&mut OsRng)
}

/// Generic key generator, useful for testing with fixed-seed rng.
pub fn get_keypair<R>(csprng: &mut R) -> (NodeID, SecretKey)
where
    R: CryptoRng + Rng,
{
    let keypair = dalek::Keypair::generate(csprng);
    (
        EdPublicKeyBytes(keypair.public.to_bytes()),
        SecretKey(keypair.to_bytes()),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Signature {
    pub part1: [u8; dalek::SIGNATURE_LENGTH / 2],
    pub part2: [u8; dalek::SIGNATURE_LENGTH / 2],
}

impl Signature {
    /// Make a new signature.
    pub fn new(digest: &Digest, secret: &SecretKey) -> Self {
        let key_pair = dalek::Keypair::from_bytes(&secret.0).unwrap();
        let signature = key_pair.sign(&digest.0);
        let sig_bytes = signature.to_bytes();

        let mut part1 = [0; 32];
        let mut part2 = [0; 32];
        part1.clone_from_slice(&sig_bytes[..32]);
        part2.clone_from_slice(&sig_bytes[32..64]);

        Signature { part1, part2 }
    }

    /// Verifies the signature.
    pub fn check(&self, digest: &Digest, author: NodeID) -> Result<(), DagError> {
        self.check_internal(digest, author)
            .map_err(|error| DagError::InvalidSignature {
                error: format!("{}", error),
            })
    }

    fn check_internal(&self, digest: &Digest, author: NodeID) -> Result<(), dalek::SignatureError> {
        let public_key = dalek::PublicKey::from_bytes(&author.0)?;
        let sig = self.to_array();
        let dalek_sig = DalekSignature::from_bytes(&sig)?;
        public_key.verify_strict(&digest.0, &dalek_sig)
    }

    fn to_array(&self) -> [u8; 64] {
        let mut sig: [u8; 64] = [0; 64];
        sig[..32].clone_from_slice(&self.part1);
        sig[32..64].clone_from_slice(&self.part2);
        sig
    }

    pub fn verify_batch<'a, I>(digest: &'a Digest, votes: I) -> Result<(), DagError>
    where
        I: IntoIterator<Item = &'a (NodeID, Signature)>,
    {
        Signature::verify_batch_internal(digest, votes).map_err(|error| {
            DagError::InvalidSignature {
                error: format!("{}", error),
            }
        })
    }

    fn verify_batch_internal<'a, I>(
        digest: &'a Digest,
        votes: I,
    ) -> Result<(), dalek::SignatureError>
    where
        I: IntoIterator<Item = &'a (NodeID, Signature)>,
    {
        let msg: &[u8] = &digest.0;
        let mut messages: Vec<&[u8]> = Vec::new();
        let mut signatures: Vec<dalek::Signature> = Vec::new();
        let mut public_keys: Vec<dalek::PublicKey> = Vec::new();
        for (id, sig) in votes.into_iter() {
            messages.push(msg);
            signatures.push(dalek::Signature::from_bytes(&sig.to_array())?);
            public_keys.push(dalek::PublicKey::from_bytes(&id.0)?);
        }
        dalek::verify_batch(&messages[..], &signatures[..], &public_keys[..])
    }
}

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
                    digest: data.digest(),
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
            let digest = data.digest();

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
        let digest = PartialCertificate::make_digest(self.digest, self.round, self.primary_id);
        Signature::verify_batch(&digest, &self.signatures)
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
        let tmp_digest = Self::make_digest(digest, round, origin_node);
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
        let tmp_digest = Self::make_digest(digest, round, origin_node);
        let signature = Signature::new(&tmp_digest, secret_key);
        Self {
            author,
            origin_node,
            digest,
            round,
            signature,
        }
    }

    pub fn make_digest(header_digest: Digest, round: RoundNumber, id: NodeID) -> Digest {
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
        let digest = PartialCertificate::make_digest(self.digest, self.round, self.origin_node);
        self.signature.check(&digest, self.author)?;
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
        self.digest = PartialCertificate::make_digest(header_digest, round, primary_id);
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

        signature.check(&self.digest, author)?;
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
        transactions: VecDeque<Transaction>,
        special: usize,
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
    SyncCertRequest(
        (Digest, RoundNumber),
        /* from */ NodeID,
        /* to */ NodeID,
    ),
    SyncCertReply(Certificate, NodeID),
    CertifiedHeader(Vec<Certificate>, BlockHeader),
    SyncHeaderRequest(Digest, NodeID, NodeID),
    SyncHeaderReply((Certificate, BlockHeader), NodeID),
}
#[derive(Debug)]
pub enum PayloadStatus {
    Accept,
    Reject,
    Wait(Vec<Vec<u8>>),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ConsensusMessage {
    Header(Certificate),
    SyncDone(RoundNumber),
    OrderedHeaders(Vec<(BlockHeader, Digest)>, RoundNumber),
    Cleanup(Certificate),
}

#[derive(Debug)]
pub enum MempoolMessage {
    GetPayload(oneshot::Sender<Vec<Vec<u8>>>),
    VerifyPayload(Vec<Vec<u8>>, oneshot::Sender<PayloadStatus>),
    CommitedCertificate(Vec<Vec<u8>>),
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
