use crate::core::RoundNumber;
use dag_core::error::DagError;
use dag_core::store::StoreError;
use dag_core::types::*;
use ed25519_dalek::ed25519;
use thiserror::Error;

#[macro_export]
macro_rules! bail {
    ($e:expr) => {
        return Err($e);
    };
}

#[macro_export(local_inner_macros)]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            bail!($e);
        }
    };
}

pub type ConsensusResult<T> = Result<T, ConsensusError>;

#[derive(Error, Debug)]
pub enum ConsensusError {
    #[error("Failed to read config file '{file}': {message}")]
    ReadError { file: String, message: String },

    #[error("Failed to write config file '{file}': {message}")]
    WriteError { file: String, message: String },

    #[error("Dag error: {0}")]
    DagError(#[from] DagError),

    #[error("Network error: {0}")]
    NetworkError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Store error: {0}")]
    StoreError(StoreError),

    #[error("Node {0} is not in the committee")]
    NotInCommittee(NodeID),

    #[error("Invalid signature")]
    InvalidSignature(#[from] ed25519::Error),

    #[error("Received more than one vote from {0}")]
    AuthorityReuse(NodeID),

    #[error("Received vote from unknown authority {0}")]
    UnknownAuthority(NodeID),

    #[error("Received QC without a quorum")]
    QCRequiresQuorum,

    #[error("Received TC without a quorum")]
    TCRequiresQuorum,

    #[error("Malformed block {0}")]
    MalformedBlock(Digest),

    #[error("Received block {digest} from leader {leader} at round {round}")]
    WrongLeader {
        digest: Digest,
        leader: NodeID,
        round: RoundNumber,
    },

    #[error("Invalid payload")]
    InvalidPayload,
}

impl From<StoreError> for ConsensusError {
    fn from(e: StoreError) -> Self {
        ConsensusError::StoreError(e)
    }
}
