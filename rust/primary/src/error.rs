use crypto::{CryptoError, Digest, PublicKey};
use rocksdb;
use store::StoreError;
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

// OLD macros

#[macro_export]
macro_rules! dag_bail {
    ($e:expr) => {
        return Err($e);
    };
}

#[macro_export(local_inner_macros)]
macro_rules! dag_ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            dag_bail!($e);
        }
    };
}

pub type DagResult<T> = Result<T, DagError>;

/// Custom error type.
#[derive(Debug, Error)]
pub enum DagError {
    #[error("Invalid signature")]
    InvalidSignature(#[from] CryptoError),

    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("Invalid header id")]
    InvalidHeaderId,

    #[error("Malformed header {0}")]
    MalformedHeader(Digest),

    #[error("Received message from unknown authority {0}")]
    UnknownAuthority(PublicKey),

    #[error("Received more than one vote from {0}")]
    AuthorityReuse(PublicKey),

    #[error("Received unexpected vote fo header {0}")]
    UnexpectedVote(Digest),

    #[error("Received certificate without a quorum")]
    CertificateRequiresQuorum,

    #[error("Parents of header {0} are not a quorum")]
    HeaderRequiresQuorum(Digest),

    // OLD
    #[error("The digest provided is different from the digest computed.DagError")]
    BadDigest,

    #[error("This block has not been delivered to consensus")]
    NoBlockAvailable,

    #[error("Value was not signed by a known authority")]
    UnknownSigner,

    #[error("Signatures in a certificate must be from different authorities")]
    CertificateAuthorityReuse,

    #[error("We received a partial certificate from ourselves?")]
    CertificateAuthorityOurselves,

    // Signatures in a certificate must form a quorum
    //CertificateRequiresQuorum,

    // Block integrity checks.
    #[error("Block must contain 2f+1 references to parents blocks")]
    BadNumberOfParents,

    // Internal Serialization
    // Header or certificate internal serialization failed
    //SerializationError(bincode::ErrorKind),
    #[error("Wrong instance Id")]
    BadInstanceId,

    #[error("We received a late or unexpected partial certificate")]
    UnexpectedOrLatePartialCertificate,

    // Storage
    #[error("Storage error: {error}")]
    StorageFailure { error: String },

    // Channels
    #[error("Channel error: {error}")]
    ChannelError { error: String },

    // Network
    #[error("Network error: {error}")]
    NetworkError { error: String },

    #[error("Unknown node name")]
    UnknownNode,

    #[error("Skip Message Round")]
    ChannelClosed,

    #[error("Received conflicting blockheader")]
    ConflictingHeader,

    #[error("parents are not from previous round or has not enough stake")]
    WrongParents,

    #[error("Message Handle is dropped and message will not be sent")]
    MessageHandleDropped,

    // Something is seriously wrong with our internal logic
    #[error("Internal error: {error}")]
    InternalError { error: String },

    #[error("The blockheader is not consistent with the provided certificate")]
    BadHeaderDigest,

    // Invalid signature
    //InvalidSignature(#[from] CryptoError),
    #[error("Store error")]
    RocksdbError(rocksdb::Error),
}

impl From<rocksdb::Error> for DagError {
    fn from(e: rocksdb::Error) -> Self {
        DagError::RocksdbError(e)
    }
}
