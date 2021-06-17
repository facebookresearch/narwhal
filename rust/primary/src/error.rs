use crypto::CryptoError;
use displaydoc::Display;
use rocksdb;
use thiserror::Error;

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
#[derive(Debug, Display, Error)]
pub enum DagError {
    /// The digest provided is different from the digest computed.DagError
    BadDigest,

    /// This block has not been delivered to consensus
    NoBlockAvailable,

    /// Value was not signed by a known authority
    UnknownSigner,

    /// Signatures in a certificate must be from different authorities
    CertificateAuthorityReuse,

    /// We received a partial certificate from ourselves?
    CertificateAuthorityOurselves,

    /// Signatures in a certificate must form a quorum
    CertificateRequiresQuorum,

    // Block integrity checks.
    /// Block must contain 2f+1 references to parents blocks
    BadNumberOfParents,

    // Internal Serialization
    /// Header or certificate internal serialization failed
    SerializationError(bincode::ErrorKind),

    /// Wrong instance Id
    BadInstanceId,

    /// We received a late or unexpected partial certificate
    UnexpectedOrLatePartialCertificate,

    // Storage
    /// Storage error: {error}
    StorageFailure { error: String },

    // Channels
    /// Channel error: {error}
    ChannelError { error: String },

    // Network
    /// Network error: {error}
    NetworkError { error: String },

    /// Unknown node name
    UnknownNode,

    /// Skip Message Round
    ChannelClosed,

    /// Received conflicting blockheader
    ConflictingHeader,

    ///parents are not from previous round or has not enough stake
    WrongParents,

    /// Message Handle is dropped and message will not be sent
    MessageHandleDropped,

    // Something is seriously wrong with our internal logic
    /// Internal error: {error}
    InternalError { error: String },

    ///The blockheader is not consistent with the provided certificate
    BadHeaderDigest,

    /// Invalid signature
    InvalidSignature(#[from] CryptoError),

    /// Store error
    RocksdbError(rocksdb::Error),
}

impl From<Box<bincode::ErrorKind>> for DagError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        DagError::SerializationError(*e)
    }
}

impl From<rocksdb::Error> for DagError {
    fn from(e: rocksdb::Error) -> Self {
        DagError::RocksdbError(e)
    }
}
