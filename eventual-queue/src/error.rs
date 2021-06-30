//! Error types exposed by this crate.

use thiserror::Error;

/// A result type where the error variant is always a `LamportMutexError`.
pub type EQResult<T> = std::result::Result<T, EQError>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum EQError {
    #[error("{0}")]
    NotAllowedToReply(anyhow::Error),
    #[error("{0}")]
    InvalidOperation(anyhow::Error),
    #[error("{0}")]
    EQNetwork(anyhow::Error),
    #[error("{0}")]
    ConflictIndex(anyhow::Error),
    #[error("{0}")]
    DoubleDequeue(anyhow::Error),
}
