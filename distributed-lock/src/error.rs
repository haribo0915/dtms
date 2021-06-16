//! Error types exposed by this crate.

use thiserror::Error;

/// A result type where the error variant is always a `LamportMutexError`.
pub type LamportMutexResult<T> = std::result::Result<T, LamportMutexError>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum LamportMutexError {
    #[error("{0}")]
    NotAllowedToRequest(anyhow::Error),
    #[error("{0}")]
    NotAllowedToReply(anyhow::Error),
    #[error("{0}")]
    NotAllowedToRelease(anyhow::Error),
    #[error("{0}")]
    InvalidOperation(anyhow::Error),
    #[error("{0}")]
    LamportMutexNetwork(anyhow::Error),
}
