//! Error types exposed by this crate.

use thiserror::Error;

/// A result type where the error variant is always a `LamportMutexError`.
pub type CRDTResult<T> = std::result::Result<T, CRDTError>;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CRDTError {
    #[error("{0}")]
    InvalidOperation(anyhow::Error),
    #[error("{0}")]
    CRDTNetwork(anyhow::Error),
}
