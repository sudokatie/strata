//! Error types for Strata.

use thiserror::Error;

/// Result type for Strata operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Strata error types.
#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Corruption: {0}")]
    Corruption(String),

    #[error("Key not found")]
    NotFound,

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Checksum mismatch")]
    ChecksumMismatch,

    #[error("Invalid record")]
    InvalidRecord,

    #[error("Database closed")]
    Closed,
}
