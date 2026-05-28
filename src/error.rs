//! Crate-wide error type.
//!
//! Mirrors `exfer-walletd::error::Error`'s code allocation so a
//! JSON-RPC client can switch between the two services without
//! changing error-handling code. The HTTP layer maps each variant to
//! a JSON-RPC `code` so clients branch on the integer rather than the
//! message string.

use serde_json::{json, Value};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    /// JSON syntax — body could not be parsed.
    #[error("parse error: {0}")]
    ParseError(String),

    /// Envelope-level validation (bad jsonrpc, missing method, …).
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// Per-method `params` shape error.
    #[error("invalid params: {0}")]
    BadParams(String),

    #[error("invalid hex: {0}")]
    BadHex(String),

    #[error("invalid address: expected 32 bytes (64 hex chars), got {0} bytes")]
    BadAddressLen(usize),

    #[error("unknown method: {0}")]
    UnknownMethod(String),

    /// Authentication required / failed.
    #[error("authentication required")]
    Unauthorized,

    /// Indexer has not yet observed the requested height — caller can
    /// retry once the follower catches up.
    #[error("requested data not yet indexed (follower at height {follower_height}, asked for {asked})")]
    NotYetIndexed {
        follower_height: u64,
        asked: u64,
    },

    /// Upstream node unreachable / timing out / returning RPC errors.
    #[error("upstream node unreachable: {0}")]
    UpstreamUnreachable(String),

    #[error("upstream node returned error code {code}: {message}")]
    UpstreamRpc { code: i32, message: String },

    /// Local storage layer error (redb / serialization).
    #[error("storage error: {0}")]
    Storage(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    pub fn rpc_code(&self) -> i32 {
        match self {
            Error::ParseError(_) => -32700,
            Error::InvalidRequest(_) => -32600,
            Error::UnknownMethod(_) => -32601,
            Error::BadParams(_) | Error::BadHex(_) | Error::BadAddressLen(_) => -32602,
            Error::Unauthorized => -32001,
            Error::UpstreamUnreachable(_) | Error::UpstreamRpc { .. } => -32020,
            Error::NotYetIndexed { .. } => -32002,
            Error::Storage(_) | Error::Internal(_) => -32603,
        }
    }

    pub fn rpc_data(&self) -> Option<Value> {
        match self {
            Error::NotYetIndexed {
                follower_height,
                asked,
            } => Some(json!({
                "follower_height": follower_height,
                "asked": asked,
            })),
            _ => None,
        }
    }
}

impl From<redb::DatabaseError> for Error {
    fn from(e: redb::DatabaseError) -> Self {
        Error::Storage(e.to_string())
    }
}
impl From<redb::TransactionError> for Error {
    fn from(e: redb::TransactionError) -> Self {
        Error::Storage(e.to_string())
    }
}
impl From<redb::TableError> for Error {
    fn from(e: redb::TableError) -> Self {
        Error::Storage(e.to_string())
    }
}
impl From<redb::StorageError> for Error {
    fn from(e: redb::StorageError) -> Self {
        Error::Storage(e.to_string())
    }
}
impl From<redb::CommitError> for Error {
    fn from(e: redb::CommitError) -> Self {
        Error::Storage(e.to_string())
    }
}
