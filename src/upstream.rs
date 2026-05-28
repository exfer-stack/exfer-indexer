//! Upstream Exfer-node JSON-RPC client.
//!
//! Implementation lands in commit #13 alongside the follower. The
//! scaffold version is a thin shell — just enough type structure for
//! the rest of the crate to compile.

use std::time::Duration;

use crate::error::Result;

/// Configured node client. Multi-URL round-robin + retry policy live
/// in the follower commit; the scaffold keeps a single URL.
#[derive(Clone)]
pub struct NodeClient {
    pub urls: Vec<String>,
    pub timeout: Duration,
}

impl NodeClient {
    pub fn new(node_rpc: &str, timeout: Duration) -> Result<Self> {
        let urls = node_rpc
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        Ok(Self { urls, timeout })
    }
}
