//! Upstream Exfer-node JSON-RPC client.
//!
//! Multi-URL round-robin + transient-error retry, modeled on
//! `exfer-walletd::upstream`. Read-only — the indexer never calls
//! `send_raw_transaction` or anything that mutates node state.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{Error, Result};

/// Default number of attempts before surfacing a transport error to
/// the caller. RPC-body errors (`error.code` set in the response) are
/// returned on the first attempt — those are deterministic failures.
const DEFAULT_RETRY_ATTEMPTS: u32 = 4;

/// Linear backoff between retries (ms). Multiplied by attempt number,
/// so the schedule is 0ms → 500ms → 1000ms → 1500ms.
const DEFAULT_BACKOFF_MS: u64 = 500;

/// Configured node client. Holds one reqwest [`Client`] (HTTP/2
/// connection pool, keep-alive enabled by default) plus the parsed
/// upstream URL list. Round-robin starting index advances atomically
/// per request so concurrent requests share load.
#[derive(Clone)]
pub struct NodeClient {
    http: Client,
    urls: Arc<Vec<String>>,
    next: Arc<AtomicUsize>,
    attempts: u32,
    backoff_ms: u64,
}

impl NodeClient {
    /// `node_rpc` is a comma-separated list of fully-qualified URLs
    /// (e.g. `"http://a:9334,http://b:9334"`). Empty list → error.
    pub fn new(node_rpc: &str, timeout: Duration) -> Result<Self> {
        let urls: Vec<String> = node_rpc
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if urls.is_empty() {
            return Err(Error::Internal(
                "node_rpc: at least one URL required".into(),
            ));
        }
        let http = Client::builder()
            .timeout(timeout)
            .user_agent(concat!("exfer-indexer/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| Error::Internal(format!("reqwest build: {e}")))?;
        Ok(Self {
            http,
            urls: Arc::new(urls),
            next: Arc::new(AtomicUsize::new(0)),
            attempts: DEFAULT_RETRY_ATTEMPTS,
            backoff_ms: DEFAULT_BACKOFF_MS,
        })
    }

    /// Configure retry behaviour. Mostly used in tests that want
    /// fail-fast behaviour against a mock server.
    pub fn with_retry(mut self, attempts: u32, backoff_ms: u64) -> Self {
        self.attempts = attempts.max(1);
        self.backoff_ms = backoff_ms;
        self
    }

    fn next_url(&self) -> &str {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.urls.len();
        &self.urls[i]
    }

    /// Generic JSON-RPC call. Tries each URL in round-robin order; on
    /// transport errors retries up to `attempts` times. Surfaces
    /// node-side RPC errors (non-null `error` field) on first sight.
    pub async fn call(&self, method: &str, params: Value) -> Result<Value> {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1,
        });

        let mut last_transport_err: Option<String> = None;
        for attempt in 0..self.attempts {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(
                    self.backoff_ms.saturating_mul(attempt as u64),
                ))
                .await;
            }
            let url = self.next_url();
            match self.http.post(url).json(&body).send().await {
                Ok(resp) => {
                    let v: Value = match resp.json().await {
                        Ok(v) => v,
                        Err(e) => {
                            last_transport_err = Some(format!("decode: {e}"));
                            continue;
                        }
                    };
                    if let Some(err) = v.get("error") {
                        let code = err
                            .get("code")
                            .and_then(|c| c.as_i64())
                            .unwrap_or(-32603) as i32;
                        let message = err
                            .get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("unspecified upstream error")
                            .to_string();
                        return Err(Error::UpstreamRpc { code, message });
                    }
                    if let Some(result) = v.get("result") {
                        return Ok(result.clone());
                    }
                    return Err(Error::UpstreamRpc {
                        code: -32603,
                        message: "missing result field".into(),
                    });
                }
                Err(e) => {
                    last_transport_err = Some(e.to_string());
                    continue;
                }
            }
        }
        Err(Error::UpstreamUnreachable(
            last_transport_err.unwrap_or_else(|| "all attempts exhausted".into()),
        ))
    }

    pub async fn get_block_height(&self) -> Result<TipResponse> {
        let v = self.call("get_block_height", serde_json::json!({})).await?;
        serde_json::from_value(v).map_err(|e| {
            Error::Internal(format!("get_block_height: decode: {e}"))
        })
    }

    pub async fn get_block_by_height(&self, height: u64) -> Result<BlockSummary> {
        let v = self
            .call("get_block", serde_json::json!({ "height": height }))
            .await?;
        serde_json::from_value(v).map_err(|e| {
            Error::Internal(format!("get_block_by_height: decode: {e}"))
        })
    }

    pub async fn get_transaction(&self, tx_id_hex: &str) -> Result<TxStatus> {
        let v = self
            .call("get_transaction", serde_json::json!({ "hash": tx_id_hex }))
            .await?;
        serde_json::from_value(v).map_err(|e| {
            Error::Internal(format!("get_transaction: decode: {e}"))
        })
    }

    /// Node-side spent-by lookup (added by the workflow B node PR).
    /// If the node doesn't expose this RPC yet, surfaces
    /// `UpstreamRpc { code: -32601, .. }` so callers can fall back
    /// to scanning.
    pub async fn get_output_spent_by(
        &self,
        tx_id_hex: &str,
        output_index: u32,
    ) -> Result<SpentByResponse> {
        let v = self
            .call(
                "get_output_spent_by",
                serde_json::json!({ "tx_id": tx_id_hex, "output_index": output_index }),
            )
            .await?;
        serde_json::from_value(v).map_err(|e| {
            Error::Internal(format!("get_output_spent_by: decode: {e}"))
        })
    }
}

// ---------------------------------------------------------------------------
// Response shapes
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TipResponse {
    pub height: u64,
    pub block_id: String,
}

/// Mirrors `exfer::rpc::handle_get_block`'s JSON output. Field names
/// match the node's wire surface, not walletd's normalized ones —
/// the indexer reads raw node bytes directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockSummary {
    #[serde(rename = "hash")]
    pub block_id: String,
    pub height: u64,
    pub timestamp: u64,
    pub tx_count: u64,
    pub transactions: Vec<String>,
    pub prev_block_id: String,
    #[serde(default)]
    pub difficulty_target: String,
    #[serde(default)]
    pub nonce: u64,
    #[serde(default)]
    pub state_root: String,
    #[serde(default)]
    pub tx_root: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxStatus {
    pub tx_id: String,
    pub tx_hex: String,
    #[serde(default)]
    pub in_mempool: bool,
    #[serde(default, rename = "block_hash")]
    pub block_id: Option<String>,
    #[serde(default)]
    pub block_height: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SpentByResponse {
    Spent {
        spent: bool, // always true on this variant
        spending_tx_id: String,
        input_index: u32,
        block_height: u64,
    },
    Unspent {
        spent: bool, // always false
    },
}
