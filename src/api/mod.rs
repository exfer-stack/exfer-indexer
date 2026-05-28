//! JSON-RPC dispatch.
//!
//! Method handlers themselves arrive in commit #14. The scaffold
//! commit nails down the dispatcher signature so the HTTP shell can
//! be wired up first.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::watch;

use crate::db::Db;
use crate::error::{Error, Result};
use crate::upstream::NodeClient;

#[derive(Clone)]
pub struct ApiState {
    pub db: Arc<Db>,
    pub node: NodeClient,
    pub tip_rx: watch::Receiver<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Value,
    pub id: Option<Value>,
}

pub async fn dispatch(_state: &ApiState, req: RpcRequest) -> Result<Value> {
    match req.method.as_str() {
        "ping" => Ok(serde_json::json!({ "ok": true })),
        // Method handlers (list_settlements, contract_stats,
        // get_address_history, htlc_lookup_by_hashlock,
        // get_output_spent_by, htlc_status, htlc_list,
        // get_indexer_status) — added in commit #14.
        unknown => Err(Error::UnknownMethod(unknown.to_string())),
    }
}
