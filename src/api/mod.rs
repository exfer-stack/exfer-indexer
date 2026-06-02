//! JSON-RPC dispatch + handlers.
//!
//! The eleven read-only methods that the indexer exposes for any
//! address on the chain. Each handler shape mirrors what `exfer-
//! walletd` exposes for owned-key queries; consumers can route their
//! questions to whichever service can answer (walletd for own, indexer
//! for everyone else).
//!
//! Method | Scope | Description
//! -------|-------|-------------
//! `ping` | read | health check
//! `get_indexer_status` | read | follower lag + counters
//! `htlc_status` | read | one HTLC by lock outpoint
//! `htlc_list` | read | paginated HTLCs by filter
//! `htlc_lookup_by_hashlock` | read | reverse lookup by hashlock
//! `list_settlements` | read | settlement history of an address
//! `contract_stats` | read | aggregated stats per contract type
//! `get_address_history` | read | address activity timeline
//! `get_output_spent_by` | read | reverse-spend lookup (local cache + node fallback)
//! `get_attestation_edges` | read | per-counterparty reputation edges for an address
//! `detect_in_chain_swaps` | read | hashlock-collision groups (atomic-swap fingerprint)
//! `get_contract_template` | read | template registry lookup by contract_hash

use std::sync::Arc;

use exfer::covenants::htlc::{HtlcRecord, HtlcRole, HtlcState};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::watch;

use crate::db::{
    AddressHistoryRow, AttestationEdge, ContractStats, Cursor, Db, HistoryCursor, HtlcFilter,
    SettlementCursor, SharedHashlockGroup,
};
use crate::error::{Error, Result};
use crate::extract::SettlementRecord;
use crate::templates;
use crate::upstream::{NodeClient, SpentByResponse};

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

pub async fn dispatch(state: &ApiState, req: RpcRequest) -> Result<Value> {
    match req.method.as_str() {
        "ping" => Ok(serde_json::json!({ "ok": true })),
        "get_indexer_status" => get_indexer_status(state).await,

        "htlc_status" => htlc_status(state, req.params).await,
        "htlc_list" => htlc_list(state, req.params).await,
        "htlc_lookup_by_hashlock" => htlc_lookup_by_hashlock(state, req.params).await,

        "list_settlements" => list_settlements(state, req.params).await,
        "contract_stats" => contract_stats(state, req.params).await,
        "get_address_history" => get_address_history(state, req.params).await,

        "get_output_spent_by" => get_output_spent_by(state, req.params).await,

        "get_attestation_edges" => get_attestation_edges(state, req.params).await,
        "detect_in_chain_swaps" => detect_in_chain_swaps(state, req.params).await,
        "get_contract_template" => get_contract_template(req.params).await,
        "resolve_name" => resolve_name(state, req.params).await,

        unknown => Err(Error::UnknownMethod(unknown.to_string())),
    }
}

// ---------------------------------------------------------------------------
// get_indexer_status
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct IndexerStatus {
    last_indexed_height: u64,
    last_indexed_block_id: String,
    tip_height: u64,
    lag: i64,
    indexed_htlc_count: u64,
    full_scan_complete: bool,
    started_at: u64,
}

async fn get_indexer_status(state: &ApiState) -> Result<Value> {
    let tip = state.node.get_block_height().await?;
    let db = state.db.clone();
    let (meta, count) = tokio::task::spawn_blocking(move || {
        let meta = db.load_meta()?;
        let count = db.htlc_count()?;
        Ok::<_, Error>((meta, count))
    })
    .await
    .map_err(|e| Error::Internal(format!("blocking task panicked: {e}")))??;

    let resp = IndexerStatus {
        last_indexed_height: meta.last_indexed_height,
        last_indexed_block_id: hex::encode(meta.last_indexed_block_id),
        tip_height: tip.height,
        lag: tip.height as i64 - meta.last_indexed_height as i64,
        indexed_htlc_count: count,
        full_scan_complete: meta.full_scan_complete,
        started_at: meta.started_at,
    };
    serde_json::to_value(resp).map_err(|e| Error::Internal(e.to_string()))
}

// ---------------------------------------------------------------------------
// htlc_status
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct HtlcStatusParams {
    lock_tx_id: String,
    #[serde(default)]
    output_index: u32,
}

async fn htlc_status(state: &ApiState, params: Value) -> Result<Value> {
    let p: HtlcStatusParams = serde_json::from_value(params)
        .map_err(|e| Error::BadParams(format!("htlc_status: {e}")))?;
    let tx_id = decode_hex32(&p.lock_tx_id)?;
    let db = state.db.clone();
    let oi = p.output_index;
    let rec = tokio::task::spawn_blocking(move || db.get_htlc(&tx_id, oi))
        .await
        .map_err(|e| Error::Internal(format!("blocking task panicked: {e}")))??;
    match rec {
        Some(r) => serde_json::to_value(r).map_err(|e| Error::Internal(e.to_string())),
        None => Err(Error::BadParams(format!(
            "no indexed HTLC at ({}, {})",
            p.lock_tx_id, p.output_index
        ))),
    }
}

// ---------------------------------------------------------------------------
// htlc_list
// ---------------------------------------------------------------------------

const HTLC_LIST_DEFAULT_LIMIT: u32 = 100;
const HTLC_LIST_MAX_LIMIT: u32 = 1000;

#[derive(Debug, Deserialize)]
struct HtlcListParams {
    #[serde(default)]
    role: Option<HtlcRole>,
    #[serde(default)]
    state: Option<HtlcStateFilter>,
    #[serde(default)]
    address: Option<String>,
    #[serde(default)]
    since_height: Option<u64>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum HtlcStateFilter {
    One(HtlcState),
    Many(Vec<HtlcState>),
}

#[derive(Serialize)]
struct HtlcListResponse {
    htlcs: Vec<HtlcRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

async fn htlc_list(state: &ApiState, params: Value) -> Result<Value> {
    let p: HtlcListParams = if params.is_null() {
        HtlcListParams {
            role: None,
            state: None,
            address: None,
            since_height: None,
            limit: None,
            cursor: None,
        }
    } else {
        serde_json::from_value(params).map_err(|e| Error::BadParams(format!("htlc_list: {e}")))?
    };
    let limit = p
        .limit
        .unwrap_or(HTLC_LIST_DEFAULT_LIMIT)
        .min(HTLC_LIST_MAX_LIMIT) as usize;
    let states = match p.state {
        Some(HtlcStateFilter::One(s)) => vec![s],
        Some(HtlcStateFilter::Many(v)) => v,
        None => Vec::new(),
    };
    let address = match p.address.as_deref() {
        Some(s) => Some(decode_hex32(s)?),
        None => None,
    };
    let cursor = match p.cursor.as_deref() {
        Some(s) => Some(Cursor::decode(s)?),
        None => None,
    };
    let filter = HtlcFilter {
        role: p.role,
        states,
        address,
        since_height: p.since_height,
    };

    let db = state.db.clone();
    let (rows, next) = tokio::task::spawn_blocking(move || db.list_htlcs(&filter, limit, cursor))
        .await
        .map_err(|e| Error::Internal(format!("blocking task panicked: {e}")))??;
    let resp = HtlcListResponse {
        htlcs: rows,
        next_cursor: next.map(|c| c.encode()),
    };
    serde_json::to_value(resp).map_err(|e| Error::Internal(e.to_string()))
}

// ---------------------------------------------------------------------------
// htlc_lookup_by_hashlock
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct HtlcLookupParams {
    hash_lock: String,
}

#[derive(Serialize)]
struct HtlcLookupResponse {
    htlcs: Vec<HtlcRecord>,
}

async fn htlc_lookup_by_hashlock(state: &ApiState, params: Value) -> Result<Value> {
    let p: HtlcLookupParams = serde_json::from_value(params)
        .map_err(|e| Error::BadParams(format!("htlc_lookup_by_hashlock: {e}")))?;
    let h = decode_hex32(&p.hash_lock)?;
    let db = state.db.clone();
    let rows = tokio::task::spawn_blocking(move || db.lookup_by_hashlock(&h))
        .await
        .map_err(|e| Error::Internal(format!("blocking task panicked: {e}")))??;
    serde_json::to_value(HtlcLookupResponse { htlcs: rows })
        .map_err(|e| Error::Internal(e.to_string()))
}

// ---------------------------------------------------------------------------
// list_settlements
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ListSettlementsParams {
    address: String,
    #[serde(default)]
    contract_hash: Option<String>,
    #[serde(default)]
    since_height: Option<u64>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Serialize)]
struct ListSettlementsResponse {
    settlements: Vec<SettlementRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

async fn list_settlements(state: &ApiState, params: Value) -> Result<Value> {
    let p: ListSettlementsParams = serde_json::from_value(params)
        .map_err(|e| Error::BadParams(format!("list_settlements: {e}")))?;
    let address = decode_hex32(&p.address)?;
    let contract_hash = match p.contract_hash.as_deref() {
        Some(s) => Some(decode_hex32(s)?),
        None => None,
    };
    let limit = p
        .limit
        .unwrap_or(HTLC_LIST_DEFAULT_LIMIT)
        .min(HTLC_LIST_MAX_LIMIT) as usize;
    let cursor = match p.cursor.as_deref() {
        Some(s) => Some(SettlementCursor::decode(s)?),
        None => None,
    };
    let db = state.db.clone();
    let (rows, next) = tokio::task::spawn_blocking(move || {
        db.list_settlements(
            &address,
            contract_hash.as_ref(),
            p.since_height,
            limit,
            cursor,
        )
    })
    .await
    .map_err(|e| Error::Internal(format!("blocking task panicked: {e}")))??;
    serde_json::to_value(ListSettlementsResponse {
        settlements: rows,
        next_cursor: next.map(|c| c.encode()),
    })
    .map_err(|e| Error::Internal(e.to_string()))
}

// ---------------------------------------------------------------------------
// contract_stats
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ContractStatsParams {
    address: String,
    #[serde(default)]
    contract_hash: Option<String>,
}

#[derive(Serialize)]
struct ContractStatsRow {
    contract_hash: String,
    total: u64,
    succeeded: u64,
    refunded: u64,
    avg_settle_blocks: u64,
    last_settled_at_height: Option<u64>,
}

#[derive(Serialize)]
struct ContractStatsResponse {
    stats: Vec<ContractStatsRow>,
}

async fn contract_stats(state: &ApiState, params: Value) -> Result<Value> {
    let p: ContractStatsParams = serde_json::from_value(params)
        .map_err(|e| Error::BadParams(format!("contract_stats: {e}")))?;
    let address = decode_hex32(&p.address)?;
    let contract_hash = match p.contract_hash.as_deref() {
        Some(s) => Some(decode_hex32(s)?),
        None => None,
    };
    let db = state.db.clone();
    let rows =
        tokio::task::spawn_blocking(move || db.contract_stats(&address, contract_hash.as_ref()))
            .await
            .map_err(|e| Error::Internal(format!("blocking task panicked: {e}")))??;

    let stats: Vec<ContractStatsRow> = rows
        .iter()
        .map(|s: &ContractStats| ContractStatsRow {
            contract_hash: hex::encode(s.contract_hash),
            total: s.total,
            succeeded: s.succeeded,
            refunded: s.refunded,
            avg_settle_blocks: s.sum_settle_blocks.checked_div(s.total).unwrap_or(0),
            last_settled_at_height: s.last_settled_at_height,
        })
        .collect();
    serde_json::to_value(ContractStatsResponse { stats })
        .map_err(|e| Error::Internal(e.to_string()))
}

// ---------------------------------------------------------------------------
// get_address_history
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct AddressHistoryParams {
    address: String,
    #[serde(default)]
    since_height: Option<u64>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Serialize)]
struct AddressHistoryRowJson {
    block_height: u64,
    tx_id: String,
    amount: u64,
    direction: &'static str,
    is_coinbase: bool,
    /// Hex addresses on the other side of this tx: senders for a received
    /// row (`direction: "output"`), recipients for a spent row
    /// (`direction: "input"`). Self excluded. Empty when none were
    /// resolvable (e.g. a coinbase, or covenant/HTLC prevouts).
    counterparties: Vec<String>,
}

#[derive(Serialize)]
struct AddressHistoryResponse {
    history: Vec<AddressHistoryRowJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

async fn get_address_history(state: &ApiState, params: Value) -> Result<Value> {
    let p: AddressHistoryParams = serde_json::from_value(params)
        .map_err(|e| Error::BadParams(format!("get_address_history: {e}")))?;
    let address = decode_hex32(&p.address)?;
    let limit = p
        .limit
        .unwrap_or(HTLC_LIST_DEFAULT_LIMIT)
        .min(HTLC_LIST_MAX_LIMIT) as usize;
    let cursor = match p.cursor.as_deref() {
        Some(s) => Some(HistoryCursor::decode(s)?),
        None => None,
    };
    let db = state.db.clone();
    let (rows, next) = tokio::task::spawn_blocking(move || {
        db.list_address_history(&address, p.since_height, limit, cursor)
    })
    .await
    .map_err(|e| Error::Internal(format!("blocking task panicked: {e}")))??;
    let history: Vec<AddressHistoryRowJson> = rows
        .iter()
        .map(|r: &AddressHistoryRow| AddressHistoryRowJson {
            block_height: r.block_height,
            tx_id: hex::encode(r.tx_id),
            amount: r.amount,
            direction: if r.is_input { "input" } else { "output" },
            is_coinbase: r.is_coinbase,
            counterparties: r.counterparties.iter().map(hex::encode).collect(),
        })
        .collect();
    serde_json::to_value(AddressHistoryResponse {
        history,
        next_cursor: next.map(|c| c.encode()),
    })
    .map_err(|e| Error::Internal(e.to_string()))
}

// ---------------------------------------------------------------------------
// get_output_spent_by
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct GetOutputSpentByParams {
    tx_id: String,
    output_index: u32,
}

async fn get_output_spent_by(state: &ApiState, params: Value) -> Result<Value> {
    let p: GetOutputSpentByParams = serde_json::from_value(params)
        .map_err(|e| Error::BadParams(format!("get_output_spent_by: {e}")))?;
    let prev_tx_id = decode_hex32(&p.tx_id)?;

    // 1. Try our local cache first.
    let db = state.db.clone();
    let oi = p.output_index;
    let cached = tokio::task::spawn_blocking(move || db.cached_spent_by(&prev_tx_id, oi))
        .await
        .map_err(|e| Error::Internal(format!("blocking task panicked: {e}")))??;
    if let Some(sb) = cached {
        return Ok(serde_json::json!({
            "spent": true,
            "spending_tx_id": hex::encode(sb.spending_tx_id),
            "input_index": sb.input_index,
            "block_height": sb.block_height,
            "source": "indexer-cache",
        }));
    }

    // 2. Fall through to the node's RPC. The node added this method
    //    in workflow B's first commit (`get_output_spent_by`); if the
    //    upstream doesn't have it (older binary), surface as
    //    `{spent: false}` since we have no other source of truth.
    match state
        .node
        .get_output_spent_by(&p.tx_id, p.output_index)
        .await
    {
        Ok(SpentByResponse::Spent {
            spending_tx_id,
            input_index,
            block_height,
            ..
        }) => Ok(serde_json::json!({
            "spent": true,
            "spending_tx_id": spending_tx_id,
            "input_index": input_index,
            "block_height": block_height,
            "source": "node",
        })),
        Ok(SpentByResponse::Unspent { .. }) => Ok(serde_json::json!({
            "spent": false,
            "source": "node",
        })),
        Err(Error::UpstreamRpc { code: -32601, .. }) => Ok(serde_json::json!({
            "spent": false,
            "source": "fallback-unknown-method",
        })),
        Err(e) => Err(e),
    }
}

// ---------------------------------------------------------------------------
// get_attestation_edges
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct AttestationEdgesParams {
    address: String,
    #[serde(default)]
    contract_hash: Option<String>,
}

#[derive(Serialize)]
struct AttestationEdgeJson {
    counterparty: String,
    contract_hash: String,
    contract_name: Option<&'static str>,
    total: u64,
    succeeded: u64,
    refunded: u64,
    last_seen_height: Option<u64>,
}

#[derive(Serialize)]
struct AttestationEdgesResponse {
    edges: Vec<AttestationEdgeJson>,
}

async fn get_attestation_edges(state: &ApiState, params: Value) -> Result<Value> {
    let p: AttestationEdgesParams = serde_json::from_value(params)
        .map_err(|e| Error::BadParams(format!("get_attestation_edges: {e}")))?;
    let address = decode_hex32(&p.address)?;
    let contract_hash = match p.contract_hash.as_deref() {
        Some(s) => Some(decode_hex32(s)?),
        None => None,
    };
    let db = state.db.clone();
    let rows =
        tokio::task::spawn_blocking(move || db.attestation_edges(&address, contract_hash.as_ref()))
            .await
            .map_err(|e| Error::Internal(format!("blocking task panicked: {e}")))??;

    let edges: Vec<AttestationEdgeJson> = rows
        .iter()
        .map(|e: &AttestationEdge| AttestationEdgeJson {
            counterparty: hex::encode(e.counterparty),
            contract_hash: hex::encode(e.contract_hash),
            contract_name: templates::lookup(&e.contract_hash).map(|t| t.name),
            total: e.total,
            succeeded: e.succeeded,
            refunded: e.refunded,
            last_seen_height: e.last_seen_height,
        })
        .collect();
    serde_json::to_value(AttestationEdgesResponse { edges })
        .map_err(|e| Error::Internal(e.to_string()))
}

// ---------------------------------------------------------------------------
// detect_in_chain_swaps
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct DetectSwapsParams {
    #[serde(default)]
    hash_lock: Option<String>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Serialize)]
struct SwapGroupJson {
    hash_lock: String,
    htlcs: Vec<HtlcRecord>,
}

#[derive(Serialize)]
struct DetectSwapsResponse {
    swaps: Vec<SwapGroupJson>,
}

async fn detect_in_chain_swaps(state: &ApiState, params: Value) -> Result<Value> {
    let p: DetectSwapsParams = serde_json::from_value(params)
        .map_err(|e| Error::BadParams(format!("detect_in_chain_swaps: {e}")))?;
    let hash_lock = match p.hash_lock.as_deref() {
        Some(s) => Some(decode_hex32(s)?),
        None => None,
    };
    let limit = p
        .limit
        .unwrap_or(HTLC_LIST_DEFAULT_LIMIT)
        .min(HTLC_LIST_MAX_LIMIT) as usize;
    let db = state.db.clone();
    let groups = tokio::task::spawn_blocking(move || {
        db.find_shared_hashlock_groups(hash_lock.as_ref(), limit)
    })
    .await
    .map_err(|e| Error::Internal(format!("blocking task panicked: {e}")))??;

    let swaps: Vec<SwapGroupJson> = groups
        .into_iter()
        .map(|g: SharedHashlockGroup| SwapGroupJson {
            hash_lock: hex::encode(g.hash_lock),
            htlcs: g.htlcs,
        })
        .collect();
    serde_json::to_value(DetectSwapsResponse { swaps }).map_err(|e| Error::Internal(e.to_string()))
}

// ---------------------------------------------------------------------------
// get_contract_template
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct GetContractTemplateParams {
    #[serde(default)]
    contract_hash: Option<String>,
}

async fn get_contract_template(params: Value) -> Result<Value> {
    // No-arg form: enumerate every registered template.
    let p: GetContractTemplateParams = serde_json::from_value(params)
        .map_err(|e| Error::BadParams(format!("get_contract_template: {e}")))?;
    let Some(hash_str) = p.contract_hash else {
        let all = templates::list_all();
        return Ok(serde_json::json!({ "templates": all }));
    };
    let hash = decode_hex32(&hash_str)?;
    let tpl = templates::lookup(&hash);
    Ok(serde_json::json!({ "template": tpl }))
}

// ---------------------------------------------------------------------------
// resolve_name — highest-cumulative-burn name registry (no consensus change)
// ---------------------------------------------------------------------------
//
// A name maps to a derived 32-byte burn-script `name_script(name)` — an
// unspendable target (no key hashes to it), so value sent there is burned.
// Ownership is an open auction: the party with the **highest cumulative
// burn** to the script owns the name and can be out-burned at any time
// (no permanence). The owner declares where the name *points* by including
// an extra output in their claim tx (any output whose recipient is neither
// the burn-script nor the owner's own address); absent that, the name
// points to the owner. Resolution reads the winner's latest claim tx for
// the current pointer.

/// Domain-separated derivation of the burn-script for a name. MUST stay
/// byte-identical to walletd's `name_script`.
pub fn name_script(name: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"EXFER-NAME-v1:");
    h.update(name.trim().to_lowercase().as_bytes());
    let mut s = [0u8; 32];
    s.copy_from_slice(&h.finalize());
    s
}

#[derive(Deserialize)]
struct ResolveNameParams {
    name: String,
}

#[derive(Serialize)]
struct ResolveNameResponse {
    name: String,
    /// The burn-script the name maps to (where claims are sent).
    script: String,
    /// The address the name resolves to (the winner's declared pointer,
    /// or the winner itself). Null if unclaimed.
    address: Option<String>,
    /// The winning (highest cumulative burn) claimant address. Null if unclaimed.
    owner: Option<String>,
    /// Total value the winner has burned to this name.
    total_burned: u64,
    /// The winner's latest claim tx (the one that set the current pointer).
    claim_tx_id: Option<String>,
    claim_height: Option<u64>,
}

/// Per-bidder accumulation while scanning a name's claim history.
#[derive(Default)]
struct BidAgg {
    total: u64,
    first_height: u64,
    latest_height: u64,
    latest_tx: [u8; 32],
}

async fn resolve_name(state: &ApiState, params: Value) -> Result<Value> {
    let p: ResolveNameParams = serde_json::from_value(params)
        .map_err(|e| Error::BadParams(format!("resolve_name: {e}")))?;
    let script = name_script(&p.name);

    // Pull the full claim history for the burn-script (sorted asc by
    // height, tx_id) and accumulate cumulative burn per bidder.
    let db = state.db.clone();
    let (rows, _next) = tokio::task::spawn_blocking(move || {
        db.list_address_history(&script, None, HTLC_LIST_MAX_LIMIT as usize, None)
    })
    .await
    .map_err(|e| Error::Internal(format!("blocking task panicked: {e}")))??;

    let mut bids: std::collections::HashMap<[u8; 32], BidAgg> = std::collections::HashMap::new();
    for r in &rows {
        if r.is_input || r.is_coinbase || r.counterparties.is_empty() {
            continue; // only inbound burns with a known sender count
        }
        let bidder = r.counterparties[0];
        let agg = bids.entry(bidder).or_insert_with(|| BidAgg {
            first_height: r.block_height,
            ..Default::default()
        });
        agg.total = agg.total.saturating_add(r.amount);
        agg.first_height = agg.first_height.min(r.block_height);
        if r.block_height >= agg.latest_height {
            agg.latest_height = r.block_height;
            agg.latest_tx = r.tx_id;
        }
    }

    // Winner = highest cumulative burn; ties broken by earliest first claim,
    // then lowest address (fully deterministic).
    let winner = bids.iter().max_by(|(a_addr, a), (b_addr, b)| {
        a.total
            .cmp(&b.total)
            .then_with(|| b.first_height.cmp(&a.first_height)) // earlier first claim wins
            .then_with(|| b_addr.cmp(a_addr)) // lower address wins
    });

    let resp = match winner {
        None => ResolveNameResponse {
            name: p.name,
            script: hex::encode(script),
            address: None,
            owner: None,
            total_burned: 0,
            claim_tx_id: None,
            claim_height: None,
        },
        Some((owner_addr, agg)) => {
            // The pointer is declared in the winner's latest claim tx: the
            // first output whose recipient is neither the burn-script nor
            // the owner. Fall back to the owner on any fetch/parse failure.
            let pointer = self_resolve_pointer(state, &agg.latest_tx, &script, owner_addr)
                .await
                .unwrap_or(*owner_addr);
            ResolveNameResponse {
                name: p.name,
                script: hex::encode(script),
                address: Some(hex::encode(pointer)),
                owner: Some(hex::encode(owner_addr)),
                total_burned: agg.total,
                claim_tx_id: Some(hex::encode(agg.latest_tx)),
                claim_height: Some(agg.latest_height),
            }
        }
    };
    serde_json::to_value(resp).map_err(|e| Error::Internal(e.to_string()))
}

/// Read the pointer address declared in a claim tx: the first 32-byte
/// output script that is neither the burn-script nor the owner. Returns
/// `None` (caller defaults to the owner) if the tx can't be fetched/parsed
/// or carries no such output.
async fn self_resolve_pointer(
    state: &ApiState,
    tx_id: &[u8; 32],
    script: &[u8; 32],
    owner: &[u8; 32],
) -> Option<[u8; 32]> {
    let tx_status = state.node.get_transaction(&hex::encode(tx_id)).await.ok()?;
    let bytes = hex::decode(&tx_status.tx_hex).ok()?;
    let (tx, _) = exfer::types::transaction::Transaction::deserialize(&bytes).ok()?;
    for out in &tx.outputs {
        if out.script.len() == 32
            && out.script.as_slice() != script.as_slice()
            && out.script.as_slice() != owner.as_slice()
        {
            let mut t = [0u8; 32];
            t.copy_from_slice(&out.script);
            return Some(t);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn decode_hex32(s: &str) -> Result<[u8; 32]> {
    let b = hex::decode(s).map_err(|e| Error::BadHex(e.to_string()))?;
    if b.len() != 32 {
        return Err(Error::BadAddressLen(b.len()));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&b);
    Ok(out)
}

#[cfg(test)]
mod naming_tests {
    use super::name_script;

    // This vector MUST match walletd's `name_script` test. If either side
    // changes the derivation, both tests fail and the registry would split.
    const ALICE: &str = "dbbce120c1d1bc12cba5ed500e1fe9c4b67ae92ec4349d3d847f01d74e711dcd";

    #[test]
    fn name_script_is_case_and_whitespace_insensitive() {
        let want = hex::decode(ALICE).unwrap();
        for n in ["alice", "Alice", "  ALICE  "] {
            assert_eq!(name_script(n).to_vec(), want, "mismatch for {n:?}");
        }
    }
}
