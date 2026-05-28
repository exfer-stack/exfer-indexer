//! Dispatch-layer integration tests for the indexer RPC methods.
//!
//! Each test seeds the redb store directly with synthetic events
//! (no real follower / no real node), then drives `dispatch` and
//! asserts the response JSON shape. The follower's wiremock path is
//! covered separately in `tests/follower_smoke.rs`.

use std::sync::Arc;
use std::time::Duration;

use exfer::covenants::htlc::{HtlcParams, HtlcRecord, HtlcRole, HtlcState};
use exfer_indexer::api::{dispatch, ApiState, RpcRequest};
use exfer_indexer::db::{BlockApplyEvents, Db, FollowerMeta};
use exfer_indexer::extract::{ExtractedHtlcLock, SettlementRecord};
use exfer_indexer::upstream::NodeClient;
use serde_json::{json, Value};
use tokio::sync::watch;
use wiremock::matchers::{body_partial_json, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct Ctx {
    state: ApiState,
    #[allow(dead_code)]
    dir: tempfile::TempDir,
    db: Arc<Db>,
}

async fn make_ctx(initial_tip: u64) -> Ctx {
    // Wiremock + tip endpoint so get_indexer_status can call.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(body_partial_json(json!({ "method": "get_block_height" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0",
            "result": { "height": initial_tip, "block_id": "ff".repeat(32) },
            "id": 1
        })))
        .mount(&mock)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let db = Arc::new(Db::open(dir.path()).unwrap());
    let node = NodeClient::new(&mock.uri(), Duration::from_secs(5))
        .unwrap()
        .with_retry(1, 0);
    let (_tip_tx, tip_rx) = watch::channel(initial_tip);
    let state = ApiState {
        db: db.clone(),
        node,
        tip_rx,
    };
    let ctx = Ctx { state, dir, db };
    // Keep the mock alive by leaking it intentionally for the test's
    // lifetime — wiremock::MockServer cleans up on drop and we want
    // the URL still bound when the indexer calls back into it.
    Box::leak(Box::new(mock));
    ctx
}

fn rpc(method_name: &str, params: Value) -> RpcRequest {
    RpcRequest {
        jsonrpc: "2.0".into(),
        method: method_name.into(),
        params,
        id: Some(json!(1)),
    }
}

fn fixed_record(
    lock_tx_id: [u8; 32],
    output_index: u32,
    height: u64,
    state: HtlcState,
    sender: [u8; 32],
    receiver: [u8; 32],
) -> HtlcRecord {
    HtlcRecord {
        lock_tx_id,
        output_index,
        params: HtlcParams {
            sender,
            receiver,
            hash_lock: [0x33; 32],
            timeout_height: 1000,
        },
        amount: 50_000,
        lock_block_height: Some(height),
        state,
        claim: None,
        reclaim: None,
        role: HtlcRole::Observer,
        last_indexed_height: height,
    }
}

async fn seed_htlc(ctx: &Ctx, rec: HtlcRecord) {
    let lock = ExtractedHtlcLock {
        record: rec.clone(),
        script: vec![],
    };
    let h = rec.lock_block_height.unwrap_or(0);
    ctx.db
        .apply_block_events(BlockApplyEvents {
            height: h,
            block_id: [(h as u8); 32],
            tx_count: 1,
            timestamp: 1_700_000_000 + h,
            full_scan_complete: true,
            started_at: 1_700_000_000,
            locks: std::slice::from_ref(&lock),
            spends: &[],
            settlements: &[],
            activity: &[],
            spent_by: &[],
        })
        .unwrap();
}

// ---------------------------------------------------------------------------
// ping / get_indexer_status
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ping_returns_ok() {
    let ctx = make_ctx(0).await;
    let v = dispatch(&ctx.state, rpc("ping", json!({}))).await.unwrap();
    assert_eq!(v["ok"].as_bool(), Some(true));
}

#[tokio::test]
async fn get_indexer_status_reports_tip_and_lag() {
    let ctx = make_ctx(1_000).await;
    let meta = FollowerMeta {
        last_indexed_height: 700,
        last_indexed_block_id: [0xAB; 32],
        full_scan_complete: false,
        started_at: 1_700_000_000,
    };
    ctx.db.save_meta(&meta).unwrap();

    let v = dispatch(&ctx.state, rpc("get_indexer_status", json!({})))
        .await
        .unwrap();
    assert_eq!(v["last_indexed_height"].as_u64(), Some(700));
    assert_eq!(v["tip_height"].as_u64(), Some(1_000));
    assert_eq!(v["lag"].as_i64(), Some(300));
    assert_eq!(v["full_scan_complete"].as_bool(), Some(false));
    assert_eq!(v["indexed_htlc_count"].as_u64(), Some(0));
}

// ---------------------------------------------------------------------------
// htlc_status / htlc_list / htlc_lookup_by_hashlock
// ---------------------------------------------------------------------------

#[tokio::test]
async fn htlc_status_returns_recorded_state() {
    let ctx = make_ctx(0).await;
    let rec = fixed_record([0xAA; 32], 0, 50, HtlcState::Locked, [0x11; 32], [0x22; 32]);
    seed_htlc(&ctx, rec).await;
    let v = dispatch(
        &ctx.state,
        rpc(
            "htlc_status",
            json!({ "lock_tx_id": hex::encode([0xAA; 32]), "output_index": 0 }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(v["state"].as_str(), Some("locked"));
    assert_eq!(v["amount"].as_u64(), Some(50_000));
    assert_eq!(v["role"].as_str(), Some("observer"));
}

#[tokio::test]
async fn htlc_status_returns_error_for_unknown() {
    let ctx = make_ctx(0).await;
    let err = dispatch(
        &ctx.state,
        rpc("htlc_status", json!({ "lock_tx_id": "aa".repeat(32) })),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(err, exfer_indexer::error::Error::BadParams(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn htlc_list_with_state_filter() {
    let ctx = make_ctx(0).await;
    let mut a = fixed_record([0x01; 32], 0, 10, HtlcState::Locked, [0x11; 32], [0x22; 32]);
    let mut b = fixed_record(
        [0x02; 32],
        0,
        20,
        HtlcState::Claimed,
        [0x11; 32],
        [0x22; 32],
    );
    let _ = (&mut a, &mut b);
    seed_htlc(&ctx, a).await;
    seed_htlc(&ctx, b).await;

    let v = dispatch(&ctx.state, rpc("htlc_list", json!({ "state": "claimed" })))
        .await
        .unwrap();
    let arr = v["htlcs"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["state"].as_str(), Some("claimed"));
}

#[tokio::test]
async fn htlc_list_address_filter_matches_sender_or_receiver() {
    let ctx = make_ctx(0).await;
    // a: sender = 0x11, receiver = 0x22
    let a = fixed_record([0x01; 32], 0, 10, HtlcState::Locked, [0x11; 32], [0x22; 32]);
    // b: sender = 0x33, receiver = 0x44
    let b = fixed_record([0x02; 32], 0, 20, HtlcState::Locked, [0x33; 32], [0x44; 32]);
    seed_htlc(&ctx, a).await;
    seed_htlc(&ctx, b).await;

    let v = dispatch(
        &ctx.state,
        rpc("htlc_list", json!({ "address": hex::encode([0x11; 32]) })),
    )
    .await
    .unwrap();
    let arr = v["htlcs"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(
        arr[0]["lock_tx_id"].as_str().unwrap(),
        hex::encode([0x01; 32])
    );
}

#[tokio::test]
async fn htlc_list_pagination_advances_cursor() {
    let ctx = make_ctx(0).await;
    for h in [10u64, 20, 30, 40, 50] {
        let mut tx_id = [0u8; 32];
        tx_id[0] = h as u8;
        let rec = fixed_record(tx_id, 0, h, HtlcState::Locked, [0x11; 32], [0x22; 32]);
        seed_htlc(&ctx, rec).await;
    }
    let p1 = dispatch(&ctx.state, rpc("htlc_list", json!({ "limit": 2 })))
        .await
        .unwrap();
    assert_eq!(p1["htlcs"].as_array().unwrap().len(), 2);
    let cur = p1["next_cursor"].as_str().unwrap().to_string();
    let p2 = dispatch(
        &ctx.state,
        rpc("htlc_list", json!({ "limit": 2, "cursor": cur })),
    )
    .await
    .unwrap();
    assert_eq!(p2["htlcs"].as_array().unwrap().len(), 2);
    let cur2 = p2["next_cursor"].as_str().unwrap().to_string();
    let p3 = dispatch(
        &ctx.state,
        rpc("htlc_list", json!({ "limit": 2, "cursor": cur2 })),
    )
    .await
    .unwrap();
    assert_eq!(p3["htlcs"].as_array().unwrap().len(), 1);
    assert!(p3.get("next_cursor").is_none(), "last page");
}

#[tokio::test]
async fn htlc_lookup_by_hashlock_returns_matching() {
    let ctx = make_ctx(0).await;
    let mut a = fixed_record([0x01; 32], 0, 10, HtlcState::Locked, [0x11; 32], [0x22; 32]);
    a.params.hash_lock = [0xFE; 32];
    let mut b = fixed_record([0x02; 32], 0, 20, HtlcState::Locked, [0x11; 32], [0x22; 32]);
    b.params.hash_lock = [0xEE; 32];
    seed_htlc(&ctx, a).await;
    seed_htlc(&ctx, b).await;

    let v = dispatch(
        &ctx.state,
        rpc(
            "htlc_lookup_by_hashlock",
            json!({ "hash_lock": hex::encode([0xFE; 32]) }),
        ),
    )
    .await
    .unwrap();
    let arr = v["htlcs"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(
        arr[0]["lock_tx_id"].as_str().unwrap(),
        hex::encode([0x01; 32])
    );
}

// ---------------------------------------------------------------------------
// list_settlements / contract_stats
// ---------------------------------------------------------------------------

async fn seed_settlements(ctx: &Ctx) {
    // Two settlements at height 100 (one Claimed, one Reclaimed) for
    // address 0x55, contract 0xBB.
    let s1 = SettlementRecord {
        tx_id: [0xAA; 32],
        block_height: 100,
        contract_hash: [0xBB; 32],
        outcome: HtlcState::Claimed,
        observer_address: [0x55; 32],
        counterparty: [0x66; 32],
        amount: 1_000,
        lock_tx_id: [0xCC; 32],
        lock_output_index: 0,
    };
    let s2 = SettlementRecord {
        tx_id: [0xBB; 32],
        block_height: 200,
        contract_hash: [0xBB; 32],
        outcome: HtlcState::Reclaimed,
        observer_address: [0x55; 32],
        counterparty: [0x66; 32],
        amount: 2_000,
        lock_tx_id: [0xDD; 32],
        lock_output_index: 0,
    };
    let settlements = vec![s1, s2];
    ctx.db
        .apply_block_events(BlockApplyEvents {
            height: 200,
            block_id: [0x42; 32],
            tx_count: 0,
            timestamp: 0,
            full_scan_complete: false,
            started_at: 0,
            locks: &[],
            spends: &[],
            settlements: &settlements,
            activity: &[],
            spent_by: &[],
        })
        .unwrap();
}

#[tokio::test]
async fn list_settlements_returns_records_for_address() {
    let ctx = make_ctx(0).await;
    seed_settlements(&ctx).await;
    let v = dispatch(
        &ctx.state,
        rpc(
            "list_settlements",
            json!({ "address": hex::encode([0x55; 32]) }),
        ),
    )
    .await
    .unwrap();
    let arr = v["settlements"].as_array().unwrap();
    assert_eq!(arr.len(), 2);
}

#[tokio::test]
async fn contract_stats_aggregates() {
    let ctx = make_ctx(0).await;
    seed_settlements(&ctx).await;
    let v = dispatch(
        &ctx.state,
        rpc(
            "contract_stats",
            json!({ "address": hex::encode([0x55; 32]) }),
        ),
    )
    .await
    .unwrap();
    let arr = v["stats"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    let row = &arr[0];
    assert_eq!(row["total"].as_u64(), Some(2));
    assert_eq!(row["succeeded"].as_u64(), Some(1));
    assert_eq!(row["refunded"].as_u64(), Some(1));
    assert_eq!(row["last_settled_at_height"].as_u64(), Some(200));
    assert_eq!(
        row["contract_hash"].as_str(),
        Some(hex::encode([0xBB; 32])).as_deref()
    );
}

#[tokio::test]
async fn contract_stats_filters_by_contract_hash() {
    let ctx = make_ctx(0).await;
    seed_settlements(&ctx).await;
    // Wrong contract — should return no rows.
    let v = dispatch(
        &ctx.state,
        rpc(
            "contract_stats",
            json!({
                "address": hex::encode([0x55; 32]),
                "contract_hash": hex::encode([0x99; 32]),
            }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(v["stats"].as_array().unwrap().len(), 0);
}

// ---------------------------------------------------------------------------
// get_address_history
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_address_history_returns_activity_rows() {
    let ctx = make_ctx(0).await;
    use exfer_indexer::extract::AddressActivity;
    let activity = vec![
        AddressActivity {
            address: [0x77; 32],
            tx_id: [0xAA; 32],
            amount: 1_000,
            is_input: false,
            is_coinbase: false,
        },
        AddressActivity {
            address: [0x77; 32],
            tx_id: [0xBB; 32],
            amount: 500,
            is_input: true,
            is_coinbase: false,
        },
    ];
    ctx.db
        .apply_block_events(BlockApplyEvents {
            height: 50,
            block_id: [0x42; 32],
            tx_count: 0,
            timestamp: 0,
            full_scan_complete: false,
            started_at: 0,
            locks: &[],
            spends: &[],
            settlements: &[],
            activity: &activity,
            spent_by: &[],
        })
        .unwrap();
    let v = dispatch(
        &ctx.state,
        rpc(
            "get_address_history",
            json!({ "address": hex::encode([0x77; 32]) }),
        ),
    )
    .await
    .unwrap();
    let arr = v["history"].as_array().unwrap();
    assert_eq!(arr.len(), 2);
    // ordered by tx_id within the same height
    assert_eq!(arr[0]["tx_id"].as_str().unwrap(), hex::encode([0xAA; 32]));
    assert_eq!(arr[0]["direction"].as_str(), Some("output"));
    assert_eq!(arr[1]["tx_id"].as_str().unwrap(), hex::encode([0xBB; 32]));
    assert_eq!(arr[1]["direction"].as_str(), Some("input"));
}

// ---------------------------------------------------------------------------
// get_output_spent_by
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_output_spent_by_hits_local_cache_first() {
    let ctx = make_ctx(0).await;
    use exfer_indexer::db::SpentByCacheEntry;
    let sb = SpentByCacheEntry {
        prev_tx_id: [0xAA; 32],
        output_index: 0,
        spending_tx_id: [0xBB; 32],
        input_index: 0,
        block_height: 7,
    };
    ctx.db
        .apply_block_events(BlockApplyEvents {
            height: 7,
            block_id: [0x42; 32],
            tx_count: 0,
            timestamp: 0,
            full_scan_complete: false,
            started_at: 0,
            locks: &[],
            spends: &[],
            settlements: &[],
            activity: &[],
            spent_by: std::slice::from_ref(&sb),
        })
        .unwrap();

    let v = dispatch(
        &ctx.state,
        rpc(
            "get_output_spent_by",
            json!({ "tx_id": hex::encode([0xAA; 32]), "output_index": 0 }),
        ),
    )
    .await
    .unwrap();
    assert_eq!(v["spent"].as_bool(), Some(true));
    assert_eq!(
        v["spending_tx_id"].as_str(),
        Some(hex::encode([0xBB; 32])).as_deref()
    );
    assert_eq!(v["block_height"].as_u64(), Some(7));
    assert_eq!(v["source"].as_str(), Some("indexer-cache"));
}

// ---------------------------------------------------------------------------
// Error / unknown method
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_method_returns_method_not_found() {
    let ctx = make_ctx(0).await;
    let err = dispatch(&ctx.state, rpc("definitely_not_a_method", json!({})))
        .await
        .unwrap_err();
    assert!(
        matches!(err, exfer_indexer::error::Error::UnknownMethod(_)),
        "got {err:?}"
    );
    assert_eq!(err.rpc_code(), -32601);
}

#[tokio::test]
async fn bad_address_hex_is_rejected() {
    let ctx = make_ctx(0).await;
    let err = dispatch(
        &ctx.state,
        rpc("list_settlements", json!({ "address": "deadbeef" })),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, exfer_indexer::error::Error::BadAddressLen(_)));
}
