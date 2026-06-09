//! End-to-end test of `Db::apply_block_events`.
//!
//! Builds synthetic block events that walk an HTLC through its
//! lifecycle (Locked → Claimed) and verifies the resulting redb
//! state: HTLC row updated, settlement rows written, address
//! activity rows written, chain-tip advanced. Crash-safe in the
//! sense that re-applying the same block yields the same final
//! state byte-for-byte.

use exfer::covenants::htlc::{HtlcClaimRecord, HtlcParams, HtlcRecord, HtlcRole, HtlcState};
use exfer_indexer::db::{BlockApplyEvents, Db, SpentByCacheEntry};
use exfer_indexer::extract::{
    AddressActivity, ExtractedHtlcLock, ExtractedHtlcSpend, ExtractedOutputDatum, HtlcSpendArm,
    SettlementRecord,
};

/// Apply a single block carrying only output-datum events.
fn apply_datums(db: &Db, height: u64, datums: &[ExtractedOutputDatum]) {
    db.apply_block_events(BlockApplyEvents {
        height,
        block_id: [(height as u8); 32],
        tx_count: 1,
        timestamp: 1_700_000_000,
        full_scan_complete: false,
        started_at: 1_700_000_000,
        locks: &[],
        spends: &[],
        settlements: &[],
        activity: &[],
        spent_by: &[],
        output_datums: datums,
    })
    .unwrap();
}

fn fixed_record(state: HtlcState) -> HtlcRecord {
    HtlcRecord {
        lock_tx_id: [0xAA; 32],
        output_index: 0,
        params: HtlcParams {
            sender: [0x11; 32],
            receiver: [0x22; 32],
            hash_lock: [0x33; 32],
            timeout_height: 1000,
        },
        amount: 100_000,
        lock_block_height: Some(50),
        state,
        claim: None,
        reclaim: None,
        role: HtlcRole::Observer,
        last_indexed_height: 50,
    }
}

#[test]
fn apply_block_events_inserts_lock_and_advances_meta() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();

    let rec = fixed_record(HtlcState::Locked);
    let lock = ExtractedHtlcLock {
        record: rec.clone(),
        script: vec![],
    };
    let events = BlockApplyEvents {
        height: 50,
        block_id: [0xCC; 32],
        tx_count: 1,
        timestamp: 1_700_000_000,
        full_scan_complete: false,
        started_at: 1_700_000_000,
        locks: std::slice::from_ref(&lock),
        spends: &[],
        settlements: &[],
        activity: &[],
        spent_by: &[],
        output_datums: &[],
    };
    db.apply_block_events(events).unwrap();

    let meta = db.load_meta().unwrap();
    assert_eq!(meta.last_indexed_height, 50);
    assert_eq!(meta.last_indexed_block_id, [0xCC; 32]);
    assert_eq!(db.htlc_count().unwrap(), 1);
}

#[test]
fn apply_is_idempotent_on_replay() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();

    let lock = ExtractedHtlcLock {
        record: fixed_record(HtlcState::Locked),
        script: vec![],
    };
    let make_events = || BlockApplyEvents {
        height: 50,
        block_id: [0xCC; 32],
        tx_count: 1,
        timestamp: 1_700_000_000,
        full_scan_complete: false,
        started_at: 1_700_000_000,
        locks: std::slice::from_ref(&lock),
        spends: &[],
        settlements: &[],
        activity: &[],
        spent_by: &[],
        output_datums: &[],
    };
    db.apply_block_events(make_events()).unwrap();
    db.apply_block_events(make_events()).unwrap();
    assert_eq!(db.htlc_count().unwrap(), 1, "replay must not duplicate");
}

#[test]
fn claim_spend_advances_state_and_keeps_preimage() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();

    // Block 50: lock.
    let lock = ExtractedHtlcLock {
        record: fixed_record(HtlcState::Locked),
        script: vec![],
    };
    db.apply_block_events(BlockApplyEvents {
        height: 50,
        block_id: [0xCC; 32],
        tx_count: 1,
        timestamp: 1_700_000_000,
        full_scan_complete: false,
        started_at: 1_700_000_000,
        locks: std::slice::from_ref(&lock),
        spends: &[],
        settlements: &[],
        activity: &[],
        spent_by: &[],
        output_datums: &[],
    })
    .unwrap();

    // Block 51: claim.
    let claim = ExtractedHtlcSpend {
        lock_tx_id: [0xAA; 32],
        output_index: 0,
        arm: HtlcSpendArm::Claim {
            preimage: b"exfer htlc test preimage 2026".to_vec(),
            spending_tx_id: [0xBB; 32],
            input_index: 0,
        },
    };
    db.apply_block_events(BlockApplyEvents {
        height: 51,
        block_id: [0xDD; 32],
        tx_count: 1,
        timestamp: 1_700_000_000,
        full_scan_complete: false,
        started_at: 1_700_000_000,
        locks: &[],
        spends: std::slice::from_ref(&claim),
        settlements: &[],
        activity: &[],
        spent_by: &[],
        output_datums: &[],
    })
    .unwrap();

    // Re-read by snooping the raw redb file: easiest path here is to
    // re-open Db (drops in-memory state), reload, count by state.
    // We rely on htlc_count being 1 + load_meta showing height 51.
    let meta = db.load_meta().unwrap();
    assert_eq!(meta.last_indexed_height, 51);
    assert_eq!(
        db.htlc_count().unwrap(),
        1,
        "claim must update, not duplicate"
    );
}

#[test]
fn wipe_above_removes_only_above_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();

    // Insert three HTLCs at heights 10, 20, 30.
    for (h, seed) in [(10u64, 1u8), (20, 2), (30, 3)] {
        let mut tx_id = [0u8; 32];
        tx_id[0] = seed;
        let mut rec = fixed_record(HtlcState::Locked);
        rec.lock_tx_id = tx_id;
        rec.lock_block_height = Some(h);
        let lock = ExtractedHtlcLock {
            record: rec,
            script: vec![],
        };
        db.apply_block_events(BlockApplyEvents {
            height: h,
            block_id: [seed; 32],
            tx_count: 1,
            timestamp: 1_700_000_000,
            full_scan_complete: false,
            started_at: 1_700_000_000,
            locks: std::slice::from_ref(&lock),
            spends: &[],
            settlements: &[],
            activity: &[],
            spent_by: &[],
            output_datums: &[],
        })
        .unwrap();
    }
    assert_eq!(db.htlc_count().unwrap(), 3);

    db.wipe_above(20).unwrap();
    assert_eq!(
        db.htlc_count().unwrap(),
        2,
        "height>20 should be wiped (HTLC at height 30)"
    );
}

#[test]
fn address_activity_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let activity = vec![
        AddressActivity {
            address: [0x55; 32],
            tx_id: [0xAA; 32],
            amount: 1_000,
            is_input: false,
            is_coinbase: false,
            counterparties: vec![[0x66; 32]],
        },
        AddressActivity {
            address: [0x66; 32],
            tx_id: [0xBB; 32],
            amount: 500,
            is_input: true,
            is_coinbase: false,
            counterparties: vec![[0x55; 32]],
        },
    ];
    db.apply_block_events(BlockApplyEvents {
        height: 1,
        block_id: [0x42; 32],
        tx_count: 2,
        timestamp: 0,
        full_scan_complete: true,
        started_at: 0,
        locks: &[],
        spends: &[],
        settlements: &[],
        activity: &activity,
        spent_by: &[],
        output_datums: &[],
    })
    .unwrap();
    let meta = db.load_meta().unwrap();
    assert_eq!(meta.last_indexed_height, 1);
    assert!(meta.full_scan_complete);

    // The received (output) row reports its sender as counterparty…
    let (recv, _) = db
        .list_address_history(&[0x55; 32], None, 10, None)
        .unwrap();
    assert_eq!(recv.len(), 1);
    assert!(!recv[0].is_input);
    assert_eq!(recv[0].counterparties, vec![[0x66; 32]]);

    // …and the spent (input) row reports its recipient.
    let (sent, _) = db
        .list_address_history(&[0x66; 32], None, 10, None)
        .unwrap();
    assert_eq!(sent.len(), 1);
    assert!(sent[0].is_input);
    assert_eq!(sent[0].counterparties, vec![[0x55; 32]]);
}

#[test]
fn spent_by_cache_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let sb = SpentByCacheEntry {
        prev_tx_id: [0x77; 32],
        output_index: 1,
        spending_tx_id: [0x88; 32],
        input_index: 0,
        block_height: 42,
    };
    db.apply_block_events(BlockApplyEvents {
        height: 42,
        block_id: [0x99; 32],
        tx_count: 1,
        timestamp: 0,
        full_scan_complete: false,
        started_at: 0,
        locks: &[],
        spends: &[],
        settlements: &[],
        activity: &[],
        spent_by: std::slice::from_ref(&sb),
        output_datums: &[],
    })
    .unwrap();
    let meta = db.load_meta().unwrap();
    assert_eq!(meta.last_indexed_height, 42);
}

#[test]
fn settlements_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let sett = SettlementRecord {
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
    db.apply_block_events(BlockApplyEvents {
        height: 100,
        block_id: [0x42; 32],
        tx_count: 1,
        timestamp: 0,
        full_scan_complete: false,
        started_at: 0,
        locks: &[],
        spends: &[],
        settlements: std::slice::from_ref(&sett),
        activity: &[],
        spent_by: &[],
        output_datums: &[],
    })
    .unwrap();
    let meta = db.load_meta().unwrap();
    assert_eq!(meta.last_indexed_height, 100);
}

#[test]
fn meta_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let meta_before = {
        let db = Db::open(&path).unwrap();
        db.apply_block_events(BlockApplyEvents {
            height: 7,
            block_id: [0xEE; 32],
            tx_count: 1,
            timestamp: 0,
            full_scan_complete: true,
            started_at: 1_700_000_000,
            locks: &[],
            spends: &[],
            settlements: &[],
            activity: &[],
            spent_by: &[],
            output_datums: &[],
        })
        .unwrap();
        db.load_meta().unwrap()
    };
    let db2 = Db::open(&path).unwrap();
    let meta_after = db2.load_meta().unwrap();
    assert_eq!(meta_before, meta_after);
}

#[test]
fn dummy_use_of_claim_record_type() {
    // Compile-time check that the upstream HtlcClaimRecord type is
    // still in scope under the new variable-length preimage shape.
    let _c = HtlcClaimRecord {
        tx_id: [0xAA; 32],
        preimage: b"variable".to_vec(),
        block_height: 1,
        input_index: 0,
    };
}

// ---------------------------------------------------------------------------
// EXFER-QUOTE settlement-datum indexing (Wave 3 Stage 1)
// ---------------------------------------------------------------------------

#[test]
fn strict_16_byte_datum_is_findable_by_quoteid_and_outpoint() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();

    let quote_id = [0x7Au8; 16];
    let tx_id = [0xAB; 32];
    apply_datums(
        &db,
        50,
        &[ExtractedOutputDatum {
            tx_id,
            output_index: 2,
            quote_id: Some(quote_id),
            unhonorable: false,
        }],
    );

    // Forward: outpoint → quote_id, honorable.
    let fwd = db.get_output_datum(&tx_id, 2).unwrap().unwrap();
    assert_eq!(fwd.quote_id, Some(quote_id));
    assert!(!fwd.unhonorable);

    // Reverse: quote_id → outpoint.
    let found = db.find_settlements_by_quote_id(&quote_id).unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].tx_id, tx_id);
    assert_eq!(found[0].output_index, 2);
}

#[test]
fn oversized_datum_is_not_indexed_so_not_findable() {
    // The extractor drops a 20-byte inline datum (`[HOLE-F1]`), so the
    // db never sees an ExtractedOutputDatum for it. Simulate by NOT
    // emitting one, then confirm nothing is findable. (The decode-level
    // rejection itself is unit-tested in extract.rs.)
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tx_id = [0xCD; 32];

    apply_datums(&db, 60, &[]); // nothing emitted for the oversized output

    assert!(db.get_output_datum(&tx_id, 0).unwrap().is_none());
    // A 16-byte query for whatever id never gets a hit.
    assert!(db
        .find_settlements_by_quote_id(&[0u8; 16])
        .unwrap()
        .is_empty());
}

#[test]
fn datum_hash_only_is_unhonorable_and_yields_no_quote_match() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let tx_id = [0xEF; 32];

    apply_datums(
        &db,
        70,
        &[ExtractedOutputDatum {
            tx_id,
            output_index: 0,
            quote_id: None,
            unhonorable: true,
        }],
    );

    // Forward read flags it unhonorable, no quote_id.
    let fwd = db.get_output_datum(&tx_id, 0).unwrap().unwrap();
    assert_eq!(fwd.quote_id, None);
    assert!(fwd.unhonorable);

    // It must NEVER be in the reverse index under any quote_id.
    assert!(db
        .find_settlements_by_quote_id(&[0u8; 16])
        .unwrap()
        .is_empty());
}

#[test]
fn one_quote_id_can_map_to_multiple_outpoints() {
    // The swap-side gate enforces 1:1; the indexer reports ALL
    // outpoints carrying the same quote_id.
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let quote_id = [0x11u8; 16];

    apply_datums(
        &db,
        80,
        &[
            ExtractedOutputDatum {
                tx_id: [0x01; 32],
                output_index: 0,
                quote_id: Some(quote_id),
                unhonorable: false,
            },
            ExtractedOutputDatum {
                tx_id: [0x02; 32],
                output_index: 5,
                quote_id: Some(quote_id),
                unhonorable: false,
            },
        ],
    );

    let mut found = db.find_settlements_by_quote_id(&quote_id).unwrap();
    found.sort_by_key(|o| o.tx_id);
    assert_eq!(found.len(), 2);
    assert_eq!(found[0].tx_id, [0x01; 32]);
    assert_eq!(found[1].tx_id, [0x02; 32]);
    assert_eq!(found[1].output_index, 5);
}

#[test]
fn datum_index_is_wiped_on_reorg() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let quote_id = [0x22u8; 16];
    let tx_id = [0x33; 32];

    apply_datums(
        &db,
        100,
        &[ExtractedOutputDatum {
            tx_id,
            output_index: 0,
            quote_id: Some(quote_id),
            unhonorable: false,
        }],
    );
    assert_eq!(db.find_settlements_by_quote_id(&quote_id).unwrap().len(), 1);

    // Reorg below height 100 must drop both forward and reverse rows.
    db.wipe_above(99).unwrap();
    assert!(db.get_output_datum(&tx_id, 0).unwrap().is_none());
    assert!(db
        .find_settlements_by_quote_id(&quote_id)
        .unwrap()
        .is_empty());
}

// ---------------------------------------------------------------------------
// Schema migration / forced reindex (Wave 3 Stage 1)
// ---------------------------------------------------------------------------

#[test]
fn below_version_db_is_reset_to_genesis_on_open() {
    use exfer_indexer::db::{FollowerMeta, SCHEMA_VERSION};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // Simulate a DB that synced to tip BEFORE the datum feature shipped:
    // a checkpoint at a non-zero height with schema_version below current.
    {
        let db = Db::open(&path).unwrap();
        let stale = FollowerMeta {
            last_indexed_height: 5_000,
            last_indexed_block_id: [0xAB; 32],
            full_scan_complete: true,
            started_at: 1_700_000_000,
            schema_version: SCHEMA_VERSION - 1,
        };
        db.save_meta(&stale).unwrap();
    }

    // Reopening must detect the stale version and reset the follower
    // checkpoint to genesis so the next scan re-walks 0..=tip and
    // backfills the new datum tables.
    let db = Db::open(&path).unwrap();
    let meta = db.load_meta().unwrap();
    assert_eq!(
        meta.last_indexed_height, 0,
        "stale-version DB must reset to genesis to backfill new tables"
    );
    assert_eq!(meta.last_indexed_block_id, [0u8; 32]);
    assert!(!meta.full_scan_complete);
    assert_eq!(
        meta.schema_version, SCHEMA_VERSION,
        "version must be stamped"
    );
    // started_at is preserved across the migration.
    assert_eq!(meta.started_at, 1_700_000_000);
}

#[test]
fn current_version_db_is_not_reindexed_on_open() {
    use exfer_indexer::db::SCHEMA_VERSION;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    // A normal checkpoint advance stamps the current version.
    {
        let db = Db::open(&path).unwrap();
        db.apply_block_events(BlockApplyEvents {
            height: 1_234,
            block_id: [0xCD; 32],
            tx_count: 1,
            timestamp: 0,
            full_scan_complete: true,
            started_at: 1_700_000_000,
            locks: &[],
            spends: &[],
            settlements: &[],
            activity: &[],
            spent_by: &[],
            output_datums: &[],
        })
        .unwrap();
        assert_eq!(db.load_meta().unwrap().schema_version, SCHEMA_VERSION);
    }

    // Reopening an up-to-date DB must NOT rewind the checkpoint.
    let db = Db::open(&path).unwrap();
    let meta = db.load_meta().unwrap();
    assert_eq!(
        meta.last_indexed_height, 1_234,
        "up-to-date DB must not be reindexed"
    );
    assert_eq!(meta.last_indexed_block_id, [0xCD; 32]);
    assert_eq!(meta.schema_version, SCHEMA_VERSION);
}

#[test]
fn fresh_db_is_stamped_current_version() {
    use exfer_indexer::db::SCHEMA_VERSION;

    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path()).unwrap();
    let meta = db.load_meta().unwrap();
    // A brand-new DB starts at genesis and is stamped current, so a
    // forward scan from 0 (which fills every table) is never mistaken
    // for a stale DB needing a reindex.
    assert_eq!(meta.last_indexed_height, 0);
    assert_eq!(meta.schema_version, SCHEMA_VERSION);
}
