//! redb-backed storage layer.
//!
//! Three groups of operations live here:
//!
//! 1. **Follower checkpoint** — `load_meta` / `save_meta` for the
//!    follower task's resumable position.
//! 2. **Upsert pipeline** — `apply_block_events` writes every event
//!    extracted from a block (HTLC lock / claim / reclaim, address
//!    activity, settlement) **plus** the chain-tip meta update in a
//!    single redb write transaction. Atomic: crash between blocks
//!    rolls back the partial work; crash after commit means the
//!    in-memory state matches disk.
//! 3. **Queries** — read helpers used by the JSON-RPC handlers
//!    (added in commit #14). The follower-side commit only needs
//!    a few of these for reorg recovery, so they live here too.

use std::path::Path;

use base64::Engine;
use exfer::covenants::htlc::{HtlcRecord, HtlcRole, HtlcState};
use redb::{ReadableTable, ReadableTableMetadata};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::extract::{
    AddressActivity, ExtractedHtlcLock, ExtractedHtlcSpend, ExtractedOutputDatum, HtlcSpendArm,
    SettlementRecord, QUOTE_ID_BYTES,
};

// ---------------------------------------------------------------------------
// Filter + cursor types used by query helpers
// ---------------------------------------------------------------------------

/// Filter for [`Db::list_htlcs`]. Multiple criteria apply conjunctively.
#[derive(Debug, Clone, Default)]
pub struct HtlcFilter {
    pub role: Option<HtlcRole>,
    /// Empty == any state.
    pub states: Vec<HtlcState>,
    /// Restrict to entries where `address` matches either the sender
    /// or receiver pubkey. (The indexer indexes raw pubkeys, not
    /// derived addresses — see `htlc_by_sender` / `htlc_by_receiver`
    /// secondary indexes.)
    pub address: Option<[u8; 32]>,
    pub since_height: Option<u64>,
}

/// Opaque pagination cursor for [`Db::list_htlcs`]. Encoded as
/// `base64url([height_u64_be(8); lock_tx_id(32); output_index_u32_be(4)])`.
/// 44 bytes pre-encode — same format as exfer-walletd's cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    pub lock_height: u64,
    pub lock_tx_id: [u8; 32],
    pub output_index: u32,
}

impl Cursor {
    pub fn encode(&self) -> String {
        let mut buf = [0u8; 44];
        buf[..8].copy_from_slice(&self.lock_height.to_be_bytes());
        buf[8..40].copy_from_slice(&self.lock_tx_id);
        buf[40..].copy_from_slice(&self.output_index.to_be_bytes());
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
    }

    pub fn decode(s: &str) -> Result<Self> {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|e| Error::BadParams(format!("invalid cursor: {e}")))?;
        if bytes.len() != 44 {
            return Err(Error::BadParams(format!(
                "invalid cursor: expected 44 bytes, got {}",
                bytes.len()
            )));
        }
        let lock_height = u64::from_be_bytes(bytes[..8].try_into().unwrap());
        let mut lock_tx_id = [0u8; 32];
        lock_tx_id.copy_from_slice(&bytes[8..40]);
        let output_index = u32::from_be_bytes(bytes[40..].try_into().unwrap());
        Ok(Cursor {
            lock_height,
            lock_tx_id,
            output_index,
        })
    }
}

/// Cursor for `list_settlements` / `list_address_history` — these
/// orderings key on `(block_height, tx_id)` not `(block_height,
/// tx_id, output_index)`. 40 bytes pre-encode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementCursor {
    pub height: u64,
    pub tx_id: [u8; 32],
}

impl SettlementCursor {
    pub fn encode(&self) -> String {
        let mut buf = [0u8; 40];
        buf[..8].copy_from_slice(&self.height.to_be_bytes());
        buf[8..].copy_from_slice(&self.tx_id);
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(buf)
    }

    pub fn decode(s: &str) -> Result<Self> {
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(s)
            .map_err(|e| Error::BadParams(format!("invalid cursor: {e}")))?;
        if bytes.len() != 40 {
            return Err(Error::BadParams(format!(
                "invalid cursor: expected 40 bytes, got {}",
                bytes.len()
            )));
        }
        let height = u64::from_be_bytes(bytes[..8].try_into().unwrap());
        let mut tx_id = [0u8; 32];
        tx_id.copy_from_slice(&bytes[8..]);
        Ok(SettlementCursor { height, tx_id })
    }
}

/// Identical encoding to [`SettlementCursor`]; separate type for
/// type-level distinction at the API boundary.
pub type HistoryCursor = SettlementCursor;

/// One row returned by [`Db::contract_stats`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractStats {
    pub contract_hash: [u8; 32],
    pub total: u64,
    pub succeeded: u64,
    pub refunded: u64,
    /// Sum of `block_height` across every settlement counted. The
    /// avg-settle-block ratio is computed by the RPC layer.
    pub sum_settle_blocks: u64,
    pub last_settled_at_height: Option<u64>,
}

/// One row returned by [`Db::attestation_edges`] — a single
/// (counterparty, contract_hash) pair the observed address has
/// settled HTLCs with.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttestationEdge {
    pub counterparty: [u8; 32],
    pub contract_hash: [u8; 32],
    pub total: u64,
    pub succeeded: u64,
    pub refunded: u64,
    pub last_seen_height: Option<u64>,
}

/// One group returned by [`Db::find_shared_hashlock_groups`] — a
/// hashlock that more than one tracked HTLC has been locked under.
/// On-chain, this is the canonical fingerprint of an atomic swap
/// (HTLC pair sharing a preimage commitment).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SharedHashlockGroup {
    pub hash_lock: [u8; 32],
    pub htlcs: Vec<HtlcRecord>,
}

/// One row returned by [`Db::list_address_history`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressHistoryRow {
    pub block_height: u64,
    pub tx_id: [u8; 32],
    pub amount: u64,
    pub is_input: bool,
    pub is_coinbase: bool,
    /// Addresses on the other side of this tx (senders for a received row,
    /// recipients for a spent row), self excluded. See
    /// [`crate::extract::AddressActivity::counterparties`].
    pub counterparties: Vec<[u8; 32]>,
}

fn filter_matches(rec: &HtlcRecord, f: &HtlcFilter) -> bool {
    if let Some(role) = f.role {
        if rec.role != role {
            return false;
        }
    }
    if !f.states.is_empty() && !f.states.contains(&rec.state) {
        return false;
    }
    if let Some(addr) = f.address {
        if rec.params.sender != addr && rec.params.receiver != addr {
            return false;
        }
    }
    if let Some(min) = f.since_height {
        if rec.lock_block_height.unwrap_or(u64::MAX) < min {
            return false;
        }
    }
    true
}

fn settlement_range_for(
    address: &[u8; 32],
    contract_hash: Option<&[u8; 32]>,
) -> ([u8; 104], [u8; 104]) {
    // SETTLEMENT_BY_CONTRACT key: [contract(32); address(32); height(8); tx_id(32)]
    // We scan by contract prefix if `contract_hash` is supplied;
    // otherwise the caller pre-filters by address from the by_address
    // table (this helper just shapes the lo/hi bounds).
    let mut lo = [0u8; 104];
    let mut hi = [0xFFu8; 104];
    if let Some(ch) = contract_hash {
        lo[..32].copy_from_slice(ch);
        hi[..32].copy_from_slice(ch);
        lo[32..64].copy_from_slice(address);
        hi[32..64].copy_from_slice(address);
    } else {
        // No contract filter — full table scan (still bounded by
        // observer_address inside the helper that calls us).
    }
    (lo, hi)
}

pub mod schema;

use schema::{
    BLOCK_META, CHAIN_TIP, CHAIN_TIP_KEY, DATUM_BY_QUOTEID, HTLC_BY_HASHLOCK, HTLC_BY_RECEIVER,
    HTLC_BY_SENDER, HTLC_BY_STATE, HTLC_FULL, OUTPUT_DATUM, SETTLEMENT_BY_ADDRESS,
    SETTLEMENT_BY_CONTRACT, SPENT_BY, TX_BY_ADDRESS,
};

/// One row returned by [`Db::get_output_datum`] — the indexed
/// settlement-datum signal for a single outpoint
/// (`WAVE3_HONOR_DESIGN.md` §4.0 / §6). Stored as the value of the
/// `OUTPUT_DATUM` table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputDatumRecord {
    /// `Some(quote_id)` iff the output's inline datum strict-decoded to
    /// exactly 16 bytes (`[HOLE-F1]`); `None` for `datum_hash`-only.
    pub quote_id: Option<[u8; QUOTE_ID_BYTES]>,
    /// `true` iff the output committed a datum by `datum_hash` with NO
    /// inline datum (`[HOLE-M2]`) — the indexer cannot read it, so it
    /// is unhonorable and must never become a quote match.
    pub unhonorable: bool,
    /// Block height the carrying output was first indexed at. Used by
    /// reorg recovery (`wipe_above`) to drop datum rows above the
    /// common ancestor; not part of the query surface.
    #[serde(default)]
    pub block_height: u64,
}

/// One outpoint returned by [`Db::find_settlements_by_quote_id`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatumOutpoint {
    pub tx_id: [u8; 32],
    pub output_index: u32,
}

// ---------------------------------------------------------------------------
// Follower checkpoint
// ---------------------------------------------------------------------------

/// On-disk schema/feature version stamped into [`FollowerMeta`].
///
/// Bumped whenever a follower-side feature requires re-walking already
/// scanned blocks to populate a newly added table. A DB whose persisted
/// meta carries a `schema_version` strictly below this constant is forced
/// to reindex from genesis on open (see [`Db::open`]) so the new tables
/// backfill over `0..=last_indexed_height` — the only correctness-safe
/// migration for a strict read-only indexer.
///
/// Version history:
/// - `0` — pre-datum indexer (no `OUTPUT_DATUM` / `DATUM_BY_QUOTEID`).
///   Any DB written before this field existed deserializes the missing
///   field as `0` via `#[serde(default)]`, so it is correctly treated as
///   "below current" and reindexed.
/// - `1` — EXFER-QUOTE settlement-datum indexing (`WAVE3_HONOR_DESIGN.md`
///   §4.0 / §6): adds the `OUTPUT_DATUM` + `DATUM_BY_QUOTEID` tables, which
///   must be backfilled from genesis because EXFER-QUOTE is new and no
///   historical settlement predates it (so a from-genesis re-walk indexes
///   every on-chain settlement datum that exists).
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FollowerMeta {
    pub last_indexed_height: u64,
    pub last_indexed_block_id: [u8; 32],
    pub full_scan_complete: bool,
    /// Unix seconds when the follower first started. Stable across
    /// restarts; only the very first save writes it.
    pub started_at: u64,
    /// On-disk schema/feature version. A DB synced before a follower
    /// feature shipped carries a value below [`SCHEMA_VERSION`] and is
    /// reindexed from genesis on [`Db::open`] so newly added tables
    /// backfill over already-scanned blocks.
    ///
    /// NOTE: this field was ADDED at v1, so a DB written before it existed
    /// has no bytes for it. `#[serde(default)]` does NOT recover that — the
    /// on-disk format is bincode, which is positional/non-self-describing, so
    /// a missing trailing field is an EOF, not a defaulted value. Such legacy
    /// blobs are instead caught at decode time in [`Db::migrate_schema`] and
    /// treated as `schema_version 0` (the attribute is kept only so the field
    /// would default cleanly under a self-describing format).
    #[serde(default)]
    pub schema_version: u32,
}

// ---------------------------------------------------------------------------
// Per-block snapshot (what the follower hands to apply_block_events)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMeta {
    pub block_id: [u8; 32],
    pub tx_count: u64,
    pub timestamp: u64,
}

/// Direction byte for [`TX_BY_ADDRESS`].
const DIR_OUTPUT_TO: u8 = 0x01;
const DIR_INPUT_FROM: u8 = 0x02;

// ---------------------------------------------------------------------------
// Db handle
// ---------------------------------------------------------------------------

pub struct Db {
    db: redb::Database,
}

impl Db {
    pub fn open(datadir: &Path) -> Result<Self> {
        std::fs::create_dir_all(datadir)
            .map_err(|e| Error::Storage(format!("create datadir: {e}")))?;
        let path = datadir.join("index.redb");
        let db = redb::Database::create(&path)
            .map_err(|e| Error::Storage(format!("open {}: {e}", path.display())))?;

        let write = db.begin_write()?;
        {
            schema::open_all_tables(&write)?;
        }
        write.commit()?;

        let this = Self { db };
        this.migrate_schema()?;
        Ok(this)
    }

    /// One-time, on-open schema migration. If the persisted follower
    /// checkpoint carries a `schema_version` below [`SCHEMA_VERSION`], the
    /// DB was synced before a follower feature that requires re-walking
    /// already-scanned blocks (e.g. the EXFER-QUOTE settlement-datum tables
    /// `OUTPUT_DATUM` / `DATUM_BY_QUOTEID`, added at version 1). The
    /// just-created tables would otherwise stay permanently empty for every
    /// pre-existing block, because the follower only ever indexes forward
    /// from `last_indexed_height + 1` and reorg recovery is incidental, not
    /// a migration.
    ///
    /// Fix: reset the checkpoint to genesis (height 0, zero block_id,
    /// `full_scan_complete = false`) and stamp the current version, so the
    /// next follower tick re-walks `0..=tip` and backfills the new tables
    /// over all of history. `apply_block_events` is idempotent on replay,
    /// so re-walking already-indexed blocks is safe. Stamping the version
    /// in the SAME write transaction that resets the cursor guarantees the
    /// reindex is triggered at most once: a crash before the reset commits
    /// leaves the old (below-current) version on disk and the migration is
    /// retried on the next open; a crash after commit sees the new version
    /// and does not reindex again.
    ///
    /// A brand-new DB (no persisted meta) is stamped to the current version
    /// directly — there is no history to re-walk, and the forward scan from
    /// genesis already populates every table.
    fn migrate_schema(&self) -> Result<()> {
        let read = self.db.begin_read()?;
        let persisted: Option<FollowerMeta> = {
            let table = read.open_table(CHAIN_TIP)?;
            match table.get(CHAIN_TIP_KEY)? {
                Some(blob) => match bincode::deserialize::<FollowerMeta>(blob.value()) {
                    Ok(meta) => Some(meta),
                    // A checkpoint that EXISTS but no longer decodes into the
                    // current FollowerMeta shape is a PRE-VERSIONING DB: it was
                    // written before a field was added to FollowerMeta. The
                    // `schema_version` field itself was introduced at v1, so a
                    // v0 record is a strict byte-prefix of the current layout
                    // and bincode hits "unexpected end of file" decoding it.
                    //
                    // Treat that as a legacy (schema_version 0) checkpoint that
                    // needs the from-genesis backfill below — NOT a fatal error.
                    // Returning the error here makes the binary CRASH-LOOP on
                    // every upgrade that grows Meta (you can't read the version
                    // to decide to migrate, because reading it is what fails),
                    // and the migration that exists precisely for this case
                    // never runs. The index is derived, rebuildable-from-chain
                    // data, so reconstructing from genesis is always a safe
                    // recovery for an unreadable cursor.
                    Err(_) => Some(FollowerMeta {
                        schema_version: 0,
                        ..FollowerMeta::default()
                    }),
                },
                None => None,
            }
        };
        drop(read);

        match persisted {
            // No checkpoint yet — fresh DB. Stamp the current version so a
            // forward scan from genesis (which already fills every table)
            // is never mistaken for a stale DB needing a reindex.
            None => {
                let meta = FollowerMeta {
                    schema_version: SCHEMA_VERSION,
                    ..FollowerMeta::default()
                };
                self.save_meta(&meta)?;
            }
            // Up to date — nothing to do.
            Some(meta) if meta.schema_version >= SCHEMA_VERSION => {}
            // Below current — force a from-genesis reindex/backfill so the
            // newly added tables are populated over already-scanned blocks.
            Some(meta) => {
                tracing::warn!(
                    "indexer: schema_version {} < {}; resetting follower checkpoint to \
                     genesis to backfill new tables (was synced to height {})",
                    meta.schema_version,
                    SCHEMA_VERSION,
                    meta.last_indexed_height,
                );
                let reset = FollowerMeta {
                    last_indexed_height: 0,
                    last_indexed_block_id: [0u8; 32],
                    full_scan_complete: false,
                    // Preserve the original start time across the migration.
                    started_at: meta.started_at,
                    schema_version: SCHEMA_VERSION,
                };
                self.save_meta(&reset)?;
            }
        }
        Ok(())
    }

    pub(crate) fn raw(&self) -> &redb::Database {
        &self.db
    }

    // ---- Query helpers (read-side, used by the RPC layer) ---------------

    /// Read a single HTLC record by primary key. Returns `None` if
    /// the indexer hasn't seen it (still in mempool, or pre-dates the
    /// follower's scan window).
    pub fn get_htlc(&self, lock_tx_id: &[u8; 32], output_index: u32) -> Result<Option<HtlcRecord>> {
        let read = self.db.begin_read()?;
        let t = read.open_table(HTLC_FULL)?;
        let key = htlc_primary_key(lock_tx_id, output_index);
        let bytes_opt: Option<Vec<u8>> = {
            let opt = t.get(key.as_slice())?;
            opt.map(|g| g.value().to_vec())
        };
        match bytes_opt {
            Some(b) => {
                Ok(Some(bincode::deserialize(&b).map_err(|e| {
                    Error::Storage(format!("decode htlc: {e}"))
                })?))
            }
            None => Ok(None),
        }
    }

    /// Look up an HTLC by hashlock. Returns every record whose
    /// `params.hash_lock` matches — typically one, but the protocol
    /// permits collisions.
    pub fn lookup_by_hashlock(&self, hash_lock: &[u8; 32]) -> Result<Vec<HtlcRecord>> {
        let read = self.db.begin_read()?;
        let by_hash = read.open_table(HTLC_BY_HASHLOCK)?;
        let primary = read.open_table(HTLC_FULL)?;

        let mut lo_buf = [0u8; 68];
        lo_buf[..32].copy_from_slice(hash_lock);
        let mut hi_buf = [0xFFu8; 68];
        hi_buf[..32].copy_from_slice(hash_lock);

        let mut primary_keys: Vec<[u8; 36]> = Vec::new();
        {
            let range = by_hash.range::<&[u8]>(lo_buf.as_slice()..=hi_buf.as_slice())?;
            for entry in range {
                let (k, _) = entry?;
                let bytes = k.value();
                if bytes.len() == 68 {
                    let mut pk = [0u8; 36];
                    pk.copy_from_slice(&bytes[32..]);
                    primary_keys.push(pk);
                }
            }
        }
        let mut out: Vec<HtlcRecord> = Vec::with_capacity(primary_keys.len());
        for pk in primary_keys {
            let bytes_opt: Option<Vec<u8>> = {
                let opt = primary.get(pk.as_slice())?;
                opt.map(|g| g.value().to_vec())
            };
            if let Some(b) = bytes_opt {
                out.push(
                    bincode::deserialize(&b)
                        .map_err(|e| Error::Storage(format!("decode htlc: {e}")))?,
                );
            }
        }
        Ok(out)
    }

    /// List HTLCs matching the filter, ordered by `(lock_block_height,
    /// lock_tx_id, output_index)` ascending. The walletd-side index
    /// does a primary-table scan; the indexer expects many more rows
    /// so it picks the most selective secondary index when possible.
    ///
    /// `cursor`, if given, restricts to entries strictly greater than
    /// that key; `limit` caps the returned page size.
    pub fn list_htlcs(
        &self,
        filter: &HtlcFilter,
        limit: usize,
        cursor: Option<Cursor>,
    ) -> Result<(Vec<HtlcRecord>, Option<Cursor>)> {
        let read = self.db.begin_read()?;
        let primary = read.open_table(HTLC_FULL)?;

        // The expected page size is small (≤1000) and the full table
        // is typically O(thousands) in the indexer's steady state.
        // Scan the primary table directly and filter in-process; the
        // secondary indexes are still useful for prefix-restricted
        // scans (sender / receiver / hashlock) which we use below
        // when a single-address filter is provided.
        let mut materialized: Vec<HtlcRecord> = Vec::new();
        if let Some(addr) = filter.address {
            // Walk by_sender and by_receiver prefixes, deduping by
            // primary key. Bounded scan: each secondary range yields
            // at most one entry per primary key in the relevant slice.
            let mut keys: std::collections::BTreeSet<[u8; 36]> = std::collections::BTreeSet::new();

            for table in [&HTLC_BY_SENDER, &HTLC_BY_RECEIVER] {
                let t = read.open_table(*table)?;
                let mut lo = [0u8; 76];
                lo[..32].copy_from_slice(&addr);
                let mut hi = [0xFFu8; 76];
                hi[..32].copy_from_slice(&addr);
                for entry in t.range::<&[u8]>(lo.as_slice()..=hi.as_slice())? {
                    let (k, _) = entry?;
                    if k.value().len() == 76 {
                        let mut pk = [0u8; 36];
                        pk[..32].copy_from_slice(&k.value()[40..72]);
                        pk[32..].copy_from_slice(&k.value()[72..]);
                        keys.insert(pk);
                    }
                }
            }
            for pk in keys {
                let bytes_opt: Option<Vec<u8>> = {
                    let opt = primary.get(pk.as_slice())?;
                    opt.map(|g| g.value().to_vec())
                };
                if let Some(b) = bytes_opt {
                    let rec: HtlcRecord = bincode::deserialize(&b)
                        .map_err(|e| Error::Storage(format!("decode htlc: {e}")))?;
                    materialized.push(rec);
                }
            }
        } else {
            // No address filter — full primary scan.
            let iter = primary.iter()?;
            let serialized: Vec<Vec<u8>> = {
                let mut v = Vec::new();
                for entry in iter {
                    let (_, value) = entry?;
                    v.push(value.value().to_vec());
                }
                v
            };
            for blob in &serialized {
                let rec: HtlcRecord = bincode::deserialize(blob)
                    .map_err(|e| Error::Storage(format!("decode htlc: {e}")))?;
                materialized.push(rec);
            }
        }

        // Apply remaining filters in-process.
        materialized.retain(|r| filter_matches(r, filter));

        // Sort by (lock_height, lock_tx_id, output_index).
        materialized.sort_by(|a, b| {
            let ah = a.lock_block_height.unwrap_or(u64::MAX);
            let bh = b.lock_block_height.unwrap_or(u64::MAX);
            ah.cmp(&bh)
                .then_with(|| a.lock_tx_id.cmp(&b.lock_tx_id))
                .then_with(|| a.output_index.cmp(&b.output_index))
        });

        // Cursor — drop everything <= cursor key.
        if let Some(c) = cursor {
            materialized.retain(|r| {
                let h = r.lock_block_height.unwrap_or(u64::MAX);
                (h, &r.lock_tx_id[..], r.output_index)
                    > (c.lock_height, &c.lock_tx_id[..], c.output_index)
            });
        }

        let next = if materialized.len() > limit {
            let last = &materialized[limit - 1];
            Some(Cursor {
                lock_height: last.lock_block_height.unwrap_or(u64::MAX),
                lock_tx_id: last.lock_tx_id,
                output_index: last.output_index,
            })
        } else {
            None
        };
        materialized.truncate(limit);
        Ok((materialized, next))
    }

    /// Stream settlements for an address. Ordered by
    /// `(block_height, tx_id)` ascending. Optionally restrict to a
    /// single `contract_hash` (the typed-trust query).
    pub fn list_settlements(
        &self,
        address: &[u8; 32],
        contract_hash: Option<&[u8; 32]>,
        since_height: Option<u64>,
        limit: usize,
        cursor: Option<SettlementCursor>,
    ) -> Result<(
        Vec<crate::extract::SettlementRecord>,
        Option<SettlementCursor>,
    )> {
        let read = self.db.begin_read()?;
        let by_contract = read.open_table(SETTLEMENT_BY_CONTRACT)?;

        let (lo, hi) = settlement_range_for(address, contract_hash);
        let mut rows: Vec<crate::extract::SettlementRecord> = Vec::new();
        for entry in by_contract.range::<&[u8]>(lo.as_slice()..=hi.as_slice())? {
            let (_k, v) = entry?;
            let s: crate::extract::SettlementRecord = bincode::deserialize(v.value())
                .map_err(|e| Error::Storage(format!("decode settlement: {e}")))?;
            // Filter by observer_address — the by_contract table is
            // keyed `[contract; address; height; tx_id]` and we want
            // settlements for the specific (address, contract) tuple.
            if &s.observer_address != address {
                continue;
            }
            if let Some(h) = since_height {
                if s.block_height < h {
                    continue;
                }
            }
            rows.push(s);
        }
        rows.sort_by(|a, b| {
            a.block_height
                .cmp(&b.block_height)
                .then_with(|| a.tx_id.cmp(&b.tx_id))
        });
        if let Some(c) = cursor {
            rows.retain(|s| (s.block_height, &s.tx_id[..]) > (c.height, &c.tx_id[..]));
        }
        let next = if rows.len() > limit {
            let last = &rows[limit - 1];
            Some(SettlementCursor {
                height: last.block_height,
                tx_id: last.tx_id,
            })
        } else {
            None
        };
        rows.truncate(limit);
        Ok((rows, next))
    }

    /// Aggregate stats for an address × contract pair (or all
    /// contracts if `contract_hash` is None — one row per distinct
    /// contract_hash the address has touched).
    pub fn contract_stats(
        &self,
        address: &[u8; 32],
        contract_hash: Option<&[u8; 32]>,
    ) -> Result<Vec<ContractStats>> {
        // Walk settlements and aggregate. The indexer's expected
        // steady state has settlements counted in thousands per
        // address, so a single pass is fine.
        let mut acc: std::collections::BTreeMap<[u8; 32], ContractStats> =
            std::collections::BTreeMap::new();
        // Use the by_address mirror so we don't have to scan every
        // contract on chain.
        let read = self.db.begin_read()?;
        let by_addr = read.open_table(SETTLEMENT_BY_ADDRESS)?;
        let by_contract = read.open_table(SETTLEMENT_BY_CONTRACT)?;

        let mut lo = [0u8; 104];
        lo[..32].copy_from_slice(address);
        let mut hi = [0xFFu8; 104];
        hi[..32].copy_from_slice(address);
        if let Some(ch) = contract_hash {
            lo[32..64].copy_from_slice(ch);
            hi[32..64].copy_from_slice(ch);
        }

        for entry in by_addr.range::<&[u8]>(lo.as_slice()..=hi.as_slice())? {
            let (k, _) = entry?;
            if k.value().len() != 104 {
                continue;
            }
            // Reconstruct the contract-keyed key to read the value.
            let mut contract = [0u8; 32];
            contract.copy_from_slice(&k.value()[32..64]);
            let mut contract_key = [0u8; 104];
            contract_key[..32].copy_from_slice(&contract);
            contract_key[32..64].copy_from_slice(address);
            contract_key[64..72].copy_from_slice(&k.value()[64..72]);
            contract_key[72..].copy_from_slice(&k.value()[72..]);

            let bytes_opt: Option<Vec<u8>> = {
                let opt = by_contract.get(contract_key.as_slice())?;
                opt.map(|g| g.value().to_vec())
            };
            let Some(b) = bytes_opt else { continue };
            let s: crate::extract::SettlementRecord = bincode::deserialize(&b)
                .map_err(|e| Error::Storage(format!("decode settlement: {e}")))?;
            let stats = acc.entry(contract).or_insert_with(|| ContractStats {
                contract_hash: contract,
                total: 0,
                succeeded: 0,
                refunded: 0,
                sum_settle_blocks: 0,
                last_settled_at_height: None,
            });
            stats.total += 1;
            match s.outcome {
                HtlcState::Claimed => stats.succeeded += 1,
                HtlcState::Reclaimed => stats.refunded += 1,
                _ => {}
            }
            stats.sum_settle_blocks += s.block_height;
            stats.last_settled_at_height = Some(
                stats
                    .last_settled_at_height
                    .map(|h| h.max(s.block_height))
                    .unwrap_or(s.block_height),
            );
        }

        Ok(acc.into_values().collect())
    }

    /// Per-counterparty reputation edges for an address. Walks
    /// `settlement_by_address` and, for each settled HTLC the address
    /// participated in, groups by `(counterparty, contract_hash)` —
    /// one row per distinct trading partner per contract type.
    ///
    /// The shape is intentionally narrower than [`Db::contract_stats`]
    /// (which aggregates across all counterparties): an attestation
    /// graph wants to know "with WHOM has X succeeded?", which
    /// `contract_stats` cannot answer.
    pub fn attestation_edges(
        &self,
        address: &[u8; 32],
        contract_hash: Option<&[u8; 32]>,
    ) -> Result<Vec<AttestationEdge>> {
        let mut acc: std::collections::BTreeMap<([u8; 32], [u8; 32]), AttestationEdge> =
            std::collections::BTreeMap::new();

        let read = self.db.begin_read()?;
        let by_addr = read.open_table(SETTLEMENT_BY_ADDRESS)?;
        let by_contract = read.open_table(SETTLEMENT_BY_CONTRACT)?;

        // settlement_by_address keys: [address(32); contract(32); height(8); tx(32)]
        let mut lo = [0u8; 104];
        lo[..32].copy_from_slice(address);
        let mut hi = [0xFFu8; 104];
        hi[..32].copy_from_slice(address);
        if let Some(ch) = contract_hash {
            lo[32..64].copy_from_slice(ch);
            hi[32..64].copy_from_slice(ch);
        }

        for entry in by_addr.range::<&[u8]>(lo.as_slice()..=hi.as_slice())? {
            let (k, _) = entry?;
            if k.value().len() != 104 {
                continue;
            }
            let mut contract = [0u8; 32];
            contract.copy_from_slice(&k.value()[32..64]);
            // The full payload lives in settlement_by_contract — we
            // need the counterparty + outcome fields.
            let mut contract_key = [0u8; 104];
            contract_key[..32].copy_from_slice(&contract);
            contract_key[32..64].copy_from_slice(address);
            contract_key[64..72].copy_from_slice(&k.value()[64..72]);
            contract_key[72..].copy_from_slice(&k.value()[72..]);

            let bytes_opt: Option<Vec<u8>> = {
                let opt = by_contract.get(contract_key.as_slice())?;
                opt.map(|g| g.value().to_vec())
            };
            let Some(b) = bytes_opt else { continue };
            let s: crate::extract::SettlementRecord = bincode::deserialize(&b)
                .map_err(|e| Error::Storage(format!("decode settlement: {e}")))?;

            let edge = acc
                .entry((s.counterparty, contract))
                .or_insert_with(|| AttestationEdge {
                    counterparty: s.counterparty,
                    contract_hash: contract,
                    total: 0,
                    succeeded: 0,
                    refunded: 0,
                    last_seen_height: None,
                });
            edge.total += 1;
            match s.outcome {
                HtlcState::Claimed => edge.succeeded += 1,
                HtlcState::Reclaimed => edge.refunded += 1,
                _ => {}
            }
            edge.last_seen_height = Some(
                edge.last_seen_height
                    .map(|h| h.max(s.block_height))
                    .unwrap_or(s.block_height),
            );
        }

        Ok(acc.into_values().collect())
    }

    /// Find groups of HTLCs that share a hashlock. On the public
    /// chain, the canonical reason for two HTLCs to commit to the same
    /// preimage is an atomic swap (one HTLC pays party A, the other
    /// pays party B, both unlockable by the same secret).
    ///
    /// If `hash_lock` is supplied, returns at most one group for that
    /// specific lock. Otherwise scans all hashlocks in the index and
    /// returns every multi-HTLC group, ordered lexicographically by
    /// hash_lock for stable cursors. `limit` caps the number of
    /// groups returned, not the number of HTLCs inside each group.
    pub fn find_shared_hashlock_groups(
        &self,
        hash_lock: Option<&[u8; 32]>,
        limit: usize,
    ) -> Result<Vec<SharedHashlockGroup>> {
        let read = self.db.begin_read()?;
        let by_hash = read.open_table(HTLC_BY_HASHLOCK)?;
        let primary = read.open_table(HTLC_FULL)?;

        // by_hash key: [hash_lock(32); lock_tx_id(32); output_index(4)]
        let (lo, hi): ([u8; 68], [u8; 68]) = match hash_lock {
            Some(h) => {
                let mut a = [0u8; 68];
                let mut b = [0xFFu8; 68];
                a[..32].copy_from_slice(h);
                b[..32].copy_from_slice(h);
                (a, b)
            }
            None => ([0u8; 68], [0xFFu8; 68]),
        };

        // Group primary-keys by hash_lock as we scan.
        let mut current: Option<([u8; 32], Vec<[u8; 36]>)> = None;
        let mut groups: Vec<([u8; 32], Vec<[u8; 36]>)> = Vec::new();

        for entry in by_hash.range::<&[u8]>(lo.as_slice()..=hi.as_slice())? {
            let (k, _) = entry?;
            let bytes = k.value();
            if bytes.len() != 68 {
                continue;
            }
            let mut hl = [0u8; 32];
            hl.copy_from_slice(&bytes[..32]);
            let mut pk = [0u8; 36];
            pk.copy_from_slice(&bytes[32..]);

            match &mut current {
                Some((existing_hl, pks)) if *existing_hl == hl => {
                    pks.push(pk);
                }
                _ => {
                    // Flush the previous group before starting a new one.
                    if let Some((existing_hl, pks)) = current.take() {
                        if pks.len() > 1 {
                            groups.push((existing_hl, pks));
                            if groups.len() >= limit {
                                break;
                            }
                        }
                    }
                    current = Some((hl, vec![pk]));
                }
            }
        }
        // Flush trailing group.
        if let Some((existing_hl, pks)) = current.take() {
            if pks.len() > 1 && groups.len() < limit {
                groups.push((existing_hl, pks));
            }
        }

        // Materialise each group's HtlcRecord rows.
        let mut out: Vec<SharedHashlockGroup> = Vec::with_capacity(groups.len());
        for (hl, pks) in groups {
            let mut htlcs: Vec<HtlcRecord> = Vec::with_capacity(pks.len());
            for pk in pks {
                let bytes_opt: Option<Vec<u8>> = {
                    let opt = primary.get(pk.as_slice())?;
                    opt.map(|g| g.value().to_vec())
                };
                if let Some(b) = bytes_opt {
                    htlcs.push(
                        bincode::deserialize(&b)
                            .map_err(|e| Error::Storage(format!("decode htlc: {e}")))?,
                    );
                }
            }
            // Skip groups that lost members between the index scan and
            // the primary read — paranoid but cheap.
            if htlcs.len() > 1 {
                out.push(SharedHashlockGroup {
                    hash_lock: hl,
                    htlcs,
                });
            }
        }
        Ok(out)
    }

    /// Walk every (address, height, tx_id, dir) row whose address
    /// prefix matches the requested address. Ordered by
    /// `(height, tx_id)` — ascending (oldest first) by default, or
    /// descending (newest first) when `reverse` is set. `reverse` lets a
    /// paged client fetch the most-recent rows first (and stop after N)
    /// instead of being forced to page from the very oldest, which means a
    /// heavily-used address's latest activity was unreachable behind a page
    /// cap.
    pub fn list_address_history(
        &self,
        address: &[u8; 32],
        since_height: Option<u64>,
        limit: usize,
        cursor: Option<HistoryCursor>,
        reverse: bool,
    ) -> Result<(Vec<AddressHistoryRow>, Option<HistoryCursor>)> {
        let read = self.db.begin_read()?;
        let t = read.open_table(TX_BY_ADDRESS)?;
        let mut lo = [0u8; 73];
        lo[..32].copy_from_slice(address);
        let mut hi = [0xFFu8; 73];
        hi[..32].copy_from_slice(address);

        let mut rows: Vec<AddressHistoryRow> = Vec::new();
        for entry in t.range::<&[u8]>(lo.as_slice()..=hi.as_slice())? {
            let (k, v) = entry?;
            if k.value().len() != 73 {
                continue;
            }
            let height = u64::from_be_bytes(k.value()[32..40].try_into().unwrap());
            if let Some(min) = since_height {
                if height < min {
                    continue;
                }
            }
            let mut tx_id = [0u8; 32];
            tx_id.copy_from_slice(&k.value()[40..72]);
            let dir_byte = k.value()[72];
            let val: ActivityValue = bincode::deserialize(v.value())
                .map_err(|e| Error::Storage(format!("decode activity: {e}")))?;
            rows.push(AddressHistoryRow {
                block_height: height,
                tx_id,
                amount: val.amount,
                is_input: dir_byte == DIR_INPUT_FROM,
                is_coinbase: val.is_coinbase,
                counterparties: val.counterparties,
            });
        }
        rows.sort_by(|a, b| {
            let ord = a
                .block_height
                .cmp(&b.block_height)
                .then_with(|| a.tx_id.cmp(&b.tx_id));
            if reverse {
                ord.reverse()
            } else {
                ord
            }
        });
        if let Some(c) = cursor {
            // Page forward through the chosen order: ascending keeps rows after
            // the cursor, descending keeps rows before it.
            if reverse {
                rows.retain(|r| (r.block_height, &r.tx_id[..]) < (c.height, &c.tx_id[..]));
            } else {
                rows.retain(|r| (r.block_height, &r.tx_id[..]) > (c.height, &c.tx_id[..]));
            }
        }
        let next = if rows.len() > limit {
            let last = &rows[limit - 1];
            Some(HistoryCursor {
                height: last.block_height,
                tx_id: last.tx_id,
            })
        } else {
            None
        };
        rows.truncate(limit);
        Ok((rows, next))
    }

    /// Local spent-by cache lookup. Used by the indexer's RPC layer
    /// to answer `get_output_spent_by` without round-tripping to the
    /// node every call; falls through to the node when the cache
    /// misses.
    pub fn cached_spent_by(
        &self,
        prev_tx_id: &[u8; 32],
        output_index: u32,
    ) -> Result<Option<SpentByCacheEntry>> {
        let read = self.db.begin_read()?;
        let t = read.open_table(SPENT_BY)?;
        let key = spent_by_key(prev_tx_id, output_index);
        let bytes_opt: Option<Vec<u8>> = {
            let opt = t.get(key.as_slice())?;
            opt.map(|g| g.value().to_vec())
        };
        match bytes_opt {
            Some(b) => {
                Ok(Some(bincode::deserialize(&b).map_err(|e| {
                    Error::Storage(format!("decode spent_by: {e}"))
                })?))
            }
            None => Ok(None),
        }
    }

    // ---- EXFER-QUOTE settlement-datum reads ------------------------------

    /// Forward outpoint → datum lookup (O(1)) for `get_output_datum`.
    /// Returns the indexed settlement-datum signal for `(tx_id,
    /// output_index)`: a strict 16-byte `quote_id` (honorable), an
    /// unhonorable `datum_hash`-only marker, or `None` if the indexer
    /// recorded no datum signal for that outpoint (no inline datum and
    /// no datum_hash, or an oversized/malformed inline datum that was
    /// dropped at index time per `[HOLE-F1]`).
    pub fn get_output_datum(
        &self,
        tx_id: &[u8; 32],
        output_index: u32,
    ) -> Result<Option<OutputDatumRecord>> {
        let read = self.db.begin_read()?;
        let t = read.open_table(OUTPUT_DATUM)?;
        let key = output_datum_key(tx_id, output_index);
        let bytes_opt: Option<Vec<u8>> = {
            let opt = t.get(key.as_slice())?;
            opt.map(|g| g.value().to_vec())
        };
        match bytes_opt {
            Some(b) => {
                Ok(Some(bincode::deserialize(&b).map_err(|e| {
                    Error::Storage(format!("decode output_datum: {e}"))
                })?))
            }
            None => Ok(None),
        }
    }

    /// Reverse index: every outpoint carrying this exact `quote_id`.
    /// Indexes ONLY strict single 16-byte datums (`[HOLE-F1]`), so a
    /// malformed/multi-id/`datum_hash`-only datum never appears here.
    /// If a quote_id maps to multiple outpoints, ALL are returned — the
    /// swap-side gate enforces 1:1; the indexer reports the facts.
    /// Empty vec if none.
    pub fn find_settlements_by_quote_id(
        &self,
        quote_id: &[u8; QUOTE_ID_BYTES],
    ) -> Result<Vec<DatumOutpoint>> {
        let read = self.db.begin_read()?;
        let t = read.open_table(DATUM_BY_QUOTEID)?;
        // Key layout: [quote_id(16); tx_id(32); output_index_be(4)] = 52 bytes.
        let mut lo = [0u8; 52];
        lo[..QUOTE_ID_BYTES].copy_from_slice(quote_id);
        let mut hi = [0xFFu8; 52];
        hi[..QUOTE_ID_BYTES].copy_from_slice(quote_id);

        let mut out: Vec<DatumOutpoint> = Vec::new();
        for entry in t.range::<&[u8]>(lo.as_slice()..=hi.as_slice())? {
            let (k, _) = entry?;
            let bytes = k.value();
            if bytes.len() != 52 {
                continue;
            }
            let mut tx_id = [0u8; 32];
            tx_id.copy_from_slice(&bytes[QUOTE_ID_BYTES..QUOTE_ID_BYTES + 32]);
            let output_index = u32::from_be_bytes(bytes[QUOTE_ID_BYTES + 32..].try_into().unwrap());
            out.push(DatumOutpoint {
                tx_id,
                output_index,
            });
        }
        Ok(out)
    }

    // ---- Follower checkpoint ---------------------------------------------

    pub fn load_meta(&self) -> Result<FollowerMeta> {
        let read = self.db.begin_read()?;
        let table = read.open_table(CHAIN_TIP)?;
        match table.get(CHAIN_TIP_KEY)? {
            Some(blob) => bincode::deserialize(blob.value())
                .map_err(|e| Error::Storage(format!("decode meta: {e}"))),
            None => Ok(FollowerMeta::default()),
        }
    }

    pub fn save_meta(&self, meta: &FollowerMeta) -> Result<()> {
        let blob =
            bincode::serialize(meta).map_err(|e| Error::Storage(format!("encode meta: {e}")))?;
        let write = self.db.begin_write()?;
        {
            let mut table = write.open_table(CHAIN_TIP)?;
            table.insert(CHAIN_TIP_KEY, blob.as_slice())?;
        }
        write.commit()?;
        Ok(())
    }

    /// Number of HTLCs currently in the primary table. Used by
    /// `get_indexer_status`.
    pub fn htlc_count(&self) -> Result<u64> {
        let read = self.db.begin_read()?;
        let table = read.open_table(HTLC_FULL)?;
        Ok(table.len()?)
    }

    // ---- The atomic per-block apply pipeline -----------------------------

    /// Apply every event extracted from one block in a single write
    /// transaction. The chain-tip meta is advanced inside the same
    /// transaction so a crash between blocks rolls back **everything**
    /// from the partial block; a crash after commit means the on-disk
    /// state is exactly "everything up through this block."
    ///
    /// Idempotent on replay: re-applying the same block produces the
    /// same final byte state, so a crash-and-resume mid-walk is safe.
    pub fn apply_block_events(&self, events: BlockApplyEvents<'_>) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            // ---- 1. block_meta row ----
            {
                let mut t = write.open_table(BLOCK_META)?;
                let key = events.height.to_be_bytes();
                let val = bincode::serialize(&BlockMeta {
                    block_id: events.block_id,
                    tx_count: events.tx_count,
                    timestamp: events.timestamp,
                })
                .map_err(|e| Error::Storage(format!("encode block_meta: {e}")))?;
                t.insert(key.as_slice(), val.as_slice())?;
            }

            // ---- 2. HTLC locks (new outputs) ----
            for lock in events.locks {
                upsert_htlc_within_txn(&write, &lock.record)?;
            }

            // ---- 3. HTLC spends (claims / reclaims) ----
            for spend in events.spends {
                advance_htlc_within_txn(&write, spend, events.height)?;
            }

            // ---- 4. Settlement records (one per claim / reclaim) ----
            for sett in events.settlements {
                write_settlement_within_txn(&write, sett)?;
            }

            // ---- 5. Address activity (every input + every output) ----
            for act in events.activity {
                write_activity_within_txn(&write, act, events.height)?;
            }

            // ---- 6. Optional spent_by cache ----
            for sb in events.spent_by {
                write_spent_by_within_txn(&write, sb)?;
            }

            // ---- 6b. EXFER-QUOTE settlement-datum index ----
            for od in events.output_datums {
                write_output_datum_within_txn(&write, od, events.height)?;
            }

            // ---- 7. Advance chain-tip meta ----
            {
                let mut t = write.open_table(CHAIN_TIP)?;
                let new_meta = FollowerMeta {
                    last_indexed_height: events.height,
                    last_indexed_block_id: events.block_id,
                    full_scan_complete: events.full_scan_complete,
                    started_at: events.started_at,
                    // Stamp the current version on every checkpoint advance
                    // so a migrated DB is not re-reindexed on the next open.
                    schema_version: SCHEMA_VERSION,
                };
                let blob = bincode::serialize(&new_meta)
                    .map_err(|e| Error::Storage(format!("encode meta: {e}")))?;
                t.insert(CHAIN_TIP_KEY, blob.as_slice())?;
            }
        }
        write.commit()?;
        Ok(())
    }

    // ---- Reorg support --------------------------------------------------

    /// Remove every record whose lock_block_height (or settlement
    /// block_height) is strictly greater than `keep_below`. Called
    /// from the follower's reorg recovery path.
    ///
    /// Conservative: walks each table. The indexer's expected steady
    /// state is "few hundred / few thousand entries"; the optimizer
    /// for a large reorg over a busy chain is a v0.2 concern.
    pub fn wipe_above(&self, keep_below: u64) -> Result<()> {
        let write = self.db.begin_write()?;
        {
            // Collect victim primary keys from HTLC_FULL by deserializing
            // each record and looking at lock_block_height.
            let mut htlc_victims: Vec<[u8; 36]> = Vec::new();
            let mut victim_records: Vec<HtlcRecord> = Vec::new();
            {
                let t = write.open_table(HTLC_FULL)?;
                let iter = t.iter()?;
                for entry in iter {
                    let (k, v) = entry?;
                    let rec: HtlcRecord = bincode::deserialize(v.value())
                        .map_err(|e| Error::Storage(format!("decode htlc: {e}")))?;
                    let h = rec.lock_block_height.unwrap_or(u64::MAX);
                    if h > keep_below {
                        let mut kk = [0u8; 36];
                        kk.copy_from_slice(k.value());
                        htlc_victims.push(kk);
                        victim_records.push(rec);
                    }
                }
            }
            for (key, rec) in htlc_victims.iter().zip(victim_records.iter()) {
                forget_htlc_within_txn(&write, key, rec)?;
            }

            // Wipe block_meta tail.
            let block_meta_tail = {
                let t = write.open_table(BLOCK_META)?;
                let iter = t.iter()?;
                let mut keys: Vec<[u8; 8]> = Vec::new();
                for entry in iter {
                    let (k, _) = entry?;
                    if k.value().len() == 8 {
                        let mut kk = [0u8; 8];
                        kk.copy_from_slice(k.value());
                        let h = u64::from_be_bytes(kk);
                        if h > keep_below {
                            keys.push(kk);
                        }
                    }
                }
                keys
            };
            {
                let mut t = write.open_table(BLOCK_META)?;
                for k in &block_meta_tail {
                    t.remove(k.as_slice())?;
                }
            }

            // TX_BY_ADDRESS, SETTLEMENT_*, SPENT_BY all key heights
            // big-endian. Same shape; collect-then-remove.
            wipe_tx_by_address_above(&write, keep_below)?;
            wipe_settlements_above(&write, keep_below)?;
            wipe_spent_by_above(&write, keep_below)?;
            wipe_output_datums_above(&write, keep_below)?;
        }
        write.commit()?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BlockApplyEvents — the typed payload `apply_block_events` consumes
// ---------------------------------------------------------------------------

pub struct BlockApplyEvents<'a> {
    pub height: u64,
    pub block_id: [u8; 32],
    pub tx_count: u64,
    pub timestamp: u64,
    pub full_scan_complete: bool,
    pub started_at: u64,
    pub locks: &'a [ExtractedHtlcLock],
    pub spends: &'a [ExtractedHtlcSpend],
    pub settlements: &'a [SettlementRecord],
    pub activity: &'a [AddressActivity],
    pub spent_by: &'a [SpentByCacheEntry],
    /// EXFER-QUOTE settlement-datum signals (`WAVE3_HONOR_DESIGN.md`
    /// §4.0 / §6). One per output carrying a strict 16-byte quote_id
    /// (honorable) or a `datum_hash`-only commitment (unhonorable).
    pub output_datums: &'a [ExtractedOutputDatum],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpentByCacheEntry {
    pub prev_tx_id: [u8; 32],
    pub output_index: u32,
    pub spending_tx_id: [u8; 32],
    pub input_index: u32,
    pub block_height: u64,
}

// ---------------------------------------------------------------------------
// Within-txn helpers
// ---------------------------------------------------------------------------

fn upsert_htlc_within_txn(write: &redb::WriteTransaction, rec: &HtlcRecord) -> Result<()> {
    let primary_key = htlc_primary_key(&rec.lock_tx_id, rec.output_index);
    let blob = bincode::serialize(rec).map_err(|e| Error::Storage(format!("encode htlc: {e}")))?;
    let lock_h = rec.lock_block_height.unwrap_or(u64::MAX);

    // Read prior, if any, so secondary index entries get rebuilt with
    // up-to-date state. Materialize the bytes inside the inner scope —
    // the AccessGuard returned by `get()` borrows the open table, so
    // we need to copy out before the table itself is dropped.
    let prior_bytes: Option<Vec<u8>> = {
        let t = write.open_table(HTLC_FULL)?;
        let opt = t.get(primary_key.as_slice())?;
        opt.map(|g| g.value().to_vec())
    };
    let prior: Option<HtlcRecord> = match prior_bytes {
        Some(b) => Some(
            bincode::deserialize(&b).map_err(|e| Error::Storage(format!("decode prior: {e}")))?,
        ),
        None => None,
    };

    if let Some(prev) = prior.as_ref() {
        forget_htlc_within_txn(write, &primary_key, prev)?;
    }

    // Primary
    {
        let mut t = write.open_table(HTLC_FULL)?;
        t.insert(primary_key.as_slice(), blob.as_slice())?;
    }
    // Secondaries
    {
        let mut t = write.open_table(HTLC_BY_SENDER)?;
        let k = sender_key(
            &rec.params.sender,
            lock_h,
            &rec.lock_tx_id,
            rec.output_index,
        );
        t.insert(k.as_slice(), ())?;
    }
    {
        let mut t = write.open_table(HTLC_BY_RECEIVER)?;
        let k = receiver_key(
            &rec.params.receiver,
            lock_h,
            &rec.lock_tx_id,
            rec.output_index,
        );
        t.insert(k.as_slice(), ())?;
    }
    {
        let mut t = write.open_table(HTLC_BY_HASHLOCK)?;
        let k = hashlock_key(&rec.params.hash_lock, &rec.lock_tx_id, rec.output_index);
        t.insert(k.as_slice(), ())?;
    }
    {
        let mut t = write.open_table(HTLC_BY_STATE)?;
        let k = state_key(rec.state, lock_h, &rec.lock_tx_id, rec.output_index);
        t.insert(k.as_slice(), ())?;
    }
    Ok(())
}

fn forget_htlc_within_txn(
    write: &redb::WriteTransaction,
    primary_key: &[u8; 36],
    rec: &HtlcRecord,
) -> Result<()> {
    let lock_h = rec.lock_block_height.unwrap_or(u64::MAX);
    {
        let mut t = write.open_table(HTLC_FULL)?;
        t.remove(primary_key.as_slice())?;
    }
    {
        let mut t = write.open_table(HTLC_BY_SENDER)?;
        let k = sender_key(
            &rec.params.sender,
            lock_h,
            &rec.lock_tx_id,
            rec.output_index,
        );
        let _ = t.remove(k.as_slice())?;
    }
    {
        let mut t = write.open_table(HTLC_BY_RECEIVER)?;
        let k = receiver_key(
            &rec.params.receiver,
            lock_h,
            &rec.lock_tx_id,
            rec.output_index,
        );
        let _ = t.remove(k.as_slice())?;
    }
    {
        let mut t = write.open_table(HTLC_BY_HASHLOCK)?;
        let k = hashlock_key(&rec.params.hash_lock, &rec.lock_tx_id, rec.output_index);
        let _ = t.remove(k.as_slice())?;
    }
    {
        let mut t = write.open_table(HTLC_BY_STATE)?;
        let k = state_key(rec.state, lock_h, &rec.lock_tx_id, rec.output_index);
        let _ = t.remove(k.as_slice())?;
    }
    Ok(())
}

fn advance_htlc_within_txn(
    write: &redb::WriteTransaction,
    spend: &ExtractedHtlcSpend,
    height: u64,
) -> Result<()> {
    let primary_key = htlc_primary_key(&spend.lock_tx_id, spend.output_index);
    let prior_bytes: Option<Vec<u8>> = {
        let t = write.open_table(HTLC_FULL)?;
        let opt = t.get(primary_key.as_slice())?;
        opt.map(|g| g.value().to_vec())
    };
    let mut rec: HtlcRecord = match prior_bytes {
        Some(b) => bincode::deserialize(&b)
            .map_err(|e| Error::Storage(format!("decode htlc to advance: {e}")))?,
        None => {
            // Spend references an outpoint we never recorded as a lock
            // — possible only if the lock predated the indexer's full
            // scan. Skip; the address-activity layer still records the
            // input.
            return Ok(());
        }
    };
    if matches!(rec.state, HtlcState::Claimed | HtlcState::Reclaimed) {
        // Idempotent replay — already classified.
        return Ok(());
    }

    // Drop prior secondaries before mutating.
    forget_htlc_within_txn(write, &primary_key, &rec)?;

    match &spend.arm {
        HtlcSpendArm::Claim {
            preimage,
            spending_tx_id,
            input_index,
        } => {
            rec.state = HtlcState::Claimed;
            rec.claim = Some(exfer::covenants::htlc::HtlcClaimRecord {
                tx_id: *spending_tx_id,
                preimage: preimage.clone(),
                block_height: height,
                input_index: *input_index,
            });
        }
        HtlcSpendArm::Reclaim {
            spending_tx_id,
            input_index,
        } => {
            rec.state = HtlcState::Reclaimed;
            rec.reclaim = Some(exfer::covenants::htlc::HtlcReclaimRecord {
                tx_id: *spending_tx_id,
                block_height: height,
                input_index: *input_index,
            });
        }
    }
    rec.last_indexed_height = height;
    // Indexer is multi-tenant — it observes, doesn't own keys.
    if rec.role != HtlcRole::Observer && rec.role != HtlcRole::Both {
        // Preserve whatever role was originally recorded.
    }
    upsert_htlc_within_txn(write, &rec)
}

fn write_settlement_within_txn(
    write: &redb::WriteTransaction,
    sett: &SettlementRecord,
) -> Result<()> {
    let blob =
        bincode::serialize(sett).map_err(|e| Error::Storage(format!("encode settlement: {e}")))?;
    {
        let mut t = write.open_table(SETTLEMENT_BY_CONTRACT)?;
        let k = settlement_by_contract_key(
            &sett.contract_hash,
            &sett.observer_address,
            sett.block_height,
            &sett.tx_id,
        );
        t.insert(k.as_slice(), blob.as_slice())?;
    }
    {
        let mut t = write.open_table(SETTLEMENT_BY_ADDRESS)?;
        let k = settlement_by_address_key(
            &sett.observer_address,
            &sett.contract_hash,
            sett.block_height,
            &sett.tx_id,
        );
        t.insert(k.as_slice(), ())?;
    }
    Ok(())
}

fn write_activity_within_txn(
    write: &redb::WriteTransaction,
    act: &AddressActivity,
    height: u64,
) -> Result<()> {
    let mut t = write.open_table(TX_BY_ADDRESS)?;
    let dir = if act.is_input {
        DIR_INPUT_FROM
    } else {
        DIR_OUTPUT_TO
    };
    let k = tx_by_address_key(&act.address, height, &act.tx_id, dir);
    let val = bincode::serialize(&ActivityValue {
        amount: act.amount,
        is_coinbase: act.is_coinbase,
        counterparties: act.counterparties.clone(),
    })
    .map_err(|e| Error::Storage(format!("encode activity: {e}")))?;
    t.insert(k.as_slice(), val.as_slice())?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivityValue {
    amount: u64,
    is_coinbase: bool,
    #[serde(default)]
    counterparties: Vec<[u8; 32]>,
}

fn write_spent_by_within_txn(write: &redb::WriteTransaction, sb: &SpentByCacheEntry) -> Result<()> {
    let mut t = write.open_table(SPENT_BY)?;
    let key = spent_by_key(&sb.prev_tx_id, sb.output_index);
    let val =
        bincode::serialize(sb).map_err(|e| Error::Storage(format!("encode spent_by: {e}")))?;
    t.insert(key.as_slice(), val.as_slice())?;
    Ok(())
}

/// Write one settlement-datum row into both the forward
/// (`OUTPUT_DATUM`) and reverse (`DATUM_BY_QUOTEID`) indexes. The
/// reverse index is populated ONLY for strict 16-byte quote_ids
/// (`[HOLE-F1]`); `datum_hash`-only (unhonorable) rows live in the
/// forward index alone so `get_output_datum` can flag them while
/// `find_settlements_by_quote_id` never produces them as candidates.
/// Idempotent on replay: re-inserting the same key is a no-op overwrite.
fn write_output_datum_within_txn(
    write: &redb::WriteTransaction,
    od: &ExtractedOutputDatum,
    height: u64,
) -> Result<()> {
    let rec = OutputDatumRecord {
        quote_id: od.quote_id,
        unhonorable: od.unhonorable,
        block_height: height,
    };
    let blob = bincode::serialize(&rec)
        .map_err(|e| Error::Storage(format!("encode output_datum: {e}")))?;
    {
        let mut t = write.open_table(OUTPUT_DATUM)?;
        let key = output_datum_key(&od.tx_id, od.output_index);
        t.insert(key.as_slice(), blob.as_slice())?;
    }
    if let Some(quote_id) = od.quote_id {
        let mut t = write.open_table(DATUM_BY_QUOTEID)?;
        let key = datum_by_quoteid_key(&quote_id, &od.tx_id, od.output_index);
        t.insert(key.as_slice(), ())?;
    }
    Ok(())
}

fn wipe_output_datums_above(write: &redb::WriteTransaction, keep_below: u64) -> Result<()> {
    // Forward table values carry block_height; collect victims, then
    // drop the matching reverse-index rows (keyed by quote_id) too.
    let victims: Vec<([u8; 36], OutputDatumRecord)> = {
        let t = write.open_table(OUTPUT_DATUM)?;
        let mut v = Vec::new();
        for entry in t.iter()? {
            let (k, val) = entry?;
            if k.value().len() != 36 {
                continue;
            }
            let rec: OutputDatumRecord = bincode::deserialize(val.value())
                .map_err(|e| Error::Storage(format!("decode output_datum: {e}")))?;
            if rec.block_height > keep_below {
                let mut kk = [0u8; 36];
                kk.copy_from_slice(k.value());
                v.push((kk, rec));
            }
        }
        v
    };
    {
        let mut t = write.open_table(OUTPUT_DATUM)?;
        for (k, _) in &victims {
            t.remove(k.as_slice())?;
        }
    }
    {
        let mut t = write.open_table(DATUM_BY_QUOTEID)?;
        for (k, rec) in &victims {
            if let Some(quote_id) = rec.quote_id {
                let mut tx_id = [0u8; 32];
                tx_id.copy_from_slice(&k[..32]);
                let output_index = u32::from_be_bytes(k[32..].try_into().unwrap());
                let rkey = datum_by_quoteid_key(&quote_id, &tx_id, output_index);
                let _ = t.remove(rkey.as_slice())?;
            }
        }
    }
    Ok(())
}

fn wipe_tx_by_address_above(write: &redb::WriteTransaction, keep_below: u64) -> Result<()> {
    let victims = {
        let t = write.open_table(TX_BY_ADDRESS)?;
        let mut v: Vec<Vec<u8>> = Vec::new();
        for entry in t.iter()? {
            let (k, _) = entry?;
            // Layout: address (32) || height_be (8) || tx_id (32) || dir (1)
            if k.value().len() >= 40 {
                let h = u64::from_be_bytes(k.value()[32..40].try_into().unwrap());
                if h > keep_below {
                    v.push(k.value().to_vec());
                }
            }
        }
        v
    };
    let mut t = write.open_table(TX_BY_ADDRESS)?;
    for k in &victims {
        t.remove(k.as_slice())?;
    }
    Ok(())
}

fn wipe_settlements_above(write: &redb::WriteTransaction, keep_below: u64) -> Result<()> {
    // SETTLEMENT_BY_CONTRACT key: contract (32) || address (32) || height (8) || tx_id (32)
    let victims_c = {
        let t = write.open_table(SETTLEMENT_BY_CONTRACT)?;
        let mut v: Vec<Vec<u8>> = Vec::new();
        for entry in t.iter()? {
            let (k, _) = entry?;
            if k.value().len() >= 72 {
                let h = u64::from_be_bytes(k.value()[64..72].try_into().unwrap());
                if h > keep_below {
                    v.push(k.value().to_vec());
                }
            }
        }
        v
    };
    {
        let mut t = write.open_table(SETTLEMENT_BY_CONTRACT)?;
        for k in &victims_c {
            t.remove(k.as_slice())?;
        }
    }
    // SETTLEMENT_BY_ADDRESS key: address (32) || contract (32) || height (8) || tx_id (32)
    let victims_a = {
        let t = write.open_table(SETTLEMENT_BY_ADDRESS)?;
        let mut v: Vec<Vec<u8>> = Vec::new();
        for entry in t.iter()? {
            let (k, _) = entry?;
            if k.value().len() >= 72 {
                let h = u64::from_be_bytes(k.value()[64..72].try_into().unwrap());
                if h > keep_below {
                    v.push(k.value().to_vec());
                }
            }
        }
        v
    };
    {
        let mut t = write.open_table(SETTLEMENT_BY_ADDRESS)?;
        for k in &victims_a {
            t.remove(k.as_slice())?;
        }
    }
    Ok(())
}

fn wipe_spent_by_above(write: &redb::WriteTransaction, keep_below: u64) -> Result<()> {
    let victims = {
        let t = write.open_table(SPENT_BY)?;
        let mut v: Vec<Vec<u8>> = Vec::new();
        for entry in t.iter()? {
            let (k, val) = entry?;
            let sb: SpentByCacheEntry = bincode::deserialize(val.value())
                .map_err(|e| Error::Storage(format!("decode spent_by: {e}")))?;
            if sb.block_height > keep_below {
                v.push(k.value().to_vec());
            }
        }
        v
    };
    let mut t = write.open_table(SPENT_BY)?;
    for k in &victims {
        t.remove(k.as_slice())?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Key encodings
// ---------------------------------------------------------------------------

fn htlc_primary_key(lock_tx_id: &[u8; 32], output_index: u32) -> [u8; 36] {
    let mut k = [0u8; 36];
    k[..32].copy_from_slice(lock_tx_id);
    k[32..].copy_from_slice(&output_index.to_be_bytes());
    k
}

fn sender_key(
    sender: &[u8; 32],
    lock_height: u64,
    lock_tx_id: &[u8; 32],
    output_index: u32,
) -> [u8; 76] {
    composite_key_address_height(sender, lock_height, lock_tx_id, output_index)
}

fn receiver_key(
    receiver: &[u8; 32],
    lock_height: u64,
    lock_tx_id: &[u8; 32],
    output_index: u32,
) -> [u8; 76] {
    composite_key_address_height(receiver, lock_height, lock_tx_id, output_index)
}

fn composite_key_address_height(
    addr: &[u8; 32],
    height: u64,
    tx_id: &[u8; 32],
    output_index: u32,
) -> [u8; 76] {
    let mut k = [0u8; 76];
    k[..32].copy_from_slice(addr);
    k[32..40].copy_from_slice(&height.to_be_bytes());
    k[40..72].copy_from_slice(tx_id);
    k[72..].copy_from_slice(&output_index.to_be_bytes());
    k
}

fn hashlock_key(hash_lock: &[u8; 32], tx_id: &[u8; 32], output_index: u32) -> [u8; 68] {
    let mut k = [0u8; 68];
    k[..32].copy_from_slice(hash_lock);
    k[32..64].copy_from_slice(tx_id);
    k[64..].copy_from_slice(&output_index.to_be_bytes());
    k
}

fn state_key(
    state: HtlcState,
    lock_height: u64,
    lock_tx_id: &[u8; 32],
    output_index: u32,
) -> [u8; 45] {
    let mut k = [0u8; 45];
    k[0] = match state {
        HtlcState::Locked => 0x00,
        HtlcState::LockedExpired => 0x01,
        HtlcState::Claimed => 0x02,
        HtlcState::Reclaimed => 0x03,
        HtlcState::Unknown => 0x04,
    };
    k[1..9].copy_from_slice(&lock_height.to_be_bytes());
    k[9..41].copy_from_slice(lock_tx_id);
    k[41..].copy_from_slice(&output_index.to_be_bytes());
    k
}

fn tx_by_address_key(address: &[u8; 32], height: u64, tx_id: &[u8; 32], dir: u8) -> [u8; 73] {
    let mut k = [0u8; 73];
    k[..32].copy_from_slice(address);
    k[32..40].copy_from_slice(&height.to_be_bytes());
    k[40..72].copy_from_slice(tx_id);
    k[72] = dir;
    k
}

fn settlement_by_contract_key(
    contract: &[u8; 32],
    address: &[u8; 32],
    height: u64,
    tx_id: &[u8; 32],
) -> [u8; 104] {
    let mut k = [0u8; 104];
    k[..32].copy_from_slice(contract);
    k[32..64].copy_from_slice(address);
    k[64..72].copy_from_slice(&height.to_be_bytes());
    k[72..].copy_from_slice(tx_id);
    k
}

fn settlement_by_address_key(
    address: &[u8; 32],
    contract: &[u8; 32],
    height: u64,
    tx_id: &[u8; 32],
) -> [u8; 104] {
    let mut k = [0u8; 104];
    k[..32].copy_from_slice(address);
    k[32..64].copy_from_slice(contract);
    k[64..72].copy_from_slice(&height.to_be_bytes());
    k[72..].copy_from_slice(tx_id);
    k
}

fn spent_by_key(prev_tx_id: &[u8; 32], output_index: u32) -> [u8; 36] {
    htlc_primary_key(prev_tx_id, output_index)
}

/// `OUTPUT_DATUM` key: `[tx_id(32); output_index_be(4)]` — same shape
/// as the HTLC/spent-by outpoint key.
fn output_datum_key(tx_id: &[u8; 32], output_index: u32) -> [u8; 36] {
    htlc_primary_key(tx_id, output_index)
}

/// `DATUM_BY_QUOTEID` key: `[quote_id(16); tx_id(32); output_index_be(4)]`.
/// A prefix range over the leading 16 bytes yields every outpoint for
/// that quote_id.
fn datum_by_quoteid_key(
    quote_id: &[u8; QUOTE_ID_BYTES],
    tx_id: &[u8; 32],
    output_index: u32,
) -> [u8; 52] {
    let mut k = [0u8; 52];
    k[..QUOTE_ID_BYTES].copy_from_slice(quote_id);
    k[QUOTE_ID_BYTES..QUOTE_ID_BYTES + 32].copy_from_slice(tx_id);
    k[QUOTE_ID_BYTES + 32..].copy_from_slice(&output_index.to_be_bytes());
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    // Regression for the upgrade crash "decode meta: unexpected end of file".
    // A DB written before the `schema_version` field existed stores a
    // FollowerMeta blob that is a strict byte-PREFIX of the current layout, so
    // bincode hits EOF decoding it into today's struct. open() must treat that
    // undecodable-but-present checkpoint as a legacy (v0) DB and reindex from
    // genesis — NOT propagate the decode error and crash-loop the binary.
    #[test]
    fn pre_versioning_meta_migrates_instead_of_crashing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Create the DB + tables, then overwrite CHAIN_TIP with a v0-layout
        // blob: every FollowerMeta field in order EXCEPT the trailing
        // schema_version u32 that did not exist yet.
        {
            let db = Db::open(&path).unwrap();
            let legacy = bincode::serialize(&(
                7_777_u64,    // last_indexed_height
                [0x11u8; 32], // last_indexed_block_id
                true,         // full_scan_complete
                1_700_000_000u64, // started_at
                              // (no schema_version — pre-versioning layout)
            ))
            .unwrap();
            // The blob really is undecodable as the current struct (the bug).
            assert!(
                bincode::deserialize::<FollowerMeta>(&legacy).is_err(),
                "test premise: the v0 blob must fail to decode as current FollowerMeta"
            );
            let write = db.raw().begin_write().unwrap();
            {
                let mut t = write.open_table(CHAIN_TIP).unwrap();
                t.insert(CHAIN_TIP_KEY, legacy.as_slice()).unwrap();
            }
            write.commit().unwrap();
        }

        // Reopen: must NOT error; must reset to genesis to backfill, and stamp
        // the current version so it migrates at most once.
        let db = Db::open(&path).unwrap();
        let meta = db.load_meta().unwrap();
        assert_eq!(
            meta.last_indexed_height, 0,
            "legacy DB must reset to genesis to backfill new tables"
        );
        assert!(!meta.full_scan_complete);
        assert_eq!(
            meta.schema_version, SCHEMA_VERSION,
            "current version must be stamped after migration"
        );

        // And it stays migrated on a subsequent open (no repeat reindex).
        drop(db);
        let db = Db::open(&path).unwrap();
        assert_eq!(db.load_meta().unwrap().schema_version, SCHEMA_VERSION);
    }
}
