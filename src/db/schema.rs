//! redb table definitions for the indexer.
//!
//! Tables match the layout in the workflow plan (`B.3 — exfer-indexer
//! service` tables section). All multi-byte integer key segments are
//! big-endian so a lexicographic redb range scan matches a numeric
//! range. Cursor encoding is shared with `exfer-walletd::index` so a
//! consumer's pagination state is interchangeable across the two
//! services.

use redb::TableDefinition;

/// `() → bincode({last_indexed_height, last_indexed_block_id, started_at, full_scan_complete})`
pub const CHAIN_TIP: TableDefinition<&str, &[u8]> = TableDefinition::new("chain_tip");

/// `height_u64_be → bincode({block_id, tx_count, timestamp})`
pub const BLOCK_META: TableDefinition<&[u8], &[u8]> = TableDefinition::new("block_meta");

/// Address activity: `[address_32; height_u64_be; tx_id_32; direction_byte]` → bincode({amount, is_coinbase}).
pub const TX_BY_ADDRESS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("tx_by_address");

/// Every HTLC observed on the canonical chain. Key: `[lock_tx_id_32; output_index_u32_be]`.
pub const HTLC_FULL: TableDefinition<&[u8], &[u8]> = TableDefinition::new("htlc_full");

/// Secondary index by sender pubkey.
pub const HTLC_BY_SENDER: TableDefinition<&[u8], ()> = TableDefinition::new("htlc_by_sender");

/// Secondary index by receiver pubkey.
pub const HTLC_BY_RECEIVER: TableDefinition<&[u8], ()> = TableDefinition::new("htlc_by_receiver");

/// Secondary index by hashlock.
pub const HTLC_BY_HASHLOCK: TableDefinition<&[u8], ()> = TableDefinition::new("htlc_by_hashlock");

/// Secondary index by lifecycle state.
pub const HTLC_BY_STATE: TableDefinition<&[u8], ()> = TableDefinition::new("htlc_by_state");

/// Settlements grouped by contract type (script Merkle root).
pub const SETTLEMENT_BY_CONTRACT: TableDefinition<&[u8], &[u8]> =
    TableDefinition::new("settlement_by_contract");

/// Settlements grouped by address (mirror of `SETTLEMENT_BY_CONTRACT`).
pub const SETTLEMENT_BY_ADDRESS: TableDefinition<&[u8], ()> =
    TableDefinition::new("settlement_by_address");

/// Local cache of node's spent-by index (so we don't re-RPC for every lookup).
pub const SPENT_BY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("spent_by");

/// EXFER-QUOTE settlement-datum forward index. Key:
/// `[tx_id_32; output_index_u32_be]` (36 bytes) → bincode of
/// [`crate::db::OutputDatumRecord`]. One row per output that carries
/// *any* honor-relevant datum signal: either a strict 16-byte
/// `quote_id` (honorable) or a `datum_hash`-only commitment with no
/// inline datum (unhonorable, `[HOLE-M2]`). Outputs with no datum at
/// all are not recorded. O(1) lookup for `get_output_datum`.
pub const OUTPUT_DATUM: TableDefinition<&[u8], &[u8]> = TableDefinition::new("output_datum");

/// EXFER-QUOTE reverse index: `[quote_id_16]` (prefix) → set of
/// outpoints carrying that exact quote_id. Full key is
/// `[quote_id_16; tx_id_32; output_index_u32_be]` (52 bytes) with a
/// unit value, so a prefix range scan over `quote_id_16` yields ALL
/// outpoints for that quote_id (the swap-side gate enforces 1:1; the
/// indexer just reports the facts). Indexes ONLY strict single
/// 16-byte quote_id datums (`[HOLE-F1]`); never `datum_hash`-only or
/// oversized/malformed datums.
pub const DATUM_BY_QUOTEID: TableDefinition<&[u8], ()> = TableDefinition::new("datum_by_quoteid");

/// Sentinel key used inside CHAIN_TIP (the only "row" in that table).
pub const CHAIN_TIP_KEY: &str = "tip";

/// Open every table in a fresh write transaction. Called at startup
/// to guarantee subsequent read transactions won't trip on
/// "table not found".
pub fn open_all_tables(write: &redb::WriteTransaction) -> Result<(), redb::TableError> {
    let _ = write.open_table(CHAIN_TIP)?;
    let _ = write.open_table(BLOCK_META)?;
    let _ = write.open_table(TX_BY_ADDRESS)?;
    let _ = write.open_table(HTLC_FULL)?;
    let _ = write.open_table(HTLC_BY_SENDER)?;
    let _ = write.open_table(HTLC_BY_RECEIVER)?;
    let _ = write.open_table(HTLC_BY_HASHLOCK)?;
    let _ = write.open_table(HTLC_BY_STATE)?;
    let _ = write.open_table(SETTLEMENT_BY_CONTRACT)?;
    let _ = write.open_table(SETTLEMENT_BY_ADDRESS)?;
    let _ = write.open_table(SPENT_BY)?;
    let _ = write.open_table(OUTPUT_DATUM)?;
    let _ = write.open_table(DATUM_BY_QUOTEID)?;
    Ok(())
}
