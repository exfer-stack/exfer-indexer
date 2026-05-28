//! Block-level extraction helpers — turn raw transactions into the
//! tagged events the indexer persists.
//!
//! Real implementation in commit #13 alongside the follower.

// Intentionally empty for the scaffold commit. The follower commit
// adds:
//   - extract_htlcs(&Transaction)  → Vec<ExtractedHtlc>
//   - extract_spends(&Transaction) → Vec<ExtractedSpend>
//   - extract_address_activity(&Block) → Vec<AddressActivity>
//   - compute_contract_hash(&Program) → Hash256
