//! Contract template registry.
//!
//! Maps a 32-byte `contract_hash` — the *structural* Merkle root from
//! [`exfer::script::serialize::structural_merkle_hash`] — to a friendly
//! description: name, version, where the canonical schema lives, what
//! an MCP / UI should call it. This is the human-readable layer over
//! the anonymous bytes that flow through `settlement_by_contract`.
//!
//! ## Why a registry at all
//!
//! `contract_hash` is a deterministic template identifier: every HTLC
//! built by `covenants::htlc::htlc(sender, receiver, hash_lock, timeout)`
//! produces the *same* structural Merkle root regardless of the
//! specific parameter values. That makes `contract_hash` the natural
//! key for typed-trust queries ("how many of THIS kind of contract has
//! address X completed?") — but the indexer needs a way to tell a
//! caller that `bb…` is `"Standard HTLC v1"`, not just an opaque hash.
//!
//! ## Why it's hard-coded for now
//!
//! Open-write registries invite squatting and naming wars; admin-only
//! writes require a key-management decision the indexer shouldn't make
//! on its own. A static catalogue compiled into the binary is the
//! lowest-risk starting point: it survives reorgs, is reproducible
//! across replicas, and any addition requires a code review.
//!
//! A future change can lift this into a redb-backed table written via
//! an `--admin-token`-gated RPC; the read-side surface (`lookup`,
//! `list_all`) is designed to be source-agnostic so it won't change
//! when that happens.

use std::sync::OnceLock;

use exfer::covenants::htlc::htlc as build_htlc_program;
use exfer::script::serialize::structural_merkle_hash;
use exfer::types::Hash256;
use serde::Serialize;

/// One row in the registry.
#[derive(Debug, Clone, Serialize)]
pub struct ContractTemplate {
    /// 32-byte hex of the structural Merkle root.
    pub contract_hash: String,
    /// Short human-readable name, e.g. `"Standard HTLC v1"`.
    pub name: &'static str,
    /// One-line description aimed at MCP / UI surfaces.
    pub description: &'static str,
    /// Optional pointer to a canonical schema / spec doc.
    pub schema_url: Option<&'static str>,
}

/// Stable handle for the HTLC v1 template — the canonical
/// `covenants::htlc::htlc(…)` output, hashed structurally so every
/// instance collapses to one root. Cached after first computation so
/// hot-path lookups don't rebuild the script.
fn htlc_v1_hash() -> [u8; 32] {
    static CACHED: OnceLock<[u8; 32]> = OnceLock::new();
    *CACHED.get_or_init(|| {
        // Sentinel param values: irrelevant by construction —
        // structural_merkle_hash collapses Const(...) values out of
        // the digest. The all-zero choice keeps the constructor's
        // intermediate buffers small.
        let prog = build_htlc_program(&[0u8; 32], &[0u8; 32], &Hash256([0u8; 32]), 0);
        structural_merkle_hash(&prog).0
    })
}

const HTLC_V1_DESCRIPTION: &str = "Hash Time-Locked Contract from \
                                   exfer::covenants::htlc. Hash arm: \
                                   receiver reveals preimage + signs. \
                                   Timeout arm: sender reclaims after \
                                   the block height exceeds the lock's \
                                   `timeout_height`.";

const HTLC_V1_SCHEMA_URL: &str =
    "https://github.com/ahuman-exfer/exfer/blob/main/src/covenants/htlc.rs";

/// Look up a registered template by 32-byte contract_hash. Returns
/// None if the hash doesn't match any known template — callers should
/// surface that as "unknown contract type" rather than an error.
pub fn lookup(contract_hash: &[u8; 32]) -> Option<ContractTemplate> {
    if *contract_hash == htlc_v1_hash() {
        return Some(ContractTemplate {
            contract_hash: hex::encode(contract_hash),
            name: "Standard HTLC v1",
            description: HTLC_V1_DESCRIPTION,
            schema_url: Some(HTLC_V1_SCHEMA_URL),
        });
    }
    None
}

/// Enumerate every registered template.
pub fn list_all() -> Vec<ContractTemplate> {
    vec![ContractTemplate {
        contract_hash: hex::encode(htlc_v1_hash()),
        name: "Standard HTLC v1",
        description: HTLC_V1_DESCRIPTION,
        schema_url: Some(HTLC_V1_SCHEMA_URL),
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_returns_htlc_v1_for_canonical_hash() {
        let h = htlc_v1_hash();
        let t = lookup(&h).expect("canonical HTLC hash must resolve");
        assert_eq!(t.name, "Standard HTLC v1");
        assert_eq!(t.contract_hash.len(), 64);
    }

    #[test]
    fn lookup_returns_none_for_unknown_hash() {
        let bogus = [0xAB; 32];
        assert!(lookup(&bogus).is_none());
    }

    #[test]
    fn list_all_contains_at_least_one_template() {
        let all = list_all();
        assert!(!all.is_empty());
        assert!(all.iter().any(|t| t.name == "Standard HTLC v1"));
    }

    #[test]
    fn htlc_v1_hash_is_invariant_under_param_choice() {
        // The reason the registry can have a single canonical hash:
        // structural_merkle_hash collapses Const values. Build two
        // HTLCs with completely different params and confirm both
        // resolve to the same registry entry.
        let h_sentinel = htlc_v1_hash();
        let real_prog = build_htlc_program(
            &[0xAAu8; 32],
            &[0xBBu8; 32],
            &Hash256([0xCCu8; 32]),
            999_999,
        );
        let h_real = structural_merkle_hash(&real_prog).0;
        assert_eq!(h_sentinel, h_real);
        assert!(lookup(&h_real).is_some());
    }
}
