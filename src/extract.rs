//! Block-level extraction: parse a fetched block + its transactions
//! into the tagged events the indexer persists.
//!
//! No I/O. Pure functions of `(Block, Vec<Transaction>)` → events.
//! The follower handles fetching; this module handles interpretation.

use exfer::covenants::htlc::{try_parse_htlc, HtlcParams, HtlcRecord, HtlcRole, HtlcState};
use exfer::script::serialize::{deserialize_program, structural_merkle_hash};
use exfer::types::transaction::Transaction;
use exfer::types::Hash256;
use serde::{Deserialize, Serialize};

/// Smallest plausible HTLC output script in bytes. Anything below
/// this is definitely a vanilla P2PKH (32-byte locking script) or
/// similar, so we skip the parse attempt entirely.
pub const MIN_HTLC_SCRIPT_BYTES: usize = 100;

/// Wire-format byte for `Value::Left(_)` — claim arm of an HTLC witness.
pub const VALUE_TAG_LEFT: u8 = 0x01;
/// Wire-format byte for `Value::Right(_)` — reclaim arm.
pub const VALUE_TAG_RIGHT: u8 = 0x02;
/// Wire-format byte for `Value::Unit`.
pub const VALUE_TAG_UNIT: u8 = 0x00;
/// Wire-format byte for `Value::Bytes(_)`.
pub const VALUE_TAG_BYTES: u8 = 0x05;

// ---------------------------------------------------------------------------
// Extracted event types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ExtractedHtlcLock {
    pub record: HtlcRecord,
    /// Convenience: the script bytes themselves. Used by the
    /// follower to compute the contract hash and to optionally
    /// pre-warm caches.
    pub script: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ExtractedHtlcSpend {
    pub lock_tx_id: [u8; 32],
    pub output_index: u32,
    pub arm: HtlcSpendArm,
}

#[derive(Debug, Clone)]
pub enum HtlcSpendArm {
    Claim {
        preimage: Vec<u8>,
        spending_tx_id: [u8; 32],
        input_index: u32,
    },
    Reclaim {
        spending_tx_id: [u8; 32],
        input_index: u32,
    },
}

/// One row for the address-activity table. `address` is the
/// 32-byte script (Phase-1 P2PKH outputs lock to the pubkey-hash
/// directly, so that hash IS the address). Non-Phase-1 outputs are
/// skipped: they don't have a single canonical "address" string.
#[derive(Debug, Clone)]
pub struct AddressActivity {
    pub address: [u8; 32],
    pub tx_id: [u8; 32],
    pub amount: u64,
    pub is_input: bool,
    pub is_coinbase: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementRecord {
    pub tx_id: [u8; 32],
    pub block_height: u64,
    pub contract_hash: [u8; 32],
    pub outcome: HtlcState,
    pub observer_address: [u8; 32],
    pub counterparty: [u8; 32],
    pub amount: u64,
    pub lock_tx_id: [u8; 32],
    pub lock_output_index: u32,
}

// ---------------------------------------------------------------------------
// Top-level extraction
// ---------------------------------------------------------------------------

/// Best-effort extraction. Anything we can't classify is silently
/// skipped — the indexer never crashes on malformed chain data.
pub fn extract_from_tx(tx: &Transaction, height: u64, last_indexed_height: u64) -> ExtractedTx {
    let tx_id = match tx.tx_id() {
        Ok(t) => *t.as_bytes(),
        Err(_) => return ExtractedTx::default(),
    };

    let mut locks: Vec<ExtractedHtlcLock> = Vec::new();
    let mut spends: Vec<ExtractedHtlcSpend> = Vec::new();
    let mut activity: Vec<AddressActivity> = Vec::new();

    let is_coinbase = tx.is_coinbase();

    // ---- Outputs ----
    for (vout, output) in tx.outputs.iter().enumerate() {
        // Address activity: Phase-1 P2PKH outputs use a 32-byte
        // pubkey-hash script. Anything else (covenants, HTLC scripts)
        // is not an "address" — record only Phase-1 here.
        if output.script.len() == 32 {
            let mut addr = [0u8; 32];
            addr.copy_from_slice(&output.script);
            activity.push(AddressActivity {
                address: addr,
                tx_id,
                amount: output.value,
                is_input: false,
                is_coinbase,
            });
        }

        if output.script.len() >= MIN_HTLC_SCRIPT_BYTES {
            if let Some(params) = try_parse_htlc(&output.script) {
                let state = if height > params.timeout_height {
                    HtlcState::LockedExpired
                } else {
                    HtlcState::Locked
                };
                locks.push(ExtractedHtlcLock {
                    record: HtlcRecord {
                        lock_tx_id: tx_id,
                        output_index: vout as u32,
                        params: HtlcParams {
                            sender: params.sender,
                            receiver: params.receiver,
                            hash_lock: params.hash_lock,
                            timeout_height: params.timeout_height,
                        },
                        amount: output.value,
                        lock_block_height: Some(height),
                        state,
                        claim: None,
                        reclaim: None,
                        role: HtlcRole::Observer,
                        last_indexed_height: height,
                    },
                    script: output.script.clone(),
                });
            }
        }
    }

    // ---- Inputs ----
    for (vin, input) in tx.inputs.iter().enumerate() {
        if is_coinbase {
            continue;
        }

        // Spend interpretation — if the witness's first byte is Left
        // or Right, we treat it as a *candidate* HTLC spend. The
        // Db::advance_htlc_within_txn step further filters by whether
        // we actually have a tracked HTLC at that outpoint, so a
        // non-HTLC witness with a coincidentally-matching first byte
        // is a no-op.
        let witness = tx
            .witnesses
            .get(vin)
            .map(|w| w.witness.as_slice())
            .unwrap_or(&[]);
        if let Some(arm) = classify_spend_arm(witness, tx_id, vin as u32) {
            spends.push(ExtractedHtlcSpend {
                lock_tx_id: *input.prev_tx_id.as_bytes(),
                output_index: input.output_index,
                arm,
            });
        }

        // Address activity for the input side requires the prev
        // output's script to know which address is being spent. The
        // follower fills that in (it already has the prev tx by the
        // time it sees this one); record a placeholder here only if
        // we DO have the prev script readily available. To keep this
        // pure we leave that join to the follower stage.

        let _ = last_indexed_height; // reserved for future "ignore re-org tail" optimization
    }

    ExtractedTx {
        tx_id,
        locks,
        spends,
        activity,
    }
}

#[derive(Debug, Default)]
pub struct ExtractedTx {
    pub tx_id: [u8; 32],
    pub locks: Vec<ExtractedHtlcLock>,
    pub spends: Vec<ExtractedHtlcSpend>,
    pub activity: Vec<AddressActivity>,
}

/// Detect a Left- or Right-arm spend from the witness bytes. The
/// preimage extracted from a Left arm is **variable length** — that's
/// the upstream-fixed bug from PR #20 review.
pub fn classify_spend_arm(
    witness: &[u8],
    spending_tx_id: [u8; 32],
    input_index: u32,
) -> Option<HtlcSpendArm> {
    if witness.len() < 2 {
        return None;
    }
    match witness[0] {
        VALUE_TAG_LEFT => extract_claim_preimage(witness).map(|preimage| HtlcSpendArm::Claim {
            preimage,
            spending_tx_id,
            input_index,
        }),
        VALUE_TAG_RIGHT => Some(HtlcSpendArm::Reclaim {
            spending_tx_id,
            input_index,
        }),
        _ => None,
    }
}

/// Witness layout for a claim:
/// `0x01 0x00 0x05 len_u32_le preimage(len) 0x05 len_u32_le sig`.
/// Returns the preimage of declared length (any length is valid per
/// the HTLC hash arm — see PR #20 review).
fn extract_claim_preimage(witness: &[u8]) -> Option<Vec<u8>> {
    if witness.len() < 7 {
        return None;
    }
    if witness[0] != VALUE_TAG_LEFT || witness[1] != VALUE_TAG_UNIT || witness[2] != VALUE_TAG_BYTES
    {
        return None;
    }
    let len = u32::from_le_bytes(witness[3..7].try_into().ok()?) as usize;
    if witness.len() < 7 + len {
        return None;
    }
    Some(witness[7..7 + len].to_vec())
}

// ---------------------------------------------------------------------------
// Contract hash + settlement construction
// ---------------------------------------------------------------------------

/// Compute the contract-hash for an HTLC output script — the
/// **structural** Merkle root of the deserialized program, with
/// `Const(_)` value bytes blinded.
///
/// Two HTLCs built from the same template — `covenants::htlc::htlc(...)`
/// — but with different sender/receiver/hashlock/timeout produce the
/// same contract_hash. That's the key invariant the
/// `settlement_by_contract` table groups on: "all locks of this kind",
/// not "this specific instance".
///
/// This is **not** the on-chain commitment used to authorise spends —
/// that's [`exfer::script::serialize::merkle_hash`] of the raw script
/// bytes. The indexer uses the structural variant on purpose: as an
/// application-layer template identifier, not a consensus commitment.
pub fn contract_hash_of_script(script: &[u8]) -> Option<Hash256> {
    deserialize_program(script)
        .ok()
        .map(|p| structural_merkle_hash(&p))
}

/// Build a [`SettlementRecord`] from an HTLC that just transitioned
/// to a settled state. The `observer_address` is whichever party's
/// address is most meaningful from this side's perspective; for the
/// indexer (multi-tenant, no owned keys), we record one row per side
/// — see [`settlements_for_settled_htlc`].
pub fn settlements_for_settled_htlc(rec: &HtlcRecord, block_height: u64) -> Vec<SettlementRecord> {
    // Compute contract_hash via the structural variant: identical
    // template, different params → identical hash. We reconstruct the
    // canonical program from the parsed params (the raw script bytes
    // aren't on the record, by design), then take its structural
    // Merkle root. Every HTLC settled on chain — whatever its specific
    // sender/receiver/hashlock/timeout — collapses to the single
    // "Standard HTLC v1" template hash here.
    let program = exfer::covenants::htlc::htlc(
        &rec.params.sender,
        &rec.params.receiver,
        &Hash256(rec.params.hash_lock),
        rec.params.timeout_height,
    );
    let contract_hash = structural_merkle_hash(&program).0;

    let outcome = rec.state;
    let tx_id = settled_tx_id(rec).unwrap_or(rec.lock_tx_id);
    vec![
        // Sender's perspective: counterparty = receiver.
        SettlementRecord {
            tx_id,
            block_height,
            contract_hash,
            outcome,
            observer_address: derive_address_from_pubkey(&rec.params.sender),
            counterparty: derive_address_from_pubkey(&rec.params.receiver),
            amount: rec.amount,
            lock_tx_id: rec.lock_tx_id,
            lock_output_index: rec.output_index,
        },
        // Receiver's perspective: counterparty = sender.
        SettlementRecord {
            tx_id,
            block_height,
            contract_hash,
            outcome,
            observer_address: derive_address_from_pubkey(&rec.params.receiver),
            counterparty: derive_address_from_pubkey(&rec.params.sender),
            amount: rec.amount,
            lock_tx_id: rec.lock_tx_id,
            lock_output_index: rec.output_index,
        },
    ]
}

fn settled_tx_id(rec: &HtlcRecord) -> Option<[u8; 32]> {
    if let Some(ref c) = rec.claim {
        return Some(c.tx_id);
    }
    if let Some(ref r) = rec.reclaim {
        return Some(r.tx_id);
    }
    None
}

/// Derive the on-chain address (32-byte Phase-1 P2PKH hash) for a
/// 32-byte pubkey. Mirrors `exfer::types::transaction::TxOutput::
/// pubkey_hash_from_key`.
pub fn derive_address_from_pubkey(pubkey: &[u8; 32]) -> [u8; 32] {
    use exfer::types::transaction::TxOutput;
    let h = TxOutput::pubkey_hash_from_key(pubkey);
    *h.as_bytes()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use exfer::script::value::Value;

    #[test]
    fn classify_left_arm_extracts_variable_length_preimage() {
        for len in [1usize, 5, 29, 32, 33, 100] {
            let preimage: Vec<u8> = (0..len).map(|i| (i % 256) as u8).collect();
            let mut w = Value::Left(Box::new(Value::Unit)).serialize();
            w.extend_from_slice(&Value::Bytes(preimage.clone()).serialize());
            w.extend_from_slice(&Value::Bytes(vec![0u8; 64]).serialize());
            let arm = classify_spend_arm(&w, [0xAB; 32], 0).expect("must classify");
            match arm {
                HtlcSpendArm::Claim {
                    preimage: got_preimage,
                    ..
                } => assert_eq!(got_preimage, preimage),
                _ => panic!("expected Claim arm, got {arm:?}"),
            }
        }
    }

    #[test]
    fn classify_right_arm_yields_reclaim() {
        let mut w = Value::Right(Box::new(Value::Unit)).serialize();
        w.extend_from_slice(&Value::Bytes(vec![0u8; 64]).serialize());
        let arm = classify_spend_arm(&w, [0xCC; 32], 3).expect("must classify");
        match arm {
            HtlcSpendArm::Reclaim {
                spending_tx_id,
                input_index,
            } => {
                assert_eq!(spending_tx_id, [0xCC; 32]);
                assert_eq!(input_index, 3);
            }
            _ => panic!("expected Reclaim, got {arm:?}"),
        }
    }

    #[test]
    fn classify_unknown_first_byte_returns_none() {
        let bogus = vec![0xAA, 0x00, 0x00, 0x00];
        assert!(classify_spend_arm(&bogus, [0; 32], 0).is_none());
        assert!(classify_spend_arm(&[], [0; 32], 0).is_none());
    }

    #[test]
    fn contract_hash_is_template_keyed_not_instance_keyed() {
        // The whole point of the structural variant: every HTLC built
        // from `covenants::htlc::htlc(...)` collapses to one hash,
        // regardless of which specific parameter values were baked in.
        use exfer::script::serialize::serialize_program;

        let sender_a = [0x11u8; 32];
        let receiver_a = [0x22u8; 32];
        let hash_lock_a = Hash256([0x33u8; 32]);
        let prog_a = exfer::covenants::htlc::htlc(&sender_a, &receiver_a, &hash_lock_a, 1000);
        let script_a = serialize_program(&prog_a);

        let a = contract_hash_of_script(&script_a).unwrap();
        let b = contract_hash_of_script(&script_a).unwrap();
        assert_eq!(a, b, "deterministic");

        // Different timeout — still the same template.
        let prog_t = exfer::covenants::htlc::htlc(&sender_a, &receiver_a, &hash_lock_a, 2000);
        let script_t = serialize_program(&prog_t);
        let c = contract_hash_of_script(&script_t).unwrap();
        assert_eq!(a, c, "different timeout must NOT change template hash");

        // Different sender / receiver / hashlock — still the same template.
        let prog_p =
            exfer::covenants::htlc::htlc(&[0xAAu8; 32], &[0xBBu8; 32], &Hash256([0xCCu8; 32]), 42);
        let script_p = serialize_program(&prog_p);
        let d = contract_hash_of_script(&script_p).unwrap();
        assert_eq!(a, d, "different params must NOT change template hash");
    }

    #[test]
    fn contract_hash_rejects_garbage() {
        assert!(contract_hash_of_script(&[]).is_none());
        assert!(contract_hash_of_script(&[0xAA, 0xBB]).is_none());
    }
}
