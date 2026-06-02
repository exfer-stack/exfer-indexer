//! Block follower — populates the indexer's redb store.
//!
//! Long-running tokio task spawned at boot. Every [`Config::poll_interval`]
//! it asks the upstream node for the current tip; whenever the tip
//! moves it walks every newly-canonical block, fetches each
//! transaction, hands the body to [`crate::extract`], and writes the
//! resulting events through [`Db::apply_block_events`].
//!
//! Differences from `exfer-walletd::follower`:
//!
//! 1. **Indexes every HTLC**, not just owned ones — every record
//!    starts with `role = Observer`.
//! 2. **Computes a `contract_hash` per HTLC** so settlements can be
//!    grouped by template ("how many of THIS kind of contract has
//!    address X completed?").
//! 3. **Writes a row per (output recipient, every input spender)
//!    into `tx_by_address`** so the `get_address_history` query is
//!    O(history-size), not O(chain-size).
//! 4. **Caches node spent-by lookups** so an MCP-side reputation
//!    query doesn't always round-trip to the node.
//!
//! Reorg handling: identical to walletd's — verify the previously-
//! indexed block_id at last_indexed_height still matches the node,
//! walk back to the common ancestor on mismatch, wipe everything
//! above, re-walk forward.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use exfer::types::transaction::Transaction;
use tokio::sync::watch;

use crate::config::Config;
use crate::db::{BlockApplyEvents, Db, SpentByCacheEntry};
use crate::error::{Error, Result};
use crate::extract::{
    self, AddressActivity, ExtractedHtlcLock, ExtractedHtlcSpend, SettlementRecord,
};
use crate::upstream::{BlockSummary, NodeClient};

pub struct Follower {
    db: Arc<Db>,
    node: NodeClient,
    tip_tx: watch::Sender<u64>,
    cfg: Config,
}

impl Follower {
    pub fn new(db: Arc<Db>, node: NodeClient, cfg: Config) -> (Arc<Self>, watch::Receiver<u64>) {
        let initial = db.load_meta().map(|m| m.last_indexed_height).unwrap_or(0);
        let (tip_tx, tip_rx) = watch::channel(initial);
        (
            Arc::new(Self {
                db,
                node,
                tip_tx,
                cfg,
            }),
            tip_rx,
        )
    }

    /// Long-running loop. Never panics; logs errors and retries.
    pub fn spawn(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { self.run().await })
    }

    pub async fn run(self: Arc<Self>) {
        tracing::info!(
            "follower: started; poll interval = {:?}",
            self.cfg.poll_interval()
        );
        loop {
            match self.tick().await {
                Ok(()) => {}
                Err(e) => tracing::warn!("follower: tick failed: {e}"),
            }
            tokio::time::sleep(self.cfg.poll_interval()).await;
        }
    }

    /// One iteration. Public so tests / embedders can drive it
    /// synchronously.
    pub async fn tick(self: &Arc<Self>) -> Result<()> {
        let tip = self.node.get_block_height().await?;
        let mut meta = self.db.load_meta()?;
        if meta.started_at == 0 {
            meta.started_at = unix_now();
        }

        // Reorg detection.
        if meta.last_indexed_height > 0 {
            let on_chain = self
                .node
                .get_block_by_height(meta.last_indexed_height)
                .await?;
            let on_chain_id = decode_hex32(&on_chain.block_id)?;
            if on_chain_id != meta.last_indexed_block_id {
                let ancestor = self.find_common_ancestor(meta.last_indexed_height).await?;
                tracing::warn!(
                    "follower: reorg at height {} detected; common ancestor = {}",
                    meta.last_indexed_height,
                    ancestor.0
                );
                self.db.wipe_above(ancestor.0)?;
                meta.last_indexed_height = ancestor.0;
                meta.last_indexed_block_id = ancestor.1;
                meta.full_scan_complete = false;
                self.db.save_meta(&meta)?;
            }
        }

        let start = if meta.last_indexed_block_id == [0u8; 32] && meta.last_indexed_height == 0 {
            0
        } else {
            meta.last_indexed_height + 1
        };
        if start > tip.height {
            if !meta.full_scan_complete {
                meta.full_scan_complete = true;
                self.db.save_meta(&meta)?;
            }
            let _ = self.tip_tx.send(tip.height);
            return Ok(());
        }

        for h in start..=tip.height {
            let block = self.node.get_block_by_height(h).await?;
            self.process_block(h, &block, meta.started_at).await?;
            let _ = self.tip_tx.send(h);
            if h.is_multiple_of(1000) {
                tracing::info!(
                    "follower: indexed block {} (tip {}, behind {})",
                    h,
                    tip.height,
                    tip.height.saturating_sub(h)
                );
            }
        }
        Ok(())
    }

    // The prev-tx cache fill is a conditional, fallible, async insert
    // (fetch only on a miss, skip on fetch failure) — the `entry` API the
    // map_entry lint wants doesn't fit an awaited fallible fetch.
    #[allow(clippy::map_entry)]
    async fn process_block(
        &self,
        height: u64,
        block: &BlockSummary,
        started_at: u64,
    ) -> Result<()> {
        let block_id = decode_hex32(&block.block_id)?;

        let mut locks: Vec<ExtractedHtlcLock> = Vec::new();
        let mut spends: Vec<ExtractedHtlcSpend> = Vec::new();
        let mut activity: Vec<AddressActivity> = Vec::new();
        let mut settlements: Vec<SettlementRecord> = Vec::new();
        let mut spent_by: Vec<SpentByCacheEntry> = Vec::new();
        // Prev-tx cache for this block: an input's address + amount live on the
        // output it spends, which is in an earlier (or same) tx. Several inputs
        // can spend the same prev tx, so cache to avoid refetching it.
        let mut prev_tx_cache: HashMap<[u8; 32], Transaction> = HashMap::new();

        // Fetch transactions. The node returns tx_ids only in
        // BlockSummary; per-tx fetch via get_transaction (which works
        // for confirmed txs by hash).
        for tx_id_hex in &block.transactions {
            let status = self.node.get_transaction(tx_id_hex).await?;
            let tx_bytes = hex::decode(&status.tx_hex)
                .map_err(|e| Error::Internal(format!("decode tx_hex: {e}")))?;
            let (tx, _) = Transaction::deserialize(&tx_bytes)
                .map_err(|e| Error::Internal(format!("decode tx: {e:?}")))?;

            let extracted = extract::extract_from_tx(&tx, height, 0);
            locks.extend(extracted.locks);
            spends.extend(extracted.spends.clone());

            // Collect this tx's activity rows locally so we can attach each
            // row's counterparty set once BOTH sides are known. Output-side
            // rows come from extract; input-side rows are resolved below.
            let mut tx_activity: Vec<AddressActivity> = extracted.activity;
            // Recipient addresses = the distinct output-side addresses.
            let mut output_addrs: Vec<[u8; 32]> = Vec::new();
            for a in &tx_activity {
                if !output_addrs.contains(&a.address) {
                    output_addrs.push(a.address);
                }
            }

            // Sender addresses = the distinct addresses of the spent prevouts.
            let mut input_addrs: Vec<[u8; 32]> = Vec::new();
            // Record spent-by entries for every non-coinbase input (cache for
            // downstream consumers) and, in the same pass, the input-side
            // address activity — funds LEAVING an address. Without this an
            // outgoing transfer never appears for the spending wallet, only the
            // matching receive for the recipient. The address + amount come
            // from the output this input spends, resolved via the prev tx
            // (cached per block). Only Phase-1 P2PKH prevouts (32-byte script)
            // have an address; covenant/HTLC prevouts are skipped, mirroring
            // the output-side rule in extract_from_tx.
            if !tx.is_coinbase() {
                for (vin, input) in tx.inputs.iter().enumerate() {
                    let prev_tx_id = *input.prev_tx_id.as_bytes();
                    spent_by.push(SpentByCacheEntry {
                        prev_tx_id,
                        output_index: input.output_index,
                        spending_tx_id: extracted.tx_id,
                        input_index: vin as u32,
                        block_height: height,
                    });

                    if !prev_tx_cache.contains_key(&prev_tx_id) {
                        if let Some(ptx) = self.fetch_tx(&prev_tx_id).await? {
                            prev_tx_cache.insert(prev_tx_id, ptx);
                        }
                    }
                    if let Some(ptx) = prev_tx_cache.get(&prev_tx_id) {
                        if let Some(out) = ptx.outputs.get(input.output_index as usize) {
                            if out.script.len() == 32 {
                                let mut addr = [0u8; 32];
                                addr.copy_from_slice(&out.script);
                                if !input_addrs.contains(&addr) {
                                    input_addrs.push(addr);
                                }
                                tx_activity.push(AddressActivity {
                                    address: addr,
                                    tx_id: extracted.tx_id,
                                    amount: out.value,
                                    is_input: true,
                                    is_coinbase: false,
                                    counterparties: Vec::new(),
                                });
                            }
                        }
                    }
                }
            }

            // Attach counterparties: a received (output) row's are the senders
            // (tx inputs); a sent (input) row's are the recipients (tx
            // outputs). Exclude the row's own address (self-change / self-send).
            for row in &mut tx_activity {
                let others = if row.is_input {
                    &output_addrs
                } else {
                    &input_addrs
                };
                row.counterparties = others
                    .iter()
                    .filter(|a| **a != row.address)
                    .copied()
                    .collect();
            }
            activity.extend(tx_activity);
        }

        // Settlement records — built from the already-classified
        // spends (which advance_htlc_within_txn will apply to the
        // index). For each spend that points at an HTLC we've
        // already indexed, build a per-side settlement record.
        for spend in &spends {
            if let Ok(Some(blob)) = self.peek_htlc(&spend.lock_tx_id, spend.output_index) {
                let mut rec: exfer::covenants::htlc::HtlcRecord = bincode::deserialize(&blob)
                    .map_err(|e| Error::Storage(format!("decode peek htlc: {e}")))?;
                // Speculatively set the outcome state so the
                // settlements_for_settled_htlc output reflects what
                // will be in the index after apply.
                rec.state = match &spend.arm {
                    crate::extract::HtlcSpendArm::Claim { .. } => {
                        exfer::covenants::htlc::HtlcState::Claimed
                    }
                    crate::extract::HtlcSpendArm::Reclaim { .. } => {
                        exfer::covenants::htlc::HtlcState::Reclaimed
                    }
                };
                let s = extract::settlements_for_settled_htlc(&rec, height);
                settlements.extend(s);
            }
        }

        let events = BlockApplyEvents {
            height,
            block_id,
            tx_count: block.tx_count,
            timestamp: block.timestamp,
            full_scan_complete: false,
            started_at,
            locks: &locks,
            spends: &spends,
            settlements: &settlements,
            activity: &activity,
            spent_by: &spent_by,
        };
        self.db.apply_block_events(events)
    }

    /// Fetch + deserialize a confirmed tx by id, for resolving an input's
    /// previous output. Returns `Ok(None)` if the node has no such tx (not
    /// expected for a prevout on the canonical chain — we stay best-effort and
    /// skip rather than wedge the block); transport errors propagate via `?` so
    /// the block is retried on the next tick.
    async fn fetch_tx(&self, tx_id: &[u8; 32]) -> Result<Option<Transaction>> {
        let status = self.node.get_transaction(&hex::encode(tx_id)).await?;
        if status.tx_hex.is_empty() {
            return Ok(None);
        }
        let tx_bytes = hex::decode(&status.tx_hex)
            .map_err(|e| Error::Internal(format!("decode prev tx_hex: {e}")))?;
        let (tx, _) = Transaction::deserialize(&tx_bytes)
            .map_err(|e| Error::Internal(format!("decode prev tx: {e:?}")))?;
        Ok(Some(tx))
    }

    fn peek_htlc(&self, lock_tx_id: &[u8; 32], output_index: u32) -> Result<Option<Vec<u8>>> {
        use crate::db::schema::HTLC_FULL;
        let read = self.db.raw().begin_read()?;
        let t = read.open_table(HTLC_FULL)?;
        let mut k = [0u8; 36];
        k[..32].copy_from_slice(lock_tx_id);
        k[32..].copy_from_slice(&output_index.to_be_bytes());
        let opt = t.get(k.as_slice())?;
        Ok(opt.map(|g| g.value().to_vec()))
    }

    /// Walk back one block on the node side. The node is the source of
    /// truth — whatever block_id it now reports at height H is
    /// canonical for H. Genesis (height 0) is the unconditional
    /// fallback.
    async fn find_common_ancestor(&self, from_height: u64) -> Result<(u64, [u8; 32])> {
        if from_height == 0 {
            let b = self.node.get_block_by_height(0).await?;
            return Ok((0, decode_hex32(&b.block_id)?));
        }
        let h = from_height - 1;
        let b = self.node.get_block_by_height(h).await?;
        Ok((h, decode_hex32(&b.block_id)?))
    }
}

fn decode_hex32(s: &str) -> Result<[u8; 32]> {
    let b = hex::decode(s).map_err(|e| Error::BadHex(e.to_string()))?;
    if b.len() != 32 {
        return Err(Error::BadAddressLen(b.len()));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&b);
    Ok(out)
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[allow(dead_code)]
fn _unused(_: Duration) {}
