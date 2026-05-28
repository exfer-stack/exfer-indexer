//! `exfer-indexer` — read-only chain indexer for Exfer.
//!
//! Follows the canonical chain via an Exfer node's JSON-RPC, persists
//! every HTLC and every address-level activity in a local redb store,
//! and exposes a query surface over JSON-RPC that walletd / MCP /
//! block explorers can call **for any address**, not just one they
//! own keys for.
//!
//! ## Why it exists
//!
//! [`exfer-walletd`] also runs a block follower, but its index is
//! scoped to the wallet's own keys — it can answer "what's the state
//! of MY HTLCs?" but not "what's the track record of agent X over
//! there?". Anything that needs the second kind of answer — agent-to-
//! agent reputation lookups, atomic-swap counterparty verification,
//! block explorers, audit tooling — needs a multi-tenant index.
//!
//! [`exfer-walletd`]: https://github.com/exfer-stack/exfer-walletd
//!
//! ## Boundaries
//!
//! Read-only. Holds no keys. Never signs anything. Cannot move funds.
//! Treats the upstream node as the only source of truth — if the node
//! reorgs, the indexer reflects the new canonical chain.
//!
//! ## Shared types
//!
//! [`HtlcRecord`] / [`HtlcState`] / [`HtlcRole`] / etc. come from
//! `exfer::covenants::htlc`. walletd and indexer serialize them
//! identically by construction, so a JSON consumer cannot tell which
//! side of the split it's talking to.
//!
//! [`HtlcRecord`]: exfer::covenants::htlc::HtlcRecord
//! [`HtlcState`]: exfer::covenants::htlc::HtlcState
//! [`HtlcRole`]: exfer::covenants::htlc::HtlcRole

pub mod api;
pub mod config;
pub mod db;
pub mod error;
pub mod extract;
pub mod follower;
pub mod server;
pub mod upstream;
