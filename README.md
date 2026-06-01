# exfer-indexer

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Read-only chain indexer for [Exfer](https://exfer.org/).** Follows
the canonical chain, records every HTLC and every address-level
activity in a local redb store, and exposes a JSON-RPC query surface
that any client — including [`exfer-walletd`](https://github.com/exfer-stack/exfer-walletd),
agent frameworks, block explorers, audit tooling — can hit to ask
questions **about arbitrary addresses**, not just ones they own keys
for.

> **Why this exists.** Walletd's block follower indexes only HTLCs
> paying its own keys — enough to power an MCP-style "AI wallet"
> safely, but it can't answer "what's the track record of agent X
> over there?". exfer-indexer is the multi-tenant complement: same
> wire types as walletd (via the upstream `exfer` crate), but every
> HTLC and every address on the canonical chain is queryable.

## Status

Working and deployed. The follower scans the full chain and keeps the
redb store in sync with the node's tip (reorg-aware); the JSON-RPC
surface answers live queries for any address. It runs as a read replica
alongside the public Exfer nodes and powers the **Activity** timeline in
[`exfer-walletd`](https://github.com/exfer-stack/exfer-walletd) /
the wallet apps (`get_address_history`, with real tx ids and
**from / to** counterparties), plus HTLC and attestation queries for
ops, MCP servers, and explorers.

**Query surface** (JSON-RPC `POST /`):

- `get_address_history` — per-address on-chain timeline (in/out,
  amounts, counterparties)
- `get_indexer_status` — follower height / sync state
- `htlc_list`, `htlc_status`, `htlc_lookup_by_hashlock` — HTLC
  observability
- `list_settlements`, `contract_stats`, `get_contract_template` —
  contract / settlement views
- `get_attestation_edges`, `detect_in_chain_swaps` — attestation graph
  + swap detection
- `get_output_spent_by` — spent-by lookups
- `ping` — liveness

walletd transparently delegates non-owned queries here when started with
its indexer flag, so its MCP surface can answer about arbitrary
addresses, not just owned keys.

## Design boundaries

- **Read-only.** Holds no keys, never signs, cannot move funds.
- **Single source of truth.** The upstream node decides the
  canonical chain. The indexer reflects whatever the node decides;
  on reorgs the indexer rewinds and re-applies the new tip.
- **Shared wire types.** `HtlcRecord` / `HtlcState` / `HtlcRole` /
  `HtlcParams` / `HtlcClaimRecord` / `HtlcReclaimRecord` come from
  `exfer::covenants::htlc`. walletd and indexer serialize them
  identically — a JSON consumer cannot tell which side served the
  request.
- **No new node RPCs required.** The indexer consumes whatever
  read-side RPCs the node already exposes (`get_block_height`,
  `get_block`, `get_transaction`, `get_output_spent_by`). It does
  not add to the node's surface.

## Architecture

```
┌── ops / MCP server / explorer ───┐
│                                  │
│  POST / JSON-RPC                 │
│                                  │
└─────────────────────▶ exfer-indexer ──▶ exfer node JSON-RPC
                                  │      (read-only)
                                  │
                          local redb store
                          (HTLCs, address
                           activity, contract
                           settlements,
                           spent-by cache)
```

walletd's MCP-facing API is the primary consumer in the v1.9.1+
deployment topology: walletd transparently delegates non-owned
queries to the indexer, so the MCP server still only talks to
walletd.

## Build

```bash
cargo build --release
# Binary at target/release/exfer-indexer
```

## Run

```bash
exfer-indexer \
  --node-rpc http://127.0.0.1:9334 \
  --datadir /var/lib/exfer-indexer \
  --bind 127.0.0.1:9335
```

### Public / anonymous deployment

The indexer is **read-only** and serves only **public chain data** (the same
address history any block explorer exposes). A bearer token here is therefore a
shared client secret with no confidentiality value, so a public read replica
can run **anonymously**:

```bash
exfer-indexer \
  --node-rpc http://127.0.0.1:9334 \
  --datadir /var/lib/exfer-indexer \
  --bind 0.0.0.0:9335 \
  --allow-public-bind          # acknowledge a plaintext public endpoint
```

`--allow-public-bind` is required to bind a non-loopback address without
`--tls` (the indexer refuses to do so *implicitly*). `--auth-token` stays
optional — set it only as a coarse abuse gate. When unset, the server ignores
any `Authorization` header, so clients that still send a stale token keep
working. Front a public endpoint with a rate-limiter/firewall to deter abuse.

## License

MIT. See [LICENSE](LICENSE).
