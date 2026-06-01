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

`v0.1.0` — initial scaffold. The follower + RPC handlers land in
subsequent commits per the workflow B plan:

- [x] **#12** Scaffold service (this commit): module structure,
      config, error type, redb schema, dispatcher signature, server
      boot path.
- [ ] **#13** Block follower + extraction: full-chain scan, HTLC /
      address / settlement extraction, populate every table.
- [ ] **#14** RPC handlers: `list_settlements`, `contract_stats`,
      `get_address_history`, `htlc_lookup_by_hashlock`,
      `get_output_spent_by`, `htlc_status`, `htlc_list`,
      `get_indexer_status`.
- [ ] **walletd v1.9.1** indexer delegation flag so walletd can
      transparently route non-owned queries here.

The current binary boots, opens the redb file, spawns a no-op
follower stub, and exits on Ctrl+C. Useful for verifying the
scaffold compiles + the volume mount + auth wiring; not useful for
answering real queries until the follower commit lands.

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
