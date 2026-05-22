# Dogecoin Explorer API

This document describes the current Dogecoin explorer read surface. It is not a projection roadmap.

## Routes

- `GET /v1/explorer/networks`
- `GET /v1/explorer/search?q=...&network=...`
- `GET /v1/explorer/mempool?offset=...&limit=...&network=...`
- `GET /v1/explorer/blocks`
- `GET /v1/explorer/blocks/:ref`
- `GET /v1/explorer/transactions/:txid`
- `GET /v1/explorer/addresses/:address`
- `GET /v1/explorer/addresses/:address/transactions`
- `GET /v1/explorer/addresses/:address/utxos`

All explorer routes require `x-api-token`, except the public health and OpenAPI routes outside the explorer group.

If one active Dogecoin network exists, it is the default explorer network. If more than one active Dogecoin network exists, callers should pass `?network=<network-id>`.

## Data Sources

Explorer reads use stored data, except `GET /v1/explorer/mempool`, which calls live Dogecoin RPC and shares a one-second in-process snapshot cache across callers.

- Raw block snapshots provide block and transaction detail shape.
- `core_utxo_creates_v1` stores append-only output creates.
- `core_utxo_spends_v1` stores append-only spend records keyed by spent output.
- `core_processed_blocks_v1` stores block identity for processed core windows.
- `utxo_outputs_current_v2` and `utxo_outputs_current_by_address_v2` provide current spendable UTXO reads.
- `balances_v2` provides current native DOGE balances.
- `applied_blocks_v2` provides explorer block identity after current-state materialization.
- Dogecoin RPC provides the node's current mempool: the set of unconfirmed transactions currently visible to that node.

Older projection tables may still exist for compatibility and investigation graph reads, but the current production Dogecoin indexer does not rebuild a full transfer/direct-link graph.

## Readiness

Current-state reads become available after core backfill and current-state materialization:

- `GET /v1/explorer/networks`
- `GET /v1/explorer/mempool`
- `GET /v1/explorer/blocks`
- `GET /v1/explorer/addresses/:address`
- `GET /v1/explorer/addresses/:address/utxos`

History-dependent reads require `dogecoin_history_ready_n{networkId} = true`:

- `GET /v1/explorer/search`
- `GET /v1/explorer/blocks/:ref`
- `GET /v1/explorer/transactions/:txid`
- `GET /v1/explorer/addresses/:address/transactions`

When history is not ready, those routes return a `425` response with a clear `dogecoin history index is not ready` error.

Processed-tip lag is measured against the Dogecoin node's confirmed chain tip. It is not caused by transactions remaining in the mempool.

## Response Notes

Address summaries combine current balances and UTXO counts with investigation metadata overlays. Transaction detail resolves input values and addresses from the core UTXO state where possible.

Mempool responses return normalized metadata only, ordered by newest node-reported entry time first with `txid` as a deterministic tie-breaker. The default page size is `100`; the maximum page size is `500`.

Cache headers are intentionally private because explorer routes are authenticated:

- mempool responses are `no-store`,
- search responses are short-lived,
- address responses may be cached briefly,
- block, transaction, and network responses may be cached longer.

## Test Coverage

Coverage is split across:

- API integration tests for auth, OpenAPI exposure, explorer reads, and history-not-ready behavior,
- warehouse tests for core ClickHouse reads and current-state materialization,
- indexer integration tests for raw snapshot persistence and core UTXO state,
- production E2E tests for deployed API behavior and teardown.
