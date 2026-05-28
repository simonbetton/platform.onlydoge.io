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

All explorer routes require `x-api-token`, except the public health and OpenAPI routes outside the explorer group. API tokens are returned once in the `POST /v1/keys` response `key` field; OnlyDoge stores only a token hash, so lost API tokens cannot be recovered.

If one active Dogecoin network exists, it is the default explorer network. If more than one active Dogecoin network exists, callers should pass `?network=<network-id>`.

## Data Sources

Explorer reads use stored data, except `GET /v1/explorer/mempool`, which calls live Dogecoin RPC and shares a one-second in-process snapshot cache across callers.

- Raw block snapshots provide block and transaction detail shape.
- `core_utxo_creates_v1` stores append-only output creates.
- `core_utxo_spends_v1` stores append-only spend records keyed by spent output.
- `core_processed_blocks_v1` stores block identity for processed core windows.
- `utxo_outputs_current_v2` and `utxo_outputs_current_by_address_v2` provide current spendable UTXO reads.
- `balances_v2` provides current native DOGE balances.
- Raw sync tail (`indexer_sync_tail_n{networkId}`) drives recent block listing from stored raw snapshots.
- Dogecoin RPC provides the node's current mempool: the set of unconfirmed transactions currently visible to that node.

Older projection tables may still exist for compatibility and investigation graph reads, but the current production Dogecoin indexer does not rebuild a full transfer/direct-link graph.

## Readiness

Network discovery is metadata-backed and mempool reads call live Dogecoin RPC:

- `GET /v1/explorer/networks`
- `GET /v1/explorer/mempool`

Current-state reads depend on the current UTXO and balance read models. A labeled address can still be returned with a zero balance overlay before it has indexed chain activity:

- `GET /v1/explorer/addresses/:address`
- `GET /v1/explorer/addresses/:address/utxos`

Indexed block listing uses stored raw block snapshots up to the raw sync tail, but it is not gated by the history-ready flag:

- `GET /v1/explorer/blocks`

History-dependent reads require `dogecoin_history_ready_n{networkId} = true`:

- `GET /v1/explorer/search`
- `GET /v1/explorer/blocks/:ref`
- `GET /v1/explorer/transactions/:txid`
- `GET /v1/explorer/addresses/:address/transactions`

When history is not ready, those routes return a `425` response with a clear `dogecoin history index is not ready` error.

Raw block sync is allowed to follow the Dogecoin node's confirmed chain tip. Current-state and history reads stay behind the configured processing confirmation distance, so `processTail` can lag while recent raw blocks are already visible. Processed-tip lag is not caused by transactions remaining in the mempool.

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
