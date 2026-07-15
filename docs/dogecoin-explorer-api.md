# Dogecoin Explorer API

OnlyDoge is a single-chain Dogecoin explorer. Public responses do not expose network ids, network names, labels, tags, entities, overlays, or token catalog data.

## Routes

- `GET /v1/explorer/search?q=...`
- `GET /v1/explorer/mempool?offset=...&limit=...`
- `GET /v1/explorer/mempool/watch?address=...&minValueBase=...`
- `GET /v1/explorer/blocks?offset=...&limit=...`
- `GET /v1/explorer/blocks/:ref?offset=...&limit=...`
- `GET /v1/explorer/transactions/:txid`
- `GET /v1/explorer/addresses/:address`
- `GET /v1/explorer/addresses/:address/transactions`
- `GET /v1/explorer/addresses/:address/utxos`

All explorer routes require `x-api-token`. API tokens are returned once in the `POST /v1/keys` response `key` field; OnlyDoge stores only a token hash.

Removed routes intentionally return 404 for authenticated callers:

- `/v1/networks`
- `/v1/tokens`
- `/v1/entities`
- `/v1/addresses`
- `/v1/tags`
- `/v1/info`
- `/v1/stats`
- `/v1/explorer/networks`

## Data Sources

Explorer reads use canonical Dogecoin data from Dogecoin Core and the warehouse:

- raw block snapshots provide block and transaction detail shape,
- `dogecoin_core_utxo_creates_v1` stores output creates,
- `dogecoin_core_utxo_spends_v1` stores spend records keyed by spent output,
- `dogecoin_core_processed_blocks_v1` stores processed block identity,
- `dogecoin_utxo_outputs_current_v1` and `dogecoin_utxo_outputs_current_by_address_v1` provide current UTXO reads,
- `dogecoin_balances_current_v1` provides current native DOGE balances,
- `indexer_sync_tail` drives recent raw block listing,
- `dogecoin_history_ready` gates history-dependent routes.

`GET /v1/explorer/mempool` calls live Dogecoin RPC and shares a one-second in-process snapshot cache. Persisted mempool analytics use `mempool_samples_v1`.

`GET /v1/explorer/mempool/watch` opens a one-shot authenticated SSE session for a single receive address:

- optional `minValueBase` requires the sum of matching outputs in base units to meet a threshold,
- on connect, the indexer rematches its shared current-mempool cache and may emit immediately,
- the first qualifying mempool appear emits `mempool.watch.appeared` and closes the stream,
- otherwise the stream emits `mempool.watch.timeout` after five minutes and closes,
- up to five concurrent sessions are allowed per API key (`409` if another is opened at the limit),
- heartbeats are sent while waiting so proxies do not idle-timeout the connection.

Live detection runs in the indexer (ZMQ `rawtx` via an out-of-process Node bridge when available, plus RPC polling while watches are active) and fans out over Postgres `NOTIFY` when API and indexer are split.
The RPC fallback polls every second by default, hydrates transactions in bounded batches, and keeps
only the current mempool. If the node reports more than the configured cache maximum, new watch
sessions return `425` until the detector recovers or its three-poll degradation marker expires.
The bounds are configurable with `ONLYDOGE_MEMPOOL_WATCH_RPC_POLL_MS` (1,000),
`ONLYDOGE_MEMPOOL_WATCH_RPC_BATCH_SIZE` (100), `ONLYDOGE_MEMPOOL_WATCH_RPC_CONCURRENCY` (4), and
`ONLYDOGE_MEMPOOL_WATCH_CACHE_MAX_TXIDS` (100,000).

## Pagination

Explorer, API-key, and audit list endpoints use `offset` and `limit` query parameters. The
default page size is 50, the maximum page size is 500, and the maximum offset is 100,000.
Values must be non-negative integers; malformed, negative, or oversized values return the
existing validation-error response rather than being clamped.

Block detail transaction summaries are paginated before input values are resolved. Responses
include `transactions`, `offset`, `limit`, `returnedCount`, and `totalCount`; each transaction's
`txIndex` remains its zero-based index in the full block. This is a contract change from the
formerly unbounded block-detail response: requests without pagination now return the first
50 transactions rather than every transaction in the block.

## Readiness

Current-state reads can return before full history is ready:

- `GET /v1/explorer/addresses/:address`
- `GET /v1/explorer/addresses/:address/utxos`

Indexed block listing uses stored raw snapshots up to `indexer_sync_tail`:

- `GET /v1/explorer/blocks`

History-dependent reads require `dogecoin_history_ready = true`:

- `GET /v1/explorer/search`
- `GET /v1/explorer/blocks/:ref`
- `GET /v1/explorer/transactions/:txid`
- `GET /v1/explorer/addresses/:address/transactions`

When history is not ready, those routes return `425` with `dogecoin history index is not ready`.

## Response Notes

Address summaries are canonical balance, received, sent, transaction count, and UTXO count values derived from outputs and spends. Transaction detail resolves input values and addresses from the UTXO state where possible.

Mempool responses return normalized metadata ordered by newest node-reported entry time first with `txid` as a deterministic tie-breaker. The default page size is `100`; the maximum page size is `500`.

Cache headers are intentionally private because explorer routes are authenticated:

- mempool responses are `no-store`,
- search responses are short-lived,
- address responses may be cached briefly,
- block and transaction responses may be cached longer.

See `docs/ai-analytics-api.md` for the guarded analytics SQL surface.
