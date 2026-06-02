# Dogecoin Explorer API

OnlyDoge is a single-chain Dogecoin explorer. Public responses do not expose network ids, network names, labels, tags, entities, overlays, or token catalog data.

## Routes

- `GET /v1/explorer/search?q=...`
- `GET /v1/explorer/mempool?offset=...&limit=...`
- `GET /v1/explorer/blocks?offset=...&limit=...`
- `GET /v1/explorer/blocks/:ref`
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
