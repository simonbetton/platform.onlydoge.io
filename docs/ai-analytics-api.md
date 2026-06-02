# AI Analytics API

OnlyDoge exposes a guarded analytics SQL surface for AI chat tools. The chat app generates SQL from `GET /v1/analytics/schema`, then submits that SQL to `POST /v1/analytics/query` with server-owned time-window parameters.

## Routes

- `GET /v1/analytics/schema`
- `POST /v1/analytics/query`

Both routes require `x-api-token`. Analytics requests use a separate per-API-key analytics rate-limit budget from ordinary API routes.

## Query Surface

V1 exposes curated ClickHouse tables for singleton Dogecoin analytics:

- `analytics_transactions_v1` for finalized confirmed transaction facts,
- `analytics_balances_current_v1` for richest-address and balance-distribution questions,
- `mempool_samples_v1` for current and recent mempool samples.

`analytics_transactions_v1` contains:

- block position and time,
- transaction id and transaction index,
- coinbase flag,
- input and output counts,
- resolved total input value,
- gross output value,
- resolved fee when available.

Amounts ending in `_base_i256` are integer base units. For DOGE, `100000000` base units equals `1 DOGE`.

Generated SQL must:

- be a single read-only `SELECT`,
- query only the curated analytics tables returned by `GET /v1/analytics/schema`,
- include the placeholders required by each referenced table,
- constrain `analytics_transactions_v1` with concrete `block_time` lower/upper bounds and `block_height <= {maxFinalizedHeight:UInt64}`,
- constrain `analytics_balances_current_v1` with `as_of_block_height <= {maxFinalizedHeight:UInt64}`,
- constrain `mempool_samples_v1` with concrete `sampled_at` lower/upper bounds using `toDateTime({fromTime:UInt64})` or `fromUnixTimestamp({fromTime:UInt64})`,
- stay within the request's maximum 7-day time window,
- avoid `SELECT *`, `FINAL`, query-level `SETTINGS`, and query-level `FORMAT`.

Unconfirmed mempool analytics are answered only from persisted `mempool_samples_v1` observations and are limited to the configured retention window.

## Operations

Existing history must be backfilled before analytics queries are available:

```bash
bun run backfill:analytics-transactions
```

For self-managed ClickHouse, configure a read-only analytics user and set:

- `ONLYDOGE_ANALYTICS_WAREHOUSE_USER`
- `ONLYDOGE_ANALYTICS_WAREHOUSE_PASSWORD`

The bundled Docker ClickHouse config creates a constrained `onlydoge_analytics` user with interactive query limits.

Run the Docker-backed analytics smoke test with:

```bash
bun run test:clickhouse-smoke
```
