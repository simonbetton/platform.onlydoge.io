# AI Analytics API

OnlyDoge exposes a guarded analytics SQL surface for AI chat tools. The chat app generates SQL from `GET /v1/analytics/schema`, then submits that SQL to `POST /v1/analytics/query` with server-owned network and time-window parameters.

## Routes

- `GET /v1/analytics/schema`
- `POST /v1/analytics/query`

Both routes require `x-api-token`. Analytics requests use a separate per-API-key analytics rate-limit budget from ordinary API routes.

## Query Surface

V1 exposes the curated `analytics_transactions_v1` ClickHouse table. It contains finalized confirmed transaction facts only:

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
- query only `analytics_transactions_v1`,
- include the placeholders `{networkId:UInt64}`, `{fromTime:UInt64}`, `{toTime:UInt64}`, and `{maxFinalizedHeight:UInt64}`,
- use those placeholders in the concrete `network_id`, `block_time`, and `block_height` predicates shown by the schema examples,
- stay within the request's maximum 7-day time window,
- avoid `SELECT *`, `FINAL`, query-level `SETTINGS`, and query-level `FORMAT`.

V1 does not answer mempool-to-confirmation latency or unconfirmed mempool analytics. Those need a persisted mempool observation model.

## Operations

Existing history must be backfilled before analytics queries are available for a network:

```bash
bun run backfill:analytics-transactions -- --network net_dogecoin
```

For self-managed ClickHouse, configure a read-only analytics user and set:

- `ONLYDOGE_ANALYTICS_WAREHOUSE_USER`
- `ONLYDOGE_ANALYTICS_WAREHOUSE_PASSWORD`

The bundled Docker ClickHouse config creates a constrained `onlydoge_analytics` user with interactive query limits.

Run the Docker-backed analytics smoke test with:

```bash
bun run test:clickhouse-smoke
```
