# Networkless Dogecoin Rebuild Runbook

This rebuild is a destructive singleton Dogecoin reset. Do not run it against shared production services until API keys, storage, and ClickHouse retention have been reviewed.

## Reset Order

1. Stop the API and indexer.
2. Delete old raw block object prefixes.
3. Delete metadata tables for API keys, audit events, labels, entities, tags, tokens, and old network catalog rows.
4. Drop old ClickHouse v1/v2 tables before recreating the networkless schema.
5. Deploy the singleton Dogecoin environment:
   - `ONLYDOGE_DOGECOIN_RPC_ENDPOINT`
   - `ONLYDOGE_DOGECOIN_RPC_RPS`
   - `ONLYDOGE_DOGECOIN_ZMQ_BLOCK_ENDPOINT`
   - `ONLYDOGE_CORE_REPROCESS_DEPTH`
   - `ONLYDOGE_MEMPOOL_SAMPLE_INTERVAL_MS`
   - `ONLYDOGE_MEMPOOL_RETENTION_SECONDS`
6. Start the indexer and resync from Dogecoin Core.
7. Bootstrap a new first admin key with `POST /v1/keys/`.
8. Run invariant checks across blocks, transactions, outputs, spends, UTXOs, balances, and address summaries.
9. Run external parity checks against BlockCypher's Dogecoin API and at least one other free Dogecoin explorer where practical, including `D8AXXiGEZeZnMKTKnC9AWB3YUU4jfMAmYU`.
10. Mark the deployment ready only after internal invariants and external parity checks pass.

## Local Verification

Run:

```bash
bun run lint
bun run typecheck
bun run test
```

Run the optional ClickHouse smoke test when Docker is available:

```bash
bun run test:clickhouse-smoke
```

Backfill finalized analytics after history is ready:

```bash
bun run backfill:analytics-transactions
```
