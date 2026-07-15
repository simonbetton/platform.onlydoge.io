# Plan 016: Index transaction references and serve address summaries from facts

> **Executor instructions**: Add a narrow transaction-reference projection during raw sync and denormalize list-summary fields. Do not put full raw transactions in metadata.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- packages/modules/indexing-pipeline packages/modules/explorer-query packages/platform/src/warehouse.ts docker/clickhouse tests`

## Status
- **Priority**: P2
- **Effort**: L
- **Risk**: MED
- **Depends on**: `plans/015-add-production-adapter-ci-lane.md`, `plans/018-version-clickhouse-migrations.md`
- **Category**: performance
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
A transaction miss scans up to 1,000 compressed raw blocks sequentially. Address history already has transaction fact rows, but the service reloads/parses one full block per distinct result height to construct summaries.

## Target design
- During validated raw-block sync, batch-write compact refs: txid, block height/hash/time and tx index.
- Validate a raw-sync ref against the current canonical core-block record before returning it, so reorg-orphan refs cannot resolve.
- Extend address transaction fact queries to return the complete `ExplorerTransactionSummary`; no raw block is needed for list pages.
- Keep raw blocks for full transaction inputs/outputs detail after the O(1) ref lookup.

## Scope
**In scope**
- New indexing-pipeline projection/port methods and ClickHouse table/migration.
- Raw-sync write path and reorg/canonical validation.
- Explorer warehouse port/query model mapping.
- ClickHouse/in-memory adapters and tests.
- `plans/README.md` (status only)

**Out of scope**
- Storing full transaction JSON in ClickHouse.
- Removing raw storage from transaction detail.
- Search-engine infrastructure.

## Steps
1. Define `TransactionRef` once in indexing-pipeline contracts. Add `dogecoin_transaction_refs_v1` with `ENGINE = ReplacingMergeTree(version)` and `ORDER BY (txid)`, including block height/hash/time, tx index and deterministic version/source fields.
2. After strict block validation (Plan 011), derive refs in block order and insert one bounded batch per sync window. The processed analytics path may upsert the same canonical ref deterministically.
3. On lookup, deduplicate candidate versions (`argMax` or `ORDER BY version DESC LIMIT 1`) and confirm the returned block hash through `ExplorerCoreBlockPort.getCoreBlockByHash`, which is wired to metadata `core_blocks`. Reject stale/orphan candidates and never fall back to a 1,000-block scan once migration/readiness is complete.
4. Expand `listAddressTransactions` by joining movement aggregates to deduplicated `analyticsTransactionsTable` rows on `(txid, block_height, block_hash)`, not txid alone. Use `argMax`/`LIMIT 1 BY`/`FINAL` so unmerged ReplacingMergeTree versions cannot duplicate rows. Return fee, counts, coinbase flag and totals directly as `ExplorerTransactionSummary`.
5. Add an explicit config marker such as `dogecoin_transaction_refs_ready`. Provide a bounded raw-block backfill resumable by height. `getTransactionRef` may use the old scan only while the marker is false; after true, a miss is a miss and no raw range fallback runs.
6. Add reorg, unprocessed-tail lookup, historical backfill and “zero raw reads for address list” tests against real ClickHouse.

## Verification
- Focused indexer/explorer tests pass.
- Production-adapter lane proves ref lookup and address paging.
- Instrumented tests assert O(1) warehouse lookup and zero list-page raw reads.
- `bun run ci` exits 0.

## Done criteria
- [ ] Transaction lookup does not scan raw block ranges after readiness.
- [ ] Unprocessed synced transactions are resolvable.
- [ ] Reorg-orphan refs are not returned.
- [ ] Address list pages read no raw blocks.
- [ ] Backfill is resumable and observable.

## STOP conditions
- Canonical block validation cannot distinguish orphan refs before process catches up; add tombstones/canonical sync projection before cutover.
- Existing transaction facts lack a summary field that cannot be derived correctly without prevouts; extend process-time fact derivation, not list-time raw reads.
- Migration framework from Plan 018 is unavailable.

## Maintenance notes
Any field added to `ExplorerTransactionSummary` must be considered for fact denormalization; list endpoints should remain fact-only.
