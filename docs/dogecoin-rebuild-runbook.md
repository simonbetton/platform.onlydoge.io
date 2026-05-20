# Dogecoin Current-State Rebuild Runbook

This runbook covers the ClickHouse-only Dogecoin current-state rebuild path. It is intentionally destructive to the Dogecoin projection/read tables and should only be used when public explorer data can be reset or unavailable during rebuild.

## What This Rebuild Produces

The fast path writes append-only core tables first:

- `core_utxo_creates_v1`
- `core_utxo_spends_v1`
- `core_processed_blocks_v1`

Then it materializes current read state:

- `utxo_outputs_current_v2`
- `utxo_outputs_current_by_address_v2`
- `balances_v2`
- `applied_blocks_v2`

History reads are core-backed after `prepare-clickhouse-core-history` completes. The old fully materialized transfer/direct-link graph is intentionally not rebuilt by this path.

## Safety Gates

Before resetting production:

1. Confirm raw Dogecoin snapshots exist for the target range.
2. Confirm ClickHouse has enough disk headroom.
3. Pause or stop the indexer.
4. Run the benchmark against worst-case large-block ranges.
5. Require worst-case throughput of at least `50,000 blocks/hour`.

If the benchmark misses the gate, do not reset production. Redesign first.

## Benchmark

Dry-run parses stored raw snapshots without writing benchmark tables:

```bash
bun run benchmark:clickhouse-core -- --networkId 1 --blocks 100 --ranges 3
```

Execute mode writes isolated benchmark tables, materializes benchmark current state, reports throughput, then drops those tables unless `--keep` is passed:

```bash
bun run benchmark:clickhouse-core -- --networkId 1 --blocks 100 --ranges 3 --execute
```

Explicit range:

```bash
bun run benchmark:clickhouse-core -- --networkId 1 --start 6000000 --end 6000099 --execute
```

The command reports:

- blocks/hour,
- rows inserted,
- raw-load time,
- parse time,
- ClickHouse insert time,
- final materialization time.

## Destructive Reset

Inspect the plan first:

```bash
bun run rebuild:clickhouse-core -- --networkId 1
```

Apply the reset only after the benchmark gate passes:

```bash
bun run rebuild:clickhouse-core -- --networkId 1 --execute
```

This drops and recreates ClickHouse Dogecoin projection/read/core tables, resets the core process tail to the beginning, and marks current state/history as not ready.

## Processing Backfill

Start the indexer after reset. It will:

- sync raw snapshots,
- process append-only core create/spend windows,
- maintain 100-block process windows by default,
- stop on invariant failure.

Monitor progress:

```bash
bun run health:indexer
```

Or from the app host:

```bash
cd /opt/onlydoge
docker compose --env-file .env -f docker-compose.managed.yml logs -f onlydoge-indexer
```

## Current-State Materialization

When core processing reaches the target height, materialize current state.

Dry-run:

```bash
bun run materialize:clickhouse-core -- --networkId 1 --asOfBlockHeight <height>
```

Execute from a clean start:

```bash
bun run materialize:clickhouse-core -- --networkId 1 --asOfBlockHeight <height> --reset --execute
```

Resume from checkpoint:

```bash
bun run materialize:clickhouse-core -- --networkId 1 --asOfBlockHeight <height> --execute
```

Bound a run to a few ranges:

```bash
bun run materialize:clickhouse-core -- --networkId 1 --asOfBlockHeight <height> --execute --rangeLimit 8
```

The script checkpoints progress in metadata and can resume safely at the same `asOfBlockHeight`.

## History Preparation

Core-backed history requires a ClickHouse skipping index on `core_utxo_creates_v1.address`.

Dry-run:

```bash
bun run scripts/prepare-clickhouse-core-history.ts -- --networkId 1
```

Add/materialize the index:

```bash
bun run scripts/prepare-clickhouse-core-history.ts -- --networkId 1 --execute --materialize-index
```

Wait for materialization and mark history ready:

```bash
bun run scripts/prepare-clickhouse-core-history.ts -- --networkId 1 --execute --wait --mark-ready
```

After this, `dogecoin_history_ready_n1` should be `true`, and `indexer_fact_tail_n1` should align with the core process tail.

## Validation

Required validation:

- `lastError = null`
- `stage = online`
- `dogecoin_current_state_ready_n1 = true`
- `dogecoin_history_ready_n1 = true`
- `processTail` within the configured online tip distance
- balance sum equals spendable current UTXO sum
- sampled transaction search/detail works
- sampled address summary/history/UTXO reads work

Example in-container stats check:

```bash
docker compose --env-file .env -f docker-compose.managed.yml exec -T onlydoge-api \
  bun -e 'const { loadSettings, RelationalMetadataStore } = await import("@onlydoge/platform"); const m=await RelationalMetadataStore.connect(loadSettings({mode:"indexer"}).database); const s=await m.getCoreIndexerState(1); const keys=["indexer_fact_tail_n1","dogecoin_current_state_ready_n1","dogecoin_history_ready_n1","block_height_n1"]; const out={state:s}; for (const k of keys) out[k]=await m.getJsonValue(k); console.log(JSON.stringify(out,null,2));'
```
