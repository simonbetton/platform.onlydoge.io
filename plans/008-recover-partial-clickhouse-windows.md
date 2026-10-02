# Plan 008: Recover partial ClickHouse core windows deterministically

> **Executor instructions**: This is a correctness change across metadata and warehouse boundaries. Complete lease plans first, preserve reorg behavior, and update `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- packages/modules/indexing-pipeline/src packages/platform/src/warehouse.ts tests/integration/indexer.test.ts tests/integration/clickhouse-analytics-smoke.test.ts`
> Stop if core window application became transactional or gained an existing write-ahead recovery marker.

## Status
- **Priority**: P1
- **Effort**: L
- **Risk**: HIGH
- **Depends on**: `plans/006-keep-indexer-leadership-alive.md`, `plans/007-make-metadata-cas-atomic.md`, `plans/015-add-production-adapter-ci-lane.md`
- **Category**: bug
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
A core window writes several ClickHouse tables sequentially. The processed-block marker is last. A timeout/crash after earlier inserts leaves facts without the marker; retry inserts them again. This is especially wrong for plain `MergeTree` address movements, where duplicate rows can double balances/activity.

## Current state
`warehouse.ts:1375-1399` inserts creates, spends, movements, transaction facts, current state and finally processed blocks. Generic errors leave partial writes untouched; cleanup only runs in the missing-prevout recovery branch.

Relevant facts:
- ClickHouse does not provide a transaction spanning these tables.
- Existing `deleteCoreDogecoinTail(..., mutations_sync: 2)` plus current-state rematerialization is the established reorg cleanup primitive.
- The indexing service publishes metadata progress only after warehouse apply returns.

## Target invariant
Before a window's first ClickHouse mutation, persist a metadata recovery marker. Clear it only after all inserts, including processed-block markers, complete. Any later leader that finds a marker must synchronously delete/rematerialize from its start height before applying more work.

## Commands you will need
- `bunx vitest run tests/integration/indexer.test.ts` → all pass.
- `bun run test:adapters` established by Plan 015 → ClickHouse failure-injection cases pass.
- `bun run ci` → exit 0.

## Scope
**In scope**
- `packages/modules/indexing-pipeline/src/domain/config-keys.ts`
- `packages/modules/indexing-pipeline/src/contracts/ports.ts`
- `packages/modules/indexing-pipeline/src/application/core-dogecoin-indexer-service.ts`
- `packages/platform/src/metadata-store.ts`
- ClickHouse warehouse recovery method using existing tail cleanup/rematerialization.
- In-memory/DuckDB implementations required by the port.
- Unit/integration failure-injection tests.
- `plans/README.md` (status only)

**Out of scope**
- Replacing ClickHouse with a transactional store.
- Broad table-engine migrations.
- Exactly-once guarantees for mempool sampling or unrelated projections.

## Steps
### Step 1: Define a versioned recovery marker
Add `configKeyCoreApplyRecovery()` returning `dogecoin_core_apply_recovery`. Define this exact validated shape:
```ts
interface CoreApplyRecoveryMarkerV1 {
  version: 1;
  instanceId: string;
  startHeight: number;
  endHeight: number;
  blockHashes: string[];
  updateCurrentState: boolean;
  startedAt: string;
}
```
Add an atomic `compareAndDeleteJsonValue(key, expectedValue)` coordinator operation implemented as a conditional database delete; do not clear another leader's marker. Invalid marker shapes must fail closed with an actionable error.

**Verify**: unit tests round-trip a valid marker and reject malformed/unsupported versions.

### Step 2: Expose synchronous warehouse recovery
Promote the existing tail cleanup/rematerialization behavior behind a narrow warehouse-port method such as `recoverCoreDogecoinWindow(fromHeight, context)`. It must wait for deletes (`mutations_sync=2`) and restore current state as of `fromHeight - 1` when enabled.

**Verify**: existing reorg tests use the same primitive and remain green.

### Step 3: Write marker before mutation and clear after commit marker
In the indexer service:
1. Persist the marker before calling `applyCoreDogecoinWindow`.
2. Leave it in place on any error/abort/timeout.
3. Clear it with owner/value-checked CAS only after warehouse apply succeeds.
4. Publish process progress only after marker clearance.

**Verify**: a unit fake records exact ordering: marker set → warehouse apply → marker CAS clear → progress publish.

### Step 4: Recover before any new work
After acquiring leadership and before processing, inspect the marker. If present, call warehouse recovery from its start height and clear the exact marker with CAS. If recovery or CAS fails, do not apply a new window.

**Verify**: simulated restart with a marker invokes recovery once and applies no new facts until recovery completes.

### Step 5: Add real ClickHouse failpoint tests
Inject failures after each insertion stage (creates, spends, movements, transactions, current state, processed marker). Restart/retry through the service and assert:
- one logical movement per `movement_id`;
- one logical processed block per height/hash;
- balances/UTXO current state match a clean one-pass apply;
- marker is absent only after successful convergence.
Use bounded test fixtures and scan limits.

**Verify**: all failpoints pass against the CI ClickHouse service.

## Test plan
- Crash after every write boundary converges to one logical result.
- Marker survives failure and is cleared after recovery/success.
- Reorg cleanup remains correct.
- A stale leader cannot clear another instance's marker.
- Recovery is safe when no partial rows were written.

## Done criteria
- [ ] Every core window is write-ahead marked.
- [ ] Generic failures no longer rely on blind reinsert.
- [ ] Real ClickHouse failpoint tests prove convergence.
- [ ] `bun run ci` exits 0.

## STOP conditions
- Recovery can race an old leader still writing; strengthen/fix Plan 006 before continuing.
- Tail deletion cannot identify all tables written by the window.
- Current-state rematerialization cannot reproduce the pre-window state.
- Real ClickHouse tests cannot observe mutation completion reliably.

## Maintenance notes
Any new table added to core-window writes must also be added to recovery cleanup and a failure-injection boundary test.
