# Plan 007: Implement metadata compare-and-swap as one atomic database operation

> **Executor instructions**: Preserve the port contract and JSON encoding. Implement true database atomicity for every supported driver, then update `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- packages/platform/src/metadata-store.ts packages/modules/indexing-pipeline/src/contracts/ports.ts tests/unit/metadata-store.test.ts`
> Stop if CAS already uses affected-row conditional SQL.

## Status
- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: `plans/001-restore-verification-baseline.md`
- **Category**: bug
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
Primary leadership acquisition calls a method named compare-and-swap, but the adapter performs a SELECT followed by an unconditional upsert. Two instances can both return true and both become primary.

## Current state
`packages/platform/src/metadata-store.ts:385-396`:
```ts
const current = await this.getJsonValue<T>(key);
if (JSON.stringify(current) !== JSON.stringify(expectedValue)) {
  return false;
}

await this.setJsonValue(key, nextValue);
return true;
```
`app_config` has `key` as primary key and stores canonical `JSON.stringify` text in `value_json`.

## Commands you will need
- `bunx vitest run tests/unit/metadata-store.test.ts tests/integration/indexer.test.ts` → all pass.
- `bun run typecheck` → exit 0.
- `bun run ci` → exit 0.

## Scope
**In scope**
- `packages/platform/src/metadata-store.ts`
- Tests in `tests/unit/metadata-store.test.ts`
- Test fakes only if the contract semantics need alignment.
- `plans/README.md` (status only)

**Out of scope**
- Changing config keys or lease JSON shape.
- Adding a distributed lock service.
- Broad metadata repository refactoring.

## Steps
### Step 1: Normalize affected-row results
Add the smallest private helper needed to execute a mutation and return affected-row count for libsql, Postgres and MySQL. Match existing `compileQuery` placeholder handling.

**Verify**: a focused test proves 0 and 1 affected rows are distinguished on the default SQLite adapter.

### Step 2: Implement CAS for an existing expected value
Serialize `expectedValue` and `nextValue` exactly as `setJsonValue` does. Execute:
`UPDATE app_config SET value_json = ?, updated_at = ? WHERE key = ? AND value_json = ?`.
Return true only when exactly one row changed.

**Verify**: two concurrent CAS calls with the same object expectation produce one true and one false; stored value equals the winner.

### Step 3: Implement atomic absent-key claim
For `expectedValue === null`, use each dialect's insert-if-absent form (`ON CONFLICT DO NOTHING` / `INSERT IGNORE` or equivalent). Return true only for the inserted row. Preserve the existing semantic treatment of absent values; document behavior if a literal JSON `null` row is possible.

**Verify**: 20 concurrent absent-key claims yield exactly one true and one persisted value.

### Step 4: Preserve compatibility tests
Keep sequential success/failure coverage and add concurrency cases. Do not test concurrency with a fake in-memory Map; use the real metadata adapter so the database primitive is exercised.

**Verify**: run the targeted suite at least 10 times.

## Test plan
- Absent key can be claimed once.
- Stale expected value returns false without mutation.
- Exact object expectation updates once under contention.
- Winner's JSON is readable through `getJsonValue`.
- Timestamps continue updating on successful swaps.

## Done criteria
- [ ] CAS has no read-then-write sequence.
- [ ] Return value derives from affected rows.
- [ ] Concurrent tests prove one winner.
- [ ] All three driver branches compile.
- [ ] `bun run ci` exits 0.

## STOP conditions
- A driver API cannot expose reliable affected-row counts; use a transaction/returning strategy and report the deviation.
- JSON serialization is non-canonical for values produced outside this repository, making text equality invalid; propose a version-column migration instead.
- Tests reveal literal JSON `null` is a supported stored value that must differ from absence.

## Maintenance notes
CAS is the foundation for primary leasing. Never replace conditional mutation with an application-level pre-read, even as an optimization.
