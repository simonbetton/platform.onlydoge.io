# Plan 011: Reject malformed transactions instead of indexing partial blocks

> **Executor instructions**: Make block parsing fail closed. Do not broaden Dogecoin transaction types just to accept malformed fixtures. Update `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- packages/modules/indexing-pipeline/src/application/core-dogecoin-indexer-service.ts tests`

## Status
- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: `plans/001-restore-verification-baseline.md`
- **Category**: correctness
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
`readDogecoinTransactions` filters malformed entries and accepts the remainder. The resulting block records a reduced transaction count and incomplete UTXO effects as valid chain state.

## Current state
```ts
function readDogecoinTransactions(value: unknown): DogecoinTransaction[] {
  return Array.isArray(value) ? value.filter(isDogecoinTransaction) : [];
}
```
The parser already throws for malformed block records and required scalar fields, so transaction filtering is inconsistent with the surrounding fail-closed convention.

## Scope
**In scope**
- `packages/modules/indexing-pipeline/src/application/core-dogecoin-indexer-service.ts`
- `tests/integration/indexer.test.ts`
- `plans/README.md` (status only)

**Out of scope**
- RPC retries or node compatibility policy.
- Relaxing `isDogecoinTransaction`.
- Skipping whole malformed blocks and continuing at later heights.

## Steps
1. Require `block.tx` to be an array. Throw an actionable validation error when absent/wrong.
2. Validate every element with `isDogecoinTransaction`; on the first invalid item throw with block context and transaction index, but do not serialize the raw payload into the message/log.
3. Preserve order and count exactly when all entries are valid. Reject an empty transaction array: a valid Dogecoin block contains at least its coinbase transaction, and this indexer requests hydrated transaction objects.
4. Add tests for non-array `tx`, one invalid item among valid items, all-invalid input, empty input decision, and a normal block. Assert no warehouse apply occurs after parse rejection.

## Verification
- `bunx vitest run tests/integration/indexer.test.ts` passes.
- `bun run typecheck` and `bun run ci` exit 0.

## Done criteria
- [ ] Parser never silently drops a transaction.
- [ ] Error identifies location without dumping RPC data.
- [ ] Malformed block produces no application/warehouse write.
- [ ] Existing valid fixtures pass unchanged.

## STOP conditions
- Real supported Dogecoin Core responses contain transaction representations intentionally excluded by `isDogecoinTransaction`; update the domain guard with captured redacted shape tests first.
- The RPC request intentionally uses txid-only verbosity; fix the request/contract rather than accepting strings here.

## Maintenance notes
Consensus-derived snapshots must be validated atomically: one malformed child invalidates the entire parent block.
