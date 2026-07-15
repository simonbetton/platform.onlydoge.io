# Plan 010: Distinguish missing raw blocks from storage failure and corruption

> **Executor instructions**: Return `null` only for a verified not-found condition. Preserve abort/deadline behavior and update `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- packages/platform/src/raw-block-storage.ts tests/unit/raw-block-storage.test.ts`

## Status
- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: `plans/001-restore-verification-baseline.md`
- **Category**: correctness
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
File and S3 reads catch every exception and return `null`. Permission failures, outages, corrupt gzip and malformed JSON are therefore misreported as an absent block and may trigger unnecessary RPC reload/rewrite.

## Current state
`FileRawBlockStorageAdapter.getPart` catches all errors at `raw-block-storage.ts:24-30`. `S3RawBlockStorageAdapter.getPart` does the same after abort handling at `:67-74`. `decodeJsonGzip` can throw for gzip or JSON corruption.

## Commands you will need
- `bunx vitest run tests/unit/raw-block-storage.test.ts` → all tests pass.
- `bun run typecheck` → exit 0.
- `bun run ci` → exit 0.

## Scope
**In scope**
- `packages/platform/src/raw-block-storage.ts`
- `tests/unit/raw-block-storage.test.ts`
- Shared error imports only if needed.
- `plans/README.md` (status only)

**Out of scope**
- Changing object keys or gzip format.
- Automatic corruption repair.
- Retrying S3 requests beyond SDK policy.

## Steps
1. Add narrow predicates for filesystem not-found (`code === 'ENOENT'`) and AWS S3 not-found (`NoSuchKey`, `NotFound`, or HTTP 404 from typed SDK metadata). Return `null` only for those.
2. Re-throw permission, network, service and decoding errors with the original error as `cause`; use `InfrastructureError` from `packages/shared-kernel/src/domain/errors.ts` if callers require domain translation.
3. Keep timeout/abort errors higher priority than not-found classification.
4. Add tests for missing file/object, EACCES, S3 403/500, corrupt gzip, invalid JSON and aborted request. Mock the S3 client at its command boundary without real credentials.

## Verification
- `bunx vitest run tests/unit/raw-block-storage.test.ts` passes.
- `bun run ci` exits 0.

## Test plan
- File `ENOENT` and S3 404 return `null`.
- File permission failure and S3 403/500 throw with causes.
- Corrupt gzip and malformed JSON throw.
- Abort/deadline errors retain their established message/status.

## Done criteria
- [ ] Only verified absence returns `null`.
- [ ] Corruption and infrastructure errors remain observable with causes.
- [ ] Abort behavior is unchanged.
- [ ] File and S3 classifications are tested.

## STOP conditions
- Existing callers depend on `null` for transient storage errors; identify and plan their explicit fallback before changing semantics.
- AWS SDK error shapes differ in the pinned version; base classification on observed typed fields, not message text.

## Maintenance notes
When adding a storage backend, define its exact not-found predicate and corruption tests before implementing `getPart`.
