# Plan 021: Update the libsql dependency chain to a patched WebSocket release

> **Executor instructions**: Re-run the audit first because advisories and registries change. Do not guess versions or add an override unless upstream constraints require it.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- package.json packages/platform/package.json bun.lock`

## Status
- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: `plans/001-restore-verification-baseline.md`
- **Category**: dependencies/security
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
The 2026-07-15 `bun audit` reported a high-severity memory-exhaustion advisory in `ws` through `@libsql/client`. SQLite/libsql deployments instantiate this chain directly.

## Current state
- `package.json:46` pins `@libsql/client` 0.17.3; `packages/platform/package.json:9` declares `^0.17.3`.
- `bun.lock:280-290` resolves the client through `@libsql/isomorphic-ws`; `bun.lock:650` records the current `ws` resolution.
- Exact vulnerable/patched ranges must come from the current advisory, not this plan.

## Commands you will need
- `bun audit` before and after update.
- `bun install --frozen-lockfile` after the updated lock is checked in.
- `bunx vitest run tests/unit/metadata-store.test.ts` and `bun run ci`.

## Scope
**In scope**
- Root/platform dependency declarations and `bun.lock`.
- SQLite metadata adapter contract tests.
- CI audit policy if absent.
- `plans/README.md` (status only)

**Out of scope**
- Replacing libsql.
- Suppressing the advisory without reachability evidence.
- Unrelated bulk upgrades.

## Steps
1. Run `bun audit` and capture package, advisory ID, vulnerable range, patched range and dependency path. If the finding no longer reproduces on the unchanged lockfile, mark this plan stale in the index and stop.
2. Use Bun to update `@libsql/client` to the latest compatible version that resolves a patched `ws`. Keep root and workspace declarations aligned.
3. If upstream still permits only a vulnerable range, first update the libsql client. Use a root resolution/override only as a temporary, tested last resort and document why API compatibility is safe.
4. Run SQLite fresh migration, CRUD, CAS/concurrency and reconnect tests. Check release notes for URL/auth/transaction behavior changes.
5. Run `bun audit` again and add/retain a CI audit gate that fails on high/critical production advisories with a documented exception mechanism.

## Verification
- `bun install --frozen-lockfile` succeeds after lock update.
- `bun audit` no longer reports the cited path/advisory.
- Metadata tests and `bun run ci` pass.

## Test plan
- Fresh SQLite metadata bootstrap and reconnect.
- API-key/config CRUD.
- CAS contention tests from Plan 007 when available.

## Done criteria
- [ ] Resolved graph contains a non-vulnerable WebSocket version.
- [ ] No broad unrelated upgrade is included.
- [ ] SQLite behavior is regression-tested.
- [ ] Audit evidence is recorded without secrets.

## STOP conditions
- No patched compatible release exists; document mitigation/reachability and upstream issue rather than forcing an incompatible override.
- Audit points to a different dependency path than libsql.
- Upgrade requires a metadata format migration.

## Maintenance notes
Dependabot/audit findings should reference the resolved lock path, not only direct dependencies.
