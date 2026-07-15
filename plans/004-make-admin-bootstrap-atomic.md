# Plan 004: Guarantee exactly one bootstrap admin API key

> **Executor instructions**: Preserve the unauthenticated-first-key product behavior while making the transition atomic. Run every gate and update `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- packages/modules/access-control/src packages/platform/src/metadata-store.ts tests/integration/api.test.ts`
> Stop if bootstrap ownership moved to another module.

## Status
- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: `plans/001-restore-verification-baseline.md`
- **Category**: security
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
The first API key is intentionally unauthenticated and becomes admin. Today concurrent requests can both observe an empty table and both create admins, turning deployment bootstrap into an authorization race.

## Current state
- The HTTP guard checks emptiness before auth at `packages/modules/access-control/src/infrastructure/http.ts:371-380`.
- The service checks again, then performs a separate insert:
  ```ts
  const hasConfiguredKeys = await this.hasConfiguredKeys();
  this.assertCanCreateKey(hasConfiguredKeys, actor);
  // ...
  const created = await this.apiKeys.createApiKey(entity.record);
  ```
- `ApiKeyRepository` exposes count and insert separately (`contracts/api-key-repository.ts:4-13`).
- Metadata supports SQLite, Postgres and MySQL; production uses Postgres.
- Domain vocabulary: “API key” is identity; “API token” is the returned secret. Do not rename either.

## Commands you will need
- `bunx vitest run tests/integration/api.test.ts tests/unit/metadata-store.test.ts` → all pass.
- `bun run typecheck` → exit 0.
- `bun run ci` → exit 0.

## Scope
**In scope**
- `packages/modules/access-control/src/contracts/api-key-repository.ts`
- `packages/modules/access-control/src/application/access-control-service.ts`
- `packages/platform/src/metadata-store.ts`
- `tests/integration/api.test.ts`
- `tests/unit/metadata-store.test.ts`
- `plans/README.md` (status only)

**Out of scope**
- Removing first-key bootstrap.
- Changing token format/hash, roles, or later admin-created keys.
- Adding Redis or an external coordinator.

## Steps
### Step 1: Define an atomic repository operation
Add a repository method whose result distinguishes “created first key” from “bootstrap already completed.” It must atomically lock/claim the empty→configured transition and insert the supplied record.

**Verify**: `bun run typecheck` should fail only at the unimplemented adapter/service call sites before Step 2.

### Step 2: Implement the operation per relational driver
Use one transaction/connection for claim, emptiness check and insert:
- Postgres: transaction-scoped advisory lock or row/sentinel lock.
- MySQL: checked named lock or transactional sentinel.
- SQLite/libsql: serialized transaction or unique sentinel claim.
Never leave a claimed sentinel committed without its API-key row.

**Verify**: unit test calls the operation concurrently and exactly one result reports creation; `countApiKeys()` and `countActiveAdminApiKeys()` both equal 1.

### Step 3: Route unauthenticated creation through the atomic operation
Retain the HTTP pre-check only as a fast authorization decision; do not treat it as the security boundary. In `createKey`, use atomic first-key creation when no actor is present. If another request wins, return the existing protected-route authorization error without exposing the losing generated token.

**Verify**: existing single-request bootstrap remains 200 with role `admin`; subsequent unauthenticated creation is 401.

### Step 4: Add a concurrent HTTP regression test
Issue multiple distinct simultaneous `POST /v1/keys/` requests against a fresh test app. Assert one success, all others unauthorized/conflict according to the established error contract, and exactly one active admin.

**Verify**: targeted integration test passes repeatedly (run it at least 10 times).

## Test plan
- One normal bootstrap succeeds and returns one API token.
- Concurrent bootstrap creates exactly one admin.
- Authenticated admin can still create member/admin keys.
- A losing bootstrap response never returns an API token.
- Repository atomicity is covered separately from HTTP behavior.

## Done criteria
- [ ] Exactly one first admin can be created under concurrency.
- [ ] No check-then-insert path remains for unauthenticated bootstrap.
- [ ] Targeted tests and `bun run ci` pass.
- [ ] No response or log contains a losing generated token.

## STOP conditions
- A supported driver cannot provide transaction/unique-claim semantics without a schema migration; report the required migration before proceeding.
- The operation would require storing plaintext API tokens.
- Existing clients rely on multiple unauthenticated bootstrap successes.

## Maintenance notes
The repository atomic operation—not the HTTP pre-check—is the authorization invariant. Review future bootstrap changes against all three drivers.
