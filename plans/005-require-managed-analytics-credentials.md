# Plan 005: Require least-privilege analytics credentials for managed deploys

> **Executor instructions**: Do not print or copy credential values. Change validation and documentation only as described, then update `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- scripts/deploy-config.ts tests/unit/deploy-config.test.ts packages/platform/src/warehouse.ts packages/platform/src/warehouse-query.ts .env.managed.example README.md`
> Stop if analytics authentication no longer falls back to primary warehouse credentials.

## Status
- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: `plans/001-restore-verification-baseline.md`
- **Category**: security
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
Managed deployment validation requires the read/write ClickHouse identity but not the analytics identity. If the latter is omitted, guarded user-authored SQL can run with broader primary warehouse privileges.

## Current state
- `scripts/deploy-config.ts:14-23` requires primary warehouse credentials but omits:
  - `ONLYDOGE_ANALYTICS_WAREHOUSE_USER`
  - `ONLYDOGE_ANALYTICS_WAREHOUSE_PASSWORD`
- `.env.managed.example:12-16` already documents both identities.
- `tests/unit/deploy-config.test.ts:60-67` covers missing runtime secrets using one primary key.
- `packages/platform/src/warehouse-query.ts:24-36` falls back from missing supplied credentials to `settings.user/password`.
- `packages/platform/src/warehouse.ts:4573-4584` returns no analytics credential override when both fields are absent, activating that fallback.

## Commands you will need
- `bunx vitest run tests/unit/deploy-config.test.ts` → all tests pass.
- `bun run ci` → exit 0.

## Scope
**In scope**
- `scripts/deploy-config.ts`
- `tests/unit/deploy-config.test.ts`
- `packages/platform/src/warehouse.ts`
- `packages/platform/src/warehouse-query.ts`
- Warehouse credential-selection tests.
- `.env.managed.example`
- Managed deployment notes in `README.md`
- `plans/README.md` (status only)

**Out of scope**
- Creating ClickHouse users or grants.
- Changing the analytics SQL guard.
- Requiring analytics to be enabled in local development; it may be explicitly unavailable when credentials are absent.
- Recording any real username/password.

## Steps
### Step 1: Remove analytics-to-primary fallback
Require analytics user/password as a complete pair when constructing the analytics ClickHouse client. If absent, mark analytics querying unavailable with an actionable configuration error; explorer/indexer use of the primary client may continue. Never pass `undefined` and inherit primary credentials.

**Verify**: fake-client option tests prove analytics username/password never equal primary values unless the operator explicitly (and visibly) configured the same pair.

### Step 2: Require both analytics identity fields in managed deploys
Add both canonical keys to `REQUIRED_RUNTIME_ENV_KEYS`. Keep the existing sorted missing-key error behavior.

**Verify**: deleting either key from the test fixture makes `createDeployConfig` throw an error naming that exact key.

### Step 3: Extend deploy-config tests
Add canonical placeholder analytics credentials to `canonicalEnv()`. Add parameterized missing-value coverage for user and password, including empty strings if validation currently treats them as absent.

**Verify**: targeted test passes and the accepted config forwards both keys.

### Step 4: Document the privilege boundary
State that managed production must provision a dedicated analytics user restricted to read-only query access and safe settings; primary warehouse credentials are not an acceptable fallback.

**Verify**: docs and example use the exact runtime names from `settings.ts`.

## Test plan
- Valid managed config forwards both analytics values.
- Missing analytics user fails before deployment.
- Missing analytics password fails before deployment.
- Existing missing primary-secret tests remain green.

## Done criteria
- [ ] Managed deploy cannot proceed without both analytics credential variables.
- [ ] Unit tests cover each missing variable.
- [ ] No credential value is committed.
- [ ] `bun run ci` exits 0.

## STOP conditions
- The managed deployment has intentionally removed analytics chat/readiness.
- ClickHouse access is now mediated by a service that does not use these variables.
- Validation changes would affect local/test environments beyond deploy tooling.

## Maintenance notes
Deployment validation is defense in depth. ClickHouse grants must still enforce read-only access even if SQL guarding regresses.
