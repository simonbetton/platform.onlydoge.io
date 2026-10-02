# Plan 015: Add a production-adapter integration lane

> **Executor instructions**: Build deterministic integration coverage without changing default unit-test fixtures. Keep all credentials test-only placeholders and clean containers in `finally`.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- package.json .github/workflows/ci.yml tests docker-compose.local.yml`

## Status
- **Priority**: P1
- **Effort**: L
- **Risk**: MED
- **Depends on**: `plans/001-restore-verification-baseline.md`, `plans/024-pin-service-images.md`
- **Category**: tests
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
Default tests use SQLite, file storage and an in-process/local warehouse. Production-specific behavior in ClickHouse, Postgres NOTIFY, S3, split API/indexer and MySQL migrations is not continuously verified.

## Current state
- `tests/helpers.ts:31-34` forces SQLite, file storage and local JSON warehouse.
- `package.json:21-24` has only default tests, opt-in ClickHouse analytics smoke and production E2E.
- `.github/workflows/ci.yml:20-64` runs quality and Docker build, with no adapter job.
- `tests/integration/clickhouse-analytics-smoke.test.ts:15-21` is opt-in and analytics-only.

## Target lane
Add `bun run test:adapters` and a required CI job that exercises:
1. ClickHouse core indexing + explorer reads + analytics read-only user.
2. Postgres metadata and cross-process-style NOTIFY bus.
3. MinIO through the S3 adapter, including not-found/corruption/error paths.
4. MySQL metadata connect/fresh bootstrap/reconnect and a minimal repository smoke.
5. Split HTTP/indexer watch event flow using Postgres.

## Scope
**In scope**
- Dedicated `tests/adapters/` or clearly named integration files.
- Reusable container lifecycle/readiness helpers.
- `package.json`, CI workflow and test docs.
- Production adapter fixes only when a test exposes a defect already covered by a selected plan; otherwise stop/report.
- `plans/README.md` (status only)

**Out of scope**
- Production E2E against live infrastructure.
- Replacing fast default tests.
- Floating service tags.

## Steps
1. Extract the current raw Docker ClickHouse startup into a reusable test helper with collision-safe names/ports, readiness deadlines, captured logs on failure and guaranteed cleanup.
2. Add pinned Postgres, MySQL and MinIO helpers or CI service containers. Never use host credentials.
3. Build a minimal shared metadata repository smoke and run it for SQLite, Postgres and MySQL: fresh connect/bootstrap, config round-trip, API-key CRUD and reconnect. Plan 019 later adds migration/interruption/lock contracts; Plans 004/007 add their own contention cases.
4. Expand ClickHouse smoke from analytics-only to a small two/three-block happy-path index plus address/transaction/block explorer reads and query limits. Plan 008 owns retry/crash/reorg failpoints.
5. Add split-role bus/SSE proof: separate runtime/bus instances over Postgres, register watch in HTTP side, publish/detect on indexer side, receive one SSE event.
6. Add S3 round-trip, exact not-found and corruption/error classification against MinIO.
7. Add a required CI job after quality, with one explicit timeout and artifact/log upload on failure. Keep `bun run test` fast and service-free.

## Commands you will need
- `bun run test:adapters` → all adapter smoke suites pass.
- `bun run ci` → exit 0.
- Production Docker build → exit 0.

## Verification
- `bun run test:adapters` passes twice from a clean Docker state.
- `docker ps` shows no leaked test containers after success or intentional failure.
- Workflow syntax is valid and required job runs on PRs.
- `bun run ci` remains green.

## Done criteria
- [ ] All four production data adapters run in CI.
- [ ] Split Postgres watch flow is proven.
- [ ] Tests use pinned images and bounded waits.
- [ ] Failures preserve useful service logs.
- [ ] Fast test lane remains independent.

## STOP conditions
- Plan 024 is not complete or any adapter service still uses a floating image.
- Hosted CI cannot run required containers/resources; propose a smaller PR lane plus scheduled full lane.
- MySQL is no longer a supported runtime; remove it from settings/docs/dependencies rather than testing dead support.
- Adapter tests need real cloud credentials.

## Maintenance notes
Every production-only bug fix should add its regression to this lane. Keep fixtures tiny; adapter fidelity, not data volume, is the goal.
