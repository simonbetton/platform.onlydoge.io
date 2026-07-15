# Plan 018: Version ClickHouse migrations from one canonical DDL source

> **Executor instructions**: Introduce the migration runner before deleting either schema source. Prove fresh, existing, partial and concurrent startup against real ClickHouse.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- docker/clickhouse/init packages/platform/src/warehouse.ts docker-compose*.yml tests/integration/clickhouse*`

## Status
- **Priority**: P1
- **Effort**: L
- **Risk**: HIGH
- **Depends on**: `plans/015-add-production-adapter-ci-lane.md`
- **Category**: migrations
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
Fresh Compose executes `docker/clickhouse/init/001_schema.sql`, while runtime repeats a second hand-maintained DDL array. Runtime uses `CREATE IF NOT EXISTS` and “backfill if target empty,” which cannot safely evolve populated or partially migrated tables.

## Target design
- One ordered migration directory/module consumed by runtime in every environment.
- A ClickHouse migration ledger with version, name, checksum, started/completed state and timestamps.
- Immutable applied migrations; checksum drift is fatal.
- Each migration declares its retry/resume verification and uses new-table/backfill/swap patterns for unsafe schema changes.
- Fresh Compose relies on the same runtime migration path, not a copied schema.
- Runtime obtains a named `clickhouse-schema` lock through the already-connected metadata store before reading/applying the ledger. Postgres uses a dedicated advisory-lock connection, MySQL a checked named lock, and SQLite/libsql a write lock. Hold it for the complete migration run.

## Scope
**In scope**
- ClickHouse migration definitions/runner and warehouse connect path.
- Existing DDL conversion and explicit backfill migration.
- Compose init removal/adjustment, tests and operator docs.
- `plans/README.md` (status only)

**Out of scope**
- Migrating metadata SQL (Plan 019).
- Rebuilding production data during this plan.
- Automatic down migrations.

## Steps
1. Define migration type `{version, name, checksum/source, up, verify}` and a ledger table. Validate strict ordering, unique versions and checksum consistency before applying.
2. Move the full current database object schema into migration `0001` as the sole canonical definition, including both address-oriented materialized views and their target tables. Keep ClickHouse users/grants in `docker/clickhouse/users.d`; they are server bootstrap, not application database migrations.
3. Convert current “backfill if empty” operations into explicit migrations. For populated-table evolution, create versioned replacement source/target tables, create a new materialized view pointing at the new target, populate deterministically, verify row/key invariants, then atomically rename/exchange and drop the old MV only after cutover. Never infer completion only from “has rows.”
4. Add a narrow metadata lock port and pass it from `createRuntime` into ClickHouse warehouse construction. Acquire the concrete `clickhouse-schema` lock described above. Migrations must still be replay-safe independently of the ledger; two concurrent runners must serialize or one must time out/fail cleanly.
5. On fresh ClickHouse, run the application migrator and then remove `docker/clickhouse/init/001_schema.sql`/mount. Keep only database/user bootstrap that cannot be performed after connecting.
6. Add real ClickHouse tests for fresh install, no-op restart, checksum mismatch, simulated interruption after each step, populated upgrade and two concurrent runners.
7. Add `clickhouse:migrate`/`clickhouse:migrate:status` operator commands and document backup/verification/roll-forward recovery.

## Commands you will need
- `bun run test:adapters` → fresh, upgrade, interruption and concurrent ClickHouse migration tests pass.
- `bun run ci` → exit 0.

## Verification
- Production-adapter ClickHouse suite passes all migration scenarios.
- Compare `system.columns`, engines, order keys and materialized views from migrated fresh DB to expected manifest.
- Repository search finds one DDL definition per object.
- `bun run ci` exits 0.

## Done criteria
- [ ] One canonical DDL source exists.
- [ ] Applied versions/checksums are durable and inspectable.
- [ ] Partial/concurrent migration tests converge safely.
- [ ] Existing-table changes no longer use empty-table inference.
- [ ] Fresh Compose boots without duplicate schema SQL.

## STOP conditions
- Current deployment cannot guarantee backup/restore before a populated-table migration.
- ClickHouse version lacks required atomic rename/exchange semantics; design a compatible cutover.
- Concurrent startup cannot be coordinated safely; add a dedicated one-shot migration job and service dependency.

## Maintenance notes
Never edit an applied migration. Add a new version with explicit validation and roll-forward recovery notes.
