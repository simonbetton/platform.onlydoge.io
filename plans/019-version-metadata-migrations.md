# Plan 019: Version metadata migrations and verify every SQL driver

> **Executor instructions**: Preserve existing data and support SQLite, Postgres and MySQL. Build the ledger/verification path before converting current migrations.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- packages/platform/src/metadata-store.ts packages/platform/src/metadata-query.ts tests/unit/metadata-store.test.ts`

## Status
- **Priority**: P1
- **Effort**: L
- **Risk**: MED
- **Depends on**: `plans/015-add-production-adapter-ci-lane.md`
- **Category**: migrations
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
Metadata startup runs ad-hoc methods and `CREATE IF NOT EXISTS` statements with no migration ledger. MySQL lock acquisition results are unchecked, rollback is undefined, and only SQLite is continuously tested.

## Target design
Ordered immutable migrations with version/name/checksum, a metadata migration ledger, one locked connection for the whole run, per-migration verification, and explicit resumability for MySQL's auto-committing DDL.

## Scope
**In scope**
- Metadata migration runner/definitions in `packages/platform`.
- Existing schema/migration conversion.
- SQLite/Postgres/MySQL contract tests and status command/docs.
- `plans/README.md` (status only)

**Out of scope**
- ClickHouse migrations.
- Adopting Drizzle as part of this plan.
- Destructive automatic down migrations.

## Steps
1. Add the ledger bootstrap table and migration definition contract. Reject duplicate/out-of-order versions and checksum changes.
2. Acquire and *verify* a driver lock on a dedicated connection:
   - Postgres advisory lock;
   - MySQL `GET_LOCK` result exactly `1`, bounded timeout, checked release;
   - SQLite/libsql immediate/write transaction or equivalent serialization.
   Execute all migration statements through that same executor.
3. Convert current `migrateApiKeys`, active statements, audit/watch alterations and legacy drops into ordered versions. Treat the SQLite table rebuild in `migrateActiveMempoolWatches` as an explicit copy/verify/swap migration; every destructive drop requires row-count/key verification before removal.
4. Handle pre-ledger databases via an explicit baseline migration: inspect required tables/columns/indexes, apply missing idempotent steps, verify, then record. Never mark a baseline based only on table existence.
5. Use transactions where the driver truly supports transactional DDL. For MySQL, split steps into replay-safe units and record completion only after verification.
6. Run the same contract suite on all three drivers: fresh, restart no-op, upgrade from fixture, interrupted migration, lock contention, failed acquisition and checksum drift.
7. Add a read-only migration status command and operator recovery notes. Recovery is roll-forward; no automatic destructive rollback.

## Commands you will need
- `bun run test:adapters` → SQLite, Postgres and MySQL migration matrices pass.
- `bun run ci` → exit 0.

## Verification
- Production-adapter metadata matrix passes.
- Two concurrent initializers serialize and produce one ledger row per version.
- Schema manifest matches across drivers modulo type syntax.
- `bun run ci` exits 0.

## Done criteria
- [ ] Every schema change has an immutable version/checksum.
- [ ] Lock acquisition failure aborts migration.
- [ ] Existing databases baseline through schema verification.
- [ ] Fresh/restart/interruption tests pass on all drivers.
- [ ] Legacy drops are guarded.

## STOP conditions
- A deployed database has schema drift not represented by known states; inventory it before baselining.
- libsql remote transactions cannot hold the required migration lock; introduce a lease/sentinel with fencing.
- A MySQL migration cannot be made replay-safe.

## Maintenance notes
Repository startup may invoke the runner, but production deployment should expose migration status separately so schema failures are diagnosed before app rollout.
