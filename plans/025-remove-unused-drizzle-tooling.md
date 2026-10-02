# Plan 025: Remove unused Drizzle migration tooling

> **Executor instructions**: The selected architecture is the versioned multi-driver metadata runner in Plan 019. Remove Drizzle only after that direction is confirmed; do not partially adopt it.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- drizzle.config.ts package.json packages/platform/package.json tsconfig.base.json .fallowrc.json bun.lock`

## Status
- **Priority**: P3
- **Effort**: S
- **Risk**: LOW
- **Depends on**: `plans/019-version-metadata-migrations.md`
- **Category**: architecture/dependencies
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
Drizzle config/dependencies imply a schema-generation workflow, but there is no Drizzle schema, generated migration directory or runtime import. The real system is hand-written multi-driver SQL, so the unused tooling adds dependency and operator ambiguity.

## Current state
- `drizzle.config.ts:1-10` points `schema` at the platform barrel and an unused `./drizzle` output.
- `package.json:49,66` and `packages/platform/package.json:15` declare Drizzle packages.
- `tsconfig.base.json:34` and `.fallowrc.json:3,132` treat the unused config as an entry/config file.

## Decision
Remove Drizzle. Adopting it would require modeling three driver dialects and reconciling existing custom migrations, with no current query-layer benefit.

## Scope
**In scope**
- Delete `drizzle.config.ts`.
- Remove `drizzle-orm` and `drizzle-kit` declarations/lock entries.
- Remove config references from TypeScript/Fallow.
- Update developer/migration docs.
- `plans/README.md` (status only)

**Out of scope**
- Rewriting SQL queries.
- Changing migration behavior from Plan 019.
- Removing libsql/MySQL/Postgres drivers.

## Steps
1. Re-run repository search for Drizzle imports/schema/generated artifacts. If real usage now exists, stop and reassess adoption.
2. Delete the unused config and remove both dependencies from root/platform manifests using Bun.
3. Remove `drizzle.config.ts` from `tsconfig.base.json` and `.fallowrc.json` entry/file groups without weakening unrelated architecture checks.
4. Update migration docs to point only at metadata and ClickHouse versioned runners.
5. Refresh lockfile and run dependency/architecture checks.

## Verification
- `rg -i "drizzle" --glob '!bun.lock' .` returns no stale tooling/docs references.
- `bun install --frozen-lockfile`, `bun run lint`, `bun run typecheck`, `bun run test` pass.
- `bun run ci` exits 0.
- `bunx fallow list --format json` exits 0 and no longer lists `drizzle.config.ts` as an entry point.

## Done criteria
- [ ] No Drizzle package/config remains.
- [ ] Migration docs name the actual runner.
- [ ] Architecture config remains valid.
- [ ] No unrelated lockfile upgrades occur.

## STOP conditions
- Plan 019 chooses Drizzle as its canonical runner.
- A branch has added a real Drizzle schema/import/migration.

## Maintenance notes
Do not add an ORM “for future use.” Introduce tooling only with a concrete schema owner, migration workflow and CI verification.
