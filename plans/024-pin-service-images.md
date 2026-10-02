# Plan 024: Pin ClickHouse and MinIO images used by development and tests

> **Executor instructions**: Resolve current official image tags/digests at execution time; do not invent digest values. Test data compatibility before changing production Compose.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- docker-compose.local.yml docker-compose.prod.yml tests/integration/clickhouse-analytics-smoke.test.ts .github/dependabot.yml docs`

## Status
- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: `plans/001-restore-verification-baseline.md`
- **Category**: dependencies/operations
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
Local, self-hosted production and smoke tests use floating `latest` tags for ClickHouse, MinIO and `mc`. Infrastructure behavior can change without a repository commit and CI may not reproduce production.

## Current state
- `docker-compose.local.yml:19-51` uses latest MinIO, `mc` and ClickHouse images.
- `docker-compose.prod.yml:18-48` repeats the same floating images.
- `tests/integration/clickhouse-analytics-smoke.test.ts:20-21` defaults to latest ClickHouse.

## Scope
**In scope**
- ClickHouse, MinIO and MinIO client images in Compose/tests.
- Consistency test/update documentation.
- Dependabot Docker coverage validation.
- `plans/README.md` (status only)

**Out of scope**
- User-selected `ONLYDOGE_IMAGE` application release tag.
- Immediate major-version upgrades.
- Base image pinning unrelated to these services.

## Steps
1. Record currently deployed/local image versions and review release notes. Select supported immutable tags, then resolve official multi-architecture digests from the registry.
2. Pin `repository:version@sha256:digest` in local and production Compose. Pin compatible MinIO server/client releases as a pair.
3. Make the existing `tests/integration/clickhouse-analytics-smoke.test.ts` default to the same pinned ClickHouse reference while retaining an explicit environment override for upgrade testing. Plan 015 later applies the manifest to new adapter tests.
4. Add a consistency test/manifest check that rejects `:latest` for infrastructure services and detects differing ClickHouse defaults.
5. Validate existing ClickHouse data directory startup on a disposable copy/fixture and MinIO bucket initialization.
6. Document the upgrade procedure: backup, pull, run adapter/migration tests, inspect release notes, deploy, verify, rollback.

## Verification
- `rg "minio/(minio|mc):latest|clickhouse/clickhouse-server:latest" docker-compose*.yml tests` returns no matches.
- Local Compose config resolves immutable references.
- `bun run test:clickhouse-smoke` and a bounded local Docker Compose startup/health smoke pass. Plan 015 later reuses these pins in the full adapter lane.
- Dependabot recognizes the pinned images.
- `bun run ci` exits 0.

## Done criteria
- [ ] All three service images are immutable.
- [ ] ClickHouse test/default version matches Compose.
- [ ] Data/startup compatibility is tested.
- [ ] Upgrade procedure is documented.

## STOP conditions
- Registry digest is architecture-specific and would break supported platforms; use the manifest-list digest.
- Current production data cannot start on the selected image; pin the deployed version and plan migration separately.

## Maintenance notes
Update tags and digests together. Digest-only changes still require release-note review because they may change binaries under a mutable tag.
