# Plan 014: Fail health checks on online errors and lag

> **Executor instructions**: Distinguish expected backfill lag from unhealthy online lag. Add recovery clearing for persisted errors, tests, and runbook updates.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- scripts/indexer-health.ts packages/modules/indexing-pipeline/src/application/core-dogecoin-indexer-service.ts scripts/deploy-docker.ts docs/production-runbook.md tests`

## Status
- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: `plans/001-restore-verification-baseline.md`
- **Category**: DX/operations
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
Health watches staleness only during two backfill stages. An `online` indexer can retain `lastError` or stop advancing far behind `onlineTip` while containers and deployments remain green.

## Current state
- `scripts/indexer-health.ts:36-46` marks only `sync_backfill` and `process_backfill` as watched.
- `scripts/deploy-docker.ts:228-232` already runs this script; the missing behavior is policy, not wiring.
- `docs/production-runbook.md:210` explicitly says container health does not prove freshness.
- Successful `publishWindowProgress` already clears `lastError`; `ensureReadyOnlineState` can clear it before a processed window proves recovery and must not do so.

## Target policy
- Missing/invalid state: unhealthy after normal container startup grace.
- Any non-null `lastError`: unhealthy.
- Backfill: allow chain lag, but fail stale progress by watchdog.
- Online: set `nodeTip = max(config block_height, state.onlineTip)` and fail when `nodeTip - processTail` exceeds `coreOnlineTipDistance` (default 6). Docker retries provide transient smoothing.
- Successful progress after a transient error clears `lastError`.

## Scope
**In scope**
- `scripts/indexer-health.ts`
- Core indexer error-clear point.
- Health/deploy tests and production runbook.
- Existing settings only; add a health-specific threshold only if current online-tip distance is semantically unsuitable.
- `plans/README.md` (status only)

**Out of scope**
- Automatic process restart policy.
- Public status endpoint.
- Requiring complete backfill before deployment succeeds.

## Steps
1. Extract health evaluation into pure exported functions and keep CLI startup thin. Evaluate state validity, persisted error, stage-aware freshness and online lag.
2. Read canonical `block_height` as well as state. Use `max(block_height, onlineTip) - processTail`, clamped at zero, for online lag so a stuck state cannot make its own stale `onlineTip` look current. Do not compare backfill tails to the tip as a health failure.
3. Preserve the existing post-window `lastError` clear and remove/prevent any clear during online promotion before successful processing.
4. Keep deploy health on this same script result. Improve error text with stage, node tip, tails, lag, age and redacted last-error text.
5. Add fake-time table tests for fresh/stale backfill, fresh online at/over threshold, online error, malformed timestamp, missing state and successful recovery.

## Verification
- Run new `tests/unit/indexer-health.test.ts` and relevant indexer integration tests.
- `bun run ci` exits 0.

## Done criteria
- [ ] Online persisted errors fail health.
- [ ] Sustained online lag fails health.
- [ ] Normal backfill lag remains healthy while progressing.
- [ ] Recovered progress clears stale errors.
- [ ] Runbook no longer says to ignore errors separately from health.

## STOP conditions
- `onlineTip` is not refreshed while online, making lag meaningless.
- Existing orchestration would restart healthy long backfills under the new policy.
- Error clearing cannot be tied to durable successful progress.

## Maintenance notes
Keep health logic pure and unit-tested; deploy, Compose and future status endpoints should share its policy rather than reimplementing thresholds.
