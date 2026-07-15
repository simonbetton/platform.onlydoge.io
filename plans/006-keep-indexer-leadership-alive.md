# Plan 006: Keep indexer leadership alive during long windows

> **Executor instructions**: Implement lease renewal without weakening single-primary guarantees. Complete Plan 007 first. Update `plans/README.md` after verification.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- packages/modules/indexing-pipeline/src/application/core-dogecoin-indexer-service.ts packages/platform/src/settings.ts tests/integration/indexer.test.ts`
> Stop if primary coordination moved or the 15-second lease no longer exists.

## Status
- **Priority**: P1
- **Effort**: M
- **Risk**: HIGH
- **Depends on**: `plans/001-restore-verification-baseline.md`, `plans/007-make-metadata-cas-atomic.md`
- **Category**: bug
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
Leadership expires after 15 seconds, while one processing window may legitimately spend 30–120 seconds in database, storage, or RPC work. A second indexer can replace the “stale” leader while the first continues applying the same window.

## Current state
- `leaseTimeoutMs` is fixed at 15 seconds (`core-dogecoin-indexer-service.ts:64`).
- Configured operation timeouts are 30–120 seconds (`settings.ts:463-472`).
- Lease refresh occurs at loop entry and as one write among progress metadata.
- `publishProgress` unconditionally writes a fresh lease:
  ```ts
  const writes = [
    this.configs.setJsonValue(configKeyPrimary(), createLease(this.instanceId)),
    // progress writes...
  ];
  ```
- Owner refresh is also unconditional at `:996-998`.
- Compose already exposes `ONLYDOGE_INDEXER_LEASE_HEARTBEAT_INTERVAL_MS` with a 5,000 ms default, but `settings.ts` does not parse it.

## Commands you will need
- `bunx vitest run tests/integration/indexer.test.ts` → all pass.
- `bun run typecheck` → exit 0.
- `bun run ci` → exit 0.

## Scope
**In scope**
- `packages/modules/indexing-pipeline/src/application/core-dogecoin-indexer-service.ts`
- Its settings contract and `packages/platform/src/settings.ts` only if timing becomes configurable.
- `tests/integration/indexer.test.ts`
- Env examples/docs only for newly public settings.
- `plans/README.md` (status only)

**Out of scope**
- Distributed consensus beyond the existing metadata store.
- Making ClickHouse transactions globally serializable.
- Changing process-window size or operation timeout defaults as the primary fix.

## Steps
### Step 1: Separate acquisition, renewal and progress publication
Remove all unconditional primary writes from `publishProgress`. Acquisition and renewal must be explicit lease operations, and progress metadata must not confer leadership.

**Verify**: `rg "setJsonValue\\(configKeyPrimary" packages/modules/indexing-pipeline/src/application/core-dogecoin-indexer-service.ts` returns no matches.

### Step 2: Add periodic owner-checked renewal
Parse the existing `ONLYDOGE_INDEXER_LEASE_HEARTBEAT_INTERVAL_MS` setting (default 5,000 ms). Define lease freshness as three heartbeat intervals (15,000 ms by default). While a leader is processing or waiting in its primary loop, renew once per heartbeat interval. Renewal must use atomic CAS against the exact lease last observed/written. Serialize renewal attempts; do not allow overlapping timer callbacks.

**Verify**: a deferred processing test advances fake time past multiple original expiry periods and observes successful owner-checked renewals.

### Step 3: Treat failed renewal as lost leadership
If CAS fails or metadata renewal errors, mark the run as no longer primary, stop scheduling new work, and return/retry through the normal outer loop. Never overwrite the competing lease. Ensure timers are cleared in `finally` on success, error, abort and process-exit paths.

**Verify**: inject a competing lease during deferred work; the old instance performs no subsequent progress/lease overwrite and its renewal loop stops.

### Step 4: Test two-instance behavior
Use controlled promises/fake time:
1. Instance A acquires and begins a window lasting over 15 seconds.
2. A renews during the window.
3. Instance B attempts acquisition and remains non-primary.
4. In a separate test, force A renewal CAS failure and confirm B can take over without A reclaiming in progress publication.

**Verify**: targeted suite passes with no real sleeps.

## Test plan
- Long processing window retains one leader.
- Healthy competing instance cannot replace a renewed lease.
- Lost owner cannot refresh or publish itself back into leadership.
- Renewal timer is disposed on every exit path.
- Legacy string lease values still migrate through the existing compatibility path.

## Done criteria
- [ ] No unconditional primary lease writes remain.
- [ ] Active work renews lease before expiry.
- [ ] Renewal is atomic and owner-checked.
- [ ] Two-instance fake-time tests pass.
- [ ] `bun run ci` exits 0.

## STOP conditions
- Atomic CAS from Plan 007 is unavailable on any supported driver.
- A single synchronous CPU-bound operation blocks the event loop beyond the lease duration; report it for worker/thread redesign.
- Correctness requires fencing warehouse writes, which exceeds this plan; document the specific unprotected apply boundary.

## Maintenance notes
The lease interval must remain comfortably above expected metadata latency. Any new long-running synchronous step must either yield to renewal or execute behind a stronger fencing design.
