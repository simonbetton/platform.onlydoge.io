# Plan 012: Centralize and bound mempool-watch catch-up work

> **Executor instructions**: Preserve catch-up semantics (`source: 'catchup'`) while removing one full hydration per session. Match the ADR's ZMQ-primary/RPC-fallback design. Update `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- packages/platform/src/mempool-watch-session.ts packages/platform/src/mempool-appear-detector.ts packages/platform/src/runtime.ts packages/platform/src/settings.ts tests/unit/mempool-*`

## Status
- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: `plans/001-restore-verification-baseline.md`, `plans/007-make-metadata-cas-atomic.md`
- **Category**: performance
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
Every new session fetches and decodes the full mempool. The detector also polls every 250 ms and retains every txid ever seen. Cost grows with session rate, mempool churn and process lifetime.

## Target design
The indexer-side detector owns one compact cache of the *current* mempool. It hydrates/deltas in bounded batches, prunes departed txids, and re-matches cached transactions when `watch_changed` arrives. Sessions subscribe/register/notify and wait; they do not hydrate the mempool independently.

Initial limits:
- `ONLYDOGE_MEMPOOL_WATCH_RPC_POLL_MS=1000`
- `ONLYDOGE_MEMPOOL_WATCH_RPC_BATCH_SIZE=100`
- `ONLYDOGE_MEMPOOL_WATCH_RPC_CONCURRENCY=4`
- `ONLYDOGE_MEMPOOL_WATCH_CACHE_MAX_TXIDS=100000`

If the current mempool exceeds the cache maximum, persist `mempool_watch_detector_status` with:
`{ version: 1, ownerInstanceId, degraded, observedTxids, observedAt, expiresAt }`.
Updates use CAS. A detector may clear degradation only when the unexpired marker is owned by its own instance; another healthy detector cannot clear another instance's failure. Refresh a degraded marker every poll and expire it after three poll intervals so a dead owner does not block sessions forever. Sessions reject with the existing temporary-unavailable/too-early contract only for an unexpired `degraded: true` marker. Do not truncate silently.

## Commands you will need
- `bunx vitest run tests/unit/mempool-appear-detector.test.ts tests/unit/mempool-watch-session.test.ts` → all pass.
- `bun run typecheck` and `bun run ci` → exit 0.

## Scope
**In scope**
- `packages/platform/src/mempool-watch-session.ts`
- `packages/platform/src/mempool-appear-detector.ts`
- `packages/platform/src/mempool-watch-types.ts`
- `packages/platform/src/runtime.ts` and `packages/platform/src/settings.ts`
- Relevant unit tests and watch docs.
- `docs/dogecoin-explorer-api.md`
- Bounded batching/concurrency using existing RPC queue conventions.
- `plans/README.md` (status only)

**Out of scope**
- Persisting the whole mempool to ClickHouse.
- Changing one-shot SSE payload/event names.
- Removing ZMQ.
- Sampling endpoint optimization unrelated to watches.

## Steps
1. Replace the unbounded historical `seenTxids` with current-mempool state. Each snapshot must remove txids no longer present. Cache only fields required by `matchWatchTransaction`, not raw RPC blobs.
   - **Verify**: add/remove churn returns cache size to current snapshot size.
2. Fetch new txids in batches of 100 with at most four batch requests in flight and through the RPC work queue. Do not issue one unbounded `getRawTransactions` call. Parse the four settings above and use the 1,000 ms fallback default; ZMQ remains the low-latency path.
   - **Verify**: a 10k-tx fixture never exceeds configured batch/concurrency.
3. On startup/first active watch, hydrate once. On `watch_changed`, refresh active watches and match the current cache so a newly registered watch can receive an existing mempool match. Publish with `source: 'catchup'`.
   - **Verify**: two sessions cause one hydration, and the second can match a cached pre-existing transaction.
4. Remove `MempoolWatchSessionService.findCatchupAppear` and its direct full-mempool RPC dependency after detector behavior is proven. Subscribe before registry exposure so a fast catch-up event cannot race past the session.
5. Enforce the 100,000-txid maximum and owner/TTL/CAS degradation policy above. Clear degradation only after that same owner caches a complete within-bound snapshot.

## Verification
- `bunx vitest run tests/unit/mempool-appear-detector.test.ts tests/unit/mempool-watch-session.test.ts` passes.
- Fake-time tests contain no real polling sleeps.
- `bun run ci` exits 0.

## Test plan
- Two watches share one initial hydration.
- New txids are fetched in 100-item batches with concurrency at most four.
- Departed txids are evicted and cache size follows the current snapshot.
- A snapshot over 100,000 sets degradation and causes new session rejection.
- Recovery under the cap clears degradation and catch-up resumes.
- A second detector cannot clear another instance's live degraded marker; an expired marker can be reclaimed.

## Done criteria
- [ ] Session creation performs no full-mempool hydration.
- [ ] Raw transaction fetches are batched/bounded.
- [ ] Departed txids are evicted.
- [ ] New watches still catch existing cached matches.
- [ ] Overflow is observable, never silent.

## STOP conditions
- Split-role bus cannot guarantee watch-change delivery; resolve topology before removing session catch-up.
- Product semantics do not require pre-registration mempool matches; document that decision and simplify rather than building cache rematching.
- Compact current-mempool state cannot fit the agreed memory budget; design a TTL metadata index.

## Maintenance notes
Tests must track RPC calls and peak cache size, not only emitted events; otherwise the original scaling regression can return unnoticed.
