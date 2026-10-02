# Plan 009: Cap pagination and ClickHouse explorer query cost

> **Executor instructions**: Reject oversized requests at the HTTP boundary and apply scan limits only to explorer reads. Preserve response shapes. Update `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- packages/modules/explorer-query packages/modules/access-control packages/platform/src/warehouse.ts packages/shared-kernel tests`
> Stop if cursor pagination or centralized query budgets already replaced numeric offset/limit.

## Status
- **Priority**: P1
- **Effort**: M
- **Risk**: MED
- **Depends on**: `plans/001-restore-verification-baseline.md`
- **Category**: performance/security
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
Explorer, key and audit endpoints accept arbitrary integers, and production explorer reads do not carry ClickHouse scan/result caps. Authentication and request-rate limits do not bound the cost of one request.

## Current state
- Explorer HTTP parsing calls `parseNonNegativeInteger` without a maximum.
- Address pages default to 50 but do not cap explicit limits (`explorer-query-service.ts:797-798`).
- Mempool alone caps at 500.
- Analytics queries apply `max_execution_time`, rows/bytes-to-read and result limits (`warehouse.ts:4562-4570`); explorer `queryRows` does not.

## Target policy
Use named constants and one shared validator: default page 50, maximum page 500, and maximum offset 100,000. Return the existing validation-error response for violations. Use this initial explorer ClickHouse budget: 30 seconds, 10,000,000 rows read, 1 GiB read, and 100,000 result rows with overflow mode `throw`.

## Scope
**In scope**
- Explorer, API-key and audit numeric pagination parsing.
- Shared-kernel bounded integer helper if useful.
- All ClickHouse methods behind `ExplorerWarehousePort`.
- Unit/integration tests and API docs.
- `plans/README.md` (status only)

**Out of scope**
- Cursor pagination redesign.
- Analytics-query limits.
- Indexer/materialization ClickHouse settings.
- Rate-limit policy.

## Steps
1. Add a bounded non-negative parser that distinguishes absent/default, malformed, negative and over-maximum input. Apply it in `packages/modules/explorer-query/src/infrastructure/http.ts` and replace/extend the local `parsePagination` path in `packages/modules/access-control/src/infrastructure/http.ts` for key and audit routes.
   - **Verify**: boundary tests cover 0, maximum, maximum+1, negative and non-integer.
2. Cap service-level limits too, so non-HTTP callers cannot bypass the boundary. Keep mempool's existing maximum unless intentionally consolidated to the same constant.
   - **Verify**: direct service tests reject/clamp according to one documented policy; prefer reject for explicit oversized values.
3. Add an explorer-specific ClickHouse query helper/settings object with finite `max_execution_time`, `max_rows_to_read`, `max_bytes_to_read`, `max_result_rows`, `result_overflow_mode='throw'`, and `timeout_before_checking_execution_speed=0`. Route every `ExplorerWarehousePort` SELECT through it.
   - **Verify**: fake-client tests inspect settings; indexer queries remain unchanged.
4. Document page maxima and error behavior in OpenAPI descriptions and `docs/dogecoin-explorer-api.md`.

## Verification
- `bunx vitest run tests/unit/explorer-query-service.test.ts tests/integration/api.test.ts` passes.
- Add/execute the relevant warehouse adapter unit test for ClickHouse settings.
- `bun run ci` exits 0.

## Done criteria
- [ ] No public list endpoint accepts an unbounded limit/offset.
- [ ] Explorer ClickHouse reads have scan, time and result caps.
- [ ] Indexing queries do not inherit explorer caps.
- [ ] Boundary and adapter-setting tests pass.

## STOP conditions
- Existing clients demonstrably require pages above the proposed maximum; report usage before selecting a new cap.
- A read method is shared with indexer mutation/materialization paths and cannot be safely classified.
- `result_overflow_mode='break'` would return misleading partial explorer data; use error semantics instead.

## Maintenance notes
Any new explorer list or ClickHouse read must use the shared pagination policy and explorer query helper.
