# Plan 013: Avoid querying the ClickHouse address summary twice

> **Executor instructions**: Change production wiring, not response semantics. Add query-count regression coverage and update `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- packages/platform/src/runtime.ts packages/platform/src/warehouse.ts tests`

## Status
- **Priority**: P2
- **Effort**: S
- **Risk**: MED
- **Depends on**: `plans/001-restore-verification-baseline.md`
- **Category**: performance
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
For ClickHouse, runtime passes the same adapter as both the composite's current and history stores. `CompositeWarehouseAdapter.getAddressSummary` therefore invokes the full three-query ClickHouse summary twice.

## Current state
`runtime.ts:44-49` selects `factWarehouse` as `explorerStateStore` for ClickHouse and still wraps it in `CompositeWarehouseAdapter`. The composite runs current/history in parallel (`warehouse.ts:3355-3360`), while ClickHouse `getCurrentAddressSummary` delegates back to `getAddressSummary`.

## Scope
**In scope**
- `packages/platform/src/runtime.ts`
- Minimal warehouse/composite tests and runtime wiring tests.
- Optional fusion of balance/count only if covered and behavior-identical.
- `plans/README.md` (status only)

**Out of scope**
- Address schema/table redesign.
- Caching summaries across requests.
- Changing DuckDB/mirrored fallback behavior.

## Steps
1. In ClickHouse mode, inject `factWarehouse` directly as the explorer warehouse. Retain `CompositeWarehouseAdapter` only where current and historical stores are genuinely distinct.
2. Add a wiring/adapter test with counted fakes proving one `getAddressSummary` call reaches the ClickHouse adapter and that non-ClickHouse composition still combines current balance/count with historical movement totals.
3. If straightforward, combine ClickHouse native balance and spendable UTXO count over the same deduplicated subquery. Do not force this optional optimization if it obscures query correctness.

## Verification
- Focused warehouse/runtime tests pass.
- Existing explorer API address snapshots remain unchanged.
- `bun run ci` exits 0.

## Done criteria
- [ ] ClickHouse address request executes one summary operation, not two.
- [ ] Non-ClickHouse fallback composition remains intact.
- [ ] Query/call count is regression-tested.
- [ ] API response is unchanged.

## STOP conditions
- The composite supplies semantics absent from direct ClickHouse `getAddressSummary`; document the mismatch before rewiring.
- Runtime type narrowing requires broad unsafe casts.

## Maintenance notes
Do not compose an adapter with itself. Future runtime wiring tests should assert collaborator identity as well as response output.
