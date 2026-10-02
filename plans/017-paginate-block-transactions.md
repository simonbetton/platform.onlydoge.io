# Plan 017: Paginate block-detail transactions before resolving inputs

> **Executor instructions**: Bound work before input resolution. Treat response-shape/default changes as an API contract change and update docs/snapshots deliberately.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- packages/modules/explorer-query/src/application/explorer-query-service.ts packages/modules/explorer-query/src/infrastructure/http.ts packages/modules/explorer-query/src/domain docs/dogecoin-explorer-api.md tests`

## Status
- **Priority**: P2
- **Effort**: M
- **Risk**: MED
- **Depends on**: `plans/009-cap-explorer-query-cost.md`
- **Category**: performance
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
`getBlock` resolves inputs and serializes every transaction in a block. One request has work and payload proportional to maximum block size.

## Target contract
`GET /v1/explorer/blocks/:ref?offset=&limit=` returns block summary plus:
`transactions`, `offset`, `limit`, `returnedCount`, and `totalCount`.
Use the shared explorer page policy (default 50, max 500 unless Plan 009 selects different constants).

## Scope
**In scope**
- Explorer HTTP/service response model.
- API snapshots and explorer documentation.
- Unit/integration tests.
- `plans/README.md` (status only)

**Out of scope**
- Cursor pagination.
- Transaction-detail response.
- Reformatting raw block storage.

## Commands you will need
- `bunx vitest run tests/unit/explorer-query-service.test.ts tests/integration/api.test.ts` → all pass.
- `bun run ci` → exit 0.

## Steps
1. Parse bounded pagination on the block-detail route with the shared helper from Plan 009.
2. First characterize that `loadResolvedInputs` for a transaction page needs only the selected transactions' prevout keys (which may reference older blocks, but not omitted transactions as a batch invariant). Then parse the block once, compute `totalCount`, slice transactions before `loadResolvedInputs`, and preserve original tx indexes by adding the page offset during serialization.
3. Return explicit page metadata. Never claim `returnedCount` larger than the serialized array.
4. Update OpenAPI descriptions, `docs/dogecoin-explorer-api.md`, snapshots and examples. Call out the formerly unbounded behavior as a contract change.
5. Add a large synthetic block test proving only page transactions are passed to input resolution and tx indexes remain global.

## Verification
- Focused explorer service and API integration tests pass.
- Snapshot updates contain only intentional pagination fields/content.
- `bun run ci` exits 0.

## Done criteria
- [ ] Default request resolves at most one bounded page.
- [ ] Explicit limit cannot exceed shared maximum.
- [ ] Page metadata and global tx indexes are correct.
- [ ] Docs and OpenAPI match implementation.

## STOP conditions
- Published clients require all transactions from this exact route; design a deprecation/version strategy before changing default behavior.
- Input resolution has hidden block-wide invariants; characterize them before slicing.

## Maintenance notes
Apply pagination before expensive enrichment. A response limit added after full computation does not solve this finding.
