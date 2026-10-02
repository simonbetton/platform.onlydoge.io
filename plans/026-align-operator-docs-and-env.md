# Plan 026: Align operator documentation and env examples with runtime behavior

> **Executor instructions**: Execute after behavior-changing plans so docs describe the final system. Verify every command/key against source; do not document aspirational behavior.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- README.md CONTEXT.md docs .env.*.example docker-compose*.yml packages/platform/src/settings.ts packages/modules/explorer-query/src/infrastructure/http.ts`

## Status
- **Priority**: P3
- **Effort**: S
- **Risk**: LOW
- **Depends on**: `plans/003-wire-production-dogecoin-runtime.md`, `plans/012-bound-mempool-watch-work.md`, `plans/017-paginate-block-transactions.md`, `plans/018-version-clickhouse-migrations.md`
- **Category**: docs
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
README claims local `--watch` although Compose explicitly avoids it, omits the watch route, and describes duplicated ClickHouse init behavior. Production env examples omit ZMQ. The mempool ADR is a one-paragraph draft rather than a durable decision record.

## Current state
- `README.md:187-196` claims `bun run --watch`; `docker-compose.local.yml:131-139` explicitly avoids it.
- `README.md:342-350` omits the watch route, while `docs/dogecoin-explorer-api.md:7-15` already lists it but lacks full operator topology detail.
- `.env.production.example:25-29` has analytics/RPC settings but no ZMQ endpoints.
- `docs/adr/0001-mempool-watch-sse-via-zmq-and-notify.md:1-3` is a heading plus one decision paragraph.

## Scope
**In scope**
- `README.md`, `CONTEXT.md` only where vocabulary/runtime shape is stale.
- `.env.local.example`, `.env.production.example`, `.env.managed.example`.
- Explorer API, production runbook and ADR 0001.
- A docs/env consistency test.
- `plans/README.md` (status only)

**Out of scope**
- New product promises.
- Publishing external documentation.
- Changing runtime behavior to match stale prose.

## Steps
1. Inventory actual scripts, Compose commands, role topology, public/protected routes and settings names from source.
2. Correct local workflow: bind mount remains, but app does not run Bun watch; explain restart behavior accurately.
3. Add `GET /v1/explorer/mempool/watch` to README and complete the existing explorer API entry with its one-shot SSE lifecycle, auth, limits, timeout, events and ZMQ/RPC fallback/topology.
4. Add RPC/ZMQ settings to applicable production examples and explain reachability from containers. Reflect Plan 003's required/optional policy.
5. Update ClickHouse bootstrap/migration sections to Plan 018's one-source versioned process; remove references to deleted init files.
6. Expand ADR 0001 into Status, Context, Decision, Alternatives, Consequences, Failure modes, Security/operations and Test strategy. Preserve the recorded decision; do not silently change it.
7. Add a lightweight consistency test that checks documented route strings against route registration and ensures canonical runtime env keys appear in the relevant examples. Allow an explicit list for internal/optional keys.

## Verification
- Every README command resolves to a `package.json` script or valid Compose command.
- `rg "bun run --watch" README.md` returns no stale claim.
- Route/env consistency test passes.
- `bun run ci` exits 0.

## Done criteria
- [ ] Local/production workflows match Compose.
- [ ] Watch route and SSE contract are documented.
- [ ] RPC/ZMQ examples are complete and scoped.
- [ ] ADR follows repository decision-record structure.
- [ ] Automated drift check covers routes/env keys.

## STOP conditions
- A prior dependent plan changed its public contract but has no settled outcome; finish/refine it before final docs.
- An env value would expose a real credential; examples must contain placeholders only.

## Maintenance notes
Update docs and consistency fixtures in the same change as future route, setting or deployment-topology changes.
