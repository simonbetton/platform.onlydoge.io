# Plan 003: Make the self-hosted production stack reach Dogecoin Core

> **Executor instructions**: Follow steps exactly, preserve the managed deployment path, and update `plans/README.md` when done.
>
> **Drift check (run first)**: `git diff --stat c90e552..HEAD -- docker-compose.prod.yml .env.production.example README.md tests`
> Stop if the service names or runtime environment blocks no longer match this plan.

## Status
- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: `plans/001-restore-verification-baseline.md`
- **Category**: bug
- **Planned at**: commit `c90e552`, 2026-07-15

## Why this matters
`bun run docker:prod:up` is documented as supported, but neither app container receives the Dogecoin RPC endpoint from `.env.production`. Both can appear healthy while API mempool reads and indexing target `127.0.0.1` inside their own containers.

## Current state
- `.env.production.example:28-29` defines RPC endpoint and RPS.
- `docker-compose.prod.yml:86-130` and `:159-203` omit all `ONLYDOGE_DOGECOIN_*` values.
- Local Compose provides the intended pattern:
  ```yaml
  ONLYDOGE_DOGECOIN_RPC_ENDPOINT: ${ONLYDOGE_DOGECOIN_RPC_ENDPOINT:-http://...@dogecoin:22555/}
  ONLYDOGE_DOGECOIN_RPC_RPS: ${ONLYDOGE_DOGECOIN_RPC_RPS:-32}
  ONLYDOGE_DOGECOIN_ZMQ_BLOCK_ENDPOINT: ${ONLYDOGE_DOGECOIN_ZMQ_BLOCK_ENDPOINT:-tcp://dogecoin:28332}
  ONLYDOGE_DOGECOIN_ZMQ_TX_ENDPOINT: ${ONLYDOGE_DOGECOIN_ZMQ_TX_ENDPOINT:-tcp://dogecoin:28332}
  ```
- The ADR requires ZMQ `rawtx` for low latency, with RPC polling as fallback.

## Commands you will need
- `docker compose --env-file .env.production.example -f docker-compose.prod.yml config` → exit 0 and resolved Dogecoin variables under expected services.
- `bunx vitest run tests/unit/production-compose.test.ts` → all tests pass.
- `bun run ci` → exit 0.

## Scope
**In scope**
- `docker-compose.prod.yml`
- `.env.production.example`
- `README.md`
- `tests/unit/production-compose.test.ts` (new)
- `plans/README.md` (status only)

**Out of scope**
- `docker-compose.managed.yml` (already forwards the full env file).
- Bundling a Dogecoin node into production Compose.
- Changing RPC defaults, authentication, or the ZMQ bridge architecture.

## Steps
### Step 1: Pass RPC settings to both app roles
Add `ONLYDOGE_DOGECOIN_RPC_ENDPOINT` and `ONLYDOGE_DOGECOIN_RPC_RPS` to API and indexer environment blocks. Do not provide a localhost production fallback; unresolved required values should fail clearly.

**Verify**: Compose config shows both values in both services.

### Step 2: Pass ZMQ settings to the indexer
Add optional block and transaction ZMQ endpoint passthrough to `onlydoge-indexer`. The API does not start the appear detector.

**Verify**: Compose config shows both optional ZMQ keys under `onlydoge-indexer`.

### Step 3: Align the example and README
Add ZMQ placeholders to `.env.production.example`. State that self-hosted production requires an externally reachable Dogecoin Core RPC endpoint and that ZMQ is optional but recommended for watch latency.

**Verify**: `rg "ZMQ_(BLOCK|TX)_ENDPOINT" .env.production.example README.md docker-compose.prod.yml` finds consistent names.

### Step 4: Add a regression test
Create a small test that reads `docker-compose.prod.yml` and asserts RPC passthrough exists in both service blocks and ZMQ passthrough exists in the indexer block. Follow the plain Vitest style in `tests/unit/deploy-config.test.ts`.

**Verify**: targeted test passes.

## Test plan
- Missing RPC interpolation remains visible during `docker compose config`.
- Both roles receive RPC endpoint/RPS.
- Only the indexer needs ZMQ detector settings.
- Managed Compose is unchanged.

## Done criteria
- [ ] Production Compose resolves required RPC settings for API and indexer.
- [ ] Indexer resolves optional ZMQ settings.
- [ ] Example/docs match runtime keys.
- [ ] Regression test and `bun run ci` pass.

## STOP conditions
- The production stack now embeds Dogecoin Core or uses a different service topology.
- Settings names changed from `packages/platform/src/settings.ts`.
- Compose verification would require real credential values.

## Maintenance notes
When adding a new Dogecoin runtime setting, update local, self-hosted, managed examples and the Compose regression test together.
