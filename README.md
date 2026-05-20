# OnlyDoge

OnlyDoge is a Bun-based investigation and explorer backend focused on Dogecoin. It indexes Dogecoin chain data, stores raw block snapshots, materializes current UTXO and balance state, and exposes authenticated APIs for explorer reads and investigation workflows.

The codebase is a modular monolith. Business logic lives in modules with explicit contracts, while infrastructure adapters live in `packages/platform`.

## Current Status

- Dogecoin current-state indexing is implemented with ClickHouse append-only core tables.
- Current spendable UTXOs, balances, applied block identity, transaction lookup, and address history are backed by ClickHouse.
- Raw block snapshots are stored in file storage locally and S3-compatible storage in production.
- API authentication, network catalog, entity labeling, tags, stats, heartbeat, and OpenAPI are implemented.
- Full transfer/direct-link graph materialization is not part of the current production indexer.

Operational details live in:

- [Production runbook](docs/production-runbook.md)
- [Dogecoin rebuild runbook](docs/dogecoin-rebuild-runbook.md)
- [Explorer API notes](docs/dogecoin-explorer-api.md)

## Repository Layout

```text
apps/
  api/          Elysia app assembly
  indexer/      indexer process entrypoint
  onlydoge/     unified CLI entrypoint

packages/
  shared-kernel/  shared errors, IDs, value objects
  platform/       runtime wiring and infrastructure adapters
  modules/
    access-control/
    network-catalog/
    entity-labeling/
    explorer-query/
    investigation-query/
    indexing-pipeline/

tests/
  e2e/          opt-in production E2E checks with teardown
  integration/
  unit/
```

## Runtime Shape

```mermaid
flowchart LR
  Client["Client"] --> Proxy["Reverse proxy"]
  Proxy --> API["onlydoge-api"]
  API --> PG["PostgreSQL"]
  API --> CH["ClickHouse"]
  API --> Raw["Raw block storage"]
  Indexer["onlydoge-indexer"] --> PG
  Indexer --> CH
  Indexer --> Raw
  Indexer --> RPC["Dogecoin RPC"]
```

Runtime modes:

- `http`: API only
- `indexer`: indexer only
- `both`: combined local/dev mode

Production should run split `http` and `indexer` containers.

## Requirements

- Bun 1.3.x
- Docker and Docker Compose for local or self-hosted stacks
- PostgreSQL
- ClickHouse
- S3-compatible object storage for production raw snapshots
- Dogecoin RPC endpoint

## Development

Install dependencies and run checks:

```bash
bun install
bun run lint
bun run typecheck
bun run test
bun run ci
```

Run locally with Docker:

```bash
cp .env.local.example .env.local
bun run docker:local:up
```

Useful local commands:

```bash
bun run docker:local:logs
bun run docker:local:down
bun run docker:local:reset
```

Local service endpoints:

- API: `http://localhost:2277`
- OpenAPI UI: `http://localhost:2277/openapi`
- OpenAPI JSON: `http://localhost:2277/openapi/json`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- ClickHouse HTTP: `http://localhost:8123`
- PostgreSQL: `localhost:5432`

Run host-native app modes:

```bash
bun run dev:both
bun run dev:http
bun run dev:indexer
```

## Configuration

Core application settings:

- `ONLYDOGE_DATABASE`
- `ONLYDOGE_DATABASE_HOST`
- `ONLYDOGE_DATABASE_PORT`
- `ONLYDOGE_DATABASE_NAME`
- `ONLYDOGE_DATABASE_USER`
- `ONLYDOGE_DATABASE_PASSWORD`
- `ONLYDOGE_DATABASE_SSLROOTCERT_PEM`
- `ONLYDOGE_DATABASE_SSLROOTCERT_BASE64`
- `ONLYDOGE_STORAGE`
- `ONLYDOGE_S3_ACCESS_KEY_ID`
- `ONLYDOGE_S3_SECRET_ACCESS_KEY`
- `ONLYDOGE_WAREHOUSE`
- `ONLYDOGE_WAREHOUSE_USER`
- `ONLYDOGE_WAREHOUSE_PASSWORD`
- `ONLYDOGE_WAREHOUSE_REQUEST_TIMEOUT_MS`
- `ONLYDOGE_MODE`
- `ONLYDOGE_IP`
- `ONLYDOGE_PORT`

Current Dogecoin indexer tuning:

- `ONLYDOGE_INDEXER_SYNC_WINDOW`
- `ONLYDOGE_INDEXER_SYNC_CONCURRENCY`
- `ONLYDOGE_CORE_BLOCK_TIMEOUT_MS`
- `ONLYDOGE_CORE_DB_STATEMENT_TIMEOUT_MS`
- `ONLYDOGE_CORE_SYNC_COMPLETE_DISTANCE`
- `ONLYDOGE_CORE_PROCESS_LOAD_CONCURRENCY`
- `ONLYDOGE_CORE_PROCESS_WINDOW`
- `ONLYDOGE_CORE_PROGRESS_WATCHDOG_MS`
- `ONLYDOGE_CORE_RAW_STORAGE_TIMEOUT_MS`
- `ONLYDOGE_CORE_ONLINE_TIP_DISTANCE`

Managed deployment settings:

- `ONLYDOGE_IMAGE`
- `ONLYDOGE_PUBLIC_HOST`
- `ONLYDOGE_REMOTE_DIR`
- `ONLYDOGE_SSH_TARGET`
- `ONLYDOGE_SSH_JUMP`

In production, startup fails fast if database config, `ONLYDOGE_STORAGE`, or `ONLYDOGE_WAREHOUSE` are missing. Production will not silently fall back to local SQLite, file storage, or DuckDB.

## Dogecoin Runsheet

Use the runsheet helper to create the first API key if needed, register Dogecoin, and verify stats:

```bash
bun run runsheet:dogecoin -- \
  --base-url http://127.0.0.1:2277 \
  --rpc-endpoint 'http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/' \
  --name 'Dogecoin Mainnet' \
  --chain-id 0 \
  --block-time 60 \
  --rps 25
```

If API keys already exist:

```bash
bun run runsheet:dogecoin -- \
  --base-url http://127.0.0.1:2277 \
  --api-token 'sk_...' \
  --rpc-endpoint 'http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/'
```

OnlyDoge starts indexing after a network is created when the runtime is in `both` or `indexer` mode.

## Production

The preferred production shape is Docker Compose with separate API and indexer containers:

- `onlydoge-api`
- `onlydoge-indexer`
- PostgreSQL
- S3-compatible raw block storage
- ClickHouse
- reverse proxy such as Caddy, Nginx, Traefik, or a cloud load balancer

Self-hosted production-style stack:

```bash
cp .env.production.example .env.production
bun run docker:prod:up
```

Stop or inspect it:

```bash
bun run docker:prod:down
bun run docker:prod:logs
```

Managed Docker deployment:

```bash
cp .env.managed.example .env.managed
bun run deploy:docker -- --envFile .env.managed
```

The managed deploy script:

- resolves the image to an immutable digest,
- uploads `docker-compose.managed.yml`,
- uploads `docker/caddy/Caddyfile`,
- writes the resolved env file to the target host,
- starts Caddy, API, and indexer with Docker Compose,
- verifies `/up`,
- verifies `/v1/heartbeat`,
- verifies indexer health.

## Images

Production images are published to GitHub Container Registry:

- `ghcr.io/simonbetton/onlydoge-indexer:latest`
- `ghcr.io/simonbetton/onlydoge-indexer:vX.Y.Z`

Push a fresh multi-arch image:

```bash
bun run image:push
```

Initialize the Buildx builder only:

```bash
bun run image:builder:init
```

## Rebuild And Benchmarks

Benchmark the ClickHouse core backfill:

```bash
bun run benchmark:clickhouse-core -- --networkId 1 --blocks 100 --ranges 3 --execute
```

Destructive Dogecoin current-state rebuild:

```bash
bun run rebuild:clickhouse-core -- --networkId 1 --execute
```

Materialize current state:

```bash
bun run materialize:clickhouse-core -- --networkId 1 --asOfBlockHeight <height> --reset --execute
```

Prepare core-backed history:

```bash
bun run scripts/prepare-clickhouse-core-history.ts -- --networkId 1 --execute --wait --mark-ready
```

See [docs/dogecoin-rebuild-runbook.md](docs/dogecoin-rebuild-runbook.md) before running destructive commands.

## API Surface

Current `/v1` route groups:

- `/v1/heartbeat`
- `/v1/explorer`
- `/v1/stats`
- `/v1/info`
- `/v1/keys`
- `/v1/networks`
- `/v1/entities`
- `/v1/addresses`
- `/v1/tokens`
- `/v1/tags`

Explorer endpoints:

- `GET /v1/explorer/networks`
- `GET /v1/explorer/search?q=...`
- `GET /v1/explorer/blocks`
- `GET /v1/explorer/blocks/:ref`
- `GET /v1/explorer/transactions/:txid`
- `GET /v1/explorer/addresses/:address`
- `GET /v1/explorer/addresses/:address/transactions`
- `GET /v1/explorer/addresses/:address/utxos`

Explorer routes require `x-api-token`, like the rest of `/v1`, except `/up`, `/v1/heartbeat`, and `/openapi`.

History-dependent routes return `425` until Dogecoin history readiness is marked true.

## Production E2E

The production E2E suite is opt-in and tears down the disposable metadata it creates:

```bash
bun run e2e:production
```

Required environment:

- `PROD_BASE_URL`
- `PROD_ADMIN_API_TOKEN`
- `EXPECTED_IMAGE_DIGEST`

Optional SSH checks:

- `PROD_SSH_TARGET`
- `PROD_SSH_JUMP`

Deploy plus E2E:

```bash
bun run e2e:production:deploy
```

## Quality Gates

```bash
bun run lint
bun run typecheck
bun run test
bun run ci
```

GitHub Actions:

- `CI`: lint, typecheck, Vitest, and production Docker build.
- `Security`: CodeQL, dependency review, secret scanning, and image vulnerability scanning.
- `Publish Image`: multi-arch GHCR publish with SBOM and provenance.
- `Deploy Production`: manual environment-protected deploy plus production E2E.

## Notes For Operators

- Keep passwords URL-safe if you inject them directly into `ONLYDOGE_DATABASE`.
- The current ClickHouse schema supports Dogecoin current-state reads and core-backed history without a separate full transfer graph.
- The local and production Compose stacks assume the warehouse database name is `onlydoge`.
- Raw block storage is written to S3-compatible object storage in Dockerized environments.
- The checked-in ClickHouse memory profile assumes a warehouse node in roughly the `16 GB RAM` class.
- ClickHouse log-retention files cap system logs, host syslog, ClickHouse file logs, and journald at 3 days.
