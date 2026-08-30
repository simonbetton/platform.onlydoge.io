# OnlyDoge

OnlyDoge is a Bun-based Dogecoin explorer backend. It indexes Dogecoin chain data, stores raw block snapshots, materializes current UTXO and balance state, and exposes authenticated APIs for explorer and analytics reads.

The codebase is a modular monolith. Business logic lives in modules with explicit contracts, while infrastructure adapters live in `packages/platform`.

## Current Status

- Dogecoin current-state indexing is implemented with ClickHouse append-only core tables in the production warehouse path.
- Production spendable UTXOs, balances, applied block identity, transaction lookup, and address history are backed by ClickHouse.
- Raw block snapshots are stored in file storage locally and S3-compatible storage in production.
- API authentication, explorer reads, analytics reads, heartbeat, and OpenAPI are implemented.
- The runtime is single-chain Dogecoin; network catalog, token catalog, labels, tags, and investigation graph workflows are intentionally absent.

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
    analytics-query/
    explorer-query/
    indexing-pipeline/

tests/
  adapters/     Docker-backed production adapter integration tests
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

- Bun 1.4.0
- Docker and Docker Compose for the bundled local or self-hosted stacks
- PostgreSQL, ClickHouse, and S3-compatible object storage for production-style stacks
- Dogecoin RPC endpoint for real indexing work

Host-native development can use the built-in local defaults for SQLite metadata, file raw-block storage, and the local warehouse file.

## Development

Install dependencies and run checks:

```bash
bun install
bun run lint
bun run typecheck
bun run test
bun run test:adapters
bun run ci
```

`test:adapters` is the service-backed CI lane. It starts pinned, isolated ClickHouse, PostgreSQL,
MySQL, and MinIO containers on collision-safe ports, verifies the production adapters and split
PostgreSQL watch flow, and removes its containers after the run. The default `test` command remains
service-free. Set `ONLYDOGE_ADAPTER_LOG_DIR` to retain service logs in a specific directory.

Treat Bun upgrades as one atomic change: update `packageManager` and
`bun-types` in `package.json`, the `Dockerfile` base image, and every
`oven-sh/setup-bun` workflow; refresh `bun.lock` with Bun, then run the Bun
version consistency test, frozen install, full CI gate, and production image
build.

The out-of-process Node ZMQ bridge has its own npm manifest and lockfile under
`scripts/zmq-rawtx-bridge`. Update it with the pinned Node toolchain, commit the
resulting `package-lock.json`, and verify with `npm ci --omit=dev`,
`npm audit --omit=dev --audit-level=high`, the bridge module smoke check, and
the production image build. Dependabot maintains this npm subtree separately
from the Bun workspace.

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

The local Docker stack includes `dogeorg/dogecoin-node` for Dogecoin Core RPC and ZMQ. First sync can take a long time and needs substantial disk space. On Apple Silicon, Docker runs the amd64 image under emulation. If you previously started a standalone `dogecoin-node` container on the same ports, stop and remove it before bringing this stack up.

Local service endpoints:

- API: `http://localhost:2277`
- OpenAPI UI: `http://localhost:2277/openapi`
- OpenAPI JSON: `http://localhost:2277/openapi/json`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- ClickHouse HTTP: `http://localhost:8123`
- PostgreSQL: `localhost:5432`
- Dogecoin RPC: `http://localhost:22555`
- Dogecoin P2P: `localhost:22556`
- Dogecoin ZMQ: `localhost:28332`

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
- `ONLYDOGE_CORE_REPROCESS_DEPTH`
- `ONLYDOGE_CORE_ONLINE_TIP_DISTANCE`

Managed deployment settings:

- `ONLYDOGE_IMAGE`
- `ONLYDOGE_PUBLIC_HOST`
- `ONLYDOGE_REMOTE_DIR`
- `ONLYDOGE_SSH_TARGET`
- `ONLYDOGE_SSH_JUMP`

Managed production uses env-based database CA material. Do not put `sslrootcert=/storage/do-ca.pem` in `ONLYDOGE_DATABASE`; use `ONLYDOGE_DATABASE_SSLROOTCERT_PEM` or `ONLYDOGE_DATABASE_SSLROOTCERT_BASE64`.

In production, startup fails fast if database config, `ONLYDOGE_STORAGE`, or `ONLYDOGE_WAREHOUSE` are missing. Production will not silently fall back to local SQLite, file storage, or DuckDB.

The default ClickHouse profile is tuned for stability over peak throughput.

- Address-heavy explorer reads use address-oriented read tables instead of scanning write-optimized facts directly.
- Current-state UTXO lookups avoid `FINAL` and use bounded `LIMIT 1 BY output_key` reads.
- The ClickHouse server memory ratio is capped below total RAM to leave headroom for merges, the page cache, and the OS.
- Default query and insert thread counts are capped to reduce memory spikes.
- External sort and group-by thresholds spill earlier instead of holding large intermediates in RAM.

The app boots ClickHouse with runtime-safe warehouse migrations. Existing deployments automatically create and backfill the address-oriented read tables on startup before serving explorer traffic. The checked-in `docker/clickhouse/init/001_schema.sql` file is retained for reference and local volume bootstrap, but production schema authority lives in the versioned ClickHouse migration ledger.

## Local Docker Workflow

The local setup intentionally favors developer feedback over immutability.

- The source tree is bind-mounted into the app container.
- The app runs `bun run apps/onlydoge/src/index.ts` without `bun --watch`; Docker restart policy recovers process exits.
- Infrastructure is persisted in Docker volumes.
- A MinIO bootstrap job creates the S3 bucket automatically.
- ClickHouse is initialized with the Dogecoin warehouse schema, including the address-oriented explorer read models.
- `/v1/heartbeat` stays open.
- `POST /v1/keys` stays open only until the first API key is created.
- Every other `/v1` route requires `x-api-token`.
- Each authenticated API key has an in-process budget of 300 protected requests per minute.

If you change dependency manifests, restart the local app container or rerun `bun run docker:local:up`.
If you change ClickHouse credentials, run `bun run docker:local:reset` before bringing the stack up again so the ClickHouse volume is recreated with the new user setup.

The local and bundled production Compose stacks mount the checked-in ClickHouse schema and memory tuning files:

- `docker/clickhouse/init/001_schema.sql`
- `docker/clickhouse/config.d/onlydoge-memory.xml`
- `docker/clickhouse/users.d/onlydoge-memory.xml`

The repository also includes ClickHouse host log-retention files for self-hosted ClickHouse operations:

- `docker/clickhouse/config.d/onlydoge-log-retention.xml`
- `docker/clickhouse/logrotate.d/clickhouse-server`
- `docker/clickhouse/logrotate.d/rsyslog`
- `docker/clickhouse/journald.conf.d/onlydoge-retention.conf`

Those files codify the current memory and 3-day log-retention profile used for heavy Dogecoin backfills. The retention files are installed on the ClickHouse host with the commands in the production runbook; they are not mounted by `docker-compose.local.yml` or `docker-compose.prod.yml`.

## Dogecoin Bootstrap

Configure Dogecoin through environment variables, start the API/indexer, then create the first API key with `POST /v1/keys`.

```bash
ONLYDOGE_DOGECOIN_RPC_ENDPOINT='http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/' \
ONLYDOGE_DOGECOIN_RPC_RPS=25 \
bun run dev:both
```

OnlyDoge starts indexing Dogecoin immediately when the runtime is in `both` or `indexer` mode.

## Production

The preferred production shape is Docker Compose with separate API and indexer containers:

- `onlydoge-api`
- `onlydoge-indexer`
- PostgreSQL
- S3-compatible raw block storage
- ClickHouse
- Reverse proxy such as Caddy, Nginx, Traefik, or a cloud load balancer

Self-hosted production-style stack:

```bash
cp .env.production.example .env.production
bun run docker:prod:up
```

Set `ONLYDOGE_DOGECOIN_RPC_ENDPOINT` in `.env.production` to an external Dogecoin
Core RPC endpoint that is reachable from both application containers; a host
`127.0.0.1` address points back into each container and will not work.
`ONLYDOGE_DOGECOIN_ZMQ_BLOCK_ENDPOINT` and `ONLYDOGE_DOGECOIN_ZMQ_TX_ENDPOINT`
are optional for the indexer, but are recommended for low-latency mempool watch
notifications. RPC polling remains the fallback when ZMQ is unavailable.

Stop or inspect it:

```bash
bun run docker:prod:down
bun run docker:prod:logs
```

Managed Docker deployment:

```bash
cp .env.managed.example .env.managed
bun run deploy:production
```

Managed production requires
`ONLYDOGE_ANALYTICS_WAREHOUSE_USER` and
`ONLYDOGE_ANALYTICS_WAREHOUSE_PASSWORD` for a dedicated ClickHouse identity.
Grant that identity read-only query access with safe resource settings; the
primary read/write warehouse credentials are never used as an analytics
fallback.

The production deploy command:

- validates canonical `.env.managed` settings,
- requires `PROD_ADMIN_API_TOKEN` unless `--skipE2e` is passed,
- runs lint, typecheck, and tests,
- builds and pushes the configured image tag,
- resolves the pushed image to an immutable digest,
- deploys that digest,
- fails fast if old `once-*` containers are still running on the target host,
- verifies `/up`, `/v1/heartbeat`, `/openapi/json`, and indexer health,
- prints production indexer stats freshness and `lastError`,
- runs production E2E by default.

The low-level deploy script remains available for rollback or automation:

```bash
bun run deploy:docker -- --envFile .env.managed --image ghcr.io/simonbetton/onlydoge-indexer@sha256:<digest>
```

The low-level deploy script:

- deploys the supplied image digest,
- uploads `docker-compose.managed.yml`,
- uploads `docker/caddy/Caddyfile`,
- writes the resolved env file to the target host,
- starts Caddy, API, and indexer with Docker Compose,
- verifies `/up`,
- verifies `/v1/heartbeat`,
- verifies `/openapi/json`,
- verifies indexer health,
- prints production indexer stats freshness and `lastError`.

`--skipE2e` is a health-check-only deploy. It skips API-key creation/teardown, disposable metadata checks, and the full production data-freshness assertions in the E2E suite.

## Images

Production images are published to GitHub Container Registry:

- `ghcr.io/simonbetton/onlydoge-indexer:latest`
- `ghcr.io/simonbetton/onlydoge-indexer:vX.Y.Z`

Push a fresh multi-arch image without deploying it:

```bash
bun run image:push
```

Initialize the Buildx builder only:

```bash
bun run image:builder:init
```

The image push script uses a `docker-container` Buildx builder named `onlydoge-multiarch`, which avoids the multi-platform limitation of the default Docker driver on tools like OrbStack. You still need to be authenticated to `ghcr.io` before pushing.

The production image defaults to `--mode=both` for compatibility, but production should run split API and indexer containers. The API container owns public HTTP health; the indexer container owns indexing progress and can restart independently.

## Rebuild

The Dogecoin rebuild is a breaking reset: stop API/indexer, delete legacy metadata and ClickHouse tables, remove old raw-block object prefixes, rotate API keys, deploy the networkless schema, resync from Dogecoin Core, then run invariant and explorer-parity gates before serving traffic.

See [docs/dogecoin-rebuild-runbook.md](docs/dogecoin-rebuild-runbook.md) before running destructive commands.

## API Surface

Current `/v1` route groups:

- `/v1/heartbeat`
- `/v1/explorer`
- `/v1/keys`
- `/v1/audit`
- `/v1/analytics`

API tokens are authentication credentials returned once by `POST /v1/keys` in the response `key` field and sent in the `x-api-token` header.

Error payload shape:

```json
{"error":"..."}
```

Explorer endpoints are networkless and do not accept `network` query parameters.
- `GET /v1/explorer/search?q=...`
- `GET /v1/explorer/mempool`
- `GET /v1/explorer/mempool/watch`
- `GET /v1/explorer/blocks`
- `GET /v1/explorer/blocks/:ref`
- `GET /v1/explorer/transactions/:txid`
- `GET /v1/explorer/addresses/:address`
- `GET /v1/explorer/addresses/:address/transactions`
- `GET /v1/explorer/addresses/:address/utxos`

Explorer routes require `x-api-token`. Public unauthenticated routes are `/up`, `/v1/heartbeat`, `/openapi`, and `/openapi/json`; `POST /v1/keys` is also unauthenticated only while bootstrapping the first API key.

`GET /v1/explorer/mempool` reads the node's current set of unconfirmed transactions through live Dogecoin RPC and returns a bounded, normalized page of metadata. The HTTP response is `no-store`; the API keeps a one-second in-process snapshot cache to keep repeated refreshes snappy.

History-dependent routes return `425` until Dogecoin history readiness is marked true. In online mode, raw sync and processing follow the Dogecoin node's confirmed chain tip, while the last `ONLYDOGE_CORE_REPROCESS_DEPTH` processed blocks remain reorg-active and are refreshed/replayed on tip updates. That reorg window is separate from the mempool.

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
bun run deploy:production
```

## Quality Gates

```bash
bun run lint
bun run typecheck
bun run test
bun run ci
```

GitHub Actions mirrors these gates:

- `CI`: lint, typecheck, Vitest, and production Docker build.
- `Security`: CodeQL, dependency review, secret scanning, and image vulnerability scanning.
- `Publish Image`: multi-arch GHCR publish with SBOM and provenance.
- `Deploy Production`: manual environment-protected deploy that runs quality checks, publishes the selected commit image, deploys that exact digest, and runs production E2E unless skipped.

## Notes For Operators

- Keep passwords URL-safe if you inject them directly into `ONLYDOGE_DATABASE`. If you need special characters, URL-encode them.
- The current ClickHouse schema supports Dogecoin current-state reads and core-backed history without a separate full transfer graph.
- The local and production Compose stacks assume the warehouse database name is `onlydoge`.
- Raw block storage is written to S3-compatible object storage in Dockerized environments.
- The checked-in ClickHouse memory profile assumes a warehouse node in roughly the `16 GB RAM` class. If you run a materially smaller box, lower the profile values before deployment.
- ClickHouse log-retention files cap system logs, host syslog, ClickHouse file logs, and journald at 3 days.
- The current checked-in indexer defaults are intentionally conservative for production backfill: `ONLYDOGE_CORE_BLOCK_TIMEOUT_MS=120000`, `ONLYDOGE_CORE_DB_STATEMENT_TIMEOUT_MS=30000`, `ONLYDOGE_CORE_SYNC_COMPLETE_DISTANCE=6`, `ONLYDOGE_CORE_PROCESS_LOAD_CONCURRENCY=8`, `ONLYDOGE_CORE_PROCESS_WINDOW=100`, `ONLYDOGE_CORE_PROGRESS_WATCHDOG_MS=180000`, `ONLYDOGE_CORE_RAW_STORAGE_TIMEOUT_MS=30000`, `ONLYDOGE_CORE_REPROCESS_DEPTH=10`, `ONLYDOGE_CORE_ONLINE_TIP_DISTANCE=6`, `ONLYDOGE_INDEXER_SYNC_WINDOW=32`, `ONLYDOGE_INDEXER_SYNC_CONCURRENCY=4`, and `ONLYDOGE_WAREHOUSE_REQUEST_TIMEOUT_MS=30000`.
