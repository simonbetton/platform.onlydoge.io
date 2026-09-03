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

## Quickstart (Docker)

No configuration is required. This starts Dogecoin Core, PostgreSQL, ClickHouse, MinIO, and the
OnlyDoge API + indexer, then prints indexer status once the app is up:

```bash
bun install
bun run docker:local:up
```

Follow progress:

```bash
bun run docker:local:status          # one line: stage, tails, blocks/s, ETA
bun run docker:local:status:watch    # refresh every 5s
curl -s localhost:2277/v1/status | jq # same data as JSON, no API key needed
bun run docker:local:logs
```

Stop or wipe:

```bash
bun run docker:local:down            # keep data
bun run docker:local:reset           # drop app volumes (Postgres, ClickHouse, MinIO, node data)
```

To override defaults (ports, credentials, where the ~250 GB of chain data lives), copy
`.env.example` to `.env.local`; it is picked up automatically when present. Put the
Dogecoin data directory on an SSD: `ONLYDOGE_DATA_DOGECOIN=/path/on/ssd`. Spinning USB disks and
macOS bind mounts make node RPC slow enough to bottleneck the indexer.

The Dogecoin node is `docker/dogecoin`, a native amd64/arm64 image built from the official
Dogecoin Core release tarballs with checksum verification, so Apple Silicon and Graviton hosts
do not run the node under emulation. Node flags are driven by `DOGECOIN_*` env vars
(`-txindex=1`, `-rpcthreads=8`, `-rpcworkqueue=256`, `-dbcache=2048` by default).

Expect the node's own initial block download to take many hours and ~250 GB. The indexer waits
for it and reports `sync_eta`/`process_eta` once it has throughput samples.

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

Dogecoin node connection:

- `ONLYDOGE_DOGECOIN_RPC_ENDPOINT`
- `ONLYDOGE_DOGECOIN_RPC_TIMEOUT_MS` (per JSON-RPC request; default 60000)
- `ONLYDOGE_DOGECOIN_RPC_RPS` (per HTTP request, not per block; default 64)
- `ONLYDOGE_DOGECOIN_ZMQ_BLOCK_ENDPOINT`
- `ONLYDOGE_DOGECOIN_ZMQ_TX_ENDPOINT`

Current Dogecoin indexer tuning:

- `ONLYDOGE_INDEXER_SYNC_WINDOW` (blocks per sync window; progress is checkpointed every batch round, default 256)
- `ONLYDOGE_INDEXER_SYNC_BATCH_SIZE` (blocks per JSON-RPC batch, default 16)
- `ONLYDOGE_INDEXER_SYNC_CONCURRENCY` (max parallel batches; adapts down when the node is slow, default 8)
- `ONLYDOGE_INDEXER_SYNC_RETRY_ATTEMPTS` / `ONLYDOGE_INDEXER_SYNC_RETRY_BASE_DELAY_MS`
- `ONLYDOGE_CORE_BLOCK_TIMEOUT_MS`
- `ONLYDOGE_CORE_DB_STATEMENT_TIMEOUT_MS`
- `ONLYDOGE_CORE_SYNC_COMPLETE_DISTANCE`
- `ONLYDOGE_CORE_PROCESS_LOAD_CONCURRENCY`
- `ONLYDOGE_CORE_PROCESS_WINDOW`
- `ONLYDOGE_CORE_PROGRESS_WATCHDOG_MS`
- `ONLYDOGE_CORE_RAW_STORAGE_TIMEOUT_MS`
- `ONLYDOGE_CORE_REPROCESS_DEPTH`
- `ONLYDOGE_CORE_ONLINE_TIP_DISTANCE`

External managed databases can pass CA material through env: use `ONLYDOGE_DATABASE_SSLROOTCERT_PEM` or `ONLYDOGE_DATABASE_SSLROOTCERT_BASE64` rather than a file path inside the container.

In production, startup fails fast if database config, `ONLYDOGE_STORAGE`, or `ONLYDOGE_WAREHOUSE` are missing. Production will not silently fall back to local SQLite, file storage, or DuckDB.

The default ClickHouse profile is tuned for stability over peak throughput.

- Address-heavy explorer reads use address-oriented read tables instead of scanning write-optimized facts directly.
- Current-state UTXO lookups avoid `FINAL` and use bounded `LIMIT 1 BY output_key` reads.
- The ClickHouse server memory ratio is capped below total RAM to leave headroom for merges, the page cache, and the OS.
- Default query and insert thread counts are capped to reduce memory spikes.
- External sort and group-by thresholds spill earlier instead of holding large intermediates in RAM.

The app boots ClickHouse with runtime-safe warehouse migrations. Existing deployments automatically create and backfill the address-oriented read tables on startup before serving explorer traffic. The checked-in `docker/clickhouse/init/001_schema.sql` file is retained for reference and local volume bootstrap, but production schema authority lives in the versioned ClickHouse migration ledger.

## Local Docker Workflow

Compose files: `docker-compose.yml` is the complete default stack; `docker-compose.local.yml`
is an override layered on top by the `docker:local:*` scripts.

The local override intentionally favors developer feedback over immutability.

- The source tree is bind-mounted into the API and indexer containers (development image target).
- Both run `bun run apps/onlydoge/src/index.ts` without `bun --watch`; Docker restart policy recovers process exits.
- Infrastructure ports (Postgres, ClickHouse, MinIO, Dogecoin RPC/ZMQ) are published on localhost.
- Infrastructure is persisted in Docker volumes.
- A MinIO bootstrap job creates the S3 bucket automatically.
- ClickHouse is initialized with the Dogecoin warehouse schema, including the address-oriented explorer read models.
- `/v1/heartbeat` stays open.
- `POST /v1/keys` stays open only until the first API key is created.
- Every other `/v1` route requires `x-api-token`.
- Each authenticated API key has an in-process budget of 300 protected requests per minute.

If you change dependency manifests, rerun `bun run docker:local:up`.
Any other Compose invocation can be run through the same file set with `bun run docker:local -- <compose args>`.
If you change ClickHouse credentials, run `bun run docker:local:reset` before bringing the stack up again so the ClickHouse volume is recreated with the new user setup.

The default Compose stack mounts the checked-in ClickHouse schema and memory tuning files:

- `docker/clickhouse/init/001_schema.sql`
- `docker/clickhouse/config.d/onlydoge-memory.xml`
- `docker/clickhouse/users.d/onlydoge-memory.xml`

The repository also includes ClickHouse host log-retention files for self-hosted ClickHouse operations:

- `docker/clickhouse/config.d/onlydoge-log-retention.xml`
- `docker/clickhouse/logrotate.d/clickhouse-server`
- `docker/clickhouse/logrotate.d/rsyslog`
- `docker/clickhouse/journald.conf.d/onlydoge-retention.conf`

Those files codify the current memory and 3-day log-retention profile used for heavy Dogecoin backfills. The retention files are installed on the ClickHouse host with the commands in the production runbook; they are not mounted by `docker-compose.yml`.

## How Sync Works

The indexer (`packages/modules/indexing-pipeline`) runs one leader (Postgres lease) through three
stages, all resumable:

1. `sync_backfill`: fetch raw blocks from Dogecoin Core and store them. Each JSON-RPC batch asks
   for `getblockhash` × N then `getblock(hash, false)` × N and decodes the raw hex locally
   (`packages/platform/src/dogecoin-raw-block.ts`: AuxPoW header, txids, P2PKH/P2SH/P2PK/multisig
   addresses). One block costs the node one sequential block-file read; `-txindex` is never hit
   during sync. Snapshots go to raw block storage (MinIO/S3), a row per block to Postgres
   `core_blocks`, and txid→block refs to ClickHouse. `sync_tail` is the highest contiguous stored
   height and is checkpointed after every round of batches, so a crash or a slow node never loses
   completed work. Parallel batches adapt (AIMD): a failed batch halves concurrency, sustained
   success ramps it back.
2. `process_backfill`: read snapshots in windows of `ONLYDOGE_CORE_PROCESS_WINDOW`, derive UTXO
   creates/spends, and append them to ClickHouse behind a write-ahead marker that is rewound on
   crash. When processing catches the tip, current UTXO/balance state is materialized once and the
   stage becomes `online`.
3. `online`: incremental sync + process per new block, re-applying the last
   `ONLYDOGE_CORE_REPROCESS_DEPTH` blocks to absorb reorgs.

Failure model: RPC timeouts and node overload are retried with backoff and reduced concurrency;
they surface in `/v1/status` as `lastError` and in `scripts/indexer-health.ts` rather than by
restarting the process. The progress watchdog only exits the process when a step shows no
activity at all for `ONLYDOGE_CORE_PROGRESS_WATCHDOG_MS`, which indicates a genuine hang.

Status is available from three places with identical data: `GET /v1/status/` (public JSON),
`bun run status` / `bun run docker:local:status` (one line, `--watch` to poll, `--json`), and
the indexer container health check.

## Dogecoin Bootstrap

Configure Dogecoin through environment variables, start the API/indexer, then create the first API key with `POST /v1/keys`.

```bash
ONLYDOGE_DOGECOIN_RPC_ENDPOINT='http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/' \
ONLYDOGE_DOGECOIN_RPC_RPS=64 \
bun run dev:both
```

OnlyDoge starts indexing Dogecoin immediately when the runtime is in `both` or `indexer` mode.

## Production

There is one deployment shape: `docker-compose.yml` on a Docker host, with a reverse proxy
(Caddy, Nginx, Traefik, or a cloud load balancer) in front of port `2277` for TLS. Operational
detail lives in the [production runbook](docs/production-runbook.md).

Self-hosted stack (`docker-compose.yml` alone, production image target, bundled node):

```bash
cp .env.example .env      # set real passwords; every key is optional
bun run docker:up         # docker compose up -d --build
bun run docker:status
```

To use an external Dogecoin Core instead of the bundled one, set
`ONLYDOGE_DOGECOIN_RPC_ENDPOINT` (and optionally the two `ONLYDOGE_DOGECOIN_ZMQ_*`
endpoints) in `.env` to an address reachable from the containers — a host
`127.0.0.1` points back into each container and will not work — and start with
`docker compose up -d --scale dogecoin=0`. ZMQ is optional but recommended for
low-latency mempool watch notifications; RPC polling is the fallback.

Stop or inspect it:

```bash
bun run docker:down
bun run docker:logs
```

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

The image push script uses a `docker-container` Buildx builder named `onlydoge-multiarch`, which avoids the multi-platform limitation of the default Docker driver on tools like OrbStack. You still need to be authenticated to `ghcr.io` before pushing.

The production image defaults to `--mode=both` for compatibility; `docker-compose.yml` runs split API and indexer containers. The API container owns public HTTP health; the indexer container owns indexing progress and can restart independently. To run a published image instead of building locally, override `image:` for both app services with an immutable `@sha256:` reference.

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

## Notes For Operators

- Keep passwords URL-safe if you inject them directly into `ONLYDOGE_DATABASE`. If you need special characters, URL-encode them.
- The current ClickHouse schema supports Dogecoin current-state reads and core-backed history without a separate full transfer graph.
- The Compose stack assumes the warehouse database name is `onlydoge`.
- Raw block storage is written to S3-compatible object storage in Dockerized environments.
- The checked-in ClickHouse memory profile assumes a warehouse node in roughly the `16 GB RAM` class. If you run a materially smaller box, lower the profile values before deployment.
- ClickHouse log-retention files cap system logs, host syslog, ClickHouse file logs, and journald at 3 days.
- The current checked-in indexer defaults are intentionally conservative for production backfill: `ONLYDOGE_CORE_BLOCK_TIMEOUT_MS=120000`, `ONLYDOGE_CORE_DB_STATEMENT_TIMEOUT_MS=30000`, `ONLYDOGE_CORE_SYNC_COMPLETE_DISTANCE=6`, `ONLYDOGE_CORE_PROCESS_LOAD_CONCURRENCY=8`, `ONLYDOGE_CORE_PROCESS_WINDOW=100`, `ONLYDOGE_CORE_PROGRESS_WATCHDOG_MS=180000`, `ONLYDOGE_CORE_RAW_STORAGE_TIMEOUT_MS=30000`, `ONLYDOGE_CORE_REPROCESS_DEPTH=10`, `ONLYDOGE_CORE_ONLINE_TIP_DISTANCE=6`, `ONLYDOGE_INDEXER_SYNC_WINDOW=256`, `ONLYDOGE_INDEXER_SYNC_BATCH_SIZE=16`, `ONLYDOGE_INDEXER_SYNC_CONCURRENCY=8`, and `ONLYDOGE_WAREHOUSE_REQUEST_TIMEOUT_MS=30000`.
