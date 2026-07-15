# Production Runbook

This runbook describes the Docker + Caddy production shape. The reusable sections use example hostnames and avoid secrets; the OnlyDoge production notes section records project-specific context from the dated recovery.

## Components

- `onlydoge-api`: HTTP API process.
- `onlydoge-indexer`: long-running indexing process.
- PostgreSQL: metadata, leases, API keys, singleton Dogecoin config, and progress.
- S3-compatible object storage: raw block snapshots.
- ClickHouse: current-state and history read models.
- Reverse proxy: Caddy, Nginx, Traefik, or a cloud load balancer.

## Required Env Files

Create a private env file from one of the tracked examples.

```bash
cp .env.managed.example .env.managed
```

Replace every `replace-with-*`, `*.example.com`, `your-*`, and RFC 5737 example IP before running a real deployment. Do not commit the real env file.
Deploy files must use the canonical `ONLYDOGE_*` keys shown in `.env.managed.example`.
The managed Caddyfile reads `ONLYDOGE_PUBLIC_HOST` from the generated remote `.env`, so the proxy host follows `.env.managed` or the explicit `--host`/`PROD_BASE_URL` deploy input.
Managed production passes database CA material through env only. Do not put `sslrootcert=/storage/do-ca.pem` in `ONLYDOGE_DATABASE`; managed Compose does not mount `/storage`. Use `ONLYDOGE_DATABASE_SSLROOTCERT_PEM` or `ONLYDOGE_DATABASE_SSLROOTCERT_BASE64` instead, and do not print or commit the certificate.

Minimum managed deployment settings:

- `ONLYDOGE_IMAGE`
- `ONLYDOGE_PUBLIC_HOST`
- `ONLYDOGE_SSH_TARGET`
- `ONLYDOGE_SSH_JUMP`, if the host is reachable only through a bastion
- `ONLYDOGE_DATABASE`
- `ONLYDOGE_STORAGE`
- `ONLYDOGE_S3_ACCESS_KEY_ID`
- `ONLYDOGE_S3_SECRET_ACCESS_KEY`
- `ONLYDOGE_DATABASE_SSLROOTCERT_PEM` or `ONLYDOGE_DATABASE_SSLROOTCERT_BASE64`, when the database requires a custom CA
- `ONLYDOGE_WAREHOUSE`
- `ONLYDOGE_WAREHOUSE_USER`
- `ONLYDOGE_WAREHOUSE_PASSWORD`

## Local Deploy

Initialize the multi-arch builder once:

```bash
bun run image:builder:init
```

Run a complete local production deploy:

```bash
bun run deploy:production
```

The command validates `.env.managed`, requires `PROD_ADMIN_API_TOKEN`, runs CI, builds and pushes the configured image tag, deploys the resolved immutable digest, verifies public and indexer health, prints indexer stats freshness, and runs production E2E.

For a health-check-only deploy, skip the destructive E2E teardown suite explicitly:

```bash
bun run deploy:production -- --skipE2e
```

`--skipE2e` is health-check-only. It does not prove disposable metadata workflows, API-key creation/teardown, image digest checks, or Dogecoin data freshness beyond the deploy script's public health and stats summary.

Use a dry run to validate the local deploy plan without CI, image build, SSH, or deployment:

```bash
bun run deploy:production -- --dryRun
```

Dry runs still validate whether production E2E is enabled. Set `PROD_ADMIN_API_TOKEN` or pass `--skipE2e` with the dry run.

`deploy:docker` remains the low-level command for deploying a specific immutable image digest:

```bash
bun run deploy:docker -- --envFile .env.managed --image ghcr.io/simonbetton/onlydoge-indexer@sha256:<digest>
```

The low-level deploy script:

- deploys the supplied image digest,
- uploads `docker-compose.managed.yml`,
- uploads `docker/caddy/Caddyfile`,
- writes the resolved env file on the target host,
- fails before deploy if running `once-*` containers are present, because they can hold ports `80/443` or compete for the indexer lease,
- starts `caddy`, `onlydoge-api`, and `onlydoge-indexer`,
- verifies `/up`,
- verifies `/v1/heartbeat`,
- verifies `/openapi/json`,
- verifies indexer health,
- prints `stage`, `lastError`, and freshness fields from production stats.

If the preflight reports legacy containers such as `once-proxy` or `once-app-onlydoge-indexer...`, stop or remove them intentionally before deploying the managed stack. The deploy script will not stop them automatically.

## GitHub Actions Deploy

The checked-in `Deploy Production` workflow runs quality checks, builds and pushes the selected commit image, deploys that exact digest with the same low-level deploy script, and then runs the production E2E teardown suite unless `skip_e2e` is selected. It is manual-only and uses the `production` GitHub Environment.

Configure these `production` environment variables:

- `PROD_BASE_URL`: public API base URL, for example `https://api.example.com`
- `PROD_HOST`: optional host override for `deploy:docker`; if absent, the workflow derives it from `PROD_BASE_URL`

Configure these `production` environment secrets:

- `PROD_ENV_FILE`: complete managed env file contents based on `.env.managed.example`
- `PROD_SSH_PRIVATE_KEY`: private key able to reach the app host
- `PROD_SSH_TARGET`: app host SSH target
- `PROD_SSH_JUMP`: optional bastion SSH target
- `PROD_ADMIN_API_TOKEN`: existing production admin API token for E2E key creation and teardown verification
- `ONLYDOGE_DOGECOIN_RPC_ENDPOINT`: Dogecoin Core JSON-RPC endpoint reachable from the API/indexer containers

The workflow runs quality checks, builds and pushes a fresh multi-arch production image for the selected commit, deploys the resulting immutable digest, and passes that digest to `bun run e2e:production` as `EXPECTED_IMAGE_DIGEST`.

## Production E2E

The production E2E suite is destructive only for disposable API-key and audit metadata it creates during the run. It does not mutate Dogecoin explorer state.

Required environment:

- `PROD_BASE_URL`: public API base URL, for example `https://api.example.com`
- `PROD_ADMIN_API_TOKEN`: existing production admin API token
- `EXPECTED_IMAGE_DIGEST`: immutable image digest expected to be running
- `ONLYDOGE_DOGECOIN_RPC_ENDPOINT`: Dogecoin Core JSON-RPC endpoint configured in the deployed runtime env

Optional environment:

- `PROD_SSH_TARGET`: app host SSH target for container digest and health checks
- `PROD_SSH_JUMP`: SSH bastion target, if needed
- `PROD_PARITY_MAX_BLOCK_LAG`: allowed OnlyDoge latest-block lag versus BlockCypher, default `12`

Run the E2E suite after a deploy:

```bash
export PROD_BASE_URL=https://api.example.com
export PROD_ADMIN_API_TOKEN=sk_...
export EXPECTED_IMAGE_DIGEST=sha256:...
export PROD_SSH_TARGET=root@10.0.0.10
export PROD_SSH_JUMP=root@203.0.113.10

bun run e2e:production
```

Production parity uses BlockCypher's unauthenticated Dogecoin endpoints, so keep runs sparse to avoid public API rate limits.

Run deploy plus E2E in one command when `.env.managed` already selects the intended image tag and host:

```bash
bun run deploy:production
```

The suite creates an ephemeral API key using the admin API token, then uses that ephemeral API token for protected explorer and auth checks. Teardown deletes the ephemeral API key. Cleanup tolerates already-missing credentials but fails on unexpected cleanup errors.

The suite verifies:

- public `/up`, `/v1/heartbeat/`, and `/openapi/json`
- protected auth failures without an API token
- per-key rate limiting for authenticated protected requests
- Dogecoin production stats are `stage = "online"` with no `lastError`
- current-state explorer address and UTXO reads
- removed network, token, entity, address-label, tag, stats, and info routes return 404
- history endpoints return either ready data or the documented `425` history-not-ready response
- optional Docker host containers match `EXPECTED_IMAGE_DIGEST`

## Health Checks

### Structured logging and request correlation

API requests accept an optional `x-request-id` header. When the header is missing, malformed, or unsafe, the API generates a UUID. Every response includes `x-request-id`, and the same value is stored on audit rows and attached to structured request/error logs.

Background services (core indexer, mempool sampler, mempool appear detector, ZMQ bridge, warehouse recovery) log JSON via Pino with stable `service` and `component` fields. Correlate API traffic to background work by time window and shared deployment; background logs do not reuse API request IDs.

Sensitive fields (`authorization`, API tokens, RPC endpoints, passwords, secrets) are redacted automatically. Do not enable body logging or paste raw credentials into log queries.

Example production log tail:

```bash
docker compose --env-file .env -f docker-compose.managed.yml logs --tail=200 onlydoge-api onlydoge-indexer \
  | rg '"service":"onlydoge"'
```

Filter API errors for a single request after capturing `x-request-id` from the client response:

```bash
curl -fsS -D - -o /dev/null https://api.example.com/v1/heartbeat/
docker compose --env-file .env -f docker-compose.managed.yml logs --tail=500 onlydoge-api \
  | rg 'requestId":"<value-from-x-request-id>"'
```

Inspect metadata migrations without applying changes:

```bash
bun run metadata:migrations:status
```

The command reports the immutable version/name/checksum ledger, pending versions, and detected
schema or checksum drift. Run it with the same `ONLYDOGE_DATABASE` and TLS environment as the
application. A drift report exits non-zero and must be inventoried before deployment; do not edit
ledger rows or mark a baseline from table existence alone.

Metadata recovery is roll-forward only. Startup serializes migrations on one verified,
driver-specific locked connection and records a version only after schema verification. If a
MySQL DDL step is interrupted, preserve the database, rerun status, and restart the current image;
the replay-safe migration verifies existing work and completes it. Do not automatically roll back
or drop/copy legacy data. A non-empty legacy table, unknown ledger version, checksum mismatch, or
unrepresented schema state is a stop condition requiring an inventory and an explicit new
migration.

On the app host:

```bash
cd /opt/onlydoge
docker compose --env-file .env -f docker-compose.managed.yml ps
docker compose --env-file .env -f docker-compose.managed.yml logs --tail=200 onlydoge-api onlydoge-indexer
```

Public checks:

```bash
curl -fsS https://api.example.com/up
curl -fsS -i https://api.example.com/v1/heartbeat/
curl -fsS https://api.example.com/openapi/json >/dev/null
```

Indexer state can be inspected from inside the API container:

```bash
docker compose --env-file .env -f docker-compose.managed.yml exec -T onlydoge-api \
  bun -e 'const { createRuntime } = await import("@onlydoge/platform"); const runtime = await createRuntime({mode:"http"}); console.log(JSON.stringify(await runtime.metadata.getCoreIndexerState(1), null, 2)); process.exit(0);'
```

Healthy Dogecoin current-state production looks like:

- `stage = "online"`
- `lastError = null`
- `dogecoin_current_state_ready = true`
- `dogecoin_history_ready = true`
- `syncTail` aligned with `block_height` once online raw sync catches the node tip
- `processTail` aligned with `block_height` once online processing catches the node tip
- `finalizedTail` behind `processTail` by `indexer_reprocess_depth`
- `factTail` aligned with `processTail` when core-backed history is enabled

Raw block sync and processing can be at the node tip while the last reprocess-depth blocks remain reorg-active. The mempool is the node's current set of unconfirmed transactions, so it is separate from this canonicality window.

## OnlyDoge Production Notes

The known OnlyDoge production host from the 2026-05-25 UTC / 2026-05-26 NZ recovery is `platform.onlydoge.io`, with the managed app directory at `/opt/onlydoge`. During that recovery, the first managed deploy failed because `ONLYDOGE_DATABASE` still referenced `sslrootcert=/storage/do-ca.pem`; the fix was to retrieve the DigitalOcean managed Postgres CA and store it privately in ignored `.env.managed` as `ONLYDOGE_DATABASE_SSLROOTCERT_PEM`.

That recovery also found stale once-managed containers. `once-proxy` held ports `80/443`, and an old once-managed indexer was still running. Keep old once-managed containers stopped unless intentionally rolling back to that stack.

Indexer container health evaluates persisted errors, stage-aware progress freshness, and
online lag against `ONLYDOGE_CORE_ONLINE_TIP_DISTANCE`. Backfill may remain far behind the
node while it is advancing, but stale backfill progress, any `lastError`, malformed state,
or sustained online lag makes the container unhealthy. A successful processed window
clears a recovered transient error.

## ClickHouse Operations

Application startup and `bun run clickhouse:migrate` use the same ordered ClickHouse migrations.
Inspect the durable version/name/checksum/state ledger before and after a deployment:

```bash
bun run clickhouse:migrate:status
bun run clickhouse:migrate
bun run clickhouse:migrate:status
```

Before a migration on a populated warehouse, stop or quiesce writers and take a tested,
restorable ClickHouse backup. Do not continue if backup/restore cannot be guaranteed. The
migrator holds the `clickhouse-schema` lock in the configured metadata database for the complete
run, records `started` before executing a migration, verifies schema/data invariants, and records
`completed` only afterward. A checksum mismatch is fatal: never edit an applied migration.

Recovery is roll-forward only. If a process is interrupted, leave the ClickHouse data and ledger
intact, correct the operational cause, and rerun `bun run clickhouse:migrate`; migration steps are
replay-safe and verification determines completion. If verification cannot pass, restore the
pre-migration backup before starting application writers, then deploy a new corrective migration.
Do not manually mark ledger rows completed or run automatic down migrations.

For self-hosted ClickHouse, install the checked-in tuning and retention files:

```bash
scp docker/clickhouse/config.d/onlydoge-memory.xml root@clickhouse.example.com:/etc/clickhouse-server/config.d/onlydoge-memory.xml
scp docker/clickhouse/config.d/onlydoge-log-retention.xml root@clickhouse.example.com:/etc/clickhouse-server/config.d/onlydoge-log-retention.xml
scp docker/clickhouse/users.d/onlydoge-memory.xml root@clickhouse.example.com:/etc/clickhouse-server/users.d/onlydoge-memory.xml
scp docker/clickhouse/logrotate.d/clickhouse-server root@clickhouse.example.com:/etc/logrotate.d/clickhouse-server
scp docker/clickhouse/logrotate.d/rsyslog root@clickhouse.example.com:/etc/logrotate.d/rsyslog
ssh root@clickhouse.example.com 'mkdir -p /etc/systemd/journald.conf.d'
scp docker/clickhouse/journald.conf.d/onlydoge-retention.conf root@clickhouse.example.com:/etc/systemd/journald.conf.d/onlydoge-retention.conf
ssh root@clickhouse.example.com 'systemctl restart clickhouse-server'
ssh root@clickhouse.example.com 'systemctl restart systemd-journald && journalctl --vacuum-time=3d --vacuum-size=512M'
```

Check disk and pending mutations:

```bash
docker compose --env-file .env -f docker-compose.managed.yml exec -T onlydoge-api \
  bun -e 'const { createClient } = await import("@clickhouse/client"); const { loadSettings } = await import("@onlydoge/platform"); const s=loadSettings({mode:"indexer"}).warehouse; const c=createClient({url:s.location,database:s.database,username:s.user,password:s.password}); for (const query of ["SELECT name, formatReadableSize(free_space) AS free, formatReadableSize(total_space) AS total FROM system.disks", "SELECT table, count() AS mutations FROM system.mutations WHERE database=currentDatabase() AND is_done=0 GROUP BY table"]) { const r=await c.query({query,format:"JSONEachRow"}); console.log((await r.text()).trim()); } await c.close();'
```

## Self-hosted Service Image Upgrades

The self-hosted Compose files pin ClickHouse, MinIO, and the MinIO client as
`repository:version@sha256:manifest-list-digest`. Keep each human-readable version and digest
together, and keep the MinIO server/client versions compatible.

Before changing a pin:

1. Record the currently deployed image references and inspect the official ClickHouse, MinIO,
   and MinIO client release notes for every version being crossed.
2. Stop or quiesce writers and take restorable backups of the ClickHouse and MinIO data. Keep the
   previous Compose file and image references with the backup.
3. Resolve the candidate tag from the official registry with
   `docker buildx imagetools inspect repository:version`. Use the top-level multi-platform digest,
   and confirm the index includes every supported deployment architecture.
4. Pull the exact candidates with `docker compose pull clickhouse minio minio-create-bucket`.
5. On disposable data copies, start ClickHouse and verify its tables, then start MinIO and run the
   pinned client bucket initialization. Run `bun run test:clickhouse-smoke` for the checked-in
   schema/adapter path and `bun run ci` for migration and application regressions.
6. Deploy the updated Compose file. Verify ClickHouse health and queries, MinIO bucket access and
   initialization, application heartbeat, indexer health, and recent logs before resuming normal
   operation.

To roll back, restore the previous tag-and-digest references and redeploy. If the candidate wrote a
data format that the previous release cannot read, stop the services and restore the matching
ClickHouse/MinIO backups before starting the previous images. Do not reuse data modified by an
incompatible candidate.

## Rollback

Deploy the previous immutable image digest:

```bash
bun run deploy:docker -- --envFile .env.managed --image ghcr.io/your-org/onlydoge-indexer@sha256:<previous-digest>
```

Do not roll back ClickHouse tables with destructive commands unless the rebuild runbook explicitly calls for it and current public data can be reset.

## Request correlation and structured logs

The API resolves one request ID per HTTP request:

- Clients may send `x-request-id` when the value is trimmed, at most 128 characters, and matches `^[\w.-]+$`.
- Otherwise the API generates a UUID.
- The same ID is returned in the `x-request-id` response header, stored on audit rows, and attached to structured Pino logs as `requestId`.

Log fields use stable component bindings such as `service`, `component`, and `requestId`. Sensitive values (`x-api-token`, RPC credentials, tokens) are redacted to `[REDACTED]`. Correlate operator incidents by matching audit `request_id` values to API log lines with the same `requestId`.
