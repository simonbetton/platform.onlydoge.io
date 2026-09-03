# Production Runbook

OnlyDoge ships one deployment shape: `docker-compose.yml`. It runs Dogecoin Core, PostgreSQL,
ClickHouse, MinIO, `onlydoge-api`, and `onlydoge-indexer` on a single Docker host. Put a reverse
proxy (Caddy, Nginx, Traefik, or a cloud load balancer) in front of port `2277` for TLS.

## Deploy

```bash
git clone <repo> && cd <repo>
cp .env.example .env            # set real passwords; every key is optional
docker compose up -d --build    # or: bun run docker:up
docker compose ps
bun run docker:status           # indexer stage, tails, blocks/s, ETA
```

Notes:

- Chain data is ~250 GB. Set `ONLYDOGE_DATA_DOGECOIN` in `.env` to a directory on an SSD or
  leave it unset for the `dogecoin_data` Docker volume.
- To use an external Dogecoin Core, set `ONLYDOGE_DOGECOIN_RPC_ENDPOINT` (and the two
  `ONLYDOGE_DOGECOIN_ZMQ_*` endpoints) to an address reachable from the containers and start with
  `docker compose up -d --scale dogecoin=0`.
- Set `ONLYDOGE_ANALYTICS_WAREHOUSE_USER` / `ONLYDOGE_ANALYTICS_WAREHOUSE_PASSWORD` to a
  dedicated read-only ClickHouse identity; the primary warehouse credentials are never used as an
  analytics fallback.
- After startup, create the first admin API key with `POST /v1/keys/`.

## Upgrade

```bash
git pull
docker compose up -d --build
bun run docker:status
```

Application containers apply metadata and ClickHouse migrations on startup (see ClickHouse
Operations below for the ledger and backup rules). To pin a published image instead of building
locally, set `image:` on `onlydoge-api` and `onlydoge-indexer` to a
`ghcr.io/<org>/onlydoge-indexer@sha256:<digest>` reference in an override file; rolling back is
redeploying the previous digest. Do not roll back ClickHouse tables with destructive commands
unless the rebuild runbook explicitly calls for it.

## Health Checks

### Structured logging and request correlation

API requests accept an optional `x-request-id` header. When the header is missing, malformed, or unsafe, the API generates a UUID. Every response includes `x-request-id`, and the same value is stored on audit rows and attached to structured request/error logs.

Background services (core indexer, mempool sampler, mempool appear detector, ZMQ bridge, warehouse recovery) log JSON via Pino with stable `service` and `component` fields. Correlate API traffic to background work by time window and shared deployment; background logs do not reuse API request IDs.

Sensitive fields (`authorization`, API tokens, RPC endpoints, passwords, secrets) are redacted automatically. Do not enable body logging or paste raw credentials into log queries.

Example log tail (run from the checkout that holds `docker-compose.yml` and `.env`):

```bash
docker compose logs --tail=200 onlydoge-api onlydoge-indexer \
  | rg '"service":"onlydoge"'
```

Filter API errors for a single request after capturing `x-request-id` from the client response:

```bash
curl -fsS -D - -o /dev/null https://api.example.com/v1/heartbeat/
docker compose logs --tail=500 onlydoge-api \
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

On the host:

```bash
docker compose ps
docker compose logs --tail=200 onlydoge-api onlydoge-indexer
bun run docker:status
```

Public checks:

```bash
curl -fsS https://api.example.com/up
curl -fsS -i https://api.example.com/v1/heartbeat/
curl -fsS https://api.example.com/openapi/json >/dev/null
```

Indexer progress is public at `GET /v1/status/` and printable with `bun run docker:status`
(`--watch 5`, `--json`). Both report stage, sync/process tails, node tip, blocks/s, ETA, readiness
flags, last activity, and the last redacted error.

Healthy Dogecoin current-state production looks like:

- `stage = "online"`
- `lastError = null`
- `dogecoin_current_state_ready = true`
- `dogecoin_history_ready = true`
- `syncTail` aligned with `block_height` once online raw sync catches the node tip
- `processTail` aligned with `block_height` once online processing catches the node tip
- `finalizedTail` behind `processTail` by `indexer_reprocess_depth`
- `factTail` aligned with `processTail` when core-backed history is enabled

Indexer container health evaluates persisted errors, stage-aware progress freshness, and
online lag against `ONLYDOGE_CORE_ONLINE_TIP_DISTANCE`. Backfill may remain far behind the
node while it is advancing, but stale backfill progress, any `lastError`, malformed state,
or sustained online lag makes the container unhealthy. A successful processed window
clears a recovered transient error. A slow or unreachable node is reported this way and
retried with backoff; the process is only restarted by the watchdog when a step shows no
activity at all.

Raw block sync and processing can be at the node tip while the last reprocess-depth blocks remain reorg-active. The mempool is the node's current set of unconfirmed transactions, so it is separate from this canonicality window.

## ClickHouse Operations

Application startup and `bun run clickhouse:migrate` use the same ordered ClickHouse migrations.
Inspect the durable version/name/checksum/state ledger before and after an upgrade:

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
docker compose exec -T onlydoge-api \
  bun -e 'const { createClient } = await import("@clickhouse/client"); const { loadSettings } = await import("@onlydoge/platform"); const s=loadSettings({mode:"indexer"}).warehouse; const c=createClient({url:s.location,database:s.database,username:s.user,password:s.password}); for (const query of ["SELECT name, formatReadableSize(free_space) AS free, formatReadableSize(total_space) AS total FROM system.disks", "SELECT table, count() AS mutations FROM system.mutations WHERE database=currentDatabase() AND is_done=0 GROUP BY table"]) { const r=await c.query({query,format:"JSONEachRow"}); console.log((await r.text()).trim()); } await c.close();'
```

## Self-hosted Service Image Upgrades

`docker-compose.yml` pins ClickHouse, MinIO, and the MinIO client as
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
6. Roll out the updated Compose file with `docker compose up -d`. Verify ClickHouse health and queries, MinIO bucket access and
   initialization, application heartbeat, indexer health, and recent logs before resuming normal
   operation.

To roll back, restore the previous tag-and-digest references and redeploy. If the candidate wrote a
data format that the previous release cannot read, stop the services and restore the matching
ClickHouse/MinIO backups before starting the previous images. Do not reuse data modified by an
incompatible candidate.

## Request correlation and structured logs

The API resolves one request ID per HTTP request:

- Clients may send `x-request-id` when the value is trimmed, at most 128 characters, and matches `^[\w.-]+$`.
- Otherwise the API generates a UUID.
- The same ID is returned in the `x-request-id` response header, stored on audit rows, and attached to structured Pino logs as `requestId`.

Log fields use stable component bindings such as `service`, `component`, and `requestId`. Sensitive values (`x-api-token`, RPC credentials, tokens) are redacted to `[REDACTED]`. Correlate operator incidents by matching audit `request_id` values to API log lines with the same `requestId`.
