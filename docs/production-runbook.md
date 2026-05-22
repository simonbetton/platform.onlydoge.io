# Production Runbook

This runbook describes the Docker + Caddy production shape. The reusable sections use example hostnames and avoid secrets; the OnlyDoge production notes section records project-specific context from the dated recovery.

## Components

- `onlydoge-api`: HTTP API process.
- `onlydoge-indexer`: long-running indexing process.
- PostgreSQL: metadata, leases, API keys, network config, and progress.
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

The workflow runs quality checks, builds and pushes a fresh multi-arch production image for the selected commit, deploys the resulting immutable digest, and passes that digest to `bun run e2e:production` as `EXPECTED_IMAGE_DIGEST`.

## Production E2E

The production E2E suite is destructive only for disposable metadata it creates during the run. It never creates or deletes networks.

Required environment:

- `PROD_BASE_URL`: public API base URL, for example `https://api.example.com`
- `PROD_ADMIN_API_TOKEN`: existing production admin API token
- `EXPECTED_IMAGE_DIGEST`: immutable image digest expected to be running

Optional environment:

- `PROD_SSH_TARGET`: app host SSH target for container digest and health checks
- `PROD_SSH_JUMP`: SSH bastion target, if needed

Run the E2E suite after a deploy:

```bash
export PROD_BASE_URL=https://api.example.com
export PROD_ADMIN_API_TOKEN=sk_...
export EXPECTED_IMAGE_DIGEST=sha256:...
export PROD_SSH_TARGET=root@10.0.0.10
export PROD_SSH_JUMP=root@203.0.113.10

bun run e2e:production
```

Run deploy plus E2E in one command when `.env.managed` already selects the intended image tag and host:

```bash
bun run deploy:production
```

The suite creates an ephemeral API key using the admin API token, then uses that ephemeral API token for protected API checks and disposable tag/entity/address metadata. Teardown deletes in this order: address, entity, tag, ephemeral API key. Cleanup tolerates already-missing resources but fails on unexpected cleanup errors.

The suite verifies:

- public `/up`, `/v1/heartbeat/`, and `/openapi/json`
- protected auth failures without an API token
- per-key rate limiting for authenticated protected requests
- Dogecoin production stats are `stage = "online"` with no `lastError`
- current-state explorer address and UTXO reads
- metadata write/read/delete behavior
- history endpoints return either ready data or the documented `425` history-not-ready response
- optional Docker host containers match `EXPECTED_IMAGE_DIGEST`

## Health Checks

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

Stats require an API token when called over HTTP. From inside the API container, use the runtime directly:

```bash
docker compose --env-file .env -f docker-compose.managed.yml exec -T onlydoge-api \
  bun -e 'const { createRuntime } = await import("@onlydoge/platform"); const runtime = await createRuntime({mode:"http"}); console.log(JSON.stringify(await runtime.investigationQuery.stats(), null, 2)); process.exit(0);'
```

Healthy Dogecoin current-state production looks like:

- `stage = "online"`
- `lastError = null`
- `dogecoin_current_state_ready_n1 = true`
- `dogecoin_history_ready_n1 = true`
- `processTail` within `ONLYDOGE_CORE_ONLINE_TIP_DISTANCE` blocks of `block_height_n1`
- `factTail` aligned with `processTail` when core-backed history is enabled

`processTail` lag is measured against the node's confirmed chain tip. The mempool is the node's current set of unconfirmed transactions, so it is separate from this block-processing lag.

## OnlyDoge Production Notes

The known OnlyDoge production host from the 2026-05-25 UTC / 2026-05-26 NZ recovery is `platform.onlydoge.io`, with the managed app directory at `/opt/onlydoge`. During that recovery, the first managed deploy failed because `ONLYDOGE_DATABASE` still referenced `sslrootcert=/storage/do-ca.pem`; the fix was to retrieve the DigitalOcean managed Postgres CA and store it privately in ignored `.env.managed` as `ONLYDOGE_DATABASE_SSLROOTCERT_PEM`.

That recovery also found stale once-managed containers. `once-proxy` held ports `80/443`, and an old once-managed indexer was still running. Keep old once-managed containers stopped unless intentionally rolling back to that stack.

Container health alone is not enough to prove indexing freshness. During the recovery, production stats showed `lastError = "missing current dogecoin prevout: fc0a935951a0358b1d5d5880dc6bd9e06f69c278024b65c5f98297ec701ede2d:0"`; investigate and clear stats errors separately from deploy health.

## ClickHouse Operations

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

## Rollback

Deploy the previous immutable image digest:

```bash
bun run deploy:docker -- --envFile .env.managed --image ghcr.io/your-org/onlydoge-indexer@sha256:<previous-digest>
```

Do not roll back ClickHouse tables with destructive commands unless the rebuild runbook explicitly calls for it and current public data can be reset.
