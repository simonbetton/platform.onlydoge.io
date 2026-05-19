# Production Runbook

This runbook describes the Docker + Caddy production shape. It avoids project-specific hostnames and secrets so it can be used by downstream operators.

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

Minimum managed deployment settings:

- `ONLYDOGE_IMAGE`
- `ONLYDOGE_PUBLIC_HOST`
- `ONCE_SSH_TARGET`
- `ONCE_SSH_JUMP`, if the host is reachable only through a bastion
- `ONLYDOGE_DATABASE`
- `ONLYDOGE_STORAGE`
- `ONLYDOGE_S3_ACCESS_KEY_ID`
- `ONLYDOGE_S3_SECRET_ACCESS_KEY`
- `ONLYDOGE_WAREHOUSE`
- `ONLYDOGE_WAREHOUSE_USER`
- `ONLYDOGE_WAREHOUSE_PASSWORD`
- `SECRET_KEY_BASE`
- `VAPID_PUBLIC_KEY`
- `VAPID_PRIVATE_KEY`

## Build And Publish

Initialize the multi-arch builder once:

```bash
bun run image:builder:init
```

Build and push the default image:

```bash
bun run image:push
```

For a one-off immutable image, build and push a tagged digest with Docker Buildx, then pass that image to the deploy script.

## Deploy

Deploy the managed Docker Compose stack:

```bash
bun run deploy:docker -- --envFile .env.managed --image ghcr.io/your-org/onlydoge-indexer:your-tag
```

The deploy script:

- resolves the image to an immutable digest,
- uploads `docker-compose.managed.yml`,
- uploads `docker/caddy/Caddyfile`,
- writes the resolved env file on the target host,
- starts `caddy`, `onlydoge-api`, and `onlydoge-indexer`,
- verifies `/up`,
- verifies `/v1/heartbeat`,
- verifies indexer health.

## GitHub Actions Deploy

The checked-in `Deploy Production` workflow wraps the same deploy script and then runs the production E2E teardown suite. It is manual-only and uses the `production` GitHub Environment.

Configure these `production` environment variables:

- `PROD_BASE_URL`: public API base URL, for example `https://api.example.com`
- `PROD_HOST`: optional host override for `deploy:docker`; if absent, the workflow derives it from `PROD_BASE_URL`

Configure these `production` environment secrets:

- `PROD_ENV_FILE`: complete managed env file contents based on `.env.managed.example`
- `PROD_SSH_PRIVATE_KEY`: private key able to reach the app host
- `PROD_SSH_TARGET`: app host SSH target
- `PROD_SSH_JUMP`: optional bastion SSH target
- `PROD_ADMIN_API_TOKEN`: existing production admin API token for E2E key creation and teardown verification

The workflow resolves the image to an immutable digest before deployment and passes that digest to `bun run e2e:production` as `EXPECTED_IMAGE_DIGEST`.

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

Run deploy plus E2E in one command when `.env.managed` already selects the intended image and host:

```bash
bun run e2e:production:deploy
```

The suite creates an ephemeral API key using the admin token, then uses that ephemeral key for protected API checks and disposable tag/entity/address metadata. Teardown deletes in this order: address, entity, tag, ephemeral API key. Cleanup tolerates already-missing resources but fails on unexpected cleanup errors.

The suite verifies:

- public `/up`, `/v1/heartbeat/`, and `/openapi/json`
- protected auth failures without a token
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
