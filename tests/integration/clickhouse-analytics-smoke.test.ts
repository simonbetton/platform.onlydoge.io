import { execFile } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { promisify } from 'node:util';

import { buildApiApp } from '@onlydoge/api';
import { createRuntime } from '@onlydoge/platform';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { dogecoinFixture } from '../fixtures/dogecoin';
import { installRpcMock, request, requireString, runIndexerUntilProcessed } from '../helpers';

const runSmoke = process.env.ONLYDOGE_RUN_CLICKHOUSE_SMOKE === '1';
const describeSmoke = runSmoke ? describe : describe.skip;
const execFileAsync = promisify(execFile);
const realFetch = globalThis.fetch.bind(globalThis);

const clickHouseImage =
  process.env.ONLYDOGE_CLICKHOUSE_SMOKE_IMAGE ?? 'clickhouse/clickhouse-server:latest';
const clickHouseUser = 'onlydoge';
const clickHousePassword = 'onlydoge';
const clickHouseAnalyticsUser = 'onlydoge_analytics';
const clickHouseAnalyticsPassword = 'onlydoge_analytics';
const smokeTimeoutMs = 180_000;

let containerName: string | null = null;
let clickHousePort = 0;

describeSmoke('clickhouse analytics smoke', () => {
  beforeAll(async () => {
    const container = await startClickHouseContainer();
    containerName = container.name;
    clickHousePort = container.port;
    await waitForClickHouse(clickHousePort);
  }, smokeTimeoutMs);

  afterAll(async () => {
    if (containerName) {
      await docker(['rm', '-f', containerName]);
      containerName = null;
    }
  }, 30_000);

  it(
    'runs the guarded analytics API against a real ClickHouse server',
    async () => {
      const restoreFetch = installRpcMock();
      const ctx = await createClickHouseTestApp(clickHousePort);

      try {
        const createdKey = await request(ctx.app, '/v1/keys/', {
          method: 'POST',
          body: {},
        }).then(readJsonObject);
        const apiToken = requireString(createdKey, 'key');
        const actor = await ctx.runtime.accessControl.authenticate(apiToken);
        if (!actor) {
          throw new Error('expected authenticated API key');
        }

        void actor;
        await runIndexerUntilProcessed(ctx, 2);

        await expect(ctx.runtime.analyticsQuery.backfill()).resolves.toMatchObject({
          throughBlockHeight: 1,
        });

        const forbiddenMutation = await clickHouseQuery(
          clickHousePort,
          clickHouseAnalyticsUser,
          clickHouseAnalyticsPassword,
          'CREATE TABLE onlydoge.analytics_smoke_forbidden (x UInt8) ENGINE = Memory',
        );
        expect(forbiddenMutation.ok).toBe(false);

        const response = await request(ctx.app, '/v1/analytics/query', {
          method: 'POST',
          headers: { 'x-api-token': apiToken },
          body: {
            from: String(dogecoinFixture.blocksByHeight[0].time),
            to: String(dogecoinFixture.blocksByHeight[2].time + 60),
            limit: 5,
            sql: highestFeeSql(),
          },
        });
        const payload = await readJsonObject(response);

        expect(response.status).toBe(200);
        expect(payload.query).toMatchObject({
          finalizedBlockHeight: 1,
        });
        expect(payload.rows).toEqual([
          expect.objectContaining({
            txid: 'doge-tx-1',
          }),
        ]);
        expect(String(readFirstRow(payload.rows).fee_base)).toBe('100000000');
        expect(payload.statistics).toEqual(
          expect.objectContaining({
            rowsRead: expect.any(Number),
          }),
        );
      } finally {
        restoreFetch.mockRestore();
        await ctx.cleanup();
      }
    },
    smokeTimeoutMs,
  );
});

async function startClickHouseContainer(): Promise<{ name: string; port: number }> {
  const name = `onlydoge-clickhouse-smoke-${randomUUID()}`;
  const root = process.cwd();

  try {
    await docker(
      [
        'run',
        '-d',
        '--rm',
        '--name',
        name,
        '-p',
        '127.0.0.1::8123',
        '-e',
        'CLICKHOUSE_DB=onlydoge',
        '-e',
        `CLICKHOUSE_USER=${clickHouseUser}`,
        '-e',
        `CLICKHOUSE_PASSWORD=${clickHousePassword}`,
        '-e',
        `CLICKHOUSE_ANALYTICS_PASSWORD=${clickHouseAnalyticsPassword}`,
        '-e',
        'CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1',
        '-v',
        `${resolve(root, 'docker/clickhouse/init/001_schema.sql')}:/docker-entrypoint-initdb.d/001_schema.sql:ro`,
        '-v',
        `${resolve(root, 'docker/clickhouse/config.d/onlydoge-memory.xml')}:/etc/clickhouse-server/config.d/onlydoge-memory.xml:ro`,
        '-v',
        `${resolve(root, 'docker/clickhouse/users.d/onlydoge-memory.xml')}:/etc/clickhouse-server/users.d/onlydoge-memory.xml:ro`,
        '-v',
        `${resolve(root, 'docker/clickhouse/users.d/onlydoge-analytics.xml')}:/etc/clickhouse-server/users.d/onlydoge-analytics.xml:ro`,
        clickHouseImage,
      ],
      smokeTimeoutMs,
    );

    return {
      name,
      port: await inspectClickHousePort(name),
    };
  } catch (error) {
    await docker(['rm', '-f', name]).catch(() => undefined);
    throw error;
  }
}

async function inspectClickHousePort(name: string): Promise<number> {
  const { stdout } = await docker(['port', name, '8123/tcp']);
  const rawPort = stdout.trim().split(':').at(-1) ?? '';
  const port = Number(rawPort);
  if (!Number.isInteger(port) || port <= 0) {
    throw new Error(`invalid ClickHouse smoke port: ${stdout}`);
  }

  return port;
}

async function waitForClickHouse(port: number): Promise<void> {
  const deadline = Date.now() + 120_000;
  let lastError = 'ClickHouse did not respond';

  while (Date.now() < deadline) {
    try {
      const response = await clickHouseQuery(
        port,
        clickHouseUser,
        clickHousePassword,
        'EXISTS TABLE onlydoge.analytics_transactions_v1',
      );
      const body = await response.text();
      if (response.ok && body.trim() === '1') {
        return;
      }
      lastError = `status=${response.status} body=${body}`;
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }

    await delay(1_000);
  }

  throw new Error(`ClickHouse smoke container was not ready: ${lastError}`);
}

async function clickHouseQuery(
  port: number,
  user: string,
  password: string,
  query: string,
): Promise<Response> {
  const url = new URL(`http://127.0.0.1:${port}/`);
  url.searchParams.set('query', query);
  return realFetch(url, {
    headers: {
      'X-ClickHouse-Key': password,
      'X-ClickHouse-User': user,
    },
  });
}

async function createClickHouseTestApp(port: number) {
  const tempRoot = await mkdtemp(join(tmpdir(), 'onlydoge-clickhouse-smoke-'));
  const previousEnv = new Map<string, string | undefined>();
  const env = {
    ONLYDOGE_ANALYTICS_WAREHOUSE_PASSWORD: clickHouseAnalyticsPassword,
    ONLYDOGE_ANALYTICS_WAREHOUSE_USER: clickHouseAnalyticsUser,
    ONLYDOGE_CORE_ONLINE_TIP_DISTANCE: '1',
    ONLYDOGE_CORE_REPROCESS_DEPTH: '1',
    ONLYDOGE_CORE_SYNC_COMPLETE_DISTANCE: '1',
    ONLYDOGE_DATABASE: `sqlite://${tempRoot}/onlydoge.sqlite.db`,
    ONLYDOGE_MODE: 'both',
    ONLYDOGE_STORAGE: `file://${tempRoot}/storage`,
    ONLYDOGE_WAREHOUSE: `http://127.0.0.1:${port}?database=onlydoge`,
    ONLYDOGE_WAREHOUSE_PASSWORD: clickHousePassword,
    ONLYDOGE_WAREHOUSE_REQUEST_TIMEOUT_MS: '30000',
    ONLYDOGE_WAREHOUSE_USER: clickHouseUser,
  } satisfies Record<string, string>;

  for (const [key, value] of Object.entries(env)) {
    previousEnv.set(key, process.env[key]);
    process.env[key] = value;
  }

  const runtime = await createRuntime({ mode: 'both', ip: '127.0.0.1', port: 2277 });
  const app = buildApiApp(runtime);

  return {
    app,
    runtime,
    async cleanup() {
      for (const [key, value] of previousEnv.entries()) {
        if (value === undefined) {
          delete process.env[key];
        } else {
          process.env[key] = value;
        }
      }

      await rm(tempRoot, { recursive: true, force: true });
    },
  };
}

function highestFeeSql(): string {
  return `
    SELECT txid, fee_base_i256 AS fee_base
    FROM analytics_transactions_v1
    WHERE block_time >= {fromTime:UInt64}
      AND block_time < {toTime:UInt64}
      AND block_height <= {maxFinalizedHeight:UInt64}
      AND is_coinbase = 0
      AND fee_base_i256 IS NOT NULL
    ORDER BY fee_base_i256 DESC, block_height DESC, tx_index DESC
    LIMIT {limit:UInt64}
  `;
}

function readFirstRow(value: unknown): Record<string, unknown> {
  if (!Array.isArray(value) || !value[0] || typeof value[0] !== 'object') {
    throw new TypeError('expected analytics row');
  }

  return value[0] as Record<string, unknown>;
}

async function readJsonObject(response: Response): Promise<Record<string, unknown>> {
  const payload = await response.json();
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    throw new TypeError('expected object response');
  }

  return payload as Record<string, unknown>;
}

async function docker(args: string[], timeout = 30_000) {
  return execFileAsync('docker', args, {
    timeout,
  });
}

function delay(ms: number): Promise<void> {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, ms));
}
