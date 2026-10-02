import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { buildApiApp } from '@onlydoge/api';
import { configKeyDogecoinHistoryReady } from '@onlydoge/indexing-pipeline';
import { createRuntime } from '@onlydoge/platform';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { DockerService } from '../adapters/docker-service';
import { clickHouseQuery, startClickHouse } from '../adapters/services';
import { dogecoinFixture, dogecoinTxid } from '../fixtures/dogecoin';
import { installRpcMock, request, requireString, runIndexerUntilProcessed } from '../helpers';

const runSmoke = process.env.ONLYDOGE_RUN_CLICKHOUSE_SMOKE === '1';
const describeSmoke = runSmoke ? describe : describe.skip;
const clickHouseImage =
  process.env.ONLYDOGE_CLICKHOUSE_SMOKE_IMAGE ??
  'clickhouse/clickhouse-server:26.6.1.1193@sha256:1d1f6508eba2dccce2cee9913907c5f7766327debc57a6b1991f2c9e3176c163';
const clickHouseUser = 'onlydoge';
const clickHousePassword = 'onlydoge';
const clickHouseAnalyticsUser = 'onlydoge_analytics';
const clickHouseAnalyticsPassword = 'onlydoge_analytics';
const smokeTimeoutMs = 180_000;

let clickHouse: DockerService | null = null;
let clickHousePort = 0;

describeSmoke('clickhouse analytics smoke', () => {
  beforeAll(async () => {
    clickHouse = await startClickHouse(clickHouseImage);
    clickHousePort = clickHouse.hostPort(8123);
  }, smokeTimeoutMs);

  afterAll(async () => {
    await clickHouse?.stop();
    clickHouse = null;
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
          requireClickHouse(),
          'CREATE TABLE onlydoge.analytics_smoke_forbidden (x UInt8) ENGINE = Memory',
          clickHouseAnalyticsUser,
          clickHouseAnalyticsPassword,
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
            txid: dogecoinTxid('doge-tx-1'),
          }),
        ]);
        expect(String(readFirstRow(payload.rows).fee_base)).toBe('100000000');
        expect(payload.statistics).toEqual(
          expect.objectContaining({
            rowsRead: expect.any(Number),
          }),
        );

        await runIndexerUntilHistoryReady(ctx);
        await expectExplorerReads(ctx.app, apiToken);
      } finally {
        restoreFetch.mockRestore();
        await ctx.cleanup();
      }
    },
    smokeTimeoutMs,
  );
});

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

async function runIndexerUntilHistoryReady(
  ctx: Awaited<ReturnType<typeof createClickHouseTestApp>>,
): Promise<void> {
  for (let attempt = 0; attempt < 4; attempt += 1) {
    await ctx.runtime.indexingPipeline.runOnce();
    if (
      (await ctx.runtime.metadata.getJsonValue<boolean>(configKeyDogecoinHistoryReady())) === true
    ) {
      return;
    }
  }
  throw new Error('ClickHouse history did not become ready');
}

async function expectExplorerReads(
  app: ReturnType<typeof buildApiApp>,
  apiToken: string,
): Promise<void> {
  const headers = { 'x-api-token': apiToken };
  const blocksResponse = await request(app, '/v1/explorer/blocks?limit=1', { headers });
  const blocks = await readJsonObject(blocksResponse);
  expect(blocksResponse.status).toBe(200);
  expect(blocks.blocks).toEqual([
    expect.objectContaining({
      hash: dogecoinFixture.blocksByHeight[2].hash,
      height: 2,
    }),
  ]);

  const blockResponse = await request(app, '/v1/explorer/blocks/2', { headers });
  expect(blockResponse.status).toBe(200);
  expect(await readJsonObject(blockResponse)).toMatchObject({
    block: { height: 2 },
    transactions: [expect.objectContaining({ txid: dogecoinTxid('doge-tx-2') })],
  });

  const transactionResponse = await request(
    app,
    `/v1/explorer/transactions/${dogecoinTxid('doge-tx-2')}`,
    {
      headers,
    },
  );
  expect(transactionResponse.status).toBe(200);
  expect(await readJsonObject(transactionResponse)).toMatchObject({
    transaction: { txid: dogecoinTxid('doge-tx-2') },
  });

  const addressResponse = await request(
    app,
    `/v1/explorer/addresses/${dogecoinFixture.targetAddress}`,
    { headers },
  );
  expect(addressResponse.status).toBe(200);
  expect(await readJsonObject(addressResponse)).toMatchObject({
    address: {
      balance: '2500000000',
    },
  });
}

function requireClickHouse(): DockerService {
  if (!clickHouse) {
    throw new Error('ClickHouse adapter was not initialized');
  }
  return clickHouse;
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
