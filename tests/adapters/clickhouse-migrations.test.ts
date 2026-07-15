import { createHash } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createClient } from '@clickhouse/client';
import {
  type ClickHouseMigration,
  clickHouseMigrations,
  type DatabaseSettings,
  RelationalMetadataStore,
  runClickHouseMigrations,
  type WarehouseSettings,
} from '@onlydoge/platform';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { DockerService } from './docker-service';
import { adapterCredentials, clickHouseUrl, startClickHouse } from './services';

const adapterTimeoutMs = 180_000;
let clickhouse: DockerService | null = null;
let metadataRoot: string | null = null;

describe.skipIf(process.env.ONLYDOGE_RUN_ADAPTER_TESTS !== '1')(
  'ClickHouse versioned migrations',
  () => {
    beforeAll(async () => {
      clickhouse = await startClickHouse();
      metadataRoot = await mkdtemp(join(tmpdir(), 'onlydoge-clickhouse-migrations-'));
    }, adapterTimeoutMs);

    afterAll(async () => {
      await clickhouse?.stop();
      if (metadataRoot) {
        await rm(metadataRoot, { force: true, recursive: true });
      }
    }, 30_000);

    it(
      'installs a fresh schema and records durable checksums',
      async () => {
        await resetClickHouse();
        const store = await openMetadata('fresh');
        try {
          const records = await runClickHouseMigrations(warehouseSettings(), store);
          expect(records.map(({ state, version }) => ({ state, version }))).toEqual([
            { state: 'completed', version: 1 },
            { state: 'completed', version: 2 },
            { state: 'completed', version: 3 },
          ]);
          expect(records.every((record) => record.checksum.length === 64)).toBe(true);
          await expectSchemaMetadata();
        } finally {
          await store.close();
        }
      },
      adapterTimeoutMs,
    );

    it(
      'is a verified no-op on restart',
      async () => {
        await resetClickHouse();
        const first = await openMetadata('restart');
        const initial = await runClickHouseMigrations(warehouseSettings(), first);
        await first.close();

        const second = await openMetadata('restart');
        try {
          const restarted = await runClickHouseMigrations(warehouseSettings(), second);
          expect(restarted).toEqual(initial);
        } finally {
          await second.close();
        }
      },
      adapterTimeoutMs,
    );

    it(
      'fails fatally when applied migration source drifts',
      async () => {
        await resetClickHouse();
        const store = await openMetadata('checksum');
        try {
          await runClickHouseMigrations(warehouseSettings(), store);
          const migrations = clickHouseMigrations();
          const changedSource = `${migrations[0]?.source}\n-- forbidden edit`;
          const drifted: ClickHouseMigration = {
            ...requireValue(migrations[0]),
            source: changedSource,
            checksum: createHash('sha256').update(changedSource).digest('hex'),
          };
          await expect(
            runClickHouseMigrations(warehouseSettings(), store, {
              migrations: [drifted, requireValue(migrations[1])],
            }),
          ).rejects.toThrow(/checksum drift at version 1/u);
        } finally {
          await store.close();
        }
      },
      adapterTimeoutMs,
    );

    it(
      'resumes safely after every declared migration step',
      async () => {
        await resetClickHouse();
        const discovery = await openMetadata('step-discovery');
        const steps: string[] = [];
        try {
          await runClickHouseMigrations(warehouseSettings(), discovery, {
            afterStep({ migration, step }) {
              steps.push(`${migration}:${step}`);
            },
          });
        } finally {
          await discovery.close();
        }
        expect(steps.length).toBeGreaterThan(2);

        for (const interruptedStep of steps) {
          await resetClickHouse();
          const store = await openMetadata(`interrupt-${interruptedStep.replace(':', '-')}`);
          try {
            await expect(
              runClickHouseMigrations(warehouseSettings(), store, {
                afterStep({ migration, step }) {
                  if (`${migration}:${step}` === interruptedStep) {
                    throw new Error(`simulated interruption after ${interruptedStep}`);
                  }
                },
              }),
            ).rejects.toThrow(/simulated interruption/u);

            const recovered = await runClickHouseMigrations(warehouseSettings(), store);
            expect(recovered.every((record) => record.state === 'completed')).toBe(true);
          } finally {
            await store.close();
          }
        }
      },
      adapterTimeoutMs,
    );

    it(
      'completes a partially populated read-model upgrade without empty-table inference',
      async () => {
        await resetClickHouse();
        const store = await openMetadata('populated');
        const client = clickHouseClient();
        try {
          const [schemaMigration] = clickHouseMigrations();
          await runClickHouseMigrations(warehouseSettings(), store, {
            migrations: [requireValue(schemaMigration)],
          });
          await seedPopulatedSources(client);
          await client.command({
            query: 'TRUNCATE TABLE dogecoin_utxo_outputs_current_by_address_v1',
          });
          await client.command({
            query: 'TRUNCATE TABLE dogecoin_address_movements_by_address_v1',
          });
          await client.command({
            query:
              'INSERT INTO dogecoin_utxo_outputs_current_by_address_v1 SELECT * FROM dogecoin_utxo_outputs_current_v1 LIMIT 1',
          });
          await client.command({
            query: `
              INSERT INTO dogecoin_address_movements_by_address_v1
              SELECT movement_id, block_height, block_hash, block_time, txid, tx_index,
                entry_index, address, asset_address, direction, amount_base, output_key,
                derivation_method
              FROM dogecoin_address_movements_v1 LIMIT 1
            `,
          });

          const records = await runClickHouseMigrations(warehouseSettings(), store);
          expect(records).toHaveLength(3);
          await expectCounts(client, 'dogecoin_utxo_outputs_current_by_address_v1', 2);
          await expectCounts(client, 'dogecoin_address_movements_by_address_v1', 2);
        } finally {
          await client.close();
          await store.close();
        }
      },
      adapterTimeoutMs,
    );

    it(
      'serializes two concurrent runners and converges on one ledger',
      async () => {
        await resetClickHouse();
        const [first, second] = await Promise.all([
          openMetadata('concurrent'),
          openMetadata('concurrent'),
        ]);
        try {
          const [left, right] = await Promise.all([
            runClickHouseMigrations(warehouseSettings(), first),
            runClickHouseMigrations(warehouseSettings(), second),
          ]);
          expect(left.every((record) => record.state === 'completed')).toBe(true);
          expect(right).toEqual(left);
        } finally {
          await Promise.all([first.close(), second.close()]);
        }
      },
      adapterTimeoutMs,
    );
  },
);

function warehouseSettings(): WarehouseSettings {
  return {
    driver: 'clickhouse',
    location: clickHouseUrl(requireValue(clickhouse)),
    database: adapterCredentials.database,
    user: adapterCredentials.user,
    password: 'onlydoge',
    requestTimeoutMs: 30_000,
  };
}

function metadataSettings(name: string): DatabaseSettings {
  return {
    driver: 'sqlite',
    location: `file:${join(requireValue(metadataRoot), `${name}.sqlite`)}`,
  };
}

function openMetadata(name: string): Promise<RelationalMetadataStore> {
  return RelationalMetadataStore.connect(metadataSettings(name));
}

function clickHouseClient(database: string = adapterCredentials.database) {
  return createClient({
    url: clickHouseUrl(requireValue(clickhouse)),
    database,
    username: adapterCredentials.user,
    password: 'onlydoge',
  });
}

async function resetClickHouse(): Promise<void> {
  const client = clickHouseClient('default');
  try {
    await client.command({ query: `DROP DATABASE IF EXISTS ${adapterCredentials.database} SYNC` });
  } finally {
    await client.close();
  }
}

async function expectSchemaMetadata(): Promise<void> {
  const client = clickHouseClient();
  try {
    const result = await client.query({
      query: `
        SELECT count() AS count
        FROM system.columns
        WHERE database = currentDatabase()
          AND table = 'analytics_transactions_v1'
          AND name IN ('fee_base_i256', 'gross_output_base_i256', 'total_input_base_i256')
      `,
      format: 'JSONEachRow',
    });
    const rows = (await result.json<{ count: number | string }>()) as Array<{
      count: number | string;
    }>;
    expect(Number(rows[0]?.count)).toBe(3);
  } finally {
    await client.close();
  }
}

async function seedPopulatedSources(client: ReturnType<typeof clickHouseClient>): Promise<void> {
  await client.command({
    query: `
      INSERT INTO dogecoin_utxo_outputs_current_v1 VALUES
        (1, 'h1', 1, 'tx1', 0, 0, 'tx1:0', 'D1', 'p2pkh', '10', 0, 1, NULL, NULL, NULL, 2),
        (2, 'h2', 2, 'tx2', 0, 0, 'tx2:0', 'D2', 'p2pkh', '20', 0, 1, NULL, NULL, NULL, 4)
    `,
  });
  await client.command({
    query: `
      INSERT INTO dogecoin_address_movements_v1 VALUES
        ('m1', 1, 'h1', 1, 'tx1', 0, 0, 'D1', 'DOGE', 'received', '10', 'tx1:0', 'output'),
        ('m2', 2, 'h2', 2, 'tx2', 0, 0, 'D2', 'DOGE', 'received', '20', 'tx2:0', 'output')
    `,
  });
}

async function expectCounts(
  client: ReturnType<typeof clickHouseClient>,
  table: string,
  expected: number,
): Promise<void> {
  const result = await client.query({
    query: `SELECT count() AS count FROM ${table}`,
    format: 'JSONEachRow',
  });
  const rows = (await result.json<{ count: number | string }>()) as Array<{
    count: number | string;
  }>;
  expect(Number(rows[0]?.count)).toBe(expected);
}

function requireValue<T>(value: T | null | undefined): T {
  if (value === null || value === undefined) {
    throw new Error('required adapter fixture is unavailable');
  }
  return value;
}
