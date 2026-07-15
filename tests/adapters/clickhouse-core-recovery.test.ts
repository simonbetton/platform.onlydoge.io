import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createClient } from '@clickhouse/client';
import type {
  CoreDogecoinBlockApplication,
  CoreWindowInsertStage,
} from '@onlydoge/indexing-pipeline';
import {
  ClickHouseWarehouseAdapter,
  type DatabaseSettings,
  RelationalMetadataStore,
  runClickHouseMigrations,
  type WarehouseSettings,
} from '@onlydoge/platform';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { DockerService } from './docker-service';
import { adapterCredentials, clickHouseUrl, startClickHouse } from './services';

const adapterTimeoutMs = 180_000;
const coreStages: CoreWindowInsertStage[] = [
  'creates',
  'spends',
  'movements',
  'transactions',
  'processed_blocks',
];

let clickhouse: DockerService | null = null;
let metadataRoot: string | null = null;

describe.skipIf(process.env.ONLYDOGE_RUN_ADAPTER_TESTS !== '1')(
  'ClickHouse core window recovery',
  () => {
    beforeAll(async () => {
      clickhouse = await startClickHouse();
      metadataRoot = await mkdtemp(join(tmpdir(), 'onlydoge-clickhouse-core-recovery-'));
    }, adapterTimeoutMs);

    afterAll(async () => {
      await clickhouse?.stop();
      if (metadataRoot) {
        await rm(metadataRoot, { force: true, recursive: true });
      }
    }, 30_000);

    it.each(coreStages)(
      'converges after a failure at the %s stage',
      async (failStage) => {
        await resetClickHouse();
        const store = await openMetadata(`recovery-${failStage}`);
        try {
          const adapter = await bootWarehouse(store);
          const applications = recoveryWindowApplications();
          const baseline = await applyCleanWindow(adapter, applications);
          await resetClickHouse();
          await bootWarehouse(store);

          await expect(
            adapter.applyCoreDogecoinWindow(applications, {
              testHooks: {
                afterStage: async (stage) => {
                  if (stage === failStage) {
                    throw new Error(`injected failure after ${stage}`);
                  }
                },
              },
            }),
          ).rejects.toThrow(`injected failure after ${failStage}`);

          const startHeight = applications[0]?.blockHeight;
          if (startHeight === undefined) {
            throw new Error('recovery window is empty');
          }

          await adapter.recoverCoreDogecoinWindow(startHeight);
          await expect(adapter.applyCoreDogecoinWindow(applications)).resolves.toEqual({
            applied: true,
            processTail: applications.at(-1)?.blockHeight,
          });

          const recovered = await readCoreWindowCounts();
          expect(recovered).toEqual(baseline);
        } finally {
          await store.close();
        }
      },
      adapterTimeoutMs,
    );
  },
);

async function applyCleanWindow(
  adapter: ClickHouseWarehouseAdapter,
  applications: CoreDogecoinBlockApplication[],
) {
  await expect(adapter.applyCoreDogecoinWindow(applications)).resolves.toEqual({
    applied: true,
    processTail: applications.at(-1)?.blockHeight,
  });
  return readCoreWindowCounts();
}

function recoveryWindowApplications(): CoreDogecoinBlockApplication[] {
  return [
    {
      blockHeight: 1,
      blockHash: 'recovery-block-1',
      blockTime: 1,
      previousBlockHash: null,
      rawStorageKey: 'block',
      txCount: 1,
      utxoCreates: [
        {
          blockHeight: 1,
          blockHash: 'recovery-block-1',
          blockTime: 1,
          txid: 'coinbase-tx',
          txIndex: 0,
          vout: 0,
          outputKey: 'coinbase-tx:0',
          address: 'DRecoveryAddress',
          scriptType: 'pubkeyhash',
          valueBase: '100000000',
          isCoinbase: true,
          isSpendable: true,
          spentByTxid: null,
          spentInBlock: null,
          spentInputIndex: null,
        },
      ],
      utxoSpends: [],
    },
    {
      blockHeight: 2,
      blockHash: 'recovery-block-2',
      blockTime: 2,
      previousBlockHash: 'recovery-block-1',
      rawStorageKey: 'block',
      txCount: 1,
      utxoCreates: [
        {
          blockHeight: 2,
          blockHash: 'recovery-block-2',
          blockTime: 2,
          txid: 'spend-tx',
          txIndex: 0,
          vout: 0,
          outputKey: 'spend-tx:0',
          address: 'DRecoveryAddress',
          scriptType: 'pubkeyhash',
          valueBase: '100000000',
          isCoinbase: false,
          isSpendable: true,
          spentByTxid: null,
          spentInBlock: null,
          spentInputIndex: null,
        },
      ],
      utxoSpends: [
        {
          outputKey: 'coinbase-tx:0',
          spentByTxid: 'spend-tx',
          spentInBlock: 2,
          spentInputIndex: 0,
        },
      ],
    },
  ];
}

async function readCoreWindowCounts(): Promise<Record<string, number>> {
  const client = clickHouseClient();
  try {
    const tables = [
      'dogecoin_core_utxo_creates_v1',
      'dogecoin_core_utxo_spends_v1',
      'dogecoin_address_movements_v1',
      'analytics_transactions_v1',
      'dogecoin_core_processed_blocks_v1',
    ];
    const counts: Record<string, number> = {};
    for (const table of tables) {
      const result = await client.query({
        query: `SELECT count() AS count FROM ${table}`,
        format: 'JSONEachRow',
      });
      const rows = (await result.json<{ count: number | string }>()) as Array<{
        count: number | string;
      }>;
      counts[table] = Number(rows[0]?.count ?? 0);
    }
    return counts;
  } finally {
    await client.close();
  }
}

async function bootWarehouse(store: RelationalMetadataStore): Promise<ClickHouseWarehouseAdapter> {
  await runClickHouseMigrations(warehouseSettings(), store);
  const adapter = new ClickHouseWarehouseAdapter(warehouseSettings(), store);
  await adapter.boot();
  return adapter;
}

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

function requireValue<T>(value: T | null | undefined): T {
  if (value === null || value === undefined) {
    throw new Error('required adapter fixture is unavailable');
  }
  return value;
}
