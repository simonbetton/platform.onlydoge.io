#!/usr/bin/env bun

import { createClient } from '@clickhouse/client';
import {
  configKeyDogecoinCurrentStateReady,
  configKeyDogecoinHistoryReady,
  configKeyIndexerFactProgress,
  configKeyIndexerFactTail,
} from '@onlydoge/indexing-pipeline';
import {
  clickHouseCoreDogecoinTables,
  loadSettings,
  RelationalMetadataStore,
} from '@onlydoge/platform';
import { Command } from 'commander';
import { parsePositiveInteger, resolveDogecoinNetworkId } from './dogecoin-script-utils';

const createsTable = clickHouseCoreDogecoinTables.coreUtxoCreates;
const addressIndex = 'core_utxo_creates_address_idx';

const program = new Command()
  .name('prepare-clickhouse-core-history')
  .description(
    'Prepare core-table-backed Dogecoin history queries without duplicating history rows.',
  )
  .option(
    '--execute',
    'apply ClickHouse and metadata changes; without this flag the script is a dry run',
  )
  .option('--networkId <id>', 'internal Dogecoin network id')
  .option('--materialize-index', 'materialize the address skipping index for existing parts')
  .option('--wait', 'wait for pending address-index materialization mutations to finish')
  .option('--mark-ready', 'mark dogecoin history ready after checks pass')
  .option('--statementTimeoutMs <ms>', 'ClickHouse statement timeout in milliseconds', '3600000')
  .parse();

const options = program.opts<{
  execute?: boolean;
  markReady?: boolean;
  materializeIndex?: boolean;
  networkId?: string;
  statementTimeoutMs: string;
  wait?: boolean;
}>();

async function main() {
  const statementTimeoutMs = parsePositiveInteger(options.statementTimeoutMs, 'statementTimeoutMs');
  const settings = loadSettings({ mode: 'indexer' });
  if (settings.warehouse.driver !== 'clickhouse') {
    throw new Error('ClickHouse warehouse is required for Dogecoin history preparation');
  }

  const metadata = await RelationalMetadataStore.connect(settings.database);
  const networkId = await resolveDogecoinNetworkId(metadata, options.networkId);
  const state = await metadata.getCoreIndexerState(networkId);
  const client = createClient({
    url: settings.warehouse.location,
    database: settings.warehouse.database,
    username: settings.warehouse.user,
    password: settings.warehouse.password,
    request_timeout: statementTimeoutMs + 60_000,
  });

  try {
    const plan = {
      mode: options.execute ? 'execute' : 'dry-run',
      networkId,
      processTail: state?.processTail ?? null,
      currentStateReady: await metadata.getJsonValue<boolean>(
        configKeyDogecoinCurrentStateReady(networkId),
      ),
      historyReady: await metadata.getJsonValue<boolean>(configKeyDogecoinHistoryReady(networkId)),
      disk: await readDisk(client),
      index: await readAddressIndex(client),
      pendingMutations: await readPendingMutations(client),
      actions: [
        'add a bloom-filter skipping index for core output address lookups',
        ...(options.materializeIndex
          ? ['materialize the index for existing core output parts']
          : []),
        ...(options.wait ? ['wait for pending index materialization mutations'] : []),
        ...(options.markReady
          ? ['mark Dogecoin history ready and advance fact progress to the core process tail']
          : []),
      ],
    };
    console.log(JSON.stringify({ plan }, null, 2));
    if (!options.execute) {
      console.log('dry run only; pass --execute to apply');
      return;
    }

    if (plan.currentStateReady !== true) {
      throw new Error('current state must be ready before enabling core-backed history');
    }
    if (!state || state.processTail < 0) {
      throw new Error('core process tail is not ready');
    }

    await client.command({
      query: `
        ALTER TABLE ${createsTable}
        ADD INDEX IF NOT EXISTS ${addressIndex} address TYPE bloom_filter(0.01) GRANULARITY 4
      `,
      clickhouse_settings: clickHouseSettings(statementTimeoutMs),
    });

    if (options.materializeIndex) {
      await client.command({
        query: `ALTER TABLE ${createsTable} MATERIALIZE INDEX ${addressIndex}`,
        clickhouse_settings: {
          ...clickHouseSettings(statementTimeoutMs),
          mutations_sync: options.wait ? '1' : '0',
        },
      });
    }

    if (options.wait) {
      await waitForMutations(client);
    }

    const pendingMutations = await readPendingMutations(client);
    if (options.markReady) {
      if (pendingMutations.length > 0) {
        throw new Error('address index materialization is still pending; rerun with --wait');
      }
      await Promise.all([
        metadata.setJsonValue(configKeyDogecoinHistoryReady(networkId), true),
        metadata.setJsonValue(configKeyIndexerFactTail(networkId), state.processTail),
        metadata.setJsonValue(configKeyIndexerFactProgress(networkId), 1),
      ]);
    }

    console.log(
      JSON.stringify(
        {
          phase: 'complete',
          index: await readAddressIndex(client),
          pendingMutations: await readPendingMutations(client),
          historyReady: await metadata.getJsonValue<boolean>(
            configKeyDogecoinHistoryReady(networkId),
          ),
          factTail: await metadata.getJsonValue<number>(configKeyIndexerFactTail(networkId)),
        },
        null,
        2,
      ),
    );
  } finally {
    await client.close();
  }
}

async function readDisk(client: ReturnType<typeof createClient>) {
  const rows = await queryJsonRows<{
    free: string;
    name: string;
    total: string;
  }>(
    client,
    `
      SELECT
        name,
        formatReadableSize(free_space) AS free,
        formatReadableSize(total_space) AS total
      FROM system.disks
      ORDER BY name ASC
    `,
  );
  return rows;
}

async function readAddressIndex(client: ReturnType<typeof createClient>) {
  return await queryJsonRows<{
    expr: string;
    name: string;
    table: string;
    type: string;
  }>(
    client,
    `
      SELECT
        table,
        name,
        expr,
        type
      FROM system.data_skipping_indices
      WHERE database = currentDatabase()
        AND table = {table:String}
        AND name = {index:String}
    `,
    { table: createsTable, index: addressIndex },
  );
}

async function readPendingMutations(client: ReturnType<typeof createClient>) {
  return await queryJsonRows<{
    command: string;
    latestFailReason: string;
    mutationId: string;
    partsToDo: string | number;
  }>(
    client,
    `
      SELECT
        mutation_id AS "mutationId",
        command,
        parts_to_do AS "partsToDo",
        latest_fail_reason AS "latestFailReason"
      FROM system.mutations
      WHERE database = currentDatabase()
        AND table = {table:String}
        AND is_done = 0
      ORDER BY create_time ASC
    `,
    { table: createsTable },
  );
}

async function waitForMutations(client: ReturnType<typeof createClient>): Promise<void> {
  while (true) {
    const pending = await readPendingMutations(client);
    if (pending.length === 0) {
      return;
    }
    console.log(JSON.stringify({ phase: 'waiting-for-mutations', pending }));
    await new Promise((resolve) => setTimeout(resolve, 10_000));
  }
}

async function queryJsonRows<T>(
  client: ReturnType<typeof createClient>,
  query: string,
  queryParams: Record<string, unknown> = {},
): Promise<T[]> {
  const result = await client.query({
    query,
    query_params: queryParams,
    format: 'JSONEachRow',
  });
  return await result.json<T[]>();
}

function clickHouseSettings(statementTimeoutMs: number) {
  return {
    max_execution_time: Math.max(1, Math.ceil(statementTimeoutMs / 1000)),
  };
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
