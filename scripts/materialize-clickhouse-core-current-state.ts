#!/usr/bin/env bun

import { createClient } from '@clickhouse/client';
import {
  configKeyDogecoinCurrentStateMaterialization,
  configKeyDogecoinCurrentStateReady,
  configKeyDogecoinHistoryReady,
  configKeyIndexerProcessProgress,
  configKeyIndexerProcessTail,
  configKeyIndexerStage,
} from '@onlydoge/indexing-pipeline';
import {
  buildCoreCurrentStateOutputKeyRanges,
  type ClickHouseStringRange,
  clickHouseCoreDogecoinTables,
  clickHouseStringRangeClause,
  clickHouseStringRangeParams,
  loadSettings,
  RelationalMetadataStore,
} from '@onlydoge/platform';
import { Command } from 'commander';
import {
  parseNonNegativeInteger,
  parsePositiveInteger,
  resolveDogecoinNetworkId,
} from './dogecoin-script-utils';

type MaterializationCheckpoint = {
  asOfBlockHeight: number;
  finalizedAt?: string;
  lastRange?: {
    elapsedMs: number;
    index: number;
    rowCount: number;
  };
  nextRangeIndex: number;
  resetAt?: string;
  startedAt: string;
  status: 'complete' | 'running';
  totalRanges: number;
  updatedAt: string;
};

const currentUtxosTable = clickHouseCoreDogecoinTables.currentUtxos;
const currentUtxosByAddressTable = clickHouseCoreDogecoinTables.currentUtxosByAddress;
const createsTable = clickHouseCoreDogecoinTables.coreUtxoCreates;
const spendsTable = clickHouseCoreDogecoinTables.coreUtxoSpends;
const processedBlocksTable = clickHouseCoreDogecoinTables.coreProcessedBlocks;
const balancesTable = clickHouseCoreDogecoinTables.balances;
const appliedBlocksTable = clickHouseCoreDogecoinTables.appliedBlocks;
const ranges = buildCoreCurrentStateOutputKeyRanges();

const program = new Command()
  .name('materialize-clickhouse-core-current-state')
  .description('Materialize ClickHouse Dogecoin current-state tables with range checkpoints.')
  .option('--execute', 'perform writes; without this flag the script is a dry run')
  .option('--networkId <id>', 'internal Dogecoin network id')
  .option('--asOfBlockHeight <height>', 'processed block height to materialize')
  .option('--reset', 'truncate current-state read tables before starting')
  .option('--fromRange <index>', 'override the checkpoint and start at a specific range index')
  .option('--rangeLimit <count>', 'process at most this many ranges')
  .option('--statementTimeoutMs <ms>', 'ClickHouse statement timeout in milliseconds', '3600000')
  .option('--no-finalize', 'skip balances/applied-block inserts and readiness handoff')
  .parse();

const options = program.opts<{
  asOfBlockHeight?: string;
  execute?: boolean;
  finalize: boolean;
  fromRange?: string;
  networkId?: string;
  rangeLimit?: string;
  reset?: boolean;
  statementTimeoutMs: string;
}>();

async function main() {
  const settings = loadSettings({ mode: 'indexer' });
  if (settings.warehouse.driver !== 'clickhouse') {
    throw new Error('ClickHouse warehouse is required for current-state materialization');
  }

  const statementTimeoutMs = parsePositiveInteger(options.statementTimeoutMs, 'statementTimeoutMs');
  const metadata = await RelationalMetadataStore.connect(settings.database);
  const networkId = await resolveDogecoinNetworkId(metadata, options.networkId);
  const state = await metadata.getCoreIndexerState(networkId);
  if (!state) {
    throw new Error(`missing core indexer state for network ${networkId}`);
  }

  const asOfBlockHeight =
    options.asOfBlockHeight === undefined
      ? state.processTail
      : parseNonNegativeInteger(options.asOfBlockHeight, 'asOfBlockHeight');
  if (asOfBlockHeight < 0) {
    throw new Error('core process tail has not been initialized');
  }

  const client = createClient({
    url: settings.warehouse.location,
    database: settings.warehouse.database,
    username: settings.warehouse.user,
    password: settings.warehouse.password,
    request_timeout: statementTimeoutMs + 60_000,
  });
  const checkpointKey = configKeyDogecoinCurrentStateMaterialization(networkId);

  try {
    const existingCheckpoint =
      await metadata.getJsonValue<MaterializationCheckpoint>(checkpointKey);
    const plan = {
      mode: options.execute ? 'execute' : 'dry-run',
      networkId,
      asOfBlockHeight,
      reset: options.reset === true,
      finalize: options.finalize,
      totalRanges: ranges.length,
      currentStateReady: await metadata.getJsonValue<boolean>(
        configKeyDogecoinCurrentStateReady(networkId),
      ),
      existingCheckpoint,
      currentRows: await countRows(client, currentUtxosTable, networkId),
      currentRowsByAddress: await countRows(client, currentUtxosByAddressTable, networkId),
      balancesRows: await countRows(client, balancesTable, networkId),
      appliedBlocksRows: await countRows(client, appliedBlocksTable, networkId),
    };
    console.log(JSON.stringify({ plan }, null, 2));
    if (!options.execute) {
      console.log('dry run only; pass --execute to apply');
      return;
    }

    await ensureScriptPubKeySchema(client);

    let checkpoint = await prepareCheckpoint({
      asOfBlockHeight,
      checkpointKey,
      client,
      existingCheckpoint,
      metadata,
      networkId,
      reset: options.reset === true,
    });

    let nextRangeIndex = checkpoint.nextRangeIndex;
    if (options.fromRange !== undefined) {
      nextRangeIndex = parseNonNegativeInteger(options.fromRange, 'fromRange');
      if (nextRangeIndex > ranges.length) {
        throw new Error(`fromRange ${nextRangeIndex} exceeds total range count ${ranges.length}`);
      }
      checkpoint = {
        ...checkpoint,
        nextRangeIndex,
        updatedAt: new Date().toISOString(),
      };
      await metadata.setJsonValue(checkpointKey, checkpoint);
    }

    const rangeLimit =
      options.rangeLimit === undefined
        ? null
        : parsePositiveInteger(options.rangeLimit, 'rangeLimit');
    const stopRangeIndex =
      rangeLimit === null ? ranges.length : Math.min(ranges.length, nextRangeIndex + rangeLimit);

    for (let index = nextRangeIndex; index < stopRangeIndex; index += 1) {
      const range = ranges[index];
      if (!range) {
        throw new Error(`missing range at index ${index}`);
      }

      await heartbeat(metadata, networkId, state.syncTail, asOfBlockHeight);
      checkpoint = {
        ...checkpoint,
        nextRangeIndex: index,
        updatedAt: new Date().toISOString(),
      };
      await metadata.setJsonValue(checkpointKey, checkpoint);

      const startedAt = Date.now();
      await insertCurrentRange(client, networkId, asOfBlockHeight, range, statementTimeoutMs);
      const rowCount = await countRowsInRange(client, currentUtxosTable, networkId, range);
      const elapsedMs = Date.now() - startedAt;
      checkpoint = {
        ...checkpoint,
        lastRange: { elapsedMs, index, rowCount },
        nextRangeIndex: index + 1,
        updatedAt: new Date().toISOString(),
      };
      await metadata.setJsonValue(checkpointKey, checkpoint);
      console.log(
        JSON.stringify({
          phase: 'range-complete',
          index,
          totalRanges: ranges.length,
          rowCount,
          elapsedMs,
          nextRangeIndex: checkpoint.nextRangeIndex,
        }),
      );
    }

    if (checkpoint.nextRangeIndex < ranges.length) {
      console.log(
        JSON.stringify({
          phase: 'range-paused',
          nextRangeIndex: checkpoint.nextRangeIndex,
          totalRanges: ranges.length,
        }),
      );
      return;
    }

    if (!options.finalize) {
      console.log(JSON.stringify({ phase: 'finalize-skipped' }));
      return;
    }

    await heartbeat(metadata, networkId, state.syncTail, asOfBlockHeight);
    await finalizeCurrentState(client, networkId, asOfBlockHeight, statementTimeoutMs);
    const completeCheckpoint: MaterializationCheckpoint = {
      ...checkpoint,
      finalizedAt: new Date().toISOString(),
      status: 'complete',
      updatedAt: new Date().toISOString(),
    };
    await Promise.all([
      metadata.setJsonValue(checkpointKey, completeCheckpoint),
      metadata.setJsonValue(configKeyDogecoinCurrentStateReady(networkId), true),
      metadata.setJsonValue(configKeyDogecoinHistoryReady(networkId), false),
      metadata.setJsonValue(configKeyIndexerStage(networkId), 'online'),
      metadata.setJsonValue(configKeyIndexerProcessTail(networkId), asOfBlockHeight),
      metadata.setJsonValue(configKeyIndexerProcessProgress(networkId), 1),
      metadata.upsertCoreIndexerState({
        lastError: null,
        networkId,
        onlineTip: Math.max(state.onlineTip, asOfBlockHeight),
        processTail: asOfBlockHeight,
        stage: 'online',
        syncTail: Math.max(state.syncTail, asOfBlockHeight),
      }),
    ]);

    console.log(
      JSON.stringify(
        {
          phase: 'complete',
          checkpoint: completeCheckpoint,
          currentRows: await countRows(client, currentUtxosTable, networkId),
          currentRowsByAddress: await countRows(client, currentUtxosByAddressTable, networkId),
          balancesRows: await countRows(client, balancesTable, networkId),
          appliedBlocksRows: await countRows(client, appliedBlocksTable, networkId),
        },
        null,
        2,
      ),
    );
  } finally {
    await client.close();
  }
}

async function prepareCheckpoint(input: {
  asOfBlockHeight: number;
  checkpointKey: string;
  client: ReturnType<typeof createClient>;
  existingCheckpoint: MaterializationCheckpoint | null;
  metadata: RelationalMetadataStore;
  networkId: number;
  reset: boolean;
}): Promise<MaterializationCheckpoint> {
  if (input.reset) {
    await Promise.all([
      input.metadata.setJsonValue(configKeyDogecoinCurrentStateReady(input.networkId), false),
      input.metadata.setJsonValue(configKeyDogecoinHistoryReady(input.networkId), false),
    ]);
    for (const table of [
      currentUtxosTable,
      currentUtxosByAddressTable,
      balancesTable,
      appliedBlocksTable,
    ]) {
      await input.client.command({ query: `TRUNCATE TABLE ${table}` });
    }
    const checkpoint = newCheckpoint(input.asOfBlockHeight, new Date().toISOString());
    await input.metadata.setJsonValue(input.checkpointKey, checkpoint);
    return checkpoint;
  }

  if (input.existingCheckpoint) {
    if (input.existingCheckpoint.asOfBlockHeight !== input.asOfBlockHeight) {
      throw new Error(
        `checkpoint height ${input.existingCheckpoint.asOfBlockHeight} does not match requested height ${input.asOfBlockHeight}; pass --reset to restart`,
      );
    }
    if (input.existingCheckpoint.status === 'complete') {
      throw new Error('current-state materialization checkpoint is already complete');
    }
    return input.existingCheckpoint;
  }

  const currentRows = await countRows(input.client, currentUtxosTable, input.networkId);
  const currentRowsByAddress = await countRows(
    input.client,
    currentUtxosByAddressTable,
    input.networkId,
  );
  if (currentRows > 0 || currentRowsByAddress > 0) {
    throw new Error(
      `current-state tables are not empty (${currentRows}/${currentRowsByAddress} rows) and no checkpoint exists; pass --reset to restart safely`,
    );
  }

  const checkpoint = newCheckpoint(input.asOfBlockHeight, new Date().toISOString());
  await input.metadata.setJsonValue(input.checkpointKey, checkpoint);
  return checkpoint;
}

async function ensureScriptPubKeySchema(client: ReturnType<typeof createClient>) {
  for (const table of [
    createsTable,
    currentUtxosTable,
    currentUtxosByAddressTable,
    'utxo_outputs_v2',
  ]) {
    await client.command({
      query: `ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS script_pub_key String AFTER script_type`,
    });
  }

  await client.command({ query: `DROP VIEW IF EXISTS ${currentUtxosByAddressTable}_mv` });
  await client.command({
    query: `
      CREATE MATERIALIZED VIEW IF NOT EXISTS ${currentUtxosByAddressTable}_mv
      TO ${currentUtxosByAddressTable}
      AS
      SELECT
        network_id,
        block_height,
        block_hash,
        block_time,
        txid,
        tx_index,
        vout,
        output_key,
        address,
        script_type,
        script_pub_key,
        value_base,
        is_coinbase,
        is_spendable,
        spent_by_txid,
        spent_in_block,
        spent_input_index,
        version
      FROM ${currentUtxosTable}
    `,
  });
}

function newCheckpoint(asOfBlockHeight: number, timestamp: string): MaterializationCheckpoint {
  return {
    asOfBlockHeight,
    nextRangeIndex: 0,
    resetAt: timestamp,
    startedAt: timestamp,
    status: 'running',
    totalRanges: ranges.length,
    updatedAt: timestamp,
  };
}

async function heartbeat(
  metadata: RelationalMetadataStore,
  networkId: number,
  syncTail: number,
  processTail: number,
) {
  await metadata.upsertCoreIndexerState({
    lastError: null,
    networkId,
    processTail,
    stage: 'process_backfill',
    syncTail: Math.max(syncTail, processTail),
  });
}

async function insertCurrentRange(
  client: ReturnType<typeof createClient>,
  networkId: number,
  asOfBlockHeight: number,
  range: ClickHouseStringRange,
  statementTimeoutMs: number,
) {
  await client.command({
    query: `
      INSERT INTO ${currentUtxosTable} (
        network_id,
        block_height,
        block_hash,
        block_time,
        txid,
        tx_index,
        vout,
        output_key,
        address,
        script_type,
        script_pub_key,
        value_base,
        is_coinbase,
        is_spendable,
        spent_by_txid,
        spent_in_block,
        spent_input_index,
        version
      )
      SELECT
        c.network_id,
        c.block_height,
        c.block_hash,
        c.block_time,
        c.txid,
        c.tx_index,
        c.vout,
        c.output_key,
        c.address,
        c.script_type,
        c.script_pub_key,
        c.value_base,
        c.is_coinbase,
        c.is_spendable,
        NULL,
        NULL,
        NULL,
        {asOfBlockHeight:UInt64}
      FROM (
        SELECT *
        FROM ${createsTable}
        WHERE
          network_id = {networkId:UInt64}
          ${clickHouseStringRangeClause('output_key', range)}
        ORDER BY output_key ASC, version DESC
        LIMIT 1 BY output_key
      ) AS c
      LEFT ANTI JOIN (
        SELECT network_id, spent_output_key
        FROM ${spendsTable}
        WHERE
          network_id = {networkId:UInt64}
          ${clickHouseStringRangeClause('spent_output_key', range)}
        ORDER BY spent_output_key ASC, version DESC
        LIMIT 1 BY spent_output_key
      ) AS s
      ON c.network_id = s.network_id AND c.output_key = s.spent_output_key
    `,
    query_params: {
      ...clickHouseStringRangeParams(range),
      asOfBlockHeight,
      networkId,
    },
    clickhouse_settings: materializationSettings(statementTimeoutMs),
  });
}

async function finalizeCurrentState(
  client: ReturnType<typeof createClient>,
  networkId: number,
  asOfBlockHeight: number,
  statementTimeoutMs: number,
) {
  for (const table of [balancesTable, appliedBlocksTable]) {
    await client.command({ query: `TRUNCATE TABLE ${table}` });
  }

  await client.command({
    query: `
      INSERT INTO ${balancesTable} (
        network_id,
        address,
        asset_address,
        balance,
        as_of_block_height,
        version
      )
      SELECT
        network_id,
        address,
        '',
        toString(sum(toInt256(value_base))),
        {asOfBlockHeight:UInt64},
        {asOfBlockHeight:UInt64}
      FROM ${currentUtxosByAddressTable}
      WHERE
        network_id = {networkId:UInt64}
        AND is_spendable = 1
        AND address != ''
        AND spent_by_txid IS NULL
      GROUP BY network_id, address
    `,
    query_params: { asOfBlockHeight, networkId },
    clickhouse_settings: {
      ...materializationSettings(statementTimeoutMs),
      optimize_aggregation_in_order: 1,
    },
  });

  await client.command({
    query: `
      INSERT INTO ${appliedBlocksTable} (network_id, block_height, block_hash)
      SELECT network_id, block_height, block_hash
      FROM (
        SELECT network_id, block_height, block_hash, version
        FROM ${processedBlocksTable}
        WHERE
          network_id = {networkId:UInt64}
          AND block_height <= {asOfBlockHeight:UInt64}
        ORDER BY block_height ASC, version DESC
        LIMIT 1 BY block_height
      )
    `,
    query_params: { asOfBlockHeight, networkId },
    clickhouse_settings: materializationSettings(statementTimeoutMs),
  });
}

async function countRows(
  client: ReturnType<typeof createClient>,
  table: string,
  networkId: number,
): Promise<number> {
  const result = await client.query({
    query: `
      SELECT count() AS count
      FROM ${table}
      WHERE network_id = {networkId:UInt64}
    `,
    query_params: { networkId },
    format: 'JSONEachRow',
  });
  const rows = (await result.json()) as Array<{ count: string | number }>;
  return Number(rows[0]?.count ?? 0);
}

async function countRowsInRange(
  client: ReturnType<typeof createClient>,
  table: string,
  networkId: number,
  range: ClickHouseStringRange,
): Promise<number> {
  const result = await client.query({
    query: `
      SELECT count() AS count
      FROM ${table}
      WHERE
        network_id = {networkId:UInt64}
        ${clickHouseStringRangeClause('output_key', range)}
    `,
    query_params: { ...clickHouseStringRangeParams(range), networkId },
    format: 'JSONEachRow',
  });
  const rows = (await result.json()) as Array<{ count: string | number }>;
  return Number(rows[0]?.count ?? 0);
}

function materializationSettings(statementTimeoutMs: number): Record<string, number | string> {
  return {
    max_execution_time: Math.max(1, Math.ceil(statementTimeoutMs / 1000)),
    max_block_size: '65536',
    max_bytes_before_external_group_by: '1073741824',
    max_bytes_before_external_sort: '1073741824',
    max_insert_block_size: '65536',
    min_insert_block_size_bytes: '0',
    min_insert_block_size_rows: '0',
  };
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
