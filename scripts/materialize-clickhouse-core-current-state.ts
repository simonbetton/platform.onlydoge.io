#!/usr/bin/env bun

import { createClient } from '@clickhouse/client';
import {
  type CoreIndexerState,
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

type ClickHouseClient = ReturnType<typeof createClient>;

type MaterializationContext = {
  asOfBlockHeight: number;
  checkpointKey: string;
  client: ClickHouseClient;
  metadata: RelationalMetadataStore;
  networkId: number;
  state: CoreIndexerState;
  statementTimeoutMs: number;
};

type MaterializationPlan = {
  appliedBlocksRows: number;
  asOfBlockHeight: number;
  balancesRows: number;
  currentRows: number;
  currentRowsByAddress: number;
  currentStateReady: boolean | null;
  existingCheckpoint: MaterializationCheckpoint | null;
  finalize: boolean;
  mode: 'dry-run' | 'execute';
  networkId: number;
  reset: boolean;
  totalRanges: number;
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
  const context = await createMaterializationContext();
  try {
    await runMaterialization(context);
  } finally {
    await context.client.close();
  }
}

async function createMaterializationContext(): Promise<MaterializationContext> {
  const settings = loadSettings({ mode: 'indexer' });
  assertClickHouseWarehouse(settings.warehouse.driver);

  const statementTimeoutMs = parsePositiveInteger(options.statementTimeoutMs, 'statementTimeoutMs');
  const metadata = await RelationalMetadataStore.connect(settings.database);
  const networkId = await resolveDogecoinNetworkId(metadata, options.networkId);
  const state = await requireCoreIndexerState(metadata, networkId);
  const client = createMaterializationClient(settings.warehouse, statementTimeoutMs);

  return {
    asOfBlockHeight: resolveAsOfBlockHeight(state),
    checkpointKey: configKeyDogecoinCurrentStateMaterialization(networkId),
    client,
    metadata,
    networkId,
    state,
    statementTimeoutMs,
  };
}

async function runMaterialization(context: MaterializationContext): Promise<void> {
  const existingCheckpoint = await context.metadata.getJsonValue<MaterializationCheckpoint>(
    context.checkpointKey,
  );
  const plan = await buildMaterializationPlan(context, existingCheckpoint);
  console.log(JSON.stringify({ plan }, null, 2));
  if (!options.execute) {
    console.log('dry run only; pass --execute to apply');
    return;
  }

  const prepared = await prepareCheckpoint({
    ...context,
    existingCheckpoint,
    reset: options.reset === true,
  });
  const ranged = await processMaterializationRanges(
    context,
    await applyRangeOverride(context, prepared),
  );
  await finishMaterialization(context, ranged);
}

function resolveAsOfBlockHeight(state: CoreIndexerState): number {
  const asOfBlockHeight = selectedAsOfBlockHeight(state);
  assertInitializedProcessTail(asOfBlockHeight);
  return asOfBlockHeight;
}

function selectedAsOfBlockHeight(state: CoreIndexerState): number {
  if (options.asOfBlockHeight === undefined) {
    return state.processTail;
  }

  return parseNonNegativeInteger(options.asOfBlockHeight, 'asOfBlockHeight');
}

function assertInitializedProcessTail(asOfBlockHeight: number): void {
  if (asOfBlockHeight < 0) {
    throw new Error('core process tail has not been initialized');
  }
}

async function buildMaterializationPlan(
  context: MaterializationContext,
  existingCheckpoint: MaterializationCheckpoint | null,
): Promise<MaterializationPlan> {
  return {
    mode: options.execute ? 'execute' : 'dry-run',
    networkId: context.networkId,
    asOfBlockHeight: context.asOfBlockHeight,
    reset: options.reset === true,
    finalize: options.finalize,
    totalRanges: ranges.length,
    currentStateReady: await context.metadata.getJsonValue<boolean>(
      configKeyDogecoinCurrentStateReady(context.networkId),
    ),
    existingCheckpoint,
    currentRows: await countRows(context.client, currentUtxosTable, context.networkId),
    currentRowsByAddress: await countRows(
      context.client,
      currentUtxosByAddressTable,
      context.networkId,
    ),
    balancesRows: await countRows(context.client, balancesTable, context.networkId),
    appliedBlocksRows: await countRows(context.client, appliedBlocksTable, context.networkId),
  };
}

async function applyRangeOverride(
  context: MaterializationContext,
  checkpoint: MaterializationCheckpoint,
): Promise<MaterializationCheckpoint> {
  if (options.fromRange === undefined) {
    return checkpoint;
  }

  const nextRangeIndex = parseNonNegativeInteger(options.fromRange, 'fromRange');
  assertRangeIndex(nextRangeIndex);
  const nextCheckpoint = {
    ...checkpoint,
    nextRangeIndex,
    updatedAt: new Date().toISOString(),
  };
  await context.metadata.setJsonValue(context.checkpointKey, nextCheckpoint);
  return nextCheckpoint;
}

async function processMaterializationRanges(
  context: MaterializationContext,
  checkpoint: MaterializationCheckpoint,
): Promise<MaterializationCheckpoint> {
  let current = checkpoint;
  for (
    let index = checkpoint.nextRangeIndex;
    index < stopRangeIndex(checkpoint.nextRangeIndex);
    index += 1
  ) {
    current = await processMaterializationRange(context, current, index);
  }
  return current;
}

async function processMaterializationRange(
  context: MaterializationContext,
  checkpoint: MaterializationCheckpoint,
  index: number,
): Promise<MaterializationCheckpoint> {
  const range = requiredRange(index);
  await heartbeat(
    context.metadata,
    context.networkId,
    context.state.syncTail,
    context.asOfBlockHeight,
  );
  const started = await markRangeStarted(context, checkpoint, index);
  const startedAt = Date.now();
  await insertCurrentRange(
    context.client,
    context.networkId,
    context.asOfBlockHeight,
    range,
    context.statementTimeoutMs,
  );
  const rowCount = await countRowsInRange(
    context.client,
    currentUtxosTable,
    context.networkId,
    range,
  );
  return markRangeComplete(context, started, index, rowCount, Date.now() - startedAt);
}

async function markRangeStarted(
  context: MaterializationContext,
  checkpoint: MaterializationCheckpoint,
  index: number,
): Promise<MaterializationCheckpoint> {
  const nextCheckpoint = {
    ...checkpoint,
    nextRangeIndex: index,
    updatedAt: new Date().toISOString(),
  };
  await context.metadata.setJsonValue(context.checkpointKey, nextCheckpoint);
  return nextCheckpoint;
}

async function markRangeComplete(
  context: MaterializationContext,
  checkpoint: MaterializationCheckpoint,
  index: number,
  rowCount: number,
  elapsedMs: number,
): Promise<MaterializationCheckpoint> {
  const nextCheckpoint = {
    ...checkpoint,
    lastRange: { elapsedMs, index, rowCount },
    nextRangeIndex: index + 1,
    updatedAt: new Date().toISOString(),
  };
  await context.metadata.setJsonValue(context.checkpointKey, nextCheckpoint);
  console.log(
    JSON.stringify({
      phase: 'range-complete',
      index,
      totalRanges: ranges.length,
      rowCount,
      elapsedMs,
      nextRangeIndex: nextCheckpoint.nextRangeIndex,
    }),
  );
  return nextCheckpoint;
}

async function finishMaterialization(
  context: MaterializationContext,
  checkpoint: MaterializationCheckpoint,
): Promise<void> {
  if (logMaterializationPauseIfNeeded(checkpoint)) {
    return;
  }

  await finalizeMaterializationIfEnabled(context, checkpoint);
}

function logMaterializationPauseIfNeeded(checkpoint: MaterializationCheckpoint): boolean {
  if (checkpoint.nextRangeIndex >= ranges.length) {
    return false;
  }

  console.log(
    JSON.stringify({
      phase: 'range-paused',
      nextRangeIndex: checkpoint.nextRangeIndex,
      totalRanges: ranges.length,
    }),
  );
  return true;
}

async function finalizeMaterializationIfEnabled(
  context: MaterializationContext,
  checkpoint: MaterializationCheckpoint,
): Promise<void> {
  if (!options.finalize) {
    console.log(JSON.stringify({ phase: 'finalize-skipped' }));
    return;
  }

  await finalizeMaterialization(context, checkpoint);
}

async function finalizeMaterialization(
  context: MaterializationContext,
  checkpoint: MaterializationCheckpoint,
): Promise<void> {
  await heartbeat(
    context.metadata,
    context.networkId,
    context.state.syncTail,
    context.asOfBlockHeight,
  );
  await finalizeCurrentState(
    context.client,
    context.networkId,
    context.asOfBlockHeight,
    context.statementTimeoutMs,
  );
  const completeCheckpoint = await markMaterializationComplete(context, checkpoint);
  await logMaterializationComplete(context, completeCheckpoint);
}

async function markMaterializationComplete(
  context: MaterializationContext,
  checkpoint: MaterializationCheckpoint,
): Promise<MaterializationCheckpoint> {
  const completeCheckpoint: MaterializationCheckpoint = {
    ...checkpoint,
    finalizedAt: new Date().toISOString(),
    status: 'complete',
    updatedAt: new Date().toISOString(),
  };
  await Promise.all([
    context.metadata.setJsonValue(context.checkpointKey, completeCheckpoint),
    context.metadata.setJsonValue(configKeyDogecoinCurrentStateReady(context.networkId), true),
    context.metadata.setJsonValue(configKeyDogecoinHistoryReady(context.networkId), false),
    context.metadata.setJsonValue(configKeyIndexerStage(context.networkId), 'online'),
    context.metadata.setJsonValue(
      configKeyIndexerProcessTail(context.networkId),
      context.asOfBlockHeight,
    ),
    context.metadata.setJsonValue(configKeyIndexerProcessProgress(context.networkId), 1),
    context.metadata.upsertCoreIndexerState({
      lastError: null,
      networkId: context.networkId,
      onlineTip: Math.max(context.state.onlineTip, context.asOfBlockHeight),
      processTail: context.asOfBlockHeight,
      stage: 'online',
      syncTail: Math.max(context.state.syncTail, context.asOfBlockHeight),
    }),
  ]);
  return completeCheckpoint;
}

async function logMaterializationComplete(
  context: MaterializationContext,
  checkpoint: MaterializationCheckpoint,
): Promise<void> {
  console.log(
    JSON.stringify(
      {
        phase: 'complete',
        checkpoint,
        currentRows: await countRows(context.client, currentUtxosTable, context.networkId),
        currentRowsByAddress: await countRows(
          context.client,
          currentUtxosByAddressTable,
          context.networkId,
        ),
        balancesRows: await countRows(context.client, balancesTable, context.networkId),
        appliedBlocksRows: await countRows(context.client, appliedBlocksTable, context.networkId),
      },
      null,
      2,
    ),
  );
}

function stopRangeIndex(nextRangeIndex: number): number {
  const rangeLimit = parseRangeLimit();
  return rangeLimit === null ? ranges.length : Math.min(ranges.length, nextRangeIndex + rangeLimit);
}

function parseRangeLimit(): number | null {
  return options.rangeLimit === undefined
    ? null
    : parsePositiveInteger(options.rangeLimit, 'rangeLimit');
}

function assertRangeIndex(nextRangeIndex: number): void {
  if (nextRangeIndex > ranges.length) {
    throw new Error(`fromRange ${nextRangeIndex} exceeds total range count ${ranges.length}`);
  }
}

function requiredRange(index: number): ClickHouseStringRange {
  const range = ranges[index];
  if (!range) {
    throw new Error(`missing range at index ${index}`);
  }
  return range;
}

async function prepareCheckpoint(input: {
  asOfBlockHeight: number;
  checkpointKey: string;
  client: ClickHouseClient;
  existingCheckpoint: MaterializationCheckpoint | null;
  metadata: RelationalMetadataStore;
  networkId: number;
  reset: boolean;
}): Promise<MaterializationCheckpoint> {
  if (input.reset) {
    return resetMaterializationCheckpoint(input);
  }

  return resumeOrCreateCheckpoint(input);
}

async function resumeOrCreateCheckpoint(input: {
  asOfBlockHeight: number;
  checkpointKey: string;
  client: ClickHouseClient;
  existingCheckpoint: MaterializationCheckpoint | null;
  metadata: RelationalMetadataStore;
  networkId: number;
}): Promise<MaterializationCheckpoint> {
  if (input.existingCheckpoint) {
    return validateExistingCheckpoint(input.existingCheckpoint, input.asOfBlockHeight);
  }

  return createCheckpointForEmptyTables(input);
}

async function resetMaterializationCheckpoint(input: {
  asOfBlockHeight: number;
  checkpointKey: string;
  client: ClickHouseClient;
  metadata: RelationalMetadataStore;
  networkId: number;
}): Promise<MaterializationCheckpoint> {
  await Promise.all([
    input.metadata.setJsonValue(configKeyDogecoinCurrentStateReady(input.networkId), false),
    input.metadata.setJsonValue(configKeyDogecoinHistoryReady(input.networkId), false),
  ]);
  for (const table of currentStateTables()) {
    await input.client.command({ query: `TRUNCATE TABLE ${table}` });
  }
  return writeNewCheckpoint(input);
}

function validateExistingCheckpoint(
  checkpoint: MaterializationCheckpoint,
  asOfBlockHeight: number,
): MaterializationCheckpoint {
  assertCheckpointHeight(checkpoint, asOfBlockHeight);
  assertCheckpointNotComplete(checkpoint);
  return checkpoint;
}

function assertCheckpointHeight(
  checkpoint: MaterializationCheckpoint,
  asOfBlockHeight: number,
): void {
  if (checkpoint.asOfBlockHeight !== asOfBlockHeight) {
    throw new Error(
      `checkpoint height ${checkpoint.asOfBlockHeight} does not match requested height ${asOfBlockHeight}; pass --reset to restart`,
    );
  }
}

function assertCheckpointNotComplete(checkpoint: MaterializationCheckpoint): void {
  if (checkpoint.status === 'complete') {
    throw new Error('current-state materialization checkpoint is already complete');
  }
}

async function createCheckpointForEmptyTables(input: {
  asOfBlockHeight: number;
  checkpointKey: string;
  client: ClickHouseClient;
  metadata: RelationalMetadataStore;
  networkId: number;
}): Promise<MaterializationCheckpoint> {
  const currentRows = await countRows(input.client, currentUtxosTable, input.networkId);
  const currentRowsByAddress = await countRows(
    input.client,
    currentUtxosByAddressTable,
    input.networkId,
  );
  assertCurrentStateTablesEmpty(currentRows, currentRowsByAddress);

  return writeNewCheckpoint(input);
}

async function writeNewCheckpoint(input: {
  asOfBlockHeight: number;
  checkpointKey: string;
  metadata: RelationalMetadataStore;
}): Promise<MaterializationCheckpoint> {
  const checkpoint = newCheckpoint(input.asOfBlockHeight, new Date().toISOString());
  await input.metadata.setJsonValue(input.checkpointKey, checkpoint);
  return checkpoint;
}

function currentStateTables(): string[] {
  return [currentUtxosTable, currentUtxosByAddressTable, balancesTable, appliedBlocksTable];
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
          AND version <= {asOfBlockHeight:UInt64}
          ${clickHouseStringRangeClause('output_key', range)}
        ORDER BY output_key ASC, version DESC
        LIMIT 1 BY output_key
      ) AS c
      LEFT ANTI JOIN (
        SELECT network_id, spent_output_key
        FROM ${spendsTable}
        WHERE
          network_id = {networkId:UInt64}
          AND version <= {asOfBlockHeight:UInt64}
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
  return firstCountValue(rows);
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
  return firstCountValue(rows);
}

function assertClickHouseWarehouse(driver: string): void {
  if (driver !== 'clickhouse') {
    throw new Error('ClickHouse warehouse is required for current-state materialization');
  }
}

async function requireCoreIndexerState(
  metadata: RelationalMetadataStore,
  networkId: number,
): Promise<CoreIndexerState> {
  const state = await metadata.getCoreIndexerState(networkId);
  if (!state) {
    throw new Error(`missing core indexer state for network ${networkId}`);
  }

  return state;
}

function createMaterializationClient(
  warehouse: ReturnType<typeof loadSettings>['warehouse'],
  statementTimeoutMs: number,
): ClickHouseClient {
  return createClient({
    url: warehouse.location,
    database: warehouse.database,
    username: warehouse.user,
    password: warehouse.password,
    request_timeout: statementTimeoutMs + 60_000,
  });
}

function assertCurrentStateTablesEmpty(currentRows: number, currentRowsByAddress: number): void {
  if (!hasCurrentStateRows(currentRows, currentRowsByAddress)) {
    return;
  }

  throw new Error(
    `current-state tables are not empty (${currentRows}/${currentRowsByAddress} rows) and no checkpoint exists; pass --reset to restart safely`,
  );
}

function hasCurrentStateRows(currentRows: number, currentRowsByAddress: number): boolean {
  return [currentRows > 0, currentRowsByAddress > 0].includes(true);
}

function firstCountValue(rows: Array<{ count: string | number }>): number {
  const [row] = rows;
  if (!row) {
    return 0;
  }

  return Number(row.count);
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
