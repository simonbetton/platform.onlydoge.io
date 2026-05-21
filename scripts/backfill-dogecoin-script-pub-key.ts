#!/usr/bin/env bun

import { randomUUID } from 'node:crypto';

import { createClient } from '@clickhouse/client';
import {
  configKeyIndexerProcessTail,
  mapWithConcurrency,
  range,
} from '@onlydoge/indexing-pipeline';
import {
  clickHouseCoreDogecoinTables,
  createRawBlockStorage,
  loadSettings,
  RelationalMetadataStore,
} from '@onlydoge/platform';
import { Command } from 'commander';
import {
  parseNonNegativeInteger,
  parsePositiveInteger,
  resolveDogecoinNetworkId,
} from './dogecoin-script-utils';

type ScriptPubKeyPatch = {
  outputKey: string;
  scriptPubKey: string;
};

type ScriptPubKeyBackfillCheckpoint = {
  completedAt?: string;
  fromHeight: number;
  lastBatch?: {
    elapsedMs: number;
    endHeight: number;
    outputCount: number;
    startHeight: number;
  };
  nextHeight: number;
  startedAt: string;
  status: 'complete' | 'running';
  toHeight: number;
  totalOutputs: number;
  updatedAt: string;
};

const rawBlockPart = 'block';
const createsTable = clickHouseCoreDogecoinTables.coreUtxoCreates;
const currentUtxosTable = clickHouseCoreDogecoinTables.currentUtxos;
const currentUtxosByAddressTable = clickHouseCoreDogecoinTables.currentUtxosByAddress;
const currentUtxosByAddressView = `${currentUtxosByAddressTable}_mv`;
const legacyUtxosTable = 'utxo_outputs_v2';
const scriptPubKeyPatchTable = 'dogecoin_script_pub_key_backfill_patches_v1';
const replacementWriteTables = [createsTable, currentUtxosTable, legacyUtxosTable];
const mutationWriteTables = [
  createsTable,
  currentUtxosTable,
  currentUtxosByAddressTable,
  legacyUtxosTable,
];
const replacementDefaultVersionIncrement = 1;

type BackfillWriteMode = 'replacement' | 'mutation';

const program = new Command()
  .name('backfill-dogecoin-script-pub-key')
  .description('Backfill Dogecoin UTXO scriptPubKey hex from stored raw block snapshots.')
  .option('--execute', 'perform writes; without this flag the script is a dry run')
  .option('--networkId <id>', 'internal Dogecoin network id')
  .option('--fromHeight <height>', 'first block height to scan')
  .option('--toHeight <height>', 'last block height to scan; defaults to core process tail')
  .option('--blockBatchSize <count>', 'raw blocks to load per checkpointed batch', '100')
  .option('--blockLimit <count>', 'process at most this many blocks in this run')
  .option(
    '--mutationBatchSize <count>',
    'UTXO outputs to update per ClickHouse write batch',
    '5000',
  )
  .option('--loadConcurrency <count>', 'raw block snapshot load concurrency', '8')
  .option('--rawTimeoutMs <ms>', 'raw block storage request timeout per block', '30000')
  .option('--statementTimeoutMs <ms>', 'ClickHouse statement timeout in milliseconds', '3600000')
  .option('--mutationsSync <0|1|2>', 'ClickHouse mutations_sync setting', '1')
  .option(
    '--writeMode <replacement|mutation>',
    'write replacement rows into ReplacingMergeTree tables, or use ALTER UPDATE mutations',
    'replacement',
  )
  .option('--resetCheckpoint', 'discard any existing scriptPubKey backfill checkpoint')
  .option('--skipLegacy', 'do not update the legacy utxo_outputs_v2 fallback table')
  .option('--skipMissingBlocks', 'continue when a raw block snapshot is missing')
  .option('--withCounts', 'include logical empty scriptPubKey counts in dry-run/final output')
  .parse();

const options = program.opts<{
  blockBatchSize: string;
  blockLimit?: string;
  execute?: boolean;
  fromHeight?: string;
  loadConcurrency: string;
  mutationBatchSize: string;
  mutationsSync: string;
  networkId?: string;
  rawTimeoutMs: string;
  resetCheckpoint?: boolean;
  skipLegacy?: boolean;
  skipMissingBlocks?: boolean;
  statementTimeoutMs: string;
  toHeight?: string;
  withCounts?: boolean;
  writeMode: string;
}>();

async function main() {
  const settings = loadSettings({ mode: 'indexer' });
  if (settings.warehouse.driver !== 'clickhouse') {
    throw new Error('ClickHouse warehouse is required for Dogecoin scriptPubKey backfill');
  }

  const metadata = await RelationalMetadataStore.connect(settings.database);
  const networkId = await resolveDogecoinNetworkId(metadata, options.networkId);
  const state = await metadata.getCoreIndexerState(networkId);
  const processTail =
    state?.processTail ??
    (await metadata.getJsonValue<number>(configKeyIndexerProcessTail(networkId))) ??
    -1;
  if (processTail < 0) {
    throw new Error('core process tail has not been initialized');
  }

  const requestedFromHeight =
    options.fromHeight === undefined
      ? 0
      : parseNonNegativeInteger(options.fromHeight, 'fromHeight');
  const requestedToHeight =
    options.toHeight === undefined
      ? processTail
      : parseNonNegativeInteger(options.toHeight, 'toHeight');
  if (requestedToHeight < requestedFromHeight) {
    throw new Error('toHeight must be greater than or equal to fromHeight');
  }
  if (requestedToHeight > processTail) {
    throw new Error(`toHeight ${requestedToHeight} exceeds process tail ${processTail}`);
  }

  const blockBatchSize = parsePositiveInteger(options.blockBatchSize, 'blockBatchSize');
  const mutationBatchSize = parsePositiveInteger(options.mutationBatchSize, 'mutationBatchSize');
  const loadConcurrency = parsePositiveInteger(options.loadConcurrency, 'loadConcurrency');
  const rawTimeoutMs = parsePositiveInteger(options.rawTimeoutMs, 'rawTimeoutMs');
  const statementTimeoutMs = parsePositiveInteger(options.statementTimeoutMs, 'statementTimeoutMs');
  const mutationsSync = parseMutationsSync(options.mutationsSync);
  const writeMode = parseWriteMode(options.writeMode);
  const blockLimit =
    options.blockLimit === undefined
      ? null
      : parsePositiveInteger(options.blockLimit, 'blockLimit');

  const schemaTables = backfillTables(mutationWriteTables, options.skipLegacy === true);
  const writeTables = backfillTables(
    writeMode === 'replacement' ? replacementWriteTables : mutationWriteTables,
    options.skipLegacy === true,
  );
  const client = createClient({
    url: settings.warehouse.location,
    database: settings.warehouse.database,
    username: settings.warehouse.user,
    password: settings.warehouse.password,
    request_timeout: statementTimeoutMs + 60_000,
  });
  const rawBlocks = createRawBlockStorage(settings.storage);
  const checkpointKey = scriptPubKeyBackfillCheckpointKey(networkId);

  try {
    const existingCheckpoint =
      await metadata.getJsonValue<ScriptPubKeyBackfillCheckpoint>(checkpointKey);
    const columnStatus = await listScriptPubKeyColumnStatus(client, schemaTables);
    const plan = {
      mode: options.execute ? 'execute' : 'dry-run',
      networkId,
      fromHeight: requestedFromHeight,
      toHeight: requestedToHeight,
      processTail,
      blockBatchSize,
      blockLimit,
      mutationBatchSize,
      loadConcurrency,
      rawTimeoutMs,
      statementTimeoutMs,
      mutationsSync,
      schemaTables,
      writeMode,
      writeTables,
      checkpointKey,
      existingCheckpoint,
      columnStatus,
      ...(options.withCounts
        ? { emptyCounts: await countEmptyScriptPubKeys(client, networkId, schemaTables) }
        : {}),
    };
    console.log(JSON.stringify({ plan }, null, 2));
    if (!options.execute) {
      console.log('dry run only; pass --execute to apply');
      return;
    }

    await ensureClickHouseScriptPubKeySchema(client, schemaTables);
    let checkpoint = await prepareCheckpoint({
      checkpointKey,
      existingCheckpoint,
      fromHeight: requestedFromHeight,
      metadata,
      reset: options.resetCheckpoint === true,
      toHeight: requestedToHeight,
    });

    const stopHeight =
      blockLimit === null
        ? checkpoint.toHeight
        : Math.min(checkpoint.toHeight, checkpoint.nextHeight + blockLimit - 1);

    for (
      let startHeight = checkpoint.nextHeight;
      startHeight <= stopHeight;
      startHeight += blockBatchSize
    ) {
      const endHeight = Math.min(stopHeight, startHeight + blockBatchSize - 1);
      const startedAt = Date.now();
      const patches = await loadScriptPubKeyPatches({
        endHeight,
        loadConcurrency,
        networkId,
        rawBlocks,
        rawTimeoutMs,
        skipMissingBlocks: options.skipMissingBlocks === true,
        startHeight,
      });

      for (const chunk of chunkArray(patches, mutationBatchSize)) {
        await applyScriptPubKeyPatch(client, networkId, chunk, writeTables, {
          mutationsSync,
          statementTimeoutMs,
          writeMode,
        });
      }

      checkpoint = {
        ...checkpoint,
        lastBatch: {
          elapsedMs: Date.now() - startedAt,
          endHeight,
          outputCount: patches.length,
          startHeight,
        },
        nextHeight: endHeight + 1,
        totalOutputs: checkpoint.totalOutputs + patches.length,
        updatedAt: new Date().toISOString(),
      };
      await metadata.setJsonValue(checkpointKey, checkpoint);
      console.log(
        JSON.stringify({
          phase: 'batch-complete',
          startHeight,
          endHeight,
          outputCount: patches.length,
          nextHeight: checkpoint.nextHeight,
          elapsedMs: checkpoint.lastBatch.elapsedMs,
        }),
      );
    }

    if (checkpoint.nextHeight <= checkpoint.toHeight) {
      console.log(
        JSON.stringify({
          phase: 'paused',
          nextHeight: checkpoint.nextHeight,
          toHeight: checkpoint.toHeight,
        }),
      );
      return;
    }

    const completeCheckpoint: ScriptPubKeyBackfillCheckpoint = {
      ...checkpoint,
      completedAt: new Date().toISOString(),
      status: 'complete',
      updatedAt: new Date().toISOString(),
    };
    await metadata.setJsonValue(checkpointKey, completeCheckpoint);
    console.log(
      JSON.stringify(
        {
          phase: 'complete',
          checkpoint: completeCheckpoint,
          ...(options.withCounts
            ? { emptyCounts: await countEmptyScriptPubKeys(client, networkId, schemaTables) }
            : {}),
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
  checkpointKey: string;
  existingCheckpoint: ScriptPubKeyBackfillCheckpoint | null;
  fromHeight: number;
  metadata: RelationalMetadataStore;
  reset: boolean;
  toHeight: number;
}): Promise<ScriptPubKeyBackfillCheckpoint> {
  if (!input.reset && input.existingCheckpoint) {
    if (
      input.existingCheckpoint.fromHeight !== input.fromHeight ||
      input.existingCheckpoint.toHeight !== input.toHeight
    ) {
      throw new Error(
        `checkpoint range ${input.existingCheckpoint.fromHeight}-${input.existingCheckpoint.toHeight} does not match requested range ${input.fromHeight}-${input.toHeight}; pass --resetCheckpoint to restart`,
      );
    }
    if (input.existingCheckpoint.status === 'complete') {
      console.log('checkpoint already complete');
    }
    return input.existingCheckpoint;
  }

  const timestamp = new Date().toISOString();
  const checkpoint: ScriptPubKeyBackfillCheckpoint = {
    fromHeight: input.fromHeight,
    nextHeight: input.fromHeight,
    startedAt: timestamp,
    status: 'running',
    toHeight: input.toHeight,
    totalOutputs: 0,
    updatedAt: timestamp,
  };
  await input.metadata.setJsonValue(input.checkpointKey, checkpoint);
  return checkpoint;
}

async function ensureClickHouseScriptPubKeySchema(
  client: ReturnType<typeof createClient>,
  tables: string[],
) {
  for (const table of tables) {
    await client.command({
      query: `ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS script_pub_key String AFTER script_type`,
    });
  }

  await client.command({ query: `DROP VIEW IF EXISTS ${currentUtxosByAddressView}` });
  await client.command({
    query: `
      CREATE MATERIALIZED VIEW IF NOT EXISTS ${currentUtxosByAddressView}
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
  await client.command({
    query: `
      CREATE TABLE IF NOT EXISTS ${scriptPubKeyPatchTable}
      (
        output_key String,
        script_pub_key String
      )
      ENGINE = Memory
    `,
  });
}

async function loadScriptPubKeyPatches(input: {
  endHeight: number;
  loadConcurrency: number;
  networkId: number;
  rawBlocks: ReturnType<typeof createRawBlockStorage>;
  rawTimeoutMs: number;
  skipMissingBlocks: boolean;
  startHeight: number;
}): Promise<ScriptPubKeyPatch[]> {
  const patchBatches = await mapWithConcurrency(
    range(input.startHeight, input.endHeight),
    input.loadConcurrency,
    async (height) => {
      const snapshot = await input.rawBlocks.getPart<Record<string, unknown>>(
        input.networkId,
        height,
        rawBlockPart,
        { timeoutMs: input.rawTimeoutMs },
      );
      if (!snapshot) {
        if (input.skipMissingBlocks) {
          console.warn(`missing raw block snapshot network=${input.networkId} height=${height}`);
          return [];
        }
        throw new Error(`missing raw block snapshot network=${input.networkId} height=${height}`);
      }
      return extractScriptPubKeyPatches(snapshot, height);
    },
  );

  const patchesByOutputKey = new Map<string, ScriptPubKeyPatch>();
  for (const patch of patchBatches.flat()) {
    patchesByOutputKey.set(patch.outputKey, patch);
  }
  return [...patchesByOutputKey.values()];
}

function extractScriptPubKeyPatches(
  snapshot: Record<string, unknown>,
  expectedHeight: number,
): ScriptPubKeyPatch[] {
  const block = recordField(snapshot, 'block');
  const height = numberField(block, 'height');
  if (height !== expectedHeight) {
    throw new Error(`raw block height mismatch expected=${expectedHeight} actual=${height}`);
  }

  const patches: ScriptPubKeyPatch[] = [];
  const transactions = arrayField(block, 'tx');
  for (const tx of transactions) {
    const transaction = asRecord(tx);
    if (!transaction) {
      continue;
    }
    const txid = stringField(transaction, 'txid');
    if (!txid) {
      continue;
    }

    const outputs = arrayField(transaction, 'vout');
    for (const [outputIndex, output] of outputs.entries()) {
      const outputRecord = asRecord(output);
      const script = outputRecord ? asRecord(outputRecord.scriptPubKey) : null;
      const scriptPubKey = script ? stringField(script, 'hex') : '';
      if (!scriptPubKey) {
        continue;
      }
      patches.push({
        outputKey: `${txid}:${outputIndex}`,
        scriptPubKey,
      });
    }
  }
  return patches;
}

async function applyScriptPubKeyPatch(
  client: ReturnType<typeof createClient>,
  networkId: number,
  patches: ScriptPubKeyPatch[],
  tables: string[],
  settings: {
    mutationsSync: 0 | 1 | 2;
    statementTimeoutMs: number;
    writeMode: BackfillWriteMode;
  },
) {
  if (patches.length === 0) {
    return;
  }

  if (settings.writeMode === 'replacement') {
    await insertScriptPubKeyReplacementRows(client, networkId, patches, tables);
    return;
  }

  const outputKeys = patches.map((patch) => patch.outputKey);
  const scriptPubKeys = patches.map((patch) => patch.scriptPubKey);
  for (const table of tables) {
    await client.command({
      query: `
        ALTER TABLE ${table}
        UPDATE script_pub_key = arrayElement(
          {scriptPubKeys:Array(String)},
          indexOf({outputKeys:Array(String)}, output_key)
        )
        WHERE
          network_id = {networkId:UInt64}
          AND output_key IN ({outputKeys:Array(String)})
          AND script_pub_key != arrayElement(
            {scriptPubKeys:Array(String)},
            indexOf({outputKeys:Array(String)}, output_key)
          )
      `,
      query_params: {
        networkId,
        outputKeys,
        scriptPubKeys,
      },
      clickhouse_settings: {
        max_execution_time: toClickHouseMaxExecutionTimeSeconds(settings.statementTimeoutMs),
        mutations_sync: settings.mutationsSync,
      },
    });
  }
}

async function insertScriptPubKeyReplacementRows(
  client: ReturnType<typeof createClient>,
  networkId: number,
  patches: ScriptPubKeyPatch[],
  tables: string[],
) {
  const runId = randomUUID();
  await clearScriptPubKeyPatchTable(client);
  await client.insert({
    table: scriptPubKeyPatchTable,
    values: patches.map((patch) => ({
      output_key: `${runId}:${patch.outputKey}`,
      script_pub_key: patch.scriptPubKey,
    })),
    format: 'JSONEachRow',
  });

  try {
    for (const table of tables) {
      const columns = replacementColumnsForTable(table);
      const selectExpressions = columns.map((column) => {
        if (column === 'script_pub_key') {
          return 'patch.script_pub_key AS script_pub_key';
        }
        if (column === 'version') {
          return `source.version + ${replacementDefaultVersionIncrement} AS version`;
        }
        return `source.${column}`;
      });
      await client.command({
        query: `
          INSERT INTO ${table} (${columns.join(', ')})
          SELECT ${selectExpressions.join(', ')}
          FROM (
            SELECT ${columns.join(', ')}
            FROM ${table}
            WHERE
              network_id = {networkId:UInt64}
              AND output_key IN (
                SELECT substring(output_key, {keyPrefixLength:UInt64})
                FROM ${scriptPubKeyPatchTable}
              )
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          ) AS source
          INNER JOIN (
            SELECT
              substring(output_key, {keyPrefixLength:UInt64}) AS output_key,
              script_pub_key
            FROM ${scriptPubKeyPatchTable}
          ) AS patch
          ON source.output_key = patch.output_key
          WHERE source.script_pub_key != patch.script_pub_key
        `,
        query_params: {
          keyPrefixLength: runId.length + 2,
          networkId,
        },
      });
    }
  } finally {
    await clearScriptPubKeyPatchTable(client);
  }
}

async function clearScriptPubKeyPatchTable(client: ReturnType<typeof createClient>) {
  await client.command({
    query: `TRUNCATE TABLE IF EXISTS ${scriptPubKeyPatchTable}`,
  });
}

function replacementColumnsForTable(table: string): string[] {
  if (table === createsTable) {
    return [
      'network_id',
      'block_height',
      'block_hash',
      'block_time',
      'txid',
      'tx_index',
      'vout',
      'output_key',
      'address',
      'script_type',
      'script_pub_key',
      'value_base',
      'is_coinbase',
      'is_spendable',
      'version',
    ];
  }

  return [
    'network_id',
    'block_height',
    'block_hash',
    'block_time',
    'txid',
    'tx_index',
    'vout',
    'output_key',
    'address',
    'script_type',
    'script_pub_key',
    'value_base',
    'is_coinbase',
    'is_spendable',
    'spent_by_txid',
    'spent_in_block',
    'spent_input_index',
    'version',
  ];
}

async function listScriptPubKeyColumnStatus(
  client: ReturnType<typeof createClient>,
  tables: string[],
): Promise<Record<string, boolean>> {
  const result = await client.query({
    query: `
      SELECT table, countIf(name = 'script_pub_key') > 0 AS hasColumn
      FROM system.columns
      WHERE database = currentDatabase() AND table IN ({tables:Array(String)})
      GROUP BY table
    `,
    query_params: { tables },
    format: 'JSONEachRow',
  });
  const rows = (await result.json()) as Array<{ hasColumn: boolean | number; table: string }>;
  const status = new Map(
    rows.map((row) => [row.table, row.hasColumn === true || row.hasColumn === 1]),
  );
  return Object.fromEntries(tables.map((table) => [table, status.get(table) ?? false]));
}

async function countEmptyScriptPubKeys(
  client: ReturnType<typeof createClient>,
  networkId: number,
  tables: string[],
): Promise<Record<string, number>> {
  const result: Record<string, number> = {};
  const columnStatus = await listScriptPubKeyColumnStatus(client, tables);
  for (const table of tables) {
    if (!columnStatus[table]) {
      result[table] = -1;
      continue;
    }
    const queryResult = await client.query({
      query: `
        SELECT count() AS count
        FROM (
          SELECT script_pub_key
          FROM ${table}
          WHERE network_id = {networkId:UInt64}
          ORDER BY output_key ASC, version DESC
          LIMIT 1 BY output_key
        )
        WHERE script_pub_key = ''
      `,
      query_params: { networkId },
      format: 'JSONEachRow',
    });
    const rows = (await queryResult.json()) as Array<{ count: number | string }>;
    result[table] = Number(rows[0]?.count ?? 0);
  }
  return result;
}

function backfillTables(tables: string[], skipLegacy: boolean): string[] {
  return tables.filter((table) => !skipLegacy || table !== legacyUtxosTable);
}

function scriptPubKeyBackfillCheckpointKey(networkId: number): string {
  return `dogecoin_script_pub_key_backfill_n${networkId}`;
}

function parseMutationsSync(value: string): 0 | 1 | 2 {
  const parsed = parseNonNegativeInteger(value, 'mutationsSync');
  if (parsed !== 0 && parsed !== 1 && parsed !== 2) {
    throw new Error(`invalid mutationsSync: ${value}`);
  }
  return parsed;
}

function parseWriteMode(value: string): BackfillWriteMode {
  if (value === 'replacement' || value === 'mutation') {
    return value;
  }
  throw new Error(`invalid writeMode: ${value}`);
}

function toClickHouseMaxExecutionTimeSeconds(timeoutMs: number): number {
  return Math.max(1, Math.ceil(timeoutMs / 1000));
}

function chunkArray<T>(values: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

function recordField(value: Record<string, unknown>, field: string): Record<string, unknown> {
  const record = asRecord(value[field]);
  if (!record) {
    throw new Error(`missing raw block ${field}`);
  }
  return record;
}

function arrayField(value: Record<string, unknown>, field: string): unknown[] {
  const candidate = value[field];
  return Array.isArray(candidate) ? candidate : [];
}

function numberField(value: Record<string, unknown>, field: string): number {
  const candidate = value[field];
  if (typeof candidate !== 'number' || !Number.isFinite(candidate)) {
    throw new Error(`missing raw block ${field}`);
  }
  return candidate;
}

function stringField(value: Record<string, unknown>, field: string): string {
  const candidate = value[field];
  return typeof candidate === 'string' ? candidate.trim() : '';
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
