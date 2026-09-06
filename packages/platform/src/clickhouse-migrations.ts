import { createHash } from 'node:crypto';

import { createClient } from '@clickhouse/client';

import type { SchemaLockPort } from './schema-lock';
import type { WarehouseSettings } from './settings';
import { clickHouseClientOptions } from './warehouse-query';

const ledgerTable = 'onlydoge_schema_migrations';
const schemaLockName = 'clickhouse-schema';

type ClickHouseClient = ReturnType<typeof createClient>;

export interface ClickHouseMigration {
  /**
   * Cheap re-check run on every boot for migrations the ledger already marks
   * completed. Must stay O(metadata) — it runs from every API and indexer
   * process start. Defaults to `verify` when omitted.
   */
  check?(context: ClickHouseMigrationContext): Promise<void>;
  checksum: string;
  name: string;
  source: string;
  up(context: ClickHouseMigrationContext): Promise<void>;
  /** Full verification, run once right after `up` succeeds. May scan data. */
  verify(context: ClickHouseMigrationContext): Promise<void>;
  version: number;
}

export interface ClickHouseMigrationRecord {
  checksum: string;
  completedAt: string | null;
  name: string;
  startedAt: string;
  state: 'completed' | 'started';
  version: number;
}

export interface ClickHouseMigrationContext {
  client: ClickHouseClient;
  step(name: string, work: () => Promise<void>): Promise<void>;
}

export interface ClickHouseMigrationOptions {
  afterStep?: (input: { migration: number; step: string }) => Promise<void> | void;
  migrations?: ClickHouseMigration[];
}

export function clickHouseMigrations(): ClickHouseMigration[] {
  return validateMigrations([
    migration(
      1,
      'canonical_schema',
      canonicalSchemaSource,
      async ({ client, step }) => {
        for (const [index, statement] of splitSqlStatements(canonicalSchemaSource).entries()) {
          await step(`statement-${index + 1}`, () =>
            client.command({ query: statement }).then(noop),
          );
        }
      },
      verifyCanonicalSchema,
    ),
    migration(
      2,
      'address_read_models_backfill',
      addressReadModelBackfillSource,
      backfillReadModels,
      verifyReadModels,
      checkReadModelsPopulated,
    ),
    migration(
      3,
      'transaction_refs_table',
      transactionRefsTableSource,
      async ({ client, step }) => {
        await step('create-transaction-refs-table', () =>
          client.command({ query: transactionRefsTableSource }).then(noop),
        );
      },
      verifyTransactionRefsTable,
    ),
  ]);
}

export async function ensureClickHouseDatabase(settings: WarehouseSettings): Promise<void> {
  const database = assertIdentifier(settings.database ?? 'default');
  const client = createClient({
    ...clickHouseClientOptions(
      { ...settings, database: 'default' },
      settings.requestTimeoutMs ?? 30_000,
    ),
    database: 'default',
  });
  try {
    await client.command({ query: `CREATE DATABASE IF NOT EXISTS ${database}` });
  } finally {
    await client.close();
  }
}

export async function runClickHouseMigrations(
  settings: WarehouseSettings,
  lock: SchemaLockPort,
  options: ClickHouseMigrationOptions = {},
): Promise<ClickHouseMigrationRecord[]> {
  await ensureClickHouseDatabase(settings);
  const client = createClient(
    clickHouseClientOptions(settings, settings.requestTimeoutMs ?? 30_000),
  );
  try {
    return await lock.withSchemaLock(schemaLockName, async () => {
      await createLedger(client);
      const migrations = validateMigrations(options.migrations ?? clickHouseMigrations());
      await assertLedgerChecksums(client, migrations);
      for (const item of migrations) {
        const current = (await readMigrationRecords(client)).find(
          (record) => record.version === item.version,
        );
        if (current?.state === 'completed') {
          await (item.check ?? item.verify)(migrationContext(client, item.version, options));
          continue;
        }
        await writeLedger(client, item, 'started');
        const context = migrationContext(client, item.version, options);
        await item.up(context);
        await item.verify(context);
        await writeLedger(client, item, 'completed');
      }
      return readMigrationRecords(client);
    });
  } finally {
    await client.close();
  }
}

export async function clickHouseMigrationStatus(
  settings: WarehouseSettings,
): Promise<ClickHouseMigrationRecord[]> {
  await ensureClickHouseDatabase(settings);
  const client = createClient(
    clickHouseClientOptions(settings, settings.requestTimeoutMs ?? 30_000),
  );
  try {
    await createLedger(client);
    const records = await readMigrationRecords(client);
    await assertLedgerChecksums(client, clickHouseMigrations());
    return records;
  } finally {
    await client.close();
  }
}

function migration(
  version: number,
  name: string,
  source: string,
  up: ClickHouseMigration['up'],
  verify: ClickHouseMigration['verify'],
  check?: ClickHouseMigration['check'],
): ClickHouseMigration {
  return {
    version,
    name,
    source,
    checksum: createHash('sha256').update(source).digest('hex'),
    up,
    verify,
    ...(check ? { check } : {}),
  };
}

function validateMigrations(migrations: ClickHouseMigration[]): ClickHouseMigration[] {
  const versions = new Set<number>();
  let previous = 0;
  for (const item of migrations) {
    if (!Number.isInteger(item.version) || item.version <= previous || versions.has(item.version)) {
      throw new Error('ClickHouse migrations must have unique, strictly increasing versions');
    }
    if (item.checksum !== createHash('sha256').update(item.source).digest('hex')) {
      throw new Error(`ClickHouse migration ${item.version} checksum does not match its source`);
    }
    versions.add(item.version);
    previous = item.version;
  }
  return migrations;
}

function migrationContext(
  client: ClickHouseClient,
  version: number,
  options: ClickHouseMigrationOptions,
): ClickHouseMigrationContext {
  return {
    client,
    async step(name, work) {
      await work();
      await options.afterStep?.({ migration: version, step: name });
    },
  };
}

async function createLedger(client: ClickHouseClient): Promise<void> {
  await client.command({
    query: `
      CREATE TABLE IF NOT EXISTS ${ledgerTable}
      (
        version UInt32,
        name String,
        checksum FixedString(64),
        state Enum8('started' = 1, 'completed' = 2),
        started_at DateTime64(3, 'UTC'),
        completed_at Nullable(DateTime64(3, 'UTC')),
        sequence UInt64
      )
      ENGINE = ReplacingMergeTree(sequence)
      ORDER BY version
    `,
  });
}

async function readMigrationRecords(
  client: ClickHouseClient,
): Promise<ClickHouseMigrationRecord[]> {
  const result = await client.query({
    query: `
      SELECT
        version,
        argMax(name, sequence) AS name,
        argMax(checksum, sequence) AS checksum,
        argMax(state, sequence) AS state,
        argMax(started_at, sequence) AS startedAt,
        argMax(completed_at, sequence) AS completedAt
      FROM ${ledgerTable}
      GROUP BY version
      ORDER BY version
    `,
    format: 'JSONEachRow',
  });
  const rows = (await result.json<{
    checksum: string;
    completedAt: string | null;
    name: string;
    startedAt: string;
    state: 'completed' | 'started';
    version: number | string;
  }>()) as Array<{
    checksum: string;
    completedAt: string | null;
    name: string;
    startedAt: string;
    state: 'completed' | 'started';
    version: number | string;
  }>;
  return rows.map((row) => ({ ...row, version: Number(row.version) }));
}

async function assertLedgerChecksums(
  client: ClickHouseClient,
  migrations: ClickHouseMigration[],
): Promise<void> {
  const expected = new Map(migrations.map((item) => [item.version, item]));
  for (const record of await readMigrationRecords(client)) {
    const item = expected.get(record.version);
    if (!item) {
      throw new Error(`ClickHouse migration ${record.version} is applied but missing from code`);
    }
    if (item.name !== record.name || item.checksum !== record.checksum) {
      throw new Error(`ClickHouse migration checksum drift at version ${record.version}`);
    }
  }
}

async function writeLedger(
  client: ClickHouseClient,
  item: ClickHouseMigration,
  state: 'completed' | 'started',
): Promise<void> {
  const previous = (await readMigrationRecords(client)).find(
    (record) => record.version === item.version,
  );
  await client.insert({
    table: ledgerTable,
    format: 'JSONEachRow',
    values: [
      {
        version: item.version,
        name: item.name,
        checksum: item.checksum,
        state,
        started_at: previous?.startedAt ?? nowClickHouse(),
        completed_at: state === 'completed' ? nowClickHouse() : null,
        sequence: Date.now() * 1_000 + Math.floor(Math.random() * 1_000),
      },
    ],
  });
}

async function verifyCanonicalSchema({ client }: ClickHouseMigrationContext): Promise<void> {
  const result = await client.query({
    query: `
      SELECT name, engine, sorting_key AS sortingKey
      FROM system.tables
      WHERE database = currentDatabase() AND name IN ({names:Array(String)})
      ORDER BY name
    `,
    query_params: { names: schemaManifest.map((entry) => entry.name) },
    format: 'JSONEachRow',
  });
  const rows = (await result.json<{
    engine: string;
    name: string;
    sortingKey: string;
  }>()) as Array<{ engine: string; name: string; sortingKey: string }>;
  for (const expected of schemaManifest) {
    const actual = rows.find((row) => row.name === expected.name);
    if (
      !actual ||
      actual.engine !== expected.engine ||
      normalizeExpression(actual.sortingKey) !== normalizeExpression(expected.sortingKey)
    ) {
      throw new Error(`ClickHouse schema verification failed for ${expected.name}`);
    }
  }
}

/**
 * `LEFT ANTI JOIN` only runs with the `hash` / `grace_hash` algorithms, and a
 * plain hash join loads the whole right table into RAM. `grace_hash` spills
 * buckets to disk once the in-memory table passes `max_bytes_in_join`, which
 * keeps the full-table read-model joins bounded regardless of table size.
 */
const boundedAntiJoinSettings = {
  join_algorithm: 'grace_hash',
  max_bytes_in_join: '1073741824',
  max_bytes_before_external_group_by: '536870912',
  max_bytes_before_external_sort: '536870912',
} as const;

const readModelPairs = [
  {
    source: 'dogecoin_utxo_outputs_current_v1',
    target: 'dogecoin_utxo_outputs_current_by_address_v1',
    keys: ['output_key', 'version'],
  },
  {
    source: 'dogecoin_address_movements_v1',
    target: 'dogecoin_address_movements_by_address_v1',
    keys: ['movement_id'],
  },
] as const;

async function backfillReadModels({ client, step }: ClickHouseMigrationContext): Promise<void> {
  await step('backfill-current-utxos-by-address', () =>
    client
      .command({ query: currentUtxoBackfill, clickhouse_settings: boundedAntiJoinSettings })
      .then(noop),
  );
  await step('backfill-movements-by-address', () =>
    client
      .command({ query: movementBackfill, clickhouse_settings: boundedAntiJoinSettings })
      .then(noop),
  );
}

async function verifyReadModels({ client }: ClickHouseMigrationContext): Promise<void> {
  for (const pair of readModelPairs) {
    await verifyKeyCoverage(client, pair.source, pair.target, [...pair.keys]);
  }
}

/**
 * Steady-state boot check. The full anti-join in `verifyReadModels` reads both
 * tables end to end (hundreds of GB on a synced Dogecoin warehouse) and was the
 * single largest memory consumer on the server, re-run from every API and
 * indexer start. Once the backfill is ledgered, the materialized views keep the
 * read models in lock-step with their sources, so a boot only needs to catch
 * the failure mode the backfill exists for: a populated source with an empty
 * read model. `count()` on MergeTree is answered from part metadata.
 */
async function checkReadModelsPopulated({ client }: ClickHouseMigrationContext): Promise<void> {
  for (const pair of readModelPairs) {
    const result = await client.query({
      query: `
        SELECT
          (SELECT count() FROM ${pair.source}) AS sourceRows,
          (SELECT count() FROM ${pair.target}) AS targetRows
      `,
      format: 'JSONEachRow',
    });
    const rows = (await result.json<{
      sourceRows: number | string;
      targetRows: number | string;
    }>()) as Array<{ sourceRows: number | string; targetRows: number | string }>;
    const [row] = rows;
    if (Number(row?.sourceRows ?? 0) > 0 && Number(row?.targetRows ?? 0) === 0) {
      throw new Error(
        `ClickHouse read model ${pair.target} is empty while ${pair.source} has rows`,
      );
    }
  }
}

async function verifyKeyCoverage(
  client: ClickHouseClient,
  source: string,
  target: string,
  keys: string[],
): Promise<void> {
  const predicate = keys.map((key) => `source.${key} = target.${key}`).join(' AND ');
  const result = await client.query({
    query: `
      SELECT count() AS missing
      FROM ${source} AS source
      LEFT ANTI JOIN ${target} AS target ON ${predicate}
    `,
    format: 'JSONEachRow',
    clickhouse_settings: boundedAntiJoinSettings,
  });
  const rows = (await result.json<{ missing: number | string }>()) as Array<{
    missing: number | string;
  }>;
  const [row] = rows;
  if (Number(row?.missing ?? 0) !== 0) {
    throw new Error(`ClickHouse migration verification found missing rows in ${target}`);
  }
}

function splitSqlStatements(source: string): string[] {
  return source
    .split(';')
    .map((statement) => statement.trim())
    .filter(Boolean);
}

function normalizeExpression(value: string): string {
  return value.replaceAll(/[()`\s]/gu, '');
}

function assertIdentifier(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/u.test(value)) {
    throw new Error(`invalid ClickHouse identifier: ${value}`);
  }
  return value;
}

function nowClickHouse(): string {
  return new Date().toISOString().replace('T', ' ').replace('Z', '');
}

function noop(): void {}

const schemaManifest = [
  {
    name: 'analytics_balances_current_v1',
    engine: 'ReplacingMergeTree',
    sortingKey: 'asset_address, balance_i256, address',
  },
  {
    name: 'analytics_transactions_v1',
    engine: 'ReplacingMergeTree',
    sortingKey: 'block_time, block_height, tx_index, txid',
  },
  {
    name: 'dogecoin_address_movements_by_address_v1',
    engine: 'MergeTree',
    sortingKey: 'address, block_height, tx_index, entry_index, movement_id',
  },
  {
    name: 'dogecoin_address_movements_by_address_v1_mv',
    engine: 'MaterializedView',
    sortingKey: '',
  },
  { name: 'dogecoin_address_movements_v1', engine: 'MergeTree', sortingKey: 'movement_id' },
  {
    name: 'dogecoin_applied_blocks_v1',
    engine: 'MergeTree',
    sortingKey: 'block_height, block_hash',
  },
  {
    name: 'dogecoin_balances_current_v1',
    engine: 'ReplacingMergeTree',
    sortingKey: 'address, asset_address',
  },
  {
    name: 'dogecoin_core_processed_blocks_v1',
    engine: 'ReplacingMergeTree',
    sortingKey: 'block_height',
  },
  { name: 'dogecoin_core_utxo_creates_v1', engine: 'ReplacingMergeTree', sortingKey: 'output_key' },
  {
    name: 'dogecoin_core_utxo_spends_v1',
    engine: 'ReplacingMergeTree',
    sortingKey: 'spent_output_key',
  },
  {
    name: 'dogecoin_utxo_outputs_current_by_address_v1',
    engine: 'ReplacingMergeTree',
    sortingKey: 'address, output_key',
  },
  {
    name: 'dogecoin_utxo_outputs_current_by_address_v1_mv',
    engine: 'MaterializedView',
    sortingKey: '',
  },
  {
    name: 'dogecoin_utxo_outputs_current_v1',
    engine: 'ReplacingMergeTree',
    sortingKey: 'output_key',
  },
  { name: 'mempool_samples_v1', engine: 'MergeTree', sortingKey: 'sampled_at, txid' },
] as const;

const currentUtxoBackfill = `
  INSERT INTO dogecoin_utxo_outputs_current_by_address_v1
  SELECT source.*
  FROM dogecoin_utxo_outputs_current_v1 AS source
  LEFT ANTI JOIN dogecoin_utxo_outputs_current_by_address_v1 AS target
    ON source.output_key = target.output_key AND source.version = target.version
`;

const movementBackfill = `
  INSERT INTO dogecoin_address_movements_by_address_v1 (
    movement_id, block_height, block_hash, block_time, txid, tx_index, entry_index,
    address, asset_address, direction, amount_base, output_key, derivation_method
  )
  SELECT
    source.movement_id, source.block_height, source.block_hash, source.block_time,
    source.txid, source.tx_index, source.entry_index, source.address, source.asset_address,
    source.direction, source.amount_base, source.output_key, source.derivation_method
  FROM dogecoin_address_movements_v1 AS source
  LEFT ANTI JOIN dogecoin_address_movements_by_address_v1 AS target
    ON source.movement_id = target.movement_id
`;

const addressReadModelBackfillSource = `${currentUtxoBackfill};\n${movementBackfill};`;

const transactionRefsTableSource = `
CREATE TABLE IF NOT EXISTS dogecoin_transaction_refs_v1
(
  txid String,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  tx_index UInt64,
  source Enum8('raw_sync' = 1, 'core_process' = 2),
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY txid
SETTINGS old_parts_lifetime = 0;
`;

async function verifyTransactionRefsTable({ client }: ClickHouseMigrationContext): Promise<void> {
  const result = await client.query({
    query: `
      SELECT name, engine, sorting_key AS sortingKey
      FROM system.tables
      WHERE database = currentDatabase() AND name = 'dogecoin_transaction_refs_v1'
      LIMIT 1
    `,
    format: 'JSONEachRow',
  });
  const rows = (await result.json<{
    engine: string;
    name: string;
    sortingKey: string;
  }>()) as Array<{
    engine: string;
    name: string;
    sortingKey: string;
  }>;
  const [row] = rows;
  if (row?.engine !== 'ReplacingMergeTree' || normalizeExpression(row.sortingKey) !== 'txid') {
    throw new Error('ClickHouse schema verification failed for dogecoin_transaction_refs_v1');
  }
}

const canonicalSchemaSource = `
CREATE TABLE IF NOT EXISTS dogecoin_utxo_outputs_current_v1
(
  block_height UInt64, block_hash String, block_time UInt64, txid String, tx_index UInt64,
  vout UInt64, output_key String, address String, script_type String, value_base String,
  is_coinbase UInt8, is_spendable UInt8, spent_by_txid Nullable(String),
  spent_in_block Nullable(UInt64), spent_input_index Nullable(UInt64), version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY output_key
SETTINGS old_parts_lifetime = 0;

CREATE TABLE IF NOT EXISTS dogecoin_utxo_outputs_current_by_address_v1
(
  block_height UInt64, block_hash String, block_time UInt64, txid String, tx_index UInt64,
  vout UInt64, output_key String, address String, script_type String, value_base String,
  is_coinbase UInt8, is_spendable UInt8, spent_by_txid Nullable(String),
  spent_in_block Nullable(UInt64), spent_input_index Nullable(UInt64), version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (address, output_key)
SETTINGS old_parts_lifetime = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS dogecoin_utxo_outputs_current_by_address_v1_mv
TO dogecoin_utxo_outputs_current_by_address_v1 AS
SELECT * FROM dogecoin_utxo_outputs_current_v1;

CREATE TABLE IF NOT EXISTS dogecoin_address_movements_v1
(
  movement_id String, block_height UInt64, block_hash String, block_time UInt64, txid String,
  tx_index UInt64, entry_index UInt64, address String, asset_address String, direction String,
  amount_base String, output_key Nullable(String), derivation_method String
)
ENGINE = MergeTree
ORDER BY movement_id;

CREATE TABLE IF NOT EXISTS dogecoin_address_movements_by_address_v1
(
  movement_id String, block_height UInt64, block_hash String, block_time UInt64, txid String,
  tx_index UInt64, entry_index UInt64, address String, asset_address String, direction String,
  amount_base String, amount_base_i256 Int256 MATERIALIZED toInt256(amount_base),
  output_key Nullable(String), derivation_method String
)
ENGINE = MergeTree
ORDER BY (address, block_height, tx_index, entry_index, movement_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS dogecoin_address_movements_by_address_v1_mv
TO dogecoin_address_movements_by_address_v1 AS
SELECT movement_id, block_height, block_hash, block_time, txid, tx_index, entry_index,
  address, asset_address, direction, amount_base, output_key, derivation_method
FROM dogecoin_address_movements_v1;

CREATE TABLE IF NOT EXISTS dogecoin_balances_current_v1
(
  address String, asset_address String, balance String, as_of_block_height UInt64, version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (address, asset_address);

CREATE TABLE IF NOT EXISTS dogecoin_applied_blocks_v1
(
  block_height UInt64, block_hash String
)
ENGINE = MergeTree
ORDER BY (block_height, block_hash);

CREATE TABLE IF NOT EXISTS dogecoin_core_utxo_creates_v1
(
  block_height UInt64, block_hash String, block_time UInt64, txid String, tx_index UInt64,
  vout UInt64, output_key String, address String, script_type String, value_base String,
  is_coinbase UInt8, is_spendable UInt8, version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY output_key;

ALTER TABLE dogecoin_core_utxo_creates_v1
ADD INDEX IF NOT EXISTS core_utxo_creates_address_idx address TYPE bloom_filter(0.01) GRANULARITY 4;

CREATE TABLE IF NOT EXISTS dogecoin_core_utxo_spends_v1
(
  spent_output_key String, spent_by_txid String, spent_in_block UInt64,
  spent_input_index UInt64, version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY spent_output_key;

CREATE TABLE IF NOT EXISTS dogecoin_core_processed_blocks_v1
(
  block_height UInt64, block_hash String, block_time UInt64, tx_count UInt64, version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY block_height;

CREATE TABLE IF NOT EXISTS analytics_transactions_v1
(
  block_height UInt64, block_hash String, block_time UInt64, txid String, tx_index UInt64,
  is_coinbase UInt8, input_count UInt64, output_count UInt64, total_input_base String,
  gross_output_base String, fee_base Nullable(String),
  total_input_base_i256 Int256 MATERIALIZED toInt256(total_input_base),
  gross_output_base_i256 Int256 MATERIALIZED toInt256(gross_output_base),
  fee_base_i256 Nullable(Int256) MATERIALIZED if(isNull(fee_base), NULL, toInt256(fee_base)),
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (block_time, block_height, tx_index, txid)
SETTINGS old_parts_lifetime = 0;

CREATE TABLE IF NOT EXISTS analytics_balances_current_v1
(
  address String, asset_address String, balance String,
  balance_i256 Int256 MATERIALIZED toInt256(balance),
  as_of_block_height UInt64, version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (asset_address, balance_i256, address)
SETTINGS old_parts_lifetime = 0;

CREATE TABLE IF NOT EXISTS mempool_samples_v1
(
  sampled_at DateTime, txid String, entry_time Nullable(UInt64), height Nullable(UInt64),
  size_bytes Nullable(UInt64), fee_base Nullable(String),
  fee_rate_base_per_kilobyte Nullable(String), raw_json String
)
ENGINE = MergeTree
ORDER BY (sampled_at, txid)
TTL sampled_at + INTERVAL 1 HOUR;
`;
