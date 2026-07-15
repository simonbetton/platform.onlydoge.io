import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createClient as createLibsqlClient } from '@libsql/client';
import { ApiKey, setApiKeyIsActive } from '@onlydoge/access-control';
import { type DatabaseSettings, RelationalMetadataStore } from '@onlydoge/platform';
import mysqlDriver from 'mysql2/promise';
import { Client as PostgresClient } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { DockerService } from './docker-service';
import { mysqlUrl, postgresUrl, startMysql, startPostgres } from './services';

const adapterTimeoutMs = 180_000;
let postgres: DockerService | null = null;
let mysql: DockerService | null = null;
let sqliteRoot: string | null = null;

describe.skipIf(process.env.ONLYDOGE_RUN_ADAPTER_TESTS !== '1')(
  'production metadata adapters',
  () => {
    beforeAll(async () => {
      try {
        postgres = await startPostgres();
        mysql = await startMysql();
        sqliteRoot = await mkdtemp(join(tmpdir(), 'onlydoge-adapter-sqlite-'));
      } catch (error) {
        await Promise.allSettled([postgres?.stop(), mysql?.stop()]);
        throw error;
      }
    }, adapterTimeoutMs);

    afterAll(async () => {
      await Promise.allSettled([postgres?.stop(), mysql?.stop()]);
      if (sqliteRoot) {
        await rm(sqliteRoot, { force: true, recursive: true });
      }
    }, 30_000);

    it.each([
      {
        driver: 'sqlite',
        settings: () =>
          ({
            driver: 'sqlite',
            location: `file:${join(requireValue(sqliteRoot), 'metadata.sqlite')}`,
          }) satisfies DatabaseSettings,
      },
      {
        driver: 'postgres',
        settings: () =>
          ({
            driver: 'postgres',
            location: postgresUrl(requireValue(postgres)),
          }) satisfies DatabaseSettings,
      },
      {
        driver: 'mysql',
        settings: () =>
          ({
            driver: 'mysql',
            location: mysqlUrl(requireValue(mysql)),
          }) satisfies DatabaseSettings,
      },
    ])(
      'bootstraps, persists, reconnects and performs repository CRUD with $driver',
      async ({ driver, settings }) => {
        const first = await RelationalMetadataStore.connect(settings());
        const key = ApiKey.create({ id: `key_adapter_${driver}`, role: 'admin' }).record;

        try {
          await expect(first.getJsonValue('adapter:config')).resolves.toBeNull();
          await first.setJsonValue('adapter:config', { driver, ready: true });
          await expect(first.createApiKey(key)).resolves.toMatchObject({
            id: key.id,
            role: 'admin',
          });
          await first.updateApiKey(setApiKeyIsActive(key, false));
          await expect(first.getApiKeyById(key.id)).resolves.toMatchObject({
            id: key.id,
            isActive: false,
          });
        } finally {
          await first.close();
        }

        const reconnected = await RelationalMetadataStore.connect(settings());
        try {
          await expect(reconnected.getJsonValue('adapter:config')).resolves.toEqual({
            driver,
            ready: true,
          });
          await expect(reconnected.getApiKeyById(key.id)).resolves.toMatchObject({
            id: key.id,
            isActive: false,
          });
          await reconnected.deleteApiKeys([key.id]);
          await expect(reconnected.getApiKeyById(key.id)).resolves.toBeNull();
        } finally {
          await reconnected.close();
        }
      },
      adapterTimeoutMs,
    );

    it.each(['sqlite', 'postgres', 'mysql'] as const)(
      'passes the complete migration contract matrix with %s',
      async (driver) => {
        const settings = metadataSettings(driver);

        await resetMetadataDatabase(settings);
        const fresh = await RelationalMetadataStore.connect(settings);
        await fresh.close();
        const freshStatus = await RelationalMetadataStore.migrationStatus(settings);
        expect(freshStatus.drift).toEqual([]);
        expect(freshStatus.pending).toEqual([]);
        expect(freshStatus.applied).toHaveLength(4);

        const restarted = await RelationalMetadataStore.connect(settings);
        await restarted.close();
        expect((await RelationalMetadataStore.migrationStatus(settings)).applied).toEqual(
          freshStatus.applied,
        );

        await resetMetadataDatabase(settings);
        await createUpgradeFixture(settings);
        const upgraded = await RelationalMetadataStore.connect(settings);
        try {
          await expect(upgraded.getApiKeyById('key_legacy')).resolves.toMatchObject({
            id: 'key_legacy',
            role: 'member',
          });
        } finally {
          await upgraded.close();
        }
        expect(await metadataColumnNames(settings, 'api_keys')).not.toContain('secret_key');

        await resetMetadataDatabase(settings);
        const configKey = driver === 'mysql' ? '`key`' : 'key';
        await executeRaw(
          settings,
          `CREATE TABLE app_config (${configKey} ${driver === 'mysql' ? 'VARCHAR(768)' : 'TEXT'} PRIMARY KEY, value_json TEXT NOT NULL, updated_at ${driver === 'mysql' ? 'VARCHAR(768)' : 'TEXT'} NOT NULL)`,
        );
        const resumed = await RelationalMetadataStore.connect(settings);
        await resumed.close();
        expect((await RelationalMetadataStore.migrationStatus(settings)).pending).toEqual([]);

        await resetMetadataDatabase(settings);
        const concurrent = await Promise.all([
          RelationalMetadataStore.connect(settings),
          RelationalMetadataStore.connect(settings),
        ]);
        await Promise.all(concurrent.map((store) => store.close()));
        expect((await RelationalMetadataStore.migrationStatus(settings)).applied).toHaveLength(4);

        await resetMetadataDatabase(settings);
        const heldLock = await holdMigrationLock(settings);
        try {
          await expect(RelationalMetadataStore.connect(settings)).rejects.toThrow(
            /migration lock|SQLITE_BUSY/iu,
          );
        } finally {
          await heldLock();
        }

        const recovered = await RelationalMetadataStore.connect(settings);
        await recovered.close();
        await executeRaw(
          settings,
          `UPDATE metadata_migrations SET checksum = ${driver === 'postgres' ? '$1' : '?'} WHERE version = 1`,
          ['tampered'],
        );
        const driftStatus = await RelationalMetadataStore.migrationStatus(settings);
        expect(driftStatus.drift).toContain('checksum/name drift for metadata migration 1');
        await expect(RelationalMetadataStore.connect(settings)).rejects.toThrow(
          /metadata migration drift/iu,
        );
      },
      adapterTimeoutMs,
    );
  },
);

function metadataSettings(driver: DatabaseSettings['driver']): DatabaseSettings {
  if (driver === 'sqlite') {
    return {
      driver,
      location: `file:${join(requireValue(sqliteRoot), 'migration-matrix.sqlite')}`,
    };
  }
  if (driver === 'postgres') {
    return { driver, location: postgresUrl(requireValue(postgres)) };
  }
  return { driver, location: mysqlUrl(requireValue(mysql)) };
}

async function resetMetadataDatabase(settings: DatabaseSettings): Promise<void> {
  if (settings.driver === 'sqlite') {
    await rm(settings.location.replace(/^file:/u, ''), { force: true });
    return;
  }
  for (const table of [
    'metadata_migrations',
    'active_mempool_watches',
    'core_blocks',
    'core_indexer_state',
    'app_config',
    'audit_events',
    'api_keys',
  ]) {
    await executeRaw(
      settings,
      `DROP TABLE IF EXISTS ${table}${settings.driver === 'postgres' ? ' CASCADE' : ''}`,
    );
  }
}

async function createUpgradeFixture(settings: DatabaseSettings): Promise<void> {
  const primaryKey =
    settings.driver === 'postgres'
      ? 'BIGSERIAL PRIMARY KEY'
      : settings.driver === 'mysql'
        ? 'BIGINT AUTO_INCREMENT PRIMARY KEY'
        : 'INTEGER PRIMARY KEY AUTOINCREMENT';
  const text = settings.driver === 'mysql' ? 'VARCHAR(768)' : 'TEXT';
  await executeRaw(
    settings,
    `CREATE TABLE api_keys (
      api_key_id ${primaryKey},
      id ${text} NOT NULL UNIQUE,
      secret_key ${text} NOT NULL,
      secret_key_hash ${text} NOT NULL,
      is_active ${settings.driver === 'postgres' ? 'BOOLEAN' : 'INTEGER'} NOT NULL,
      updated_at ${text} NULL,
      created_at ${text} NOT NULL
    )`,
  );
  const placeholders =
    settings.driver === 'postgres' ? '$1, $2, $3, $4, $5, $6' : '?, ?, ?, ?, ?, ?';
  await executeRaw(
    settings,
    `INSERT INTO api_keys (id, secret_key, secret_key_hash, is_active, updated_at, created_at) VALUES (${placeholders})`,
    [
      'key_legacy',
      'legacy-secret',
      'legacy-hash',
      settings.driver === 'postgres' ? true : 1,
      null,
      '2026-01-01T00:00:00.000Z',
    ],
  );
}

async function metadataColumnNames(settings: DatabaseSettings, table: string): Promise<string[]> {
  if (settings.driver === 'sqlite') {
    return queryRaw(settings, `PRAGMA table_info(${table})`).then((rows) =>
      rows.map((row) => String(row.name)),
    );
  }
  const sql =
    settings.driver === 'postgres'
      ? 'SELECT column_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = $1'
      : 'SELECT COLUMN_NAME AS column_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ?';
  return queryRaw(settings, sql, [table]).then((rows) =>
    rows.map((row) => String(row.column_name)),
  );
}

async function holdMigrationLock(settings: DatabaseSettings): Promise<() => Promise<void>> {
  if (settings.driver === 'sqlite') {
    const client = createLibsqlClient({ url: settings.location });
    const transaction = await client.transaction('write');
    return async () => {
      await transaction.rollback().catch(() => undefined);
      transaction.close();
      client.close();
    };
  }
  if (settings.driver === 'postgres') {
    const client = new PostgresClient({ connectionString: settings.location });
    await client.connect();
    await client.query('SELECT pg_advisory_lock($1)', [762_319_045]);
    return async () => {
      await client.query('SELECT pg_advisory_unlock($1)', [762_319_045]);
      await client.end();
    };
  }
  const connection = await mysqlDriver.createConnection(settings.location);
  await connection.query("SELECT GET_LOCK('onlydoge_metadata_migrate', 1)");
  return async () => {
    await connection.query("SELECT RELEASE_LOCK('onlydoge_metadata_migrate')");
    await connection.end();
  };
}

async function executeRaw(
  settings: DatabaseSettings,
  sql: string,
  args: unknown[] = [],
): Promise<void> {
  await queryRaw(settings, sql, args);
}

async function queryRaw(
  settings: DatabaseSettings,
  sql: string,
  args: unknown[] = [],
): Promise<Record<string, unknown>[]> {
  if (settings.driver === 'sqlite') {
    const client = createLibsqlClient({ url: settings.location });
    try {
      const result = await client.execute({ sql, args: args as never[] });
      return result.rows.map((row) => Object.fromEntries(Object.entries(row)));
    } finally {
      client.close();
    }
  }
  if (settings.driver === 'postgres') {
    const client = new PostgresClient({ connectionString: settings.location });
    try {
      await client.connect();
      return (await client.query(sql, args)).rows as Record<string, unknown>[];
    } finally {
      await client.end().catch(() => undefined);
    }
  }
  const connection = await mysqlDriver.createConnection(settings.location);
  try {
    const [rows] = await connection.query(sql, args);
    return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
  } finally {
    await connection.end();
  }
}

function requireValue<T>(value: T | null): T {
  if (value === null) {
    throw new Error('adapter service was not initialized');
  }
  return value;
}
