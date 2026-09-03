import { loadSettings } from '@onlydoge/platform';

import { ExternalId, maskRpcEndpointAuth, parseMode, RpcEndpoint } from '@onlydoge/shared-kernel';
import { describe, expect, it } from 'vitest';

describe('shared kernel', () => {
  it('creates and validates external ids', () => {
    const id = ExternalId.create('key');
    expect(id.value).toMatch(/^key_[A-Za-z0-9]+$/u);
    expect(() => ExternalId.parse('key_bad!', 'key')).toThrow();
  });

  it('masks RPC credentials', () => {
    const endpoint = RpcEndpoint.parse('https://user:pass@example.com/rpc');
    expect(maskRpcEndpointAuth(endpoint)).toContain('***');
    expect(maskRpcEndpointAuth(endpoint)).not.toContain('pass');
  });

  it('loads default runtime settings', () => {
    const homePlaceholder = '${' + 'HOME}';
    const settings = loadSettings({
      env: {
        ONLYDOGE_DATABASE: `sqlite://${homePlaceholder}/.onlydoge/test.sqlite.db`,
        ONLYDOGE_STORAGE: `file://${homePlaceholder}/.onlydoge/storage`,
        ONLYDOGE_WAREHOUSE: `${homePlaceholder}/.onlydoge/test.duckdb.db`,
      },
      mode: parseMode('both'),
    });

    expect(settings.mode).toBe('both');
    expect(settings.database.driver).toBe('sqlite');
    expect(settings.indexer).toMatchObject({
      coreBlockTimeoutMs: 120000,
      coreDbStatementTimeoutMs: 30000,
      coreOnlineTipDistance: 6,
      coreProcessLoadConcurrency: 8,
      coreProcessWindow: 100,
      coreProgressWatchdogMs: 180000,
      coreRawStorageTimeoutMs: 30000,
      coreSyncCompleteDistance: 6,
      syncBatchSize: 16,
      syncConcurrency: 8,
      syncRetryAttempts: 6,
      syncRetryBaseDelayMs: 500,
      syncWindow: 256,
    });
    expect(settings.storage.driver).toBe('file');
    expect(settings.warehouse.driver).toBe('duckdb');
  });

  it('loads indexer timeout and heartbeat settings from env', () => {
    const settings = loadSettings({
      env: {
        ONLYDOGE_DATABASE: 'sqlite:///tmp/onlydoge.sqlite.db',
        ONLYDOGE_STORAGE: 'file:///tmp/storage',
        ONLYDOGE_WAREHOUSE: '/tmp/warehouse.json',
        ONLYDOGE_CORE_BLOCK_TIMEOUT_MS: '55000',
        ONLYDOGE_CORE_DB_STATEMENT_TIMEOUT_MS: '7000',
        ONLYDOGE_CORE_ONLINE_TIP_DISTANCE: '3',
        ONLYDOGE_CORE_PROCESS_LOAD_CONCURRENCY: '5',
        ONLYDOGE_CORE_PROCESS_WINDOW: '24',
        ONLYDOGE_CORE_PROGRESS_WATCHDOG_MS: '65000',
        ONLYDOGE_CORE_RAW_STORAGE_TIMEOUT_MS: '15000',
        ONLYDOGE_CORE_REPROCESS_DEPTH: '9',
        ONLYDOGE_CORE_SYNC_COMPLETE_DISTANCE: '2',
        ONLYDOGE_INDEXER_SYNC_WINDOW: '16',
        ONLYDOGE_INDEXER_SYNC_CONCURRENCY: '6',
      },
      mode: parseMode('indexer'),
    });

    expect(settings.indexer).toMatchObject({
      coreBlockTimeoutMs: 55000,
      coreDbStatementTimeoutMs: 7000,
      coreOnlineTipDistance: 3,
      coreProcessLoadConcurrency: 5,
      coreProcessWindow: 24,
      coreProgressWatchdogMs: 65000,
      coreRawStorageTimeoutMs: 15000,
      coreReprocessDepth: 9,
      coreSyncCompleteDistance: 2,
      syncConcurrency: 6,
      syncWindow: 16,
    });
  });

  it('requires explicit database, storage, and warehouse env in production', () => {
    expect(() =>
      loadSettings({
        env: {
          NODE_ENV: 'production',
        },
        mode: parseMode('both'),
      }),
    ).toThrow(
      'Missing required environment variables: ONLYDOGE_DATABASE, ONLYDOGE_STORAGE, ONLYDOGE_WAREHOUSE',
    );
  });

  it('parses clickhouse database from the warehouse URL', () => {
    const settings = loadSettings({
      env: {
        ONLYDOGE_DATABASE: 'postgres://onlydoge:onlydoge@localhost:5432/onlydoge',
        ONLYDOGE_STORAGE: 'http://localhost:9000/onlydoge-raw/storage',
        ONLYDOGE_WAREHOUSE: 'http://clickhouse:8123?database=onlydoge',
        ONLYDOGE_WAREHOUSE_USER: 'default',
      },
      mode: parseMode('http'),
    });

    expect(settings.warehouse).toMatchObject({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123/',
      database: 'onlydoge',
      requestTimeoutMs: 30000,
      user: 'default',
    });
  });

  it('loads warehouse request timeout from env', () => {
    const settings = loadSettings({
      env: {
        ONLYDOGE_DATABASE: 'postgres://onlydoge:onlydoge@localhost:5432/onlydoge',
        ONLYDOGE_STORAGE: 'http://localhost:9000/onlydoge-raw/storage',
        ONLYDOGE_WAREHOUSE: 'http://clickhouse:8123?database=onlydoge',
        ONLYDOGE_WAREHOUSE_REQUEST_TIMEOUT_MS: '45000',
      },
      mode: parseMode('http'),
    });

    expect(settings.warehouse).toMatchObject({
      driver: 'clickhouse',
      requestTimeoutMs: 45000,
    });
  });

  it('builds a postgres connection string from granular database env vars', () => {
    const settings = loadSettings({
      env: {
        ONLYDOGE_DATABASE_HOST: 'db.internal',
        ONLYDOGE_DATABASE_PORT: '5433',
        ONLYDOGE_DATABASE_NAME: 'onlydoge_prod',
        ONLYDOGE_DATABASE_USER: 'onlydoge',
        ONLYDOGE_DATABASE_PASSWORD: 'p@ss word',
        ONLYDOGE_STORAGE: 'http://localhost:9000/onlydoge-raw/storage',
        ONLYDOGE_WAREHOUSE: 'http://clickhouse:8123?database=onlydoge',
      },
      mode: parseMode('http'),
    });

    expect(settings.database).toMatchObject({
      driver: 'postgres',
      location: 'postgres://onlydoge:p%40ss%20word@db.internal:5433/onlydoge_prod',
    });
  });

  it('accepts granular database env vars in production', () => {
    const settings = loadSettings({
      env: {
        NODE_ENV: 'production',
        ONLYDOGE_DATABASE_HOST: 'db.internal',
        ONLYDOGE_STORAGE: 'http://localhost:9000/onlydoge-raw/storage',
        ONLYDOGE_WAREHOUSE: 'http://clickhouse:8123?database=onlydoge',
      },
      mode: parseMode('http'),
    });

    expect(settings.database).toMatchObject({
      driver: 'postgres',
      location: 'postgres://onlydoge@db.internal:5432/onlydoge',
    });
  });

  it('loads a postgres CA certificate from pem env', () => {
    const certificate = ['-----BEGIN CERTIFICATE-----', 'MIIB', '-----END CERTIFICATE-----'].join(
      '\n',
    );

    const settings = loadSettings({
      env: {
        ONLYDOGE_DATABASE: 'postgres://onlydoge:onlydoge@localhost:5432/onlydoge',
        ONLYDOGE_DATABASE_SSLROOTCERT_PEM: certificate,
        ONLYDOGE_STORAGE: 'http://localhost:9000/onlydoge-raw/storage',
        ONLYDOGE_WAREHOUSE: 'http://clickhouse:8123?database=onlydoge',
      },
      mode: parseMode('http'),
    });

    expect(settings.database).toMatchObject({
      driver: 'postgres',
      location: 'postgres://onlydoge:onlydoge@localhost:5432/onlydoge',
      ssl: {
        ca: certificate,
        rejectUnauthorized: true,
      },
    });
  });

  it('loads a postgres CA certificate from base64 env', () => {
    const certificate = ['-----BEGIN CERTIFICATE-----', 'MIIB', '-----END CERTIFICATE-----'].join(
      '\n',
    );

    const settings = loadSettings({
      env: {
        ONLYDOGE_DATABASE: 'postgres://onlydoge:onlydoge@localhost:5432/onlydoge',
        ONLYDOGE_DATABASE_SSLROOTCERT_BASE64: Buffer.from(certificate, 'utf8').toString('base64'),
        ONLYDOGE_STORAGE: 'http://localhost:9000/onlydoge-raw/storage',
        ONLYDOGE_WAREHOUSE: 'http://clickhouse:8123?database=onlydoge',
      },
      mode: parseMode('http'),
    });

    expect(settings.database).toMatchObject({
      driver: 'postgres',
      ssl: {
        ca: certificate,
        rejectUnauthorized: true,
      },
    });
  });

  it('strips ssl query params when a postgres CA certificate env is provided', () => {
    const certificate = ['-----BEGIN CERTIFICATE-----', 'MIIB', '-----END CERTIFICATE-----'].join(
      '\n',
    );

    const settings = loadSettings({
      env: {
        ONLYDOGE_DATABASE:
          'postgres://onlydoge:onlydoge@localhost:5432/onlydoge?sslmode=verify-full&sslrootcert=/tmp/ca.crt',
        ONLYDOGE_DATABASE_SSLROOTCERT_PEM: certificate,
        ONLYDOGE_STORAGE: 'http://localhost:9000/onlydoge-raw/storage',
        ONLYDOGE_WAREHOUSE: 'http://clickhouse:8123?database=onlydoge',
      },
      mode: parseMode('http'),
    });

    expect(settings.database).toMatchObject({
      driver: 'postgres',
      location: 'postgres://onlydoge:onlydoge@localhost:5432/onlydoge',
      ssl: {
        ca: certificate,
        rejectUnauthorized: true,
      },
    });
  });
});
