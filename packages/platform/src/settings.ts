import { URL } from 'node:url';

import type { CoreDogecoinIndexerSettings } from '@onlydoge/indexing-pipeline';
import {
  type ChainFamily,
  expandHomePath,
  type Mode,
  type PrimaryId,
  parseMode,
} from '@onlydoge/shared-kernel';

export interface DatabaseSettings {
  driver: 'sqlite' | 'postgres' | 'mysql';
  location: string;
  ssl?: {
    ca?: string;
    rejectUnauthorized?: boolean;
  };
}

export interface StorageSettings {
  driver: 'file' | 's3';
  location: string;
  accessKeyId?: string;
  secretAccessKey?: string;
}

export interface WarehouseSettings {
  driver: 'duckdb' | 'clickhouse';
  location: string;
  database?: string;
  requestTimeoutMs?: number;
  user?: string;
  password?: string;
}

export interface IndexerSettings extends CoreDogecoinIndexerSettings {}

export interface AppSettings {
  mode: Mode;
  isIndexer: boolean;
  isHttp: boolean;
  ip: string;
  port: number;
  database: DatabaseSettings;
  indexer: IndexerSettings;
  storage: StorageSettings;
  warehouse: WarehouseSettings;
}

export function loadSettings(input?: {
  env?: NodeJS.ProcessEnv;
  ip?: string;
  mode?: string;
  port?: number;
}): AppSettings {
  const homePlaceholder = '${' + 'HOME}';
  const env = resolveSettingsEnv(input);
  assertRequiredEnvironment(env);
  const mode = parseMode(resolveSettingsValue(input?.mode, env.ONLYDOGE_MODE));
  const locations = resolveStorageLocations(env, homePlaceholder);

  return {
    mode,
    isIndexer: isIndexerMode(mode),
    isHttp: isHttpMode(mode),
    ip: resolveSettingsValue(input?.ip, env.ONLYDOGE_IP, '127.0.0.1'),
    port: resolveSettingsPort(input, env),
    database: parseDatabaseSettings(locations.database, env),
    indexer: parseIndexerSettings(env),
    storage: parseStorageSettings(locations.storage, env),
    warehouse: parseWarehouseSettings(locations.warehouse, env),
  };
}

function isIndexerMode(mode: Mode): boolean {
  return mode === 'both' || mode === 'indexer';
}

function isHttpMode(mode: Mode): boolean {
  return mode === 'both' || mode === 'http';
}

function resolveSettingsPort(input: { port?: number } | undefined, env: NodeJS.ProcessEnv): number {
  return input?.port ?? Number(resolveSettingsValue(env.ONLYDOGE_PORT, undefined, '2277'));
}

function resolveSettingsEnv(input?: { env?: NodeJS.ProcessEnv }): NodeJS.ProcessEnv {
  return input?.env ?? process.env;
}

function resolveSettingsValue(
  primary: string | undefined,
  secondary: string | undefined,
  fallback = '',
): string {
  return primary ?? secondary ?? fallback;
}

function resolveStorageLocations(
  env: NodeJS.ProcessEnv,
  homePlaceholder: string,
): {
  database: string;
  storage: string;
  warehouse: string;
} {
  return {
    database: expandHomePath(
      resolveSettingsValue(
        resolveDatabaseLocation(env),
        undefined,
        `sqlite://${homePlaceholder}/.onlydoge/onlydoge.sqlite.db`,
      ),
    ),
    storage: expandHomePath(
      resolveSettingsValue(
        env.ONLYDOGE_STORAGE,
        undefined,
        `file://${homePlaceholder}/.onlydoge/storage`,
      ),
    ),
    warehouse: expandHomePath(
      resolveSettingsValue(
        env.ONLYDOGE_WAREHOUSE,
        undefined,
        `${homePlaceholder}/.onlydoge/onlydoge.duckdb.db`,
      ),
    ),
  };
}

function assertRequiredEnvironment(env: NodeJS.ProcessEnv): void {
  if (env.NODE_ENV !== 'production') {
    return;
  }

  const missing = missingRequiredEnvironmentKeys(env);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

function missingRequiredEnvironmentKeys(env: NodeJS.ProcessEnv): string[] {
  const missing: string[] = [];
  if (!hasDatabaseConfiguration(env)) {
    missing.push('ONLYDOGE_DATABASE');
  }
  if (!env.ONLYDOGE_STORAGE) {
    missing.push('ONLYDOGE_STORAGE');
  }
  if (!env.ONLYDOGE_WAREHOUSE) {
    missing.push('ONLYDOGE_WAREHOUSE');
  }

  return missing;
}

function hasDatabaseConfiguration(env: NodeJS.ProcessEnv): boolean {
  return Boolean(resolveDatabaseLocation(env));
}

function resolveDatabaseLocation(env: NodeJS.ProcessEnv): string | undefined {
  if (env.ONLYDOGE_DATABASE) {
    return env.ONLYDOGE_DATABASE;
  }

  if (!env.ONLYDOGE_DATABASE_HOST) {
    return undefined;
  }

  return buildPostgresDatabaseLocation(env);
}

function buildPostgresDatabaseLocation(env: NodeJS.ProcessEnv): string {
  const user = resolveSettingsValue(env.ONLYDOGE_DATABASE_USER, undefined, 'onlydoge');
  const password = env.ONLYDOGE_DATABASE_PASSWORD ?? '';
  const port = resolveSettingsValue(env.ONLYDOGE_DATABASE_PORT, undefined, '5432');
  const database = resolveSettingsValue(env.ONLYDOGE_DATABASE_NAME, undefined, 'onlydoge');
  const credentials = encodeDatabaseCredentials(user, password);

  return `postgres://${credentials}@${env.ONLYDOGE_DATABASE_HOST}:${port}/${database}`;
}

function encodeDatabaseCredentials(user: string, password: string): string {
  if (!password) {
    return encodeURIComponent(user);
  }

  return `${encodeURIComponent(user)}:${encodeURIComponent(password)}`;
}

function parseDatabaseSettings(location: string, env: NodeJS.ProcessEnv): DatabaseSettings {
  if (location.startsWith('sqlite://')) {
    return sqliteDatabaseSettings(location);
  }

  if (isPostgresLocation(location)) {
    return postgresDatabaseSettings(location, env);
  }

  if (location.startsWith('mysql://')) {
    return { driver: 'mysql', location };
  }

  throw new Error(`Unsupported database configuration: ${location}`);
}

function sqliteDatabaseSettings(location: string): DatabaseSettings {
  return {
    driver: 'sqlite',
    location: `file:${new URL(location).pathname}`,
  };
}

function isPostgresLocation(location: string): boolean {
  return location.startsWith('postgres://') || location.startsWith('postgresql://');
}

function postgresDatabaseSettings(location: string, env: NodeJS.ProcessEnv): DatabaseSettings {
  const ssl = parseDatabaseSslSettings(env);
  if (!ssl) {
    return { driver: 'postgres', location };
  }

  return {
    driver: 'postgres',
    location: stripPostgresSslQueryParams(location),
    ssl,
  };
}

function decodeMaybeBase64(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    return trimmed;
  }

  if (trimmed.includes('-----BEGIN CERTIFICATE-----')) {
    return trimmed;
  }

  return decodeBase64Certificate(trimmed) ?? trimmed;
}

function decodeBase64Certificate(value: string): string | null {
  try {
    const decoded = Buffer.from(value, 'base64').toString('utf8');
    return decoded.includes('-----BEGIN CERTIFICATE-----') ? decoded : null;
  } catch {
    return null;
  }
}

function parseDatabaseSslSettings(env: NodeJS.ProcessEnv): DatabaseSettings['ssl'] | undefined {
  const caValue = env.ONLYDOGE_DATABASE_SSLROOTCERT_PEM ?? env.ONLYDOGE_DATABASE_SSLROOTCERT_BASE64;
  if (!caValue) {
    return undefined;
  }

  return {
    ca: decodeMaybeBase64(caValue),
    rejectUnauthorized: true,
  };
}

function stripPostgresSslQueryParams(location: string): string {
  const url = new URL(location);
  for (const parameter of ['sslcert', 'sslkey', 'sslmode', 'sslrootcert']) {
    url.searchParams.delete(parameter);
  }
  return url.toString();
}

function parseStorageSettings(location: string, env: NodeJS.ProcessEnv): StorageSettings {
  if (location.startsWith('file://')) {
    return {
      driver: 'file',
      location: new URL(location).pathname,
    };
  }

  return {
    driver: 's3',
    location,
    ...(env.ONLYDOGE_S3_ACCESS_KEY_ID ? { accessKeyId: env.ONLYDOGE_S3_ACCESS_KEY_ID } : {}),
    ...(env.ONLYDOGE_S3_SECRET_ACCESS_KEY
      ? { secretAccessKey: env.ONLYDOGE_S3_SECRET_ACCESS_KEY }
      : {}),
  };
}

function parseWarehouseSettings(location: string, env: NodeJS.ProcessEnv): WarehouseSettings {
  if (location.startsWith('http://') || location.startsWith('https://')) {
    return parseClickHouseWarehouseSettings(location, env);
  }

  return {
    driver: 'duckdb',
    location: location.replace(/^file:/u, ''),
  };
}

function parseClickHouseWarehouseSettings(
  location: string,
  env: NodeJS.ProcessEnv,
): WarehouseSettings {
  const url = new URL(location);
  const database = url.searchParams.get('database');
  url.searchParams.delete('database');

  const settings: WarehouseSettings = {
    driver: 'clickhouse',
    location: url.toString(),
    requestTimeoutMs: parsePositiveInteger(env.ONLYDOGE_WAREHOUSE_REQUEST_TIMEOUT_MS, 30_000),
  };
  applyClickHouseDatabase(settings, database);
  applyClickHouseCredentials(settings, env);

  return settings;
}

function applyClickHouseDatabase(settings: WarehouseSettings, database: string | null): void {
  if (database) {
    settings.database = database;
  }
}

function applyClickHouseCredentials(settings: WarehouseSettings, env: NodeJS.ProcessEnv): void {
  if (env.ONLYDOGE_WAREHOUSE_USER) {
    settings.user = env.ONLYDOGE_WAREHOUSE_USER;
  }
  if (env.ONLYDOGE_WAREHOUSE_PASSWORD) {
    settings.password = env.ONLYDOGE_WAREHOUSE_PASSWORD;
  }
}

function parseIndexerSettings(env: NodeJS.ProcessEnv): IndexerSettings {
  return {
    coreBlockTimeoutMs: parsePositiveInteger(env.ONLYDOGE_CORE_BLOCK_TIMEOUT_MS, 120_000),
    coreDbStatementTimeoutMs: parsePositiveInteger(
      env.ONLYDOGE_CORE_DB_STATEMENT_TIMEOUT_MS,
      30_000,
    ),
    coreOnlineTipDistance: parsePositiveInteger(env.ONLYDOGE_CORE_ONLINE_TIP_DISTANCE, 6),
    coreProcessLoadConcurrency: parsePositiveInteger(env.ONLYDOGE_CORE_PROCESS_LOAD_CONCURRENCY, 8),
    coreProcessWindow: parsePositiveInteger(env.ONLYDOGE_CORE_PROCESS_WINDOW, 100),
    coreProgressWatchdogMs: parsePositiveInteger(env.ONLYDOGE_CORE_PROGRESS_WATCHDOG_MS, 180_000),
    coreRawStorageTimeoutMs: parsePositiveInteger(env.ONLYDOGE_CORE_RAW_STORAGE_TIMEOUT_MS, 30_000),
    coreSyncCompleteDistance: parsePositiveInteger(env.ONLYDOGE_CORE_SYNC_COMPLETE_DISTANCE, 6),
    syncWindow: parsePositiveInteger(env.ONLYDOGE_INDEXER_SYNC_WINDOW, 32),
    syncConcurrency: parsePositiveInteger(env.ONLYDOGE_INDEXER_SYNC_CONCURRENCY, 4),
  };
}

function parsePositiveInteger(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`Invalid positive integer: ${value}`);
  }

  return parsed;
}

export interface RpcBackedNetwork {
  architecture: ChainFamily;
  blockTime: number;
  id: string;
  networkId: PrimaryId;
  rpcEndpoint: string;
  rps: number;
  zmqBlockEndpoint?: string | null;
}
