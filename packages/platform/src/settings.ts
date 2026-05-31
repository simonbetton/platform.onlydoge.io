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
  analyticsPassword?: string;
  analyticsUser?: string;
  driver: 'duckdb' | 'clickhouse';
  location: string;
  database?: string;
  requestTimeoutMs?: number;
  user?: string;
  password?: string;
}

export interface IndexerSettings extends CoreDogecoinIndexerSettings {}

export interface AppSettings {
  auditRetentionDays: number;
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
  const env = resolveSettingsEnv(input);
  assertRequiredEnvironment(env);
  const mode = parseMode(resolveSettingsValue(inputMode(input), env.ONLYDOGE_MODE));
  const locations = resolveStorageLocations(env, homePlaceholder());

  return {
    mode,
    auditRetentionDays: parsePositiveInteger(env.ONLYDOGE_AUDIT_RETENTION_DAYS, 365),
    isIndexer: isIndexerMode(mode),
    isHttp: isHttpMode(mode),
    ip: resolveSettingsValue(inputIp(input), env.ONLYDOGE_IP, '127.0.0.1'),
    port: resolveSettingsPort(input, env),
    database: parseDatabaseSettings(locations.database, env),
    indexer: parseIndexerSettings(env),
    storage: parseStorageSettings(locations.storage, env),
    warehouse: parseWarehouseSettings(locations.warehouse, env),
  };
}

function homePlaceholder(): string {
  return '${' + 'HOME}';
}

function isIndexerMode(mode: Mode): boolean {
  return mode === 'both' || mode === 'indexer';
}

function isHttpMode(mode: Mode): boolean {
  return mode === 'both' || mode === 'http';
}

function resolveSettingsPort(input: { port?: number } | undefined, env: NodeJS.ProcessEnv): number {
  return inputPort(input) ?? Number(resolveSettingsValue(env.ONLYDOGE_PORT, undefined, '2277'));
}

function resolveSettingsEnv(input?: { env?: NodeJS.ProcessEnv }): NodeJS.ProcessEnv {
  return inputEnv(input) ?? process.env;
}

function resolveSettingsValue(
  primary: string | undefined,
  secondary: string | undefined,
  fallback = '',
): string {
  return [primary, secondary, fallback].find((value) => value !== undefined) ?? '';
}

function inputPort(input: { port?: number } | undefined): number | undefined {
  return input?.port;
}

function inputMode(input: { mode?: string } | undefined): string | undefined {
  return input?.mode;
}

function inputIp(input: { ip?: string } | undefined): string | undefined {
  return input?.ip;
}

function inputEnv(input?: { env?: NodeJS.ProcessEnv }): NodeJS.ProcessEnv | undefined {
  return input?.env;
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
  if (!isProductionEnv(env)) {
    return;
  }

  assertRequiredProductionEnvironment(env);
}

function assertRequiredProductionEnvironment(env: NodeJS.ProcessEnv): void {
  const missing = missingRequiredEnvironmentKeys(env);
  if (missing.length > 0) {
    throw new Error(`Missing required environment variables: ${missing.join(', ')}`);
  }
}

function isProductionEnv(env: NodeJS.ProcessEnv): boolean {
  return env.NODE_ENV === 'production';
}

function missingRequiredEnvironmentKeys(env: NodeJS.ProcessEnv): string[] {
  return requiredEnvironmentChecks
    .filter((check) => !check.isPresent(env))
    .map((check) => check.key);
}

const requiredEnvironmentChecks = [
  { key: 'ONLYDOGE_DATABASE', isPresent: hasDatabaseConfiguration },
  { key: 'ONLYDOGE_STORAGE', isPresent: (env: NodeJS.ProcessEnv) => Boolean(env.ONLYDOGE_STORAGE) },
  {
    key: 'ONLYDOGE_WAREHOUSE',
    isPresent: (env: NodeJS.ProcessEnv) => Boolean(env.ONLYDOGE_WAREHOUSE),
  },
];

function hasDatabaseConfiguration(env: NodeJS.ProcessEnv): boolean {
  return Boolean(resolveDatabaseLocation(env));
}

function resolveDatabaseLocation(env: NodeJS.ProcessEnv): string | undefined {
  if (env.ONLYDOGE_DATABASE) {
    return env.ONLYDOGE_DATABASE;
  }

  return postgresDatabaseLocationFromEnv(env);
}

function postgresDatabaseLocationFromEnv(env: NodeJS.ProcessEnv): string | undefined {
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
  const settings = databaseSettingsParsers
    .map((parser) => parser(location, env))
    .find((parsed) => parsed !== null);
  if (settings) {
    return settings;
  }

  throw new Error(`Unsupported database configuration: ${location}`);
}

const databaseSettingsParsers = [
  parseSqliteDatabaseSettings,
  parsePostgresDatabaseSettings,
  parseMysqlDatabaseSettings,
];

function parseSqliteDatabaseSettings(
  location: string,
  _env: NodeJS.ProcessEnv,
): DatabaseSettings | null {
  return location.startsWith('sqlite://') ? sqliteDatabaseSettings(location) : null;
}

function parsePostgresDatabaseSettings(
  location: string,
  env: NodeJS.ProcessEnv,
): DatabaseSettings | null {
  return isPostgresLocation(location) ? postgresDatabaseSettings(location, env) : null;
}

function parseMysqlDatabaseSettings(
  location: string,
  _env: NodeJS.ProcessEnv,
): DatabaseSettings | null {
  return location.startsWith('mysql://') ? { driver: 'mysql', location } : null;
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
  if (isDecodedCertificateValue(trimmed)) {
    return trimmed;
  }

  return decodedCertificateOrOriginal(trimmed);
}

function decodedCertificateOrOriginal(value: string): string {
  return decodeBase64Certificate(value) ?? value;
}

function isDecodedCertificateValue(value: string): boolean {
  return value.length === 0 || value.includes('-----BEGIN CERTIFICATE-----');
}

function decodeBase64Certificate(value: string): string | null {
  try {
    const decoded = Buffer.from(value, 'base64').toString('utf8');
    return decodedCertificateValue(decoded);
  } catch {
    return null;
  }
}

function decodedCertificateValue(value: string): string | null {
  return value.includes('-----BEGIN CERTIFICATE-----') ? value : null;
}

function parseDatabaseSslSettings(env: NodeJS.ProcessEnv): DatabaseSettings['ssl'] | undefined {
  const caValue = databaseSslRootCertValue(env);
  if (!caValue) {
    return undefined;
  }

  return {
    ca: decodeMaybeBase64(caValue),
    rejectUnauthorized: true,
  };
}

function databaseSslRootCertValue(env: NodeJS.ProcessEnv): string | undefined {
  return env.ONLYDOGE_DATABASE_SSLROOTCERT_PEM ?? env.ONLYDOGE_DATABASE_SSLROOTCERT_BASE64;
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
    return fileStorageSettings(location);
  }

  return s3StorageSettings(location, env);
}

function fileStorageSettings(location: string): StorageSettings {
  return {
    driver: 'file',
    location: new URL(location).pathname,
  };
}

function s3StorageSettings(location: string, env: NodeJS.ProcessEnv): StorageSettings {
  return {
    driver: 's3',
    location,
    ...optionalStorageCredential('accessKeyId', env.ONLYDOGE_S3_ACCESS_KEY_ID),
    ...optionalStorageCredential('secretAccessKey', env.ONLYDOGE_S3_SECRET_ACCESS_KEY),
  };
}

function optionalStorageCredential(
  key: 'accessKeyId' | 'secretAccessKey',
  value: string | undefined,
): Partial<StorageSettings> {
  return value ? { [key]: value } : {};
}

function parseWarehouseSettings(location: string, env: NodeJS.ProcessEnv): WarehouseSettings {
  if (isClickHouseLocation(location)) {
    return parseClickHouseWarehouseSettings(location, env);
  }

  return {
    driver: 'duckdb',
    location: location.replace(/^file:/u, ''),
  };
}

function isClickHouseLocation(location: string): boolean {
  return ['http://', 'https://'].some((prefix) => location.startsWith(prefix));
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
  applyClickHouseCredential(settings, 'user', env.ONLYDOGE_WAREHOUSE_USER);
  applyClickHouseCredential(settings, 'password', env.ONLYDOGE_WAREHOUSE_PASSWORD);
  applyClickHouseCredential(settings, 'analyticsUser', env.ONLYDOGE_ANALYTICS_WAREHOUSE_USER);
  applyClickHouseCredential(
    settings,
    'analyticsPassword',
    env.ONLYDOGE_ANALYTICS_WAREHOUSE_PASSWORD,
  );
}

function applyClickHouseCredential(
  settings: WarehouseSettings,
  key: 'analyticsPassword' | 'analyticsUser' | 'password' | 'user',
  value: string | undefined,
): void {
  if (value) {
    settings[key] = value;
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
    coreReprocessDepth: parsePositiveInteger(env.ONLYDOGE_CORE_REPROCESS_DEPTH, 10),
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
  return requirePositiveInteger(parsed, value);
}

function requirePositiveInteger(parsed: number, raw: string): number {
  if (!isPositiveInteger(parsed)) {
    throw new Error(`Invalid positive integer: ${raw}`);
  }

  return parsed;
}

function isPositiveInteger(value: number): boolean {
  return [Number.isInteger(value), value > 0].every(Boolean);
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
