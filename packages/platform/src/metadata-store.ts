import { createHash, randomUUID } from 'node:crypto';
import { mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import {
  createClient as createLibsqlClient,
  type Client as LibsqlClient,
  type Transaction as LibsqlTransaction,
} from '@libsql/client';
import type {
  ApiKeyRecord,
  ApiKeyRepository,
  AuditEventFilters,
  AuditEventRecord,
  AuditEventRepository,
  BootstrapApiKeyResult,
  CreateAuditEventInput,
} from '@onlydoge/access-control';
import {
  type BlockProjectionBatch,
  type CoordinatorConfigPort,
  type CoreBlockRecord,
  type CoreIndexerStage,
  type CoreIndexerState,
  configKeyDogecoinHistoryReady,
  type ProjectionBalanceSnapshot,
  type ProjectionStateBootstrapSnapshot,
  type ProjectionStateStorePort,
  type ProjectionUtxoOutput,
} from '@onlydoge/indexing-pipeline';
import {
  ConflictError,
  InfrastructureError,
  nowIsoString,
  OnlyDogeError,
  type PrimaryId,
  safeJsonParse,
} from '@onlydoge/shared-kernel';
import mysql from 'mysql2/promise';
import { Pool, type PoolClient } from 'pg';
import { createLogger } from './logger';
import type { ActiveMempoolWatch } from './mempool-watch-types';
import { MEMPOOL_WATCH_MAX_CONCURRENT } from './mempool-watch-types';
import {
  compileQuery,
  metadataInfrastructureMessage,
  type SqlValue,
  toBoolean,
} from './metadata-query';
import type { SchemaLockPort } from './schema-lock';
import type { DatabaseSettings } from './settings';

type SupportedClient =
  | { kind: 'sqlite'; raw: LibsqlClient }
  | { kind: 'postgres'; raw: Pool }
  | { kind: 'mysql'; raw: mysql.Pool };

type SupportedExecutor =
  | SupportedClient
  | { kind: 'sqlite'; raw: LibsqlTransaction }
  | { kind: 'postgres'; raw: PoolClient }
  | { kind: 'mysql'; raw: mysql.PoolConnection };

type DatabaseRow = Record<string, SqlValue>;

export interface MetadataMigrationStatus {
  applied: Array<{ appliedAt: string; checksum: string; name: string; version: number }>;
  currentVersion: number;
  drift: string[];
  ledgerExists: boolean;
  pending: Array<{ checksum: string; name: string; version: number }>;
}

interface MetadataMigration {
  checksum: string;
  name: string;
  version: number;
}

const metadataMigrationLockKey = 762_319_045;
const metadataMigrationLockName = 'onlydoge_metadata_migrate';
const metadataMigrationLockTimeoutSeconds = 5;

export interface CoreIndexerStateUpdate {
  lastError?: string | null;
  onlineTip?: number;
  processTail?: number;
  stage?: CoreIndexerStage;
  syncTail?: number;
}

const auditOutcomeValues = new Set<AuditEventRecord['outcome']>([
  'denied',
  'failure',
  'rate_limited',
  'success',
]);

const legacyMetadataTables = [
  'entity_tags',
  'addresses',
  'tokens',
  'tags',
  'entities',
  'networks',
  'core_block_undo',
  'core_processed_blocks',
] as const;

export class RelationalMetadataStore
  implements
    ApiKeyRepository,
    AuditEventRepository,
    CoordinatorConfigPort,
    ProjectionStateStorePort,
    SchemaLockPort
{
  private auditEventsHasLegacyResourceIds = false;
  private migratePromise: Promise<void> | null = null;
  private migrating = false;
  private schemaReady = false;
  private sqliteBootstrapQueue: Promise<void> = Promise.resolve();
  private sqliteSchemaLockQueue: Promise<void> = Promise.resolve();

  private constructor(private readonly client: SupportedClient) {}

  public static async connect(
    settings: DatabaseSettings,
    options?: { migrate?: boolean },
  ): Promise<RelationalMetadataStore> {
    const store = await RelationalMetadataStore.open(settings);
    if (options?.migrate === false) {
      return store;
    }
    try {
      await store.migrate();
      return store;
    } catch (error) {
      await store.close().catch(() => undefined);
      throw error;
    }
  }

  public static async migrationStatus(
    settings: DatabaseSettings,
  ): Promise<MetadataMigrationStatus> {
    const store = await RelationalMetadataStore.open(settings);
    try {
      return await store.readMigrationStatus();
    } finally {
      await store.close();
    }
  }

  private static async open(settings: DatabaseSettings): Promise<RelationalMetadataStore> {
    if (settings.driver === 'sqlite') {
      const path = settings.location.replace(/^file:/u, '');
      await mkdir(dirname(path), { recursive: true });
      return new RelationalMetadataStore({
        kind: 'sqlite',
        raw: createLibsqlClient({ url: settings.location }),
      });
    }
    if (settings.driver === 'postgres') {
      return new RelationalMetadataStore({
        kind: 'postgres',
        raw: createPostgresPool(settings),
      });
    }

    return new RelationalMetadataStore({
      kind: 'mysql',
      raw: createMysqlPool(settings.location),
    });
  }

  public async close(): Promise<void> {
    if (this.client.kind === 'sqlite') {
      this.client.raw.close();
      return;
    }
    await this.client.raw.end();
  }

  public async withSchemaLock<T>(name: string, work: () => Promise<T>): Promise<T> {
    if (!/^[a-z0-9-]+$/u.test(name)) {
      throw new Error(`invalid schema lock name: ${name}`);
    }
    if (this.client.kind === 'sqlite') {
      return this.withSqliteSchemaLock(work);
    }
    if (this.client.kind === 'postgres') {
      return this.withPostgresSchemaLock(name, work);
    }
    return this.withMysqlSchemaLock(name, work);
  }

  private async withSqliteSchemaLock<T>(work: () => Promise<T>): Promise<T> {
    if (this.client.kind !== 'sqlite') {
      throw new TypeError('expected sqlite metadata client');
    }
    const previous = this.sqliteSchemaLockQueue;
    let release = (): void => undefined;
    this.sqliteSchemaLockQueue = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;

    let transaction: LibsqlTransaction | null = null;
    for (let attempt = 0; transaction === null; attempt += 1) {
      let candidate: LibsqlTransaction | null = null;
      try {
        candidate = await this.client.raw.transaction('write');
        await candidate.execute(
          "UPDATE app_config SET updated_at = updated_at WHERE key = '__onlydoge_schema_lock__'",
        );
        transaction = candidate;
      } catch (error) {
        await candidate?.rollback().catch(() => undefined);
        candidate?.close();
        if (!isSqliteBusyError(error) || attempt >= 599) {
          release();
          throw error;
        }
        await sleep(100);
      }
    }
    try {
      const result = await work();
      // The transaction is only an inter-process write lock; the protected
      // ClickHouse migration does not write through this SQLite connection.
      await transaction.rollback();
      return result;
    } catch (error) {
      await transaction.rollback().catch(() => undefined);
      throw error;
    } finally {
      transaction.close();
      release();
    }
  }

  private async withPostgresSchemaLock<T>(name: string, work: () => Promise<T>): Promise<T> {
    if (this.client.kind !== 'postgres') {
      throw new TypeError('expected postgres metadata client');
    }
    const connection = await this.postgresConnect();
    const key = advisoryLockKey(name);
    try {
      await connection.query('SELECT pg_advisory_lock($1)', [key]);
      return await work();
    } finally {
      await connection.query('SELECT pg_advisory_unlock($1)', [key]).catch(() => undefined);
      connection.release();
    }
  }

  private async withMysqlSchemaLock<T>(name: string, work: () => Promise<T>): Promise<T> {
    if (this.client.kind !== 'mysql') {
      throw new TypeError('expected mysql metadata client');
    }
    const connection = await this.client.raw.getConnection();
    const executor = { kind: 'mysql' as const, raw: connection };
    const lockName = `onlydoge_${name}`;
    let acquired = false;
    try {
      const [row] = await this.queryRows<{ acquired: number | string }>(
        'SELECT GET_LOCK(?, 60) AS acquired',
        [lockName],
        executor,
      );
      acquired = Number(row?.acquired) === 1;
      if (!acquired) {
        throw new Error(`failed to acquire schema lock: ${name}`);
      }
      return await work();
    } finally {
      if (acquired) {
        await connection.query('SELECT RELEASE_LOCK(?)', [lockName]).catch(() => undefined);
      }
      connection.release();
    }
  }

  public async countApiKeys(): Promise<number> {
    return this.countApiKeysWithExecutor();
  }

  public async countActiveAdminApiKeys(): Promise<number> {
    const row = await this.one<{ count: number | string }>(
      `
        SELECT COUNT(*) AS count
        FROM api_keys
        WHERE role = 'admin' AND ${this.booleanCondition('is_active', true)}
      `,
    );
    return countRowValue(row);
  }

  public async createApiKey(record: ApiKeyRecord): Promise<ApiKeyRecord> {
    await this.insertApiKey(record);

    return this.getApiKeyById(record.id).then(assertFound);
  }

  public async createBootstrapApiKey(record: ApiKeyRecord): Promise<BootstrapApiKeyResult> {
    if (this.client.kind === 'sqlite') {
      return this.createSqliteBootstrapApiKey(record);
    }
    if (this.client.kind === 'postgres') {
      return this.createPostgresBootstrapApiKey(record);
    }
    return this.createMysqlBootstrapApiKey(record);
  }

  private async createSqliteBootstrapApiKey(record: ApiKeyRecord): Promise<BootstrapApiKeyResult> {
    if (this.client.kind !== 'sqlite') {
      throw new TypeError('expected sqlite metadata client');
    }
    const client = this.client.raw;

    return this.withSqliteBootstrapLock(async () => {
      for (let attempt = 0; ; attempt += 1) {
        try {
          return await this.createSqliteBootstrapApiKeyAttempt(record, client);
        } catch (error) {
          if (!isSqliteBusyError(error) || attempt >= 49) {
            throw error;
          }
          await sleep(Math.min(5 * (attempt + 1), 50));
        }
      }
    });
  }

  private async withSqliteBootstrapLock<T>(work: () => Promise<T>): Promise<T> {
    const previous = this.sqliteBootstrapQueue;
    let release = (): void => undefined;
    this.sqliteBootstrapQueue = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await work();
    } finally {
      release();
    }
  }

  private async createSqliteBootstrapApiKeyAttempt(
    record: ApiKeyRecord,
    client: LibsqlClient,
  ): Promise<BootstrapApiKeyResult> {
    const transaction = await client.transaction('write');
    const executor = { kind: 'sqlite' as const, raw: transaction };
    try {
      const result = await this.createBootstrapApiKeyInTransaction(record, executor);
      await transaction.commit();
      return result;
    } finally {
      transaction.close();
    }
  }

  private async createPostgresBootstrapApiKey(
    record: ApiKeyRecord,
  ): Promise<BootstrapApiKeyResult> {
    if (this.client.kind !== 'postgres') {
      throw new TypeError('expected postgres metadata client');
    }

    const connection = await this.postgresConnect();
    const executor = { kind: 'postgres' as const, raw: connection };
    try {
      await connection.query('BEGIN');
      await connection.query('SELECT pg_advisory_xact_lock($1)', [762_319_046]);
      const result = await this.createBootstrapApiKeyInTransaction(record, executor);
      await connection.query('COMMIT');
      return result;
    } catch (error) {
      await connection.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      connection.release();
    }
  }

  private async createMysqlBootstrapApiKey(record: ApiKeyRecord): Promise<BootstrapApiKeyResult> {
    if (this.client.kind !== 'mysql') {
      throw new TypeError('expected mysql metadata client');
    }

    const connection = await this.client.raw.getConnection();
    const executor = { kind: 'mysql' as const, raw: connection };
    let lockAcquired = false;
    try {
      const [lockRow] = await this.queryRows<{ acquired: number | string }>(
        "SELECT GET_LOCK('onlydoge_api_key_bootstrap', 60) AS acquired",
        [],
        executor,
      );
      lockAcquired = Number(lockRow?.acquired) === 1;
      if (!lockAcquired) {
        throw new Error('failed to acquire API key bootstrap lock');
      }

      await connection.beginTransaction();
      const result = await this.createBootstrapApiKeyInTransaction(record, executor);
      await connection.commit();
      return result;
    } catch (error) {
      await connection.rollback().catch(() => undefined);
      throw error;
    } finally {
      if (lockAcquired) {
        await connection
          .query("SELECT RELEASE_LOCK('onlydoge_api_key_bootstrap')")
          .catch(() => connection.destroy());
      }
      connection.release();
    }
  }

  private async createBootstrapApiKeyInTransaction(
    record: ApiKeyRecord,
    executor: SupportedExecutor,
  ): Promise<BootstrapApiKeyResult> {
    if ((await this.countApiKeysWithExecutor(executor)) > 0) {
      return { created: false };
    }

    await this.insertApiKey(record, executor);
    const created = await this.getApiKeyByIdWithExecutor(record.id, executor);
    return { created: true, record: assertFound(created) };
  }

  private async countApiKeysWithExecutor(
    executor: SupportedExecutor = this.client,
  ): Promise<number> {
    const [row] = await this.queryRows<{ count: number | string }>(
      'SELECT COUNT(*) AS count FROM api_keys',
      [],
      executor,
    );
    return countRowValue(row ?? null);
  }

  private async insertApiKey(
    record: ApiKeyRecord,
    executor: SupportedExecutor = this.client,
  ): Promise<void> {
    await this.run(
      `
        INSERT INTO api_keys (id, secret_key_hash, is_active, role, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
      `,
      [
        record.id,
        record.secretKeyHash,
        this.booleanValue(record.isActive),
        record.role,
        record.updatedAt,
        record.createdAt,
      ],
      executor,
    );
  }

  private async getApiKeyByIdWithExecutor(
    id: string,
    executor: SupportedExecutor,
  ): Promise<ApiKeyRecord | null> {
    const [row] = await this.queryRows<DatabaseRow>(
      'SELECT * FROM api_keys WHERE id = ? LIMIT 1',
      [id],
      executor,
    );
    return row ? this.mapApiKey(row) : null;
  }

  public async getApiKeyByHash(secretKeyHash: string): Promise<ApiKeyRecord | null> {
    const row = await this.one<DatabaseRow>(
      'SELECT * FROM api_keys WHERE secret_key_hash = ? LIMIT 1',
      [secretKeyHash],
    );
    return row ? this.mapApiKey(row) : null;
  }

  public async getApiKeyById(id: string): Promise<ApiKeyRecord | null> {
    const row = await this.one<DatabaseRow>('SELECT * FROM api_keys WHERE id = ? LIMIT 1', [id]);
    return row ? this.mapApiKey(row) : null;
  }

  public async getApiKeyByInternalId(apiKeyId: PrimaryId): Promise<ApiKeyRecord | null> {
    const row = await this.one<DatabaseRow>('SELECT * FROM api_keys WHERE api_key_id = ? LIMIT 1', [
      apiKeyId,
    ]);
    return row ? this.mapApiKey(row) : null;
  }

  public async listApiKeys(offset = 0, limit?: number): Promise<ApiKeyRecord[]> {
    const rows = await this.queryRows<DatabaseRow>(
      `
        SELECT *
        FROM api_keys
        ORDER BY api_key_id ASC
        ${limit === undefined ? '' : 'LIMIT ?'}
        ${offset > 0 ? 'OFFSET ?' : ''}
      `,
      [...(limit === undefined ? [] : [limit]), ...(offset > 0 ? [offset] : [])],
    );
    return rows.map((row) => this.mapApiKey(row));
  }

  public async updateApiKey(record: ApiKeyRecord): Promise<void> {
    await this.execute(
      `
        UPDATE api_keys
        SET secret_key_hash = ?, is_active = ?, role = ?, updated_at = ?
        WHERE id = ?
      `,
      [
        record.secretKeyHash,
        this.booleanValue(record.isActive),
        record.role,
        record.updatedAt,
        record.id,
      ],
    );
  }

  public async deleteApiKeys(ids: string[]): Promise<void> {
    if (ids.length === 0) {
      return;
    }

    await this.execute(`DELETE FROM api_keys WHERE id IN (${placeholders(ids.length)})`, ids);
  }

  public async createAuditEvent(input: CreateAuditEventInput): Promise<void> {
    const id = input.id ?? `evt_${randomUUID().replaceAll('-', '')}`;
    const resourceIdsJson = JSON.stringify(input.resourceIds);
    if (this.auditEventsHasLegacyResourceIds) {
      await this.execute(
        `
          INSERT INTO audit_events (
            id, actor_api_key_id, actor_api_key, actor_role, owner_api_key_id, owner_api_key,
            method, path, route, operation, resource_type, resource_ids, resource_ids_json,
            status_code, outcome, error, request_id, ip, user_agent, created_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
          id,
          input.actorApiKeyId,
          input.actorApiKey,
          input.actorRole,
          input.ownerApiKeyId,
          input.ownerApiKey,
          input.method,
          input.path,
          input.route,
          input.operation,
          input.resourceType,
          resourceIdsJson,
          resourceIdsJson,
          input.statusCode,
          input.outcome,
          input.error,
          input.requestId,
          input.ip,
          input.userAgent,
          input.createdAt,
        ],
      );
      return;
    }

    await this.execute(
      `
        INSERT INTO audit_events (
          id, actor_api_key_id, actor_api_key, actor_role, owner_api_key_id, owner_api_key,
          method, path, route, operation, resource_type, resource_ids_json, status_code,
          outcome, error, request_id, ip, user_agent, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        id,
        input.actorApiKeyId,
        input.actorApiKey,
        input.actorRole,
        input.ownerApiKeyId,
        input.ownerApiKey,
        input.method,
        input.path,
        input.route,
        input.operation,
        input.resourceType,
        resourceIdsJson,
        input.statusCode,
        input.outcome,
        input.error,
        input.requestId,
        input.ip,
        input.userAgent,
        input.createdAt,
      ],
    );
  }

  public async deleteAuditEventsBefore(cutoffIso: string): Promise<void> {
    await this.execute('DELETE FROM audit_events WHERE created_at < ?', [cutoffIso]);
  }

  public async createActiveMempoolWatch(input: {
    address: string;
    apiKeyId: string;
    expiresAt: string;
    id: string;
    minValueBase: string | null;
  }): Promise<ActiveMempoolWatch> {
    return this.withMempoolWatchCreateLock(input.apiKeyId, async () => {
      await this.deleteExpiredMempoolWatches();
      const activeCount = await this.countActiveMempoolWatchesByApiKeyId(input.apiKeyId);
      if (activeCount >= MEMPOOL_WATCH_MAX_CONCURRENT) {
        throw new ConflictError(
          `mempool watch session limit reached for this API key (${MEMPOOL_WATCH_MAX_CONCURRENT})`,
        );
      }

      const createdAt = nowIsoString();
      await this.execute(
        `
          INSERT INTO active_mempool_watches (
            id, api_key_id, address, min_value_base, expires_at, created_at
          )
          VALUES (?, ?, ?, ?, ?, ?)
        `,
        [input.id, input.apiKeyId, input.address, input.minValueBase, input.expiresAt, createdAt],
      );

      return {
        id: input.id,
        apiKeyId: input.apiKeyId,
        address: input.address,
        minValueBase: input.minValueBase,
        expiresAt: input.expiresAt,
        createdAt,
      };
    });
  }

  public async deleteActiveMempoolWatch(id: string): Promise<void> {
    await this.execute('DELETE FROM active_mempool_watches WHERE id = ?', [id]);
  }

  public async countActiveMempoolWatchesByApiKeyId(apiKeyId: string): Promise<number> {
    await this.deleteExpiredMempoolWatches();
    const row = await this.one<{ count: number | string }>(
      'SELECT COUNT(*) AS count FROM active_mempool_watches WHERE api_key_id = ?',
      [apiKeyId],
    );
    return countRowValue(row);
  }

  public async getActiveMempoolWatchByApiKeyId(
    apiKeyId: string,
  ): Promise<ActiveMempoolWatch | null> {
    await this.deleteExpiredMempoolWatches();
    const row = await this.one<DatabaseRow>(
      'SELECT * FROM active_mempool_watches WHERE api_key_id = ? ORDER BY created_at ASC LIMIT 1',
      [apiKeyId],
    );
    return row ? mapActiveMempoolWatch(row) : null;
  }

  public async listActiveMempoolWatches(): Promise<ActiveMempoolWatch[]> {
    await this.deleteExpiredMempoolWatches();
    const rows = await this.queryRows<DatabaseRow>(
      'SELECT * FROM active_mempool_watches ORDER BY created_at ASC',
    );
    return rows.map(mapActiveMempoolWatch);
  }

  public async deleteExpiredMempoolWatches(now = nowIsoString()): Promise<void> {
    await this.execute('DELETE FROM active_mempool_watches WHERE expires_at <= ?', [now]);
  }

  private async withMempoolWatchCreateLock<T>(
    apiKeyId: string,
    work: () => Promise<T>,
  ): Promise<T> {
    if (this.client.kind !== 'postgres') {
      return work();
    }

    const client = await this.postgresConnect();
    try {
      await client.query('BEGIN');
      await client.query('SELECT pg_advisory_xact_lock($1)', [advisoryLockKey(apiKeyId)]);
      const result = await work();
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  public async listAuditEvents(
    filters: AuditEventFilters & { actorApiKeyId?: PrimaryId },
  ): Promise<AuditEventRecord[]> {
    const query = auditEventQuery(filters);
    const rows = await this.queryRows<DatabaseRow>(query.sql, query.params);
    return rows.map((row) => this.mapAuditEvent(row));
  }

  public async compareAndDeleteJsonValue<T>(key: string, expectedValue: T): Promise<boolean> {
    const expectedJson = JSON.stringify(expectedValue);
    return (
      (await this.mutate('DELETE FROM app_config WHERE key = ? AND value_json = ?', [
        key,
        expectedJson,
      ])) === 1
    );
  }

  public async compareAndSwapJsonValue<T>(
    key: string,
    expectedValue: T | null,
    nextValue: T,
  ): Promise<boolean> {
    const now = nowIsoString();
    const nextJson = JSON.stringify(nextValue);
    if (expectedValue === null) {
      // Null is the port's absent-key sentinel, so a persisted JSON null row is treated as occupied.
      const insertSql =
        this.client.kind === 'mysql'
          ? `
              INSERT IGNORE INTO app_config (key, value_json, updated_at)
              VALUES (?, ?, ?)
            `
          : `
              INSERT INTO app_config (key, value_json, updated_at)
              VALUES (?, ?, ?)
              ON CONFLICT(key) DO NOTHING
            `;
      return (await this.mutate(insertSql, [key, nextJson, now])) === 1;
    }

    const expectedJson = JSON.stringify(expectedValue);
    return (
      (await this.mutate(
        `
          UPDATE app_config
          SET value_json = ?, updated_at = ?
          WHERE key = ? AND value_json = ?
        `,
        [nextJson, now, key, expectedJson],
      )) === 1
    );
  }

  public async deleteByPrefix(prefix: string): Promise<void> {
    await this.execute('DELETE FROM app_config WHERE key LIKE ?', [`${prefix}%`]);
  }

  public async getJsonValue<T>(key: string): Promise<T | null> {
    const row = await this.one<DatabaseRow>(
      'SELECT value_json FROM app_config WHERE key = ? LIMIT 1',
      [key],
    );
    if (!row) {
      return null;
    }

    return safeJsonParse<T | null>(String(row.value_json), null);
  }

  public async setJsonValue<T>(key: string, value: T): Promise<void> {
    const now = nowIsoString();
    if (this.client.kind === 'mysql') {
      await this.execute(
        `
          INSERT INTO app_config (key, value_json, updated_at)
          VALUES (?, ?, ?)
          ON DUPLICATE KEY UPDATE value_json = VALUES(value_json), updated_at = VALUES(updated_at)
        `,
        [key, JSON.stringify(value), now],
      );
      return;
    }

    await this.execute(
      `
        INSERT INTO app_config (key, value_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
      `,
      [key, JSON.stringify(value), now],
    );
  }

  public async canReadDogecoinHistory(): Promise<boolean> {
    return (await this.getJsonValue<boolean>(configKeyDogecoinHistoryReady())) === true;
  }

  public async getCoreIndexerState(): Promise<CoreIndexerState | null> {
    const row = await this.one<DatabaseRow>(
      'SELECT * FROM core_indexer_state WHERE id = 1 LIMIT 1',
    );
    return row ? mapCoreIndexerState(row) : null;
  }

  public async upsertCoreIndexerState(input: CoreIndexerStateUpdate): Promise<CoreIndexerState> {
    const current = await this.getCoreIndexerState();
    const now = nowIsoString();
    const next: CoreIndexerState = {
      stage: input.stage ?? current?.stage ?? 'sync_backfill',
      syncTail: input.syncTail ?? current?.syncTail ?? -1,
      processTail: input.processTail ?? current?.processTail ?? -1,
      onlineTip: input.onlineTip ?? current?.onlineTip ?? -1,
      lastError: 'lastError' in input ? (input.lastError ?? null) : (current?.lastError ?? null),
      updatedAt: now,
    };

    if (this.client.kind === 'mysql') {
      await this.execute(
        `
          INSERT INTO core_indexer_state (
            id, stage, sync_tail, process_tail, online_tip, last_error, updated_at
          )
          VALUES (1, ?, ?, ?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
            stage = VALUES(stage),
            sync_tail = VALUES(sync_tail),
            process_tail = VALUES(process_tail),
            online_tip = VALUES(online_tip),
            last_error = VALUES(last_error),
            updated_at = VALUES(updated_at)
        `,
        [next.stage, next.syncTail, next.processTail, next.onlineTip, next.lastError, now],
      );
      return next;
    }

    await this.execute(
      `
        INSERT INTO core_indexer_state (
          id, stage, sync_tail, process_tail, online_tip, last_error, updated_at
        )
        VALUES (1, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          stage = excluded.stage,
          sync_tail = excluded.sync_tail,
          process_tail = excluded.process_tail,
          online_tip = excluded.online_tip,
          last_error = excluded.last_error,
          updated_at = excluded.updated_at
      `,
      [next.stage, next.syncTail, next.processTail, next.onlineTip, next.lastError, now],
    );
    return next;
  }

  public async setCoreIndexerStage(stage: CoreIndexerStage): Promise<void> {
    await this.upsertCoreIndexerState({ stage });
  }

  public async setCoreIndexerError(error: string | null): Promise<void> {
    await this.upsertCoreIndexerState({ lastError: error });
  }

  public async upsertCoreBlock(record: CoreBlockRecord): Promise<void> {
    if (this.client.kind === 'mysql') {
      await this.execute(
        `
          INSERT INTO core_blocks (
            block_height, block_hash, previous_block_hash, block_time, tx_count,
            raw_storage_key, fetched_at, processed_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
            block_hash = VALUES(block_hash),
            previous_block_hash = VALUES(previous_block_hash),
            block_time = VALUES(block_time),
            tx_count = VALUES(tx_count),
            raw_storage_key = VALUES(raw_storage_key),
            fetched_at = VALUES(fetched_at),
            processed_at = VALUES(processed_at)
        `,
        coreBlockParams(record),
      );
      return;
    }

    await this.execute(
      `
        INSERT INTO core_blocks (
          block_height, block_hash, previous_block_hash, block_time, tx_count,
          raw_storage_key, fetched_at, processed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(block_height) DO UPDATE SET
          block_hash = excluded.block_hash,
          previous_block_hash = excluded.previous_block_hash,
          block_time = excluded.block_time,
          tx_count = excluded.tx_count,
          raw_storage_key = excluded.raw_storage_key,
          fetched_at = excluded.fetched_at,
          processed_at = excluded.processed_at
      `,
      coreBlockParams(record),
    );
  }

  public async getCoreBlockByHash(blockHash: string): Promise<{
    blockHash: string;
    blockHeight: number;
  } | null> {
    const row = await this.one<{
      block_hash: string;
      block_height: number | string;
    }>(
      `
        SELECT block_height, block_hash
        FROM core_blocks
        WHERE block_hash = ?
        LIMIT 1
      `,
      [blockHash],
    );

    if (!row) {
      return null;
    }

    return {
      blockHash: String(row.block_hash),
      blockHeight: Number(row.block_height),
    };
  }

  public async applyProjectionWindow(_batches: BlockProjectionBatch[]): Promise<void> {}
  public async clearProjectionBootstrapState(): Promise<void> {}
  public async finalizeProjectionBootstrap(_processTail: number): Promise<void> {}
  public async getCurrentAddressSummary(): Promise<{ balance: string; utxoCount: number } | null> {
    return null;
  }
  public async getBalanceSnapshots(): Promise<Map<string, ProjectionBalanceSnapshot>> {
    return new Map();
  }
  public async getProjectionBootstrapTail(): Promise<number | null> {
    return null;
  }
  public async getUtxoOutputs(): Promise<Map<string, ProjectionUtxoOutput>> {
    return new Map();
  }
  public async hasAppliedBlock(): Promise<boolean> {
    return false;
  }
  public async hasProjectionState(): Promise<boolean> {
    return false;
  }
  public async importProjectionStateSnapshot(
    _snapshot: ProjectionStateBootstrapSnapshot,
    _processTail: number,
  ): Promise<void> {}
  public async listAddressUtxos(): Promise<ProjectionUtxoOutput[]> {
    return [];
  }
  public async listAppliedBlockSet(): Promise<Set<string>> {
    return new Set();
  }
  public async upsertProjectionBootstrapBalances(
    _rows: ProjectionBalanceSnapshot[],
  ): Promise<void> {}
  public async upsertProjectionBootstrapUtxoOutputs(_rows: ProjectionUtxoOutput[]): Promise<void> {}

  public async execute(sql: string, args: SqlValue[] = []): Promise<void> {
    await this.run(sql, args);
  }

  public async migrate(): Promise<void> {
    this.migrating = true;
    try {
      await this.runMetadataMigrations();
      this.schemaReady = true;
    } finally {
      this.migrating = false;
    }
  }

  private async runMetadataMigrations(): Promise<void> {
    validateMigrationDefinitions(metadataMigrations);
    await this.withMigrationLock(async (executor) => {
      await this.bootstrapMigrationLedger(executor);
      const applied = await this.readAppliedMigrations(executor);
      this.assertAppliedMigrations(applied);

      for (const migration of metadataMigrations) {
        if (applied.some((row) => row.version === migration.version)) {
          await this.verifyMigration(migration.version, executor);
          continue;
        }
        await this.runMigration(migration, executor);
      }
    });
    this.auditEventsHasLegacyResourceIds = (await this.tableColumns('audit_events')).has(
      'resource_ids',
    );
  }

  private async readMigrationStatus(): Promise<MetadataMigrationStatus> {
    validateMigrationDefinitions(metadataMigrations);
    const ledgerExists = await this.tableExists('metadata_migrations');
    if (!ledgerExists) {
      return {
        applied: [],
        currentVersion: metadataMigrations.at(-1)?.version ?? 0,
        drift: [],
        ledgerExists: false,
        pending: metadataMigrations.map(migrationSummary),
      };
    }

    const applied = await this.readAppliedMigrations();
    const drift = this.appliedMigrationDrift(applied);
    if (drift.length === 0) {
      for (const migration of metadataMigrations.filter((candidate) =>
        applied.some((row) => row.version === candidate.version),
      )) {
        try {
          await this.verifyMigration(migration.version, this.client);
        } catch (error) {
          drift.push(errorMessage(error));
        }
      }
    }
    return {
      applied,
      currentVersion: metadataMigrations.at(-1)?.version ?? 0,
      drift,
      ledgerExists: true,
      pending: metadataMigrations
        .filter((migration) => !applied.some((row) => row.version === migration.version))
        .map(migrationSummary),
    };
  }

  private async withMigrationLock(
    work: (executor: SupportedExecutor) => Promise<void>,
  ): Promise<void> {
    if (this.client.kind === 'sqlite') {
      await this.withSqliteMigrationLock(work);
      return;
    }

    const connection =
      this.client.kind === 'postgres' ? await this.postgresConnect() : await this.mysqlConnect();
    const executor =
      this.client.kind === 'postgres'
        ? ({ kind: 'postgres', raw: connection as PoolClient } as const)
        : ({ kind: 'mysql', raw: connection as mysql.PoolConnection } as const);
    let lockAcquired = false;
    try {
      await this.acquireMigrationLock(executor);
      lockAcquired = true;
      await work(executor);
    } finally {
      try {
        if (lockAcquired) {
          await this.releaseMigrationLock(executor);
        }
      } finally {
        connection.release();
      }
    }
  }

  private async withSqliteMigrationLock(
    work: (executor: SupportedExecutor) => Promise<void>,
  ): Promise<void> {
    if (this.client.kind !== 'sqlite') {
      throw new TypeError('expected sqlite metadata client');
    }
    const deadline = Date.now() + metadataMigrationLockTimeoutSeconds * 1_000;
    for (;;) {
      let transaction: LibsqlTransaction | null = null;
      try {
        transaction = await this.client.raw.transaction('write');
        const executor = { kind: 'sqlite' as const, raw: transaction };
        await work(executor);
        await transaction.commit();
        return;
      } catch (error) {
        if (!isSqliteBusyError(error) || Date.now() >= deadline) {
          throw error;
        }
        await sleep(50);
      } finally {
        transaction?.close();
      }
    }
  }

  private async acquireMigrationLock(executor: SupportedExecutor): Promise<void> {
    if (executor.kind === 'postgres') {
      const deadline = Date.now() + metadataMigrationLockTimeoutSeconds * 1_000;
      while (Date.now() < deadline) {
        const [row] = await this.queryRows<{ acquired: boolean }>(
          'SELECT pg_try_advisory_lock(?) AS acquired',
          [metadataMigrationLockKey],
          executor,
        );
        if (row?.acquired === true) {
          return;
        }
        await sleep(50);
      }
      throw new Error('failed to acquire metadata migration lock');
    }
    if (executor.kind !== 'mysql') {
      throw new TypeError('expected server metadata executor');
    }
    const [row] = await this.queryRows<{ acquired: number | string }>(
      'SELECT GET_LOCK(?, ?) AS acquired',
      [metadataMigrationLockName, metadataMigrationLockTimeoutSeconds],
      executor,
    );
    if (Number(row?.acquired) !== 1) {
      throw new Error('failed to acquire metadata migration lock');
    }
  }

  private async releaseMigrationLock(executor: SupportedExecutor): Promise<void> {
    if (executor.kind === 'postgres') {
      const [row] = await this.queryRows<{ released: boolean }>(
        'SELECT pg_advisory_unlock(?) AS released',
        [metadataMigrationLockKey],
        executor,
      );
      if (row?.released !== true) {
        throw new Error('failed to release metadata migration lock');
      }
      return;
    }
    if (executor.kind !== 'mysql') {
      throw new TypeError('expected server metadata executor');
    }
    const [row] = await this.queryRows<{ released: number | string }>(
      'SELECT RELEASE_LOCK(?) AS released',
      [metadataMigrationLockName],
      executor,
    );
    if (Number(row?.released) !== 1) {
      throw new Error('failed to release metadata migration lock');
    }
  }

  private async bootstrapMigrationLedger(executor: SupportedExecutor): Promise<void> {
    await this.run(
      `
        CREATE TABLE IF NOT EXISTS metadata_migrations (
          version BIGINT PRIMARY KEY,
          name ${textType(executor.kind)} NOT NULL,
          checksum VARCHAR(64) NOT NULL,
          applied_at ${textType(executor.kind)} NOT NULL
        )
      `,
      [],
      executor,
    );
    await this.requireColumns(
      'metadata_migrations',
      ['version', 'name', 'checksum', 'applied_at'],
      executor,
    );
  }

  private async readAppliedMigrations(
    executor: SupportedExecutor = this.client,
  ): Promise<MetadataMigrationStatus['applied']> {
    const rows = await this.queryRows<DatabaseRow>(
      'SELECT version, name, checksum, applied_at FROM metadata_migrations ORDER BY version ASC',
      [],
      executor,
    );
    return rows.map((row) => ({
      appliedAt: String(row.applied_at),
      checksum: String(row.checksum),
      name: String(row.name),
      version: Number(row.version),
    }));
  }

  private appliedMigrationDrift(applied: MetadataMigrationStatus['applied']): string[] {
    const drift: string[] = [];
    let previous = 0;
    for (const row of applied) {
      const expected = metadataMigrations.find((migration) => migration.version === row.version);
      if (!expected) {
        drift.push(`unknown metadata migration version ${row.version}`);
      } else if (expected.name !== row.name || expected.checksum !== row.checksum) {
        drift.push(`checksum/name drift for metadata migration ${row.version}`);
      }
      if (row.version <= previous) {
        drift.push(`metadata migration ledger is out of order at version ${row.version}`);
      }
      previous = row.version;
    }
    const appliedVersions = new Set(applied.map((row) => row.version));
    for (const migration of metadataMigrations) {
      if (
        appliedVersions.has(migration.version) &&
        metadataMigrations.some(
          (earlier) => earlier.version < migration.version && !appliedVersions.has(earlier.version),
        )
      ) {
        drift.push(`metadata migration ledger has a gap before version ${migration.version}`);
      }
    }
    return drift;
  }

  private assertAppliedMigrations(applied: MetadataMigrationStatus['applied']): void {
    const drift = this.appliedMigrationDrift(applied);
    if (drift.length > 0) {
      throw new Error(`metadata migration drift: ${drift.join('; ')}`);
    }
  }

  private async runMigration(
    migration: MetadataMigration,
    executor: SupportedExecutor,
  ): Promise<void> {
    const transactional = executor.kind === 'postgres';
    if (transactional) {
      await this.run('BEGIN', [], executor);
    }
    try {
      await this.applyMigration(migration.version, executor);
      await this.verifyMigration(migration.version, executor);
      await this.run(
        'INSERT INTO metadata_migrations (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)',
        [migration.version, migration.name, migration.checksum, nowIsoString()],
        executor,
      );
      if (transactional) {
        await this.run('COMMIT', [], executor);
      }
    } catch (error) {
      if (transactional) {
        await this.run('ROLLBACK', [], executor).catch(() => undefined);
      }
      throw error;
    }
  }

  private async applyMigration(version: number, executor: SupportedExecutor): Promise<void> {
    if (version === 1) {
      await this.applyBaseline(executor);
      return;
    }
    if (version === 2) {
      await this.migrateAuditEvents(executor);
      return;
    }
    if (version === 3) {
      await this.migrateActiveMempoolWatches(executor);
      return;
    }
    if (version === 4) {
      await this.dropLegacyMetadataTables(executor);
      return;
    }
    throw new Error(`unknown metadata migration version ${version}`);
  }

  private async verifyMigration(version: number, executor: SupportedExecutor): Promise<void> {
    if (version === 1) {
      await this.verifyBaseline(executor);
      return;
    }
    if (version === 2) {
      await this.requireColumns('audit_events', ['resource_ids_json'], executor);
      const [row] = await this.queryRows<{ count: number | string }>(
        'SELECT COUNT(*) AS count FROM audit_events WHERE resource_ids_json IS NULL',
        [],
        executor,
      );
      if (Number(row?.count) !== 0) {
        throw new Error('metadata schema drift: audit_events.resource_ids_json contains nulls');
      }
      return;
    }
    if (version === 3) {
      await this.requireIndexes(['idx_active_mempool_watches_api_key_id'], executor);
      return;
    }
    if (version === 4) {
      await this.verifyKnownCurrentSchema(executor);
      const remaining: string[] = [];
      for (const table of legacyMetadataTables) {
        if (await this.tableExists(table, executor)) {
          remaining.push(table);
        }
      }
      if (remaining.length > 0) {
        throw new Error(`metadata schema drift: legacy tables remain: ${remaining.join(', ')}`);
      }
      return;
    }
    throw new Error(`unknown metadata migration version ${version}`);
  }

  private async applyBaseline(executor: SupportedExecutor): Promise<void> {
    await this.migrateApiKeys(executor);
    await this.dropLegacyCoreTables(executor);
    for (const statement of activeMetadataStatements(executor.kind)) {
      await this.run(statement, [], executor);
    }
  }

  private async verifyBaseline(executor: SupportedExecutor): Promise<void> {
    const manifest: Record<string, string[]> = {
      active_mempool_watches: [
        'id',
        'api_key_id',
        'address',
        'min_value_base',
        'expires_at',
        'created_at',
      ],
      api_keys: [
        'api_key_id',
        'id',
        'secret_key_hash',
        'is_active',
        'role',
        'updated_at',
        'created_at',
      ],
      app_config: ['key', 'value_json', 'updated_at'],
      audit_events: ['audit_event_id', 'id', 'actor_api_key_id', 'created_at'],
      core_blocks: ['block_height', 'block_hash', 'raw_storage_key', 'processed_at'],
      core_indexer_state: [
        'id',
        'stage',
        'sync_tail',
        'process_tail',
        'online_tip',
        'last_error',
        'updated_at',
      ],
    };
    for (const [table, columns] of Object.entries(manifest)) {
      await this.requireColumns(table, columns, executor);
    }
    await this.requireIndexes(
      [
        'uq_api_keys_secret_key_hash',
        'idx_audit_events_created_at',
        'idx_core_blocks_block_hash',
        'idx_active_mempool_watches_address',
        'idx_active_mempool_watches_expires_at',
        'idx_active_mempool_watches_api_key_id',
      ],
      executor,
    );
  }

  private async verifyKnownCurrentSchema(executor: SupportedExecutor): Promise<void> {
    const manifests: Record<string, { allowed?: string[]; required: string[] }> = {
      active_mempool_watches: {
        required: ['id', 'api_key_id', 'address', 'min_value_base', 'expires_at', 'created_at'],
      },
      api_keys: {
        required: [
          'api_key_id',
          'id',
          'secret_key_hash',
          'is_active',
          'role',
          'updated_at',
          'created_at',
        ],
      },
      app_config: { required: ['key', 'value_json', 'updated_at'] },
      audit_events: {
        allowed: ['resource_ids'],
        required: [
          'audit_event_id',
          'id',
          'actor_api_key_id',
          'actor_api_key',
          'actor_role',
          'owner_api_key_id',
          'owner_api_key',
          'method',
          'path',
          'route',
          'operation',
          'resource_type',
          'resource_ids_json',
          'status_code',
          'outcome',
          'error',
          'request_id',
          'ip',
          'user_agent',
          'created_at',
        ],
      },
      core_blocks: {
        required: [
          'block_height',
          'block_hash',
          'previous_block_hash',
          'block_time',
          'tx_count',
          'raw_storage_key',
          'fetched_at',
          'processed_at',
        ],
      },
      core_indexer_state: {
        required: [
          'id',
          'stage',
          'sync_tail',
          'process_tail',
          'online_tip',
          'last_error',
          'updated_at',
        ],
      },
    };
    for (const [table, manifest] of Object.entries(manifests)) {
      const columns = await this.tableColumns(table, executor);
      const known = new Set([...manifest.required, ...(manifest.allowed ?? [])]);
      const missing = manifest.required.filter((column) => !columns.has(column));
      const unknown = [...columns].filter((column) => !known.has(column));
      if (missing.length > 0 || unknown.length > 0) {
        throw new Error(
          `metadata schema drift: ${table} has missing [${missing.join(', ')}] and unknown [${unknown.join(', ')}] columns; inventory it before baselining`,
        );
      }
    }
  }

  private async dropLegacyCoreTables(executor: SupportedExecutor): Promise<void> {
    await this.dropTableIfLegacyNetworkScoped('core_indexer_state', executor);
    await this.dropTableIfLegacyNetworkScoped('core_blocks', executor);
  }

  private async dropTableIfLegacyNetworkScoped(
    table: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    const columns = await this.tableColumns(table, executor);
    if (columns.has('network_id')) {
      await this.requireEmptyTableBeforeDrop(table, executor);
      await this.run(`DROP TABLE IF EXISTS ${table}`, [], executor);
    }
  }

  private async migrateApiKeys(executor: SupportedExecutor): Promise<void> {
    if (executor.kind === 'sqlite') {
      await this.migrateSqliteApiKeys(executor);
      return;
    }
    await this.run(apiKeysCreateStatement(executor.kind), [], executor);
    const columns = await this.tableColumns('api_keys', executor);
    if (!columns.has('role')) {
      await this.run(
        `ALTER TABLE api_keys ADD COLUMN role ${textType(executor.kind)} NOT NULL DEFAULT 'member'`,
        [],
        executor,
      );
    }
    if (columns.has('secret_key')) {
      const [invalid] = await this.queryRows<{ count: number | string }>(
        'SELECT COUNT(*) AS count FROM api_keys WHERE id IS NULL OR secret_key_hash IS NULL',
        [],
        executor,
      );
      if (Number(invalid?.count) !== 0) {
        throw new Error('metadata schema drift: cannot remove api_keys.secret_key safely');
      }
      await this.run('ALTER TABLE api_keys DROP COLUMN secret_key', [], executor);
    }
    await this.run(
      'CREATE UNIQUE INDEX IF NOT EXISTS uq_api_keys_secret_key_hash ON api_keys (secret_key_hash)',
      [],
      executor,
    );
  }

  private async migrateSqliteApiKeys(executor: SupportedExecutor): Promise<void> {
    const exists = await this.tableExists('api_keys', executor);
    if (!exists) {
      await this.run(apiKeysCreateStatement('sqlite'), [], executor);
      await this.run(
        'CREATE UNIQUE INDEX IF NOT EXISTS uq_api_keys_secret_key_hash ON api_keys (secret_key_hash)',
        [],
        executor,
      );
      return;
    }
    const columns = await this.tableColumns('api_keys', executor);
    if (columns.has('secret_key')) {
      await this.rebuildSqliteApiKeysWithoutSecret(columns, executor);
      return;
    }
    if (!columns.has('role')) {
      await this.run(
        "ALTER TABLE api_keys ADD COLUMN role TEXT NOT NULL DEFAULT 'member'",
        [],
        executor,
      );
    }
    await this.run(
      'CREATE UNIQUE INDEX IF NOT EXISTS uq_api_keys_secret_key_hash ON api_keys (secret_key_hash)',
      [],
      executor,
    );
  }

  private async rebuildSqliteApiKeysWithoutSecret(
    columns: Set<string>,
    executor: SupportedExecutor,
  ): Promise<void> {
    await this.run('DROP TABLE IF EXISTS api_keys_next', [], executor);
    await this.run(
      `
      CREATE TABLE api_keys_next (
        api_key_id INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT NOT NULL UNIQUE,
        secret_key_hash TEXT NOT NULL,
        is_active INTEGER NOT NULL,
        role TEXT NOT NULL DEFAULT 'member',
        updated_at TEXT NULL,
        created_at TEXT NOT NULL
      )
    `,
      [],
      executor,
    );
    await this.run(
      `
      INSERT INTO api_keys_next (
        api_key_id, id, secret_key_hash, is_active, role, updated_at, created_at
      )
      SELECT
        api_key_id,
        id,
        secret_key_hash,
        is_active,
        ${columns.has('role') ? 'role' : "'member'"},
        updated_at,
        created_at
      FROM api_keys
    `,
      [],
      executor,
    );
    await this.verifyCopiedTable('api_keys', 'api_keys_next', 'id', executor);
    await this.run('DROP TABLE api_keys', [], executor);
    await this.run('ALTER TABLE api_keys_next RENAME TO api_keys', [], executor);
    await this.run(
      'CREATE UNIQUE INDEX IF NOT EXISTS uq_api_keys_secret_key_hash ON api_keys (secret_key_hash)',
      [],
      executor,
    );
  }

  private async migrateAuditEvents(executor: SupportedExecutor): Promise<void> {
    const columns = await this.tableColumns('audit_events', executor);
    if (!columns.has('resource_ids_json')) {
      await this.addAuditEventResourceIdsJsonColumn(executor);
    }
    const nextColumns = await this.tableColumns('audit_events', executor);
    if (nextColumns.has('resource_ids')) {
      await this.run(
        `
          UPDATE audit_events
          SET resource_ids_json = resource_ids
          WHERE resource_ids_json = '[]' AND resource_ids IS NOT NULL
        `,
        [],
        executor,
      );
    }
    await this.enforceAuditEventResourceIdsJsonNotNull(executor);
  }

  private async addAuditEventResourceIdsJsonColumn(executor: SupportedExecutor): Promise<void> {
    if (executor.kind === 'sqlite') {
      await this.run(
        "ALTER TABLE audit_events ADD COLUMN resource_ids_json TEXT NOT NULL DEFAULT '[]'",
        [],
        executor,
      );
      return;
    }
    await this.run('ALTER TABLE audit_events ADD COLUMN resource_ids_json TEXT NULL', [], executor);
  }

  private async enforceAuditEventResourceIdsJsonNotNull(
    executor: SupportedExecutor,
  ): Promise<void> {
    await this.run(
      "UPDATE audit_events SET resource_ids_json = '[]' WHERE resource_ids_json IS NULL",
      [],
      executor,
    );
    if (executor.kind === 'sqlite') {
      return;
    }
    if (executor.kind === 'postgres') {
      await this.run(
        'ALTER TABLE audit_events ALTER COLUMN resource_ids_json SET NOT NULL',
        [],
        executor,
      );
      return;
    }
    await this.run('ALTER TABLE audit_events MODIFY resource_ids_json TEXT NOT NULL', [], executor);
  }

  private async migrateActiveMempoolWatches(executor: SupportedExecutor): Promise<void> {
    const exists = await this.tableExists('active_mempool_watches', executor);
    if (!exists) {
      return;
    }
    if (executor.kind === 'sqlite') {
      await this.rebuildSqliteActiveMempoolWatchesWithoutApiKeyUnique(executor);
      return;
    }
    if (executor.kind === 'postgres') {
      await this.run(
        'ALTER TABLE active_mempool_watches DROP CONSTRAINT IF EXISTS active_mempool_watches_api_key_id_key',
        [],
        executor,
      );
    } else if (await this.indexExists('api_key_id', executor)) {
      await this.run('ALTER TABLE active_mempool_watches DROP INDEX api_key_id', [], executor);
    }
    await this.run(
      'CREATE INDEX IF NOT EXISTS idx_active_mempool_watches_api_key_id ON active_mempool_watches (api_key_id)',
      [],
      executor,
    );
  }

  private async rebuildSqliteActiveMempoolWatchesWithoutApiKeyUnique(
    executor: SupportedExecutor,
  ): Promise<void> {
    const indexes = await this.queryRows<DatabaseRow>(
      "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = 'active_mempool_watches'",
      [],
      executor,
    );
    const hasApiKeyUnique = indexes.some((row) => {
      const sql = String(row.sql ?? '');
      return /api_key_id/u.test(sql) && /UNIQUE/iu.test(sql);
    });
    const [tableSql] = await this.queryRows<DatabaseRow>(
      "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'active_mempool_watches' LIMIT 1",
      [],
      executor,
    );
    const inlineUnique = /api_key_id[^,\n]*UNIQUE/iu.test(String(tableSql?.sql ?? ''));
    if (!hasApiKeyUnique && !inlineUnique) {
      await this.run(
        'CREATE INDEX IF NOT EXISTS idx_active_mempool_watches_api_key_id ON active_mempool_watches (api_key_id)',
        [],
        executor,
      );
      return;
    }
    await this.run('DROP TABLE IF EXISTS active_mempool_watches_next', [], executor);
    await this.run(
      `
      CREATE TABLE active_mempool_watches_next (
        id TEXT PRIMARY KEY,
        api_key_id TEXT NOT NULL,
        address TEXT NOT NULL,
        min_value_base TEXT NULL,
        expires_at TEXT NOT NULL,
        created_at TEXT NOT NULL
      )
    `,
      [],
      executor,
    );
    await this.run(
      `
      INSERT INTO active_mempool_watches_next (
        id, api_key_id, address, min_value_base, expires_at, created_at
      )
      SELECT id, api_key_id, address, min_value_base, expires_at, created_at
      FROM active_mempool_watches
    `,
      [],
      executor,
    );
    await this.verifyCopiedTable(
      'active_mempool_watches',
      'active_mempool_watches_next',
      'id',
      executor,
    );
    await this.run('DROP TABLE active_mempool_watches', [], executor);
    await this.run(
      'ALTER TABLE active_mempool_watches_next RENAME TO active_mempool_watches',
      [],
      executor,
    );
    for (const statement of [
      'CREATE INDEX IF NOT EXISTS idx_active_mempool_watches_address ON active_mempool_watches (address)',
      'CREATE INDEX IF NOT EXISTS idx_active_mempool_watches_expires_at ON active_mempool_watches (expires_at)',
      'CREATE INDEX IF NOT EXISTS idx_active_mempool_watches_api_key_id ON active_mempool_watches (api_key_id)',
    ]) {
      await this.run(statement, [], executor);
    }
  }

  private async dropLegacyMetadataTables(executor: SupportedExecutor): Promise<void> {
    for (const table of legacyMetadataTables) {
      if (await this.tableExists(table, executor)) {
        await this.requireEmptyTableBeforeDrop(table, executor);
        await this.run(`DROP TABLE ${table}`, [], executor);
      }
    }
  }

  private async verifyCopiedTable(
    source: string,
    target: string,
    key: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    const [counts] = await this.queryRows<{
      source_count: number | string;
      target_count: number | string;
    }>(
      `SELECT (SELECT COUNT(*) FROM ${source}) AS source_count, (SELECT COUNT(*) FROM ${target}) AS target_count`,
      [],
      executor,
    );
    if (Number(counts?.source_count) !== Number(counts?.target_count)) {
      throw new Error(`metadata migration copy count mismatch for ${source}`);
    }
    const [missing] = await this.queryRows<{ count: number | string }>(
      `SELECT COUNT(*) AS count FROM ${source} source LEFT JOIN ${target} target ON target.${key} = source.${key} WHERE target.${key} IS NULL`,
      [],
      executor,
    );
    if (Number(missing?.count) !== 0) {
      throw new Error(`metadata migration copy key mismatch for ${source}`);
    }
  }

  private async requireEmptyTableBeforeDrop(
    table: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    const [row] = await this.queryRows<{ count: number | string }>(
      `SELECT COUNT(*) AS count FROM ${table}`,
      [],
      executor,
    );
    if (Number(row?.count) !== 0) {
      throw new Error(
        `metadata schema drift: refusing to drop non-empty legacy table ${table}; inventory it before baselining`,
      );
    }
  }

  private async requireColumns(
    table: string,
    required: string[],
    executor: SupportedExecutor,
  ): Promise<void> {
    const columns = await this.tableColumns(table, executor);
    const missing = required.filter((column) => !columns.has(column));
    if (missing.length > 0) {
      throw new Error(`metadata schema drift: ${table} is missing columns ${missing.join(', ')}`);
    }
  }

  private async requireIndexes(indexes: string[], executor: SupportedExecutor): Promise<void> {
    const missing: string[] = [];
    for (const index of indexes) {
      if (!(await this.indexExists(index, executor))) {
        missing.push(index);
      }
    }
    if (missing.length > 0) {
      throw new Error(`metadata schema drift: missing indexes ${missing.join(', ')}`);
    }
  }

  private async indexExists(index: string, executor: SupportedExecutor): Promise<boolean> {
    if (executor.kind === 'sqlite') {
      const rows = await this.queryRows<DatabaseRow>(
        "SELECT 1 AS present FROM sqlite_master WHERE type = 'index' AND name = ? LIMIT 1",
        [index],
        executor,
      );
      return rows.length === 1;
    }
    const sql =
      executor.kind === 'postgres'
        ? `
          SELECT 1 AS present FROM pg_indexes
          WHERE schemaname = current_schema() AND indexname = ? LIMIT 1
        `
        : `
          SELECT 1 AS present FROM information_schema.statistics
          WHERE table_schema = DATABASE() AND index_name = ? LIMIT 1
        `;
    return (await this.queryRows<DatabaseRow>(sql, [index], executor)).length === 1;
  }

  private async tableExists(
    table: string,
    executor: SupportedExecutor = this.client,
  ): Promise<boolean> {
    if (executor.kind === 'sqlite') {
      const rows = await this.queryRows<DatabaseRow>(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        [table],
        executor,
      );
      return rows.length === 1;
    }
    if (executor.kind === 'postgres') {
      const rows = await this.queryRows<DatabaseRow>(
        `
          SELECT 1 AS present
          FROM information_schema.tables
          WHERE table_schema = current_schema() AND table_name = ?
          LIMIT 1
        `,
        [table],
        executor,
      );
      return rows.length === 1;
    }
    const rows = await this.queryRows<DatabaseRow>(
      `
        SELECT 1 AS present
        FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = ?
        LIMIT 1
      `,
      [table],
      executor,
    );
    return rows.length === 1;
  }

  private async tableColumns(
    table: string,
    executor: SupportedExecutor = this.client,
  ): Promise<Set<string>> {
    if (executor.kind === 'sqlite') {
      const rows = await this.queryRows<DatabaseRow>(`PRAGMA table_info(${table})`, [], executor);
      return new Set(rows.map((row) => String(row.name)));
    }
    if (executor.kind === 'postgres') {
      const rows = await this.queryRows<DatabaseRow>(
        `
          SELECT column_name
          FROM information_schema.columns
          WHERE table_schema = current_schema() AND table_name = ?
        `,
        [table],
        executor,
      );
      return new Set(rows.map((row) => String(row.column_name)));
    }
    const rows = await this.queryRows<DatabaseRow>(
      `
        SELECT COLUMN_NAME AS column_name
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = ?
      `,
      [table],
      executor,
    );
    return new Set(rows.map((row) => String(row.column_name)));
  }

  private async one<T extends DatabaseRow>(sql: string, args: SqlValue[] = []): Promise<T | null> {
    const [row] = await this.queryRows<T>(sql, args);
    return row ?? null;
  }

  private async queryRows<T extends DatabaseRow>(
    sql: string,
    args: SqlValue[] = [],
    executor: SupportedExecutor = this.client,
  ): Promise<T[]> {
    await this.ensureSchema(executor);
    return this.metadataQuery(async () => {
      if (executor.kind === 'sqlite') {
        const result = await executor.raw.execute({ sql, args });
        return result.rows.map((row) => Object.fromEntries(Object.entries(row)) as T);
      }
      if (executor.kind === 'postgres') {
        const result = await executor.raw.query(compileQuery('postgres', sql), args);
        return result.rows as T[];
      }

      const [rows] = await executor.raw.query(
        compileQuery('mysql', compileMysqlStatement(sql)),
        args,
      );
      return rows as T[];
    });
  }

  private async run(
    sql: string,
    args: SqlValue[] = [],
    executor: SupportedExecutor = this.client,
  ): Promise<void> {
    await this.ensureSchema(executor);
    await this.metadataQuery(async () => {
      if (executor.kind === 'sqlite') {
        await executor.raw.execute({ sql, args });
        return;
      }
      if (executor.kind === 'postgres') {
        await executor.raw.query(compileQuery('postgres', sql), args);
        return;
      }

      const mysqlSql = compileMysqlStatement(sql);
      try {
        await executor.raw.query(compileQuery('mysql', mysqlSql), args);
      } catch (error) {
        if (isDuplicateMysqlIndexError(error) && mysqlSql !== sql) {
          return;
        }
        throw error;
      }
    });
  }

  private async mutate(
    sql: string,
    args: SqlValue[] = [],
    executor: SupportedExecutor = this.client,
  ): Promise<number> {
    await this.ensureSchema(executor);
    return this.metadataQuery(async () => {
      if (executor.kind === 'sqlite') {
        const result = await executor.raw.execute({ sql, args });
        return result.rowsAffected;
      }
      if (executor.kind === 'postgres') {
        const result = await executor.raw.query(compileQuery('postgres', sql), args);
        return result.rowCount ?? 0;
      }

      const [result] = await executor.raw.query(compileQuery('mysql', sql), args);
      return 'affectedRows' in result ? result.affectedRows : 0;
    });
  }

  private async ensureSchema(executor: SupportedExecutor): Promise<void> {
    if (this.schemaReady || this.migrating || executor !== this.client) {
      return;
    }

    this.migratePromise ??= this.migrate().finally(() => {
      if (!this.schemaReady) {
        this.migratePromise = null;
      }
    });
    await this.migratePromise;
  }

  private async postgresConnect(): Promise<PoolClient> {
    if (this.client.kind !== 'postgres') {
      throw new TypeError('expected postgres metadata client');
    }

    const pool = this.client.raw;
    return this.metadataQuery(() => pool.connect());
  }

  private async mysqlConnect(): Promise<mysql.PoolConnection> {
    if (this.client.kind !== 'mysql') {
      throw new TypeError('expected mysql metadata client');
    }

    const pool = this.client.raw;
    return this.metadataQuery(() => pool.getConnection());
  }

  private async metadataQuery<T>(work: () => Promise<T>): Promise<T> {
    try {
      return await work();
    } catch (error) {
      throw this.toMetadataInfrastructureError(error);
    }
  }

  private toMetadataInfrastructureError(error: unknown): Error {
    if (error instanceof OnlyDogeError) {
      return error;
    }

    return new InfrastructureError(metadataInfrastructureMessage(error), { cause: error });
  }

  private booleanCondition(column: string, expected: boolean): string {
    return `${column} = ${this.booleanValue(expected)}`;
  }

  private booleanValue(value: boolean): SqlValue {
    return this.client.kind === 'postgres' ? value : value ? 1 : 0;
  }

  private mapApiKey(row: DatabaseRow): ApiKeyRecord {
    return {
      apiKeyId: Number(row.api_key_id),
      id: String(row.id),
      secretKeyHash: String(row.secret_key_hash),
      isActive: toBoolean(row.is_active),
      role: row.role === 'admin' ? 'admin' : 'member',
      updatedAt: nullableText(row.updated_at),
      createdAt: String(row.created_at),
    };
  }

  private mapAuditEvent(row: DatabaseRow): AuditEventRecord {
    return {
      auditEventId: Number(row.audit_event_id),
      id: String(row.id),
      actorApiKeyId: Number(row.actor_api_key_id),
      actorApiKey: String(row.actor_api_key),
      actorRole: row.actor_role === 'admin' ? 'admin' : 'member',
      ownerApiKeyId: nullableNumberValue(row.owner_api_key_id),
      ownerApiKey: nullableText(row.owner_api_key),
      method: String(row.method),
      path: String(row.path),
      route: String(row.route),
      operation: String(row.operation),
      resourceType: String(row.resource_type),
      resourceIds: safeJsonParse<string[]>(String(row.resource_ids_json), []),
      statusCode: Number(row.status_code),
      outcome: auditOutcome(String(row.outcome)),
      error: nullableText(row.error),
      requestId: String(row.request_id),
      ip: nullableText(row.ip),
      userAgent: nullableText(row.user_agent),
      createdAt: String(row.created_at),
    };
  }
}

const metadataMigrations: readonly MetadataMigration[] = [
  immutableMigration(
    1,
    'baseline_current_metadata_schema',
    'api_keys,audit_events,app_config,core_indexer_state,core_blocks,active_mempool_watches;required indexes;guarded legacy core replacement',
  ),
  immutableMigration(
    2,
    'audit_resource_ids_json',
    'add audit_events.resource_ids_json;copy resource_ids;replace nulls;enforce not null',
  ),
  immutableMigration(
    3,
    'non_unique_active_watch_api_key',
    'remove api_key_id uniqueness;sqlite copy-count-key-verify-swap;create lookup index',
  ),
  immutableMigration(
    4,
    'remove_empty_legacy_metadata_tables',
    `require empty then drop:${legacyMetadataTables.join(',')}`,
  ),
] as const;

function immutableMigration(
  version: number,
  name: string,
  canonicalDefinition: string,
): MetadataMigration {
  return {
    checksum: createHash('sha256')
      .update(`${version}\n${name}\n${canonicalDefinition}`)
      .digest('hex'),
    name,
    version,
  };
}

function validateMigrationDefinitions(migrations: readonly MetadataMigration[]): void {
  const versions = new Set<number>();
  let previous = 0;
  for (const migration of migrations) {
    if (!Number.isSafeInteger(migration.version) || migration.version <= previous) {
      throw new Error(`metadata migration definitions are out of order at ${migration.version}`);
    }
    if (versions.has(migration.version)) {
      throw new Error(`duplicate metadata migration version ${migration.version}`);
    }
    versions.add(migration.version);
    previous = migration.version;
  }
}

function migrationSummary(migration: MetadataMigration): {
  checksum: string;
  name: string;
  version: number;
} {
  return {
    checksum: migration.checksum,
    name: migration.name,
    version: migration.version,
  };
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function activeMetadataStatements(kind: SupportedClient['kind']): string[] {
  return [
    apiKeysCreateStatement(kind),
    `
      CREATE TABLE IF NOT EXISTS audit_events (
        audit_event_id ${primaryKeySql(kind)},
        id ${textType(kind)} NOT NULL UNIQUE,
        actor_api_key_id BIGINT NOT NULL,
        actor_api_key ${textType(kind)} NOT NULL,
        actor_role ${textType(kind)} NOT NULL,
        owner_api_key_id BIGINT NULL,
        owner_api_key ${textType(kind)} NULL,
        method ${textType(kind)} NOT NULL,
        path ${textType(kind)} NOT NULL,
        route ${textType(kind)} NOT NULL,
        operation ${textType(kind)} NOT NULL,
        resource_type ${textType(kind)} NOT NULL,
        resource_ids_json ${longTextType(kind)} NOT NULL,
        status_code INTEGER NOT NULL,
        outcome ${textType(kind)} NOT NULL,
        error ${longTextType(kind)} NULL,
        request_id ${textType(kind)} NOT NULL,
        ip ${textType(kind)} NULL,
        user_agent ${textType(kind)} NULL,
        created_at ${textType(kind)} NOT NULL
      )
    `,
    `
      CREATE TABLE IF NOT EXISTS app_config (
        key ${textType(kind)} PRIMARY KEY,
        value_json ${longTextType(kind)} NOT NULL,
        updated_at ${textType(kind)} NOT NULL
      )
    `,
    `
      CREATE TABLE IF NOT EXISTS core_indexer_state (
        id INTEGER PRIMARY KEY,
        stage ${textType(kind)} NOT NULL,
        sync_tail BIGINT NOT NULL,
        process_tail BIGINT NOT NULL,
        online_tip BIGINT NOT NULL,
        last_error ${textType(kind)} NULL,
        updated_at ${textType(kind)} NOT NULL
      )
    `,
    `
      CREATE TABLE IF NOT EXISTS core_blocks (
        block_height BIGINT PRIMARY KEY,
        block_hash ${textType(kind)} NOT NULL,
        previous_block_hash ${textType(kind)} NULL,
        block_time BIGINT NOT NULL,
        tx_count BIGINT NOT NULL,
        raw_storage_key ${textType(kind)} NOT NULL,
        fetched_at ${textType(kind)} NOT NULL,
        processed_at ${textType(kind)} NULL
      )
    `,
    `
      CREATE TABLE IF NOT EXISTS active_mempool_watches (
        id ${textType(kind)} PRIMARY KEY,
        api_key_id ${textType(kind)} NOT NULL,
        address ${textType(kind)} NOT NULL,
        min_value_base ${textType(kind)} NULL,
        expires_at ${textType(kind)} NOT NULL,
        created_at ${textType(kind)} NOT NULL
      )
    `,
    'CREATE UNIQUE INDEX IF NOT EXISTS uq_api_keys_secret_key_hash ON api_keys (secret_key_hash)',
    'CREATE INDEX IF NOT EXISTS idx_audit_events_created_at ON audit_events (created_at)',
    'CREATE INDEX IF NOT EXISTS idx_core_blocks_block_hash ON core_blocks (block_hash)',
    'CREATE INDEX IF NOT EXISTS idx_active_mempool_watches_address ON active_mempool_watches (address)',
    'CREATE INDEX IF NOT EXISTS idx_active_mempool_watches_expires_at ON active_mempool_watches (expires_at)',
    'CREATE INDEX IF NOT EXISTS idx_active_mempool_watches_api_key_id ON active_mempool_watches (api_key_id)',
  ];
}

function apiKeysCreateStatement(kind: SupportedClient['kind']): string {
  return `
    CREATE TABLE IF NOT EXISTS api_keys (
      api_key_id ${primaryKeySql(kind)},
      id ${textType(kind)} NOT NULL UNIQUE,
      secret_key_hash ${textType(kind)} NOT NULL,
      is_active ${booleanType(kind)} NOT NULL,
      role ${textType(kind)} NOT NULL DEFAULT 'member',
      updated_at ${textType(kind)} NULL,
      created_at ${textType(kind)} NOT NULL
    )
  `;
}

function primaryKeySql(kind: SupportedClient['kind']): string {
  if (kind === 'postgres') {
    return 'BIGSERIAL PRIMARY KEY';
  }
  if (kind === 'mysql') {
    return 'BIGINT AUTO_INCREMENT PRIMARY KEY';
  }
  return 'INTEGER PRIMARY KEY AUTOINCREMENT';
}

function textType(kind: SupportedClient['kind']): string {
  return kind === 'mysql' ? 'VARCHAR(768)' : 'TEXT';
}

function longTextType(_kind: SupportedClient['kind']): string {
  return 'TEXT';
}

function booleanType(kind: SupportedClient['kind']): string {
  return kind === 'postgres' ? 'BOOLEAN' : 'INTEGER';
}

function auditEventQuery(filters: AuditEventFilters & { actorApiKeyId?: PrimaryId }): {
  params: SqlValue[];
  sql: string;
} {
  const clauses: string[] = [];
  const params: SqlValue[] = [];
  appendFilter(clauses, params, 'actor_api_key = ?', filters.actor);
  appendFilter(clauses, params, 'actor_api_key_id = ?', filters.actorApiKeyId);
  appendFilter(clauses, params, 'created_at >= ?', filters.from);
  appendFilter(clauses, params, 'created_at <= ?', filters.to);
  appendFilter(clauses, params, 'method = ?', filters.method);
  appendFilter(clauses, params, 'outcome = ?', filters.outcome);
  appendFilter(clauses, params, 'resource_type = ?', filters.resourceType);
  appendFilter(clauses, params, 'status_code = ?', filters.statusCode);
  if (filters.resourceId) {
    clauses.push('resource_ids_json LIKE ?');
    params.push(`%${filters.resourceId}%`);
  }

  const limit = filters.limit ?? 100;
  const offset = filters.offset ?? 0;
  return {
    sql: `
      SELECT *
      FROM audit_events
      ${clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : ''}
      ORDER BY created_at DESC, audit_event_id DESC
      LIMIT ?
      ${offset > 0 ? 'OFFSET ?' : ''}
    `,
    params: [...params, limit, ...(offset > 0 ? [offset] : [])],
  };
}

function appendFilter(
  clauses: string[],
  params: SqlValue[],
  clause: string,
  value: SqlValue | undefined,
): void {
  if (value === undefined || value === null || value === '') {
    return;
  }

  clauses.push(clause);
  params.push(value);
}

function mapCoreIndexerState(row: DatabaseRow): CoreIndexerState {
  return {
    stage: coreIndexerStage(String(row.stage)),
    syncTail: Number(row.sync_tail),
    processTail: Number(row.process_tail),
    onlineTip: Number(row.online_tip),
    lastError: nullableText(row.last_error),
    updatedAt: String(row.updated_at),
  };
}

function coreIndexerStage(value: string): CoreIndexerStage {
  if (value === 'process_backfill' || value === 'online') {
    return value;
  }
  return 'sync_backfill';
}

function coreBlockParams(record: CoreBlockRecord): SqlValue[] {
  return [
    record.blockHeight,
    record.blockHash,
    record.previousBlockHash,
    record.blockTime,
    record.txCount,
    record.rawStorageKey,
    record.fetchedAt,
    record.processedAt,
  ];
}

function createPostgresPool(settings: DatabaseSettings): Pool {
  const pool = new Pool(postgresPoolOptions(settings));
  pool.on('error', (error) => {
    createLogger({ component: 'metadata', service: 'onlydoge' }).error(
      { err: error },
      'metadata postgres pool error',
    );
  });
  return pool;
}

function createMysqlPool(location: string): mysql.Pool {
  return mysql.createPool(location);
}

function postgresPoolOptions(settings: DatabaseSettings): ConstructorParameters<typeof Pool>[0] {
  return {
    connectionString: settings.location,
    connectionTimeoutMillis: 5_000,
    ...(settings.ssl ? { ssl: settings.ssl } : {}),
  };
}

function placeholders(count: number): string {
  return Array.from({ length: count }, () => '?').join(', ');
}

function countRowValue(row: { count: number | string } | null): number {
  if (!row) {
    return 0;
  }

  return Number(row.count);
}

function assertFound<T>(value: T | null): T {
  if (!value) {
    throw new Error('expected inserted row');
  }

  return value;
}

function isSqliteBusyError(error: unknown): boolean {
  return (
    typeof error === 'object' && error !== null && 'code' in error && error.code === 'SQLITE_BUSY'
  );
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function nullableText(value: SqlValue | undefined): string | null {
  return value == null ? null : String(value);
}

function nullableNumberValue(value: SqlValue | undefined): number | null {
  return value == null ? null : Number(value);
}

function auditOutcome(value: string): AuditEventRecord['outcome'] {
  return auditOutcomeValues.has(value as AuditEventRecord['outcome'])
    ? (value as AuditEventRecord['outcome'])
    : 'failure';
}

function compileMysqlStatement(sql: string): string {
  return sql
    .replace(/CREATE\s+(UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS/iu, 'CREATE $1INDEX')
    .replace(/^(\s*)key(?=\s+(?:VARCHAR|TEXT))/gimu, '$1`key`')
    .replace(/\(\s*key(?=\s*,)/giu, '(`key`')
    .replace(/\bWHERE\s+key\b/giu, 'WHERE `key`');
}

function isDuplicateMysqlIndexError(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    error.code === 'ER_DUP_KEYNAME'
  );
}

function mapActiveMempoolWatch(row: DatabaseRow): ActiveMempoolWatch {
  return {
    id: String(row.id),
    apiKeyId: String(row.api_key_id),
    address: String(row.address),
    minValueBase: nullableText(row.min_value_base),
    expiresAt: String(row.expires_at),
    createdAt: String(row.created_at),
  };
}

function advisoryLockKey(value: string): number {
  let hash = 0;
  for (const char of value) {
    hash = (hash * 31 + char.charCodeAt(0)) | 0;
  }
  return hash === 0 ? 1 : hash;
}
