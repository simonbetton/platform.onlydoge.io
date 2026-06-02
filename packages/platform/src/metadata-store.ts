import { randomUUID } from 'node:crypto';
import { mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { createClient as createLibsqlClient, type Client as LibsqlClient } from '@libsql/client';
import type {
  ApiKeyRecord,
  ApiKeyRepository,
  AuditEventFilters,
  AuditEventRecord,
  AuditEventRepository,
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
import { nowIsoString, type PrimaryId, safeJsonParse } from '@onlydoge/shared-kernel';
import mysql from 'mysql2/promise';
import { Pool, type PoolClient } from 'pg';

import { compileQuery, type SqlValue, toBoolean } from './metadata-query';
import type { DatabaseSettings } from './settings';

type SupportedClient =
  | { kind: 'sqlite'; raw: LibsqlClient }
  | { kind: 'postgres'; raw: Pool }
  | { kind: 'mysql'; raw: mysql.Pool };

type SupportedExecutor =
  | SupportedClient
  | { kind: 'postgres'; raw: PoolClient }
  | { kind: 'mysql'; raw: mysql.PoolConnection };

type DatabaseRow = Record<string, SqlValue>;

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
  implements ApiKeyRepository, AuditEventRepository, CoordinatorConfigPort, ProjectionStateStorePort
{
  private constructor(private readonly client: SupportedClient) {}

  public static async connect(settings: DatabaseSettings): Promise<RelationalMetadataStore> {
    const store = await RelationalMetadataStore.open(settings);
    await store.migrate();
    return store;
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
        raw: new Pool(postgresPoolOptions(settings)),
      });
    }

    return new RelationalMetadataStore({ kind: 'mysql', raw: mysql.createPool(settings.location) });
  }

  public async countApiKeys(): Promise<number> {
    const row = await this.one<{ count: number | string }>(
      'SELECT COUNT(*) AS count FROM api_keys',
    );
    return countRowValue(row);
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
    await this.execute(
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
    );

    return this.getApiKeyById(record.id).then(assertFound);
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
        JSON.stringify(input.resourceIds),
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

  public async listAuditEvents(
    filters: AuditEventFilters & { actorApiKeyId?: PrimaryId },
  ): Promise<AuditEventRecord[]> {
    const query = auditEventQuery(filters);
    const rows = await this.queryRows<DatabaseRow>(query.sql, query.params);
    return rows.map((row) => this.mapAuditEvent(row));
  }

  public async compareAndSwapJsonValue<T>(
    key: string,
    expectedValue: T | null,
    nextValue: T,
  ): Promise<boolean> {
    const current = await this.getJsonValue<T>(key);
    if (JSON.stringify(current) !== JSON.stringify(expectedValue)) {
      return false;
    }

    await this.setJsonValue(key, nextValue);
    return true;
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

  private async migrate(): Promise<void> {
    await this.withMigrationLock(async () => {
      await this.migrateApiKeys();
      await this.dropLegacyCoreTables();
      for (const statement of activeMetadataStatements(this.client.kind)) {
        await this.execute(statement);
      }
      await this.dropLegacyMetadataTables();
    });
  }

  private async withMigrationLock(work: () => Promise<void>): Promise<void> {
    if (this.client.kind === 'sqlite') {
      await work();
      return;
    }

    await this.acquireMigrationLock();
    try {
      await work();
    } finally {
      await this.releaseMigrationLock();
    }
  }

  private async acquireMigrationLock(): Promise<void> {
    if (this.client.kind === 'postgres') {
      await this.execute('SELECT pg_advisory_lock(762319045)');
      return;
    }

    await this.execute("SELECT GET_LOCK('onlydoge_metadata_migrate', 60)");
  }

  private async releaseMigrationLock(): Promise<void> {
    if (this.client.kind === 'postgres') {
      await this.execute('SELECT pg_advisory_unlock(762319045)');
      return;
    }

    await this.execute("SELECT RELEASE_LOCK('onlydoge_metadata_migrate')");
  }

  private async dropLegacyCoreTables(): Promise<void> {
    await this.dropTableIfLegacyNetworkScoped('core_indexer_state');
    await this.dropTableIfLegacyNetworkScoped('core_blocks');
  }

  private async dropTableIfLegacyNetworkScoped(table: string): Promise<void> {
    const columns = await this.tableColumns(table);
    if (columns.has('network_id')) {
      await this.execute(`DROP TABLE IF EXISTS ${table}`);
    }
  }

  private async migrateApiKeys(): Promise<void> {
    if (this.client.kind === 'sqlite') {
      await this.migrateSqliteApiKeys();
      return;
    }

    await this.execute(apiKeysCreateStatement(this.client.kind));
    await this.execute(
      'CREATE UNIQUE INDEX IF NOT EXISTS uq_api_keys_secret_key_hash ON api_keys (secret_key_hash)',
    );
  }

  private async migrateSqliteApiKeys(): Promise<void> {
    const exists = await this.sqliteTableExists('api_keys');
    if (!exists) {
      await this.execute(apiKeysCreateStatement('sqlite'));
      await this.execute(
        'CREATE UNIQUE INDEX IF NOT EXISTS uq_api_keys_secret_key_hash ON api_keys (secret_key_hash)',
      );
      return;
    }

    const columns = await this.sqliteTableColumns('api_keys');
    if (columns.has('secret_key')) {
      await this.rebuildSqliteApiKeysWithoutSecret(columns);
      return;
    }

    if (!columns.has('role')) {
      await this.execute("ALTER TABLE api_keys ADD COLUMN role TEXT NOT NULL DEFAULT 'member'");
    }
    await this.execute(
      'CREATE UNIQUE INDEX IF NOT EXISTS uq_api_keys_secret_key_hash ON api_keys (secret_key_hash)',
    );
  }

  private async rebuildSqliteApiKeysWithoutSecret(columns: Set<string>): Promise<void> {
    await this.execute(`
      CREATE TABLE api_keys_next (
        api_key_id INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT NOT NULL UNIQUE,
        secret_key_hash TEXT NOT NULL,
        is_active INTEGER NOT NULL,
        role TEXT NOT NULL DEFAULT 'member',
        updated_at TEXT NULL,
        created_at TEXT NOT NULL
      )
    `);
    await this.execute(`
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
    `);
    await this.execute('DROP TABLE api_keys');
    await this.execute('ALTER TABLE api_keys_next RENAME TO api_keys');
    await this.execute(
      'CREATE UNIQUE INDEX IF NOT EXISTS uq_api_keys_secret_key_hash ON api_keys (secret_key_hash)',
    );
  }

  private async dropLegacyMetadataTables(): Promise<void> {
    for (const table of legacyMetadataTables) {
      await this.execute(`DROP TABLE IF EXISTS ${table}`);
    }
  }

  private async sqliteTableExists(table: string): Promise<boolean> {
    const row = await this.one<DatabaseRow>(
      "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
      [table],
    );
    return Boolean(row);
  }

  private async sqliteTableColumns(table: string): Promise<Set<string>> {
    const rows = await this.queryRows<DatabaseRow>(`PRAGMA table_info(${table})`);
    return new Set(rows.map((row) => String(row.name)));
  }

  private async tableColumns(table: string): Promise<Set<string>> {
    if (this.client.kind === 'sqlite') {
      return this.sqliteTableColumns(table);
    }
    if (this.client.kind === 'postgres') {
      const rows = await this.queryRows<DatabaseRow>(
        `
          SELECT column_name
          FROM information_schema.columns
          WHERE table_schema = current_schema() AND table_name = ?
        `,
        [table],
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
    if (executor.kind === 'sqlite') {
      const result = await executor.raw.execute({ sql, args });
      return result.rows.map((row) => Object.fromEntries(Object.entries(row)) as T);
    }
    if (executor.kind === 'postgres') {
      const result = await executor.raw.query(compileQuery('postgres', sql), args);
      return result.rows as T[];
    }

    const [rows] = await executor.raw.query(compileQuery('mysql', sql), args);
    return rows as T[];
  }

  private async run(
    sql: string,
    args: SqlValue[] = [],
    executor: SupportedExecutor = this.client,
  ): Promise<void> {
    if (executor.kind === 'sqlite') {
      await executor.raw.execute({ sql, args });
      return;
    }
    if (executor.kind === 'postgres') {
      await executor.raw.query(compileQuery('postgres', sql), args);
      return;
    }

    await executor.raw.query(compileQuery('mysql', sql), args);
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
        resource_ids_json ${textType(kind)} NOT NULL,
        status_code INTEGER NOT NULL,
        outcome ${textType(kind)} NOT NULL,
        error ${textType(kind)} NULL,
        request_id ${textType(kind)} NOT NULL,
        ip ${textType(kind)} NULL,
        user_agent ${textType(kind)} NULL,
        created_at ${textType(kind)} NOT NULL
      )
    `,
    `
      CREATE TABLE IF NOT EXISTS app_config (
        key ${textType(kind)} PRIMARY KEY,
        value_json ${textType(kind)} NOT NULL,
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
    'CREATE UNIQUE INDEX IF NOT EXISTS uq_api_keys_secret_key_hash ON api_keys (secret_key_hash)',
    'CREATE INDEX IF NOT EXISTS idx_audit_events_created_at ON audit_events (created_at)',
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
    return 'BIGINT PRIMARY KEY AUTO_INCREMENT';
  }
  return 'INTEGER PRIMARY KEY AUTOINCREMENT';
}

function textType(kind: SupportedClient['kind']): string {
  return kind === 'mysql' ? 'TEXT' : 'TEXT';
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

function postgresPoolOptions(settings: DatabaseSettings): ConstructorParameters<typeof Pool>[0] {
  return {
    connectionString: settings.location,
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
