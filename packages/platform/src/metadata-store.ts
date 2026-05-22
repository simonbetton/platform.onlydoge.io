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
import type {
  AddressRecord,
  AddressRepository,
  ConfigMutationPort,
  EntityRecord,
  EntityRepository,
  EntityTagRepository,
  NetworkReader,
  TagRecord,
  TagRepository,
} from '@onlydoge/entity-labeling';
import {
  applyDirectLinkDeltasToSnapshots,
  type BlockProjectionBatch,
  buildProjectionStateChanges,
  type CoordinatorConfigPort,
  type CoreBlockRecord,
  type CoreDogecoinApplyContext,
  type CoreDogecoinApplyResult,
  type CoreDogecoinBlockApplication,
  type CoreDogecoinStateStorePort,
  type CoreIndexerStage,
  type CoreIndexerState,
  collectProjectionDirectLinkSnapshotKeys,
  configKeyDogecoinHistoryReady,
  configKeyNewlyAddedAddress,
  configKeyProjectionBootstrapTail,
  type DirectLinkDelta,
  type DirectLinkRecord,
  formatAmountBase,
  type IndexedNetworkPort,
  type ProjectionAppliedBlock,
  type ProjectionBalanceSnapshot,
  type ProjectionDirectLinkBatch,
  type ProjectionLinkSeedPort,
  type ProjectionStateBootstrapSnapshot,
  type ProjectionStateStorePort,
  type ProjectionUtxoOutput,
  parseAmountBase,
  parseProjectionDirectLinkSnapshotKey,
  projectionBalanceSnapshotKey,
  projectionBlockIdentity,
  projectionDirectLinkSnapshotKey,
  resolvePendingProjectionWindow,
  type SourceLinkRecord,
  toProjectionAppliedBlocks,
} from '@onlydoge/indexing-pipeline';
import type {
  ConfigReader,
  InvestigationMetadataPort,
  InvestigationWarehousePort,
} from '@onlydoge/investigation-query';
import type {
  NetworkRecord,
  NetworkRepository,
  TokenRecord,
  TokenRepository,
} from '@onlydoge/network-catalog';
import {
  nowIsoString,
  type PrimaryId,
  parseChainFamily,
  parseRiskLevel,
  safeJsonParse,
} from '@onlydoge/shared-kernel';
import { drizzle as drizzleLibsql } from 'drizzle-orm/libsql';
import { drizzle as drizzleMysql } from 'drizzle-orm/mysql2';
import { drizzle as drizzlePg } from 'drizzle-orm/node-postgres';
import mysql from 'mysql2/promise';
import { Pool, type PoolClient } from 'pg';

import {
  compileQuery,
  currentAddressSummary,
  nullableNumber,
  nullableString,
  type SqlValue,
  sqlLimitClause,
  sqlNullableOffsetClause,
  sqlNullablePaginationParams,
  sqlOffsetClause,
  sqlPaginationParams,
  toBoolean,
} from './metadata-query';
import type { DatabaseSettings } from './settings';

type SupportedClient =
  | { kind: 'sqlite'; raw: LibsqlClient }
  | { kind: 'postgres'; raw: Pool }
  | { kind: 'mysql'; raw: mysql.Pool };

type SupportedExecutor =
  | SupportedClient
  | { kind: 'sqlite'; raw: LibsqlClient }
  | { kind: 'postgres'; raw: PoolClient }
  | { kind: 'mysql'; raw: mysql.PoolConnection };

type DatabaseRow = Record<string, SqlValue>;
type CoreDogecoinSpend = CoreDogecoinBlockApplication['utxoSpends'][number];

interface CoreUtxoMutation {
  affectedAddresses: Set<string>;
  nextOutputs: Map<string, ProjectionUtxoOutput>;
}

interface UtxoUpdateClauses {
  mysql: string;
  standard: string;
}

interface CoreBalanceAccumulator {
  balance: bigint;
  utxoCount: number;
}

interface CoreBalanceRow {
  address: string;
  asOfBlockHeight: number;
  assetAddress: string;
  balance: string;
  networkId: PrimaryId;
  utxoCount: number;
}

export interface CoreIndexerStateUpdate {
  lastError?: string | null;
  networkId: PrimaryId;
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

const utxoOutputColumns = `
  network_id, output_key, block_height, block_hash, block_time, txid, tx_index, vout,
  address, script_type, value_base, is_coinbase, is_spendable,
  spent_by_txid, spent_in_block, spent_input_index, updated_at
`;
const projectionUtxoUpdateClauses: UtxoUpdateClauses = {
  mysql: `
    block_height = VALUES(block_height),
    block_hash = VALUES(block_hash),
    block_time = VALUES(block_time),
    txid = VALUES(txid),
    tx_index = VALUES(tx_index),
    vout = VALUES(vout),
    address = VALUES(address),
    script_type = VALUES(script_type),
    value_base = VALUES(value_base),
    is_coinbase = VALUES(is_coinbase),
    is_spendable = VALUES(is_spendable),
    spent_by_txid = VALUES(spent_by_txid),
    spent_in_block = VALUES(spent_in_block),
    spent_input_index = VALUES(spent_input_index),
    updated_at = VALUES(updated_at)
  `,
  standard: `
    block_height = excluded.block_height,
    block_hash = excluded.block_hash,
    block_time = excluded.block_time,
    txid = excluded.txid,
    tx_index = excluded.tx_index,
    vout = excluded.vout,
    address = excluded.address,
    script_type = excluded.script_type,
    value_base = excluded.value_base,
    is_coinbase = excluded.is_coinbase,
    is_spendable = excluded.is_spendable,
    spent_by_txid = excluded.spent_by_txid,
    spent_in_block = excluded.spent_in_block,
    spent_input_index = excluded.spent_input_index,
    updated_at = excluded.updated_at
  `,
};
const coreUtxoUpdateClauses: UtxoUpdateClauses = {
  mysql: `
    spent_by_txid = VALUES(spent_by_txid),
    spent_in_block = VALUES(spent_in_block),
    spent_input_index = VALUES(spent_input_index),
    updated_at = VALUES(updated_at)
  `,
  standard: `
    spent_by_txid = excluded.spent_by_txid,
    spent_in_block = excluded.spent_in_block,
    spent_input_index = excluded.spent_input_index,
    updated_at = excluded.updated_at
  `,
};

export class RelationalMetadataStore
  implements
    ApiKeyRepository,
    AuditEventRepository,
    NetworkRepository,
    TokenRepository,
    EntityRepository,
    AddressRepository,
    TagRepository,
    EntityTagRepository,
    ConfigReader,
    CoordinatorConfigPort,
    ConfigMutationPort,
    InvestigationMetadataPort,
    InvestigationWarehousePort,
    IndexedNetworkPort,
    CoreDogecoinStateStorePort,
    ProjectionStateStorePort,
    ProjectionLinkSeedPort,
    NetworkReader
{
  private constructor(private readonly client: SupportedClient) {}

  public static async connect(settings: DatabaseSettings): Promise<RelationalMetadataStore> {
    return {
      sqlite: RelationalMetadataStore.connectSqlite,
      postgres: RelationalMetadataStore.connectPostgres,
      mysql: RelationalMetadataStore.connectMysql,
    }[settings.driver](settings);
  }

  private static async connectSqlite(settings: DatabaseSettings): Promise<RelationalMetadataStore> {
    const path = settings.location.replace(/^file:/u, '');
    await mkdir(dirname(path), { recursive: true });
    const raw = createLibsqlClient({ url: settings.location });
    drizzleLibsql(raw);
    return RelationalMetadataStore.migrateStore(
      new RelationalMetadataStore({ kind: 'sqlite', raw }),
    );
  }

  private static async connectPostgres(
    settings: DatabaseSettings,
  ): Promise<RelationalMetadataStore> {
    const raw = new Pool(postgresPoolOptions(settings));
    drizzlePg(raw);
    return RelationalMetadataStore.migrateStore(
      new RelationalMetadataStore({ kind: 'postgres', raw }),
    );
  }

  private static async connectMysql(settings: DatabaseSettings): Promise<RelationalMetadataStore> {
    const raw = mysql.createPool(settings.location);
    drizzleMysql(raw);
    return RelationalMetadataStore.migrateStore(
      new RelationalMetadataStore({ kind: 'mysql', raw }),
    );
  }

  private static async migrateStore(
    store: RelationalMetadataStore,
  ): Promise<RelationalMetadataStore> {
    await store.migrate();
    return store;
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

  public async createApiKey(record: {
    createdAt: string;
    id: string;
    isActive: boolean;
    role: string;
    secretKeyHash: string;
    updatedAt: string | null;
  }) {
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

  public async getApiKeyByHash(secretKeyHash: string) {
    const row = await this.one<DatabaseRow>(
      'SELECT * FROM api_keys WHERE secret_key_hash = ? LIMIT 1',
      [secretKeyHash],
    );
    return row ? this.mapApiKey(row) : null;
  }

  public async getApiKeyById(id: string) {
    const row = await this.one<DatabaseRow>('SELECT * FROM api_keys WHERE id = ? LIMIT 1', [id]);
    return row ? this.mapApiKey(row) : null;
  }

  public async getApiKeyByInternalId(apiKeyId: PrimaryId) {
    const row = await this.one<DatabaseRow>('SELECT * FROM api_keys WHERE api_key_id = ? LIMIT 1', [
      apiKeyId,
    ]);
    return row ? this.mapApiKey(row) : null;
  }

  public async listApiKeys(offset?: number, limit?: number) {
    return this.listTable('api_keys', 'api_key_id', (row) => this.mapApiKey(row), offset, limit);
  }

  public async updateApiKey(record: {
    id: string;
    isActive: boolean;
    role: string;
    secretKeyHash: string;
    updatedAt: string | null;
  }): Promise<void> {
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
        record.updatedAt ?? nowIsoString(),
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

  public async createAuditEvent(record: CreateAuditEventInput): Promise<void> {
    await this.execute(
      `
        INSERT INTO audit_events (
          id,
          actor_api_key_id,
          actor_api_key,
          actor_role,
          method,
          path,
          route,
          operation,
          resource_type,
          resource_ids,
          owner_api_key_id,
          owner_api_key,
          status_code,
          outcome,
          error,
          request_id,
          ip,
          user_agent,
          created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        record.id ?? createAuditEventId(),
        record.actorApiKeyId,
        record.actorApiKey,
        record.actorRole,
        record.method,
        record.path,
        record.route,
        record.operation,
        record.resourceType,
        JSON.stringify(record.resourceIds),
        record.ownerApiKeyId,
        record.ownerApiKey,
        record.statusCode,
        record.outcome,
        record.error,
        record.requestId,
        record.ip,
        record.userAgent,
        record.createdAt,
      ],
    );
  }

  public async deleteAuditEventsBefore(cutoffIso: string): Promise<void> {
    await this.execute('DELETE FROM audit_events WHERE created_at < ?', [cutoffIso]);
  }

  public async listAuditEvents(filters: AuditEventFilters & { actorApiKeyId?: PrimaryId }) {
    const conditions: string[] = [];
    const params: SqlValue[] = [];
    addOptionalCondition(conditions, params, 'actor_api_key_id = ?', filters.actorApiKeyId);
    addOptionalCondition(conditions, params, 'actor_api_key = ?', filters.actor);
    addOptionalCondition(conditions, params, 'created_at >= ?', filters.from);
    addOptionalCondition(conditions, params, 'created_at < ?', filters.to);
    addOptionalCondition(conditions, params, 'UPPER(method) = UPPER(?)', filters.method);
    addOptionalCondition(conditions, params, 'outcome = ?', filters.outcome);
    addOptionalCondition(conditions, params, 'resource_type = ?', filters.resourceType);
    addOptionalCondition(conditions, params, 'status_code = ?', filters.statusCode);
    addAuditResourceIdCondition(conditions, params, filters.resourceId);

    const rows = await this.query<DatabaseRow>(
      `
        SELECT *
        FROM audit_events
        ${whereClause(conditions)}
        ORDER BY audit_event_id DESC
        ${sqlLimitClause(filters.limit)}
        ${sqlNullableOffsetClause(filters.offset)}
      `,
      [...params, ...sqlNullablePaginationParams(filters.offset, filters.limit)],
    );

    return rows.map((row) => this.mapAuditEvent(row));
  }

  public async createNetwork(record: NetworkRecord) {
    await this.execute(
      `
        INSERT INTO networks (id, name, architecture, chain_id, block_time, rpc_endpoint, rps, zmq_block_endpoint, is_deleted, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        record.id,
        record.name,
        record.architecture,
        record.chainId,
        record.blockTime,
        record.rpcEndpoint,
        record.rps,
        record.zmqBlockEndpoint,
        record.isDeleted ? 1 : 0,
        record.updatedAt,
        record.createdAt,
      ],
    );

    return this.getNetworkById(record.id).then(assertFound);
  }

  public async getNetworkByArchitectureAndChainId(
    architecture: string,
    chainId: number,
    includeDeleted = false,
  ) {
    const row = await this.one<DatabaseRow>(
      `
        SELECT * FROM networks
        WHERE architecture = ? AND chain_id = ?
        ${this.notDeletedCondition('is_deleted', includeDeleted)}
        LIMIT 1
      `,
      [architecture, chainId],
    );
    return row ? this.mapNetwork(row) : null;
  }

  public async getNetworkById(id: string) {
    const row = await this.one<DatabaseRow>('SELECT * FROM networks WHERE id = ? LIMIT 1', [id]);
    return row ? this.mapNetwork(row) : null;
  }

  public async getNetworkByInternalId(id: PrimaryId) {
    const row = await this.one<DatabaseRow>('SELECT * FROM networks WHERE network_id = ? LIMIT 1', [
      id,
    ]);
    return row ? this.mapNetwork(row) : null;
  }

  public async getNetworkByName(name: string, includeDeleted = false) {
    return this.getNamedRecord('networks', name, includeDeleted, (row) => this.mapNetwork(row));
  }

  public async listNetworks(offset?: number, limit?: number) {
    return this.listTable('networks', 'network_id', (row) => this.mapNetwork(row), offset, limit);
  }

  public async updateNetworkRecord(record: NetworkRecord): Promise<void> {
    await this.execute(
      `
        UPDATE networks
        SET name = ?, architecture = ?, chain_id = ?, block_time = ?, rpc_endpoint = ?, rps = ?, zmq_block_endpoint = ?, is_deleted = ?, updated_at = ?
        WHERE id = ?
      `,
      [
        record.name,
        record.architecture,
        record.chainId,
        record.blockTime,
        record.rpcEndpoint,
        record.rps,
        record.zmqBlockEndpoint,
        this.booleanValue(record.isDeleted),
        record.updatedAt ?? nowIsoString(),
        record.id,
      ],
    );
  }

  public async softDeleteNetworks(ids: string[]) {
    return this.softDeleteRecords('networks', ids, (id) => this.getNetworkById(id));
  }

  public async createToken(record: TokenRecord) {
    await this.execute(
      `
        INSERT INTO tokens (network_id, id, name, symbol, address, decimals, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        record.networkId,
        record.id,
        record.name,
        record.symbol,
        record.address,
        record.decimals,
        record.updatedAt,
        record.createdAt,
      ],
    );

    return this.getTokenById(record.id).then(assertFound);
  }

  public async getTokenById(id: string) {
    const row = await this.one<DatabaseRow>('SELECT * FROM tokens WHERE id = ? LIMIT 1', [id]);
    return row ? this.mapToken(row) : null;
  }

  public async listTokens(offset?: number, limit?: number) {
    return this.listTable('tokens', 'token_id', (row) => this.mapToken(row), offset, limit);
  }

  public async listTokensByNetworkIds(networkIds: PrimaryId[]) {
    return this.listRowsByIds('tokens', 'network_id', networkIds, (row) => this.mapToken(row));
  }

  public async listEntitiesByIds(entityIds: PrimaryId[]) {
    return this.listRowsByIds('entities', 'entity_id', entityIds, (row) => this.mapEntity(row));
  }

  public async listAddressesByEntityIds(entityIds: PrimaryId[]) {
    return this.listRowsByIds('addresses', 'entity_id', entityIds, (row) => this.mapAddress(row));
  }

  public async listAddressesByNetworkIds(networkIds: PrimaryId[]) {
    return this.listRowsByIds('addresses', 'network_id', networkIds, (row) => this.mapAddress(row));
  }

  public async getTokenByNetworkAndAddress(networkId: PrimaryId, address: string) {
    const row = await this.one<DatabaseRow>(
      'SELECT * FROM tokens WHERE network_id = ? AND address = ? LIMIT 1',
      [networkId, address],
    );
    return row ? this.mapToken(row) : null;
  }

  public async deleteTokens(ids: string[]): Promise<void> {
    if (ids.length === 0) {
      return;
    }

    await this.execute(`DELETE FROM tokens WHERE id IN (${placeholders(ids.length)})`, ids);
  }

  public async createEntity(record: EntityRecord) {
    await this.execute(
      `
        INSERT INTO entities (id, owner_api_key_id, name, description, data, is_deleted, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        record.id,
        record.ownerApiKeyId,
        record.name,
        record.description,
        JSON.stringify(record.data),
        this.booleanValue(record.isDeleted),
        record.updatedAt,
        record.createdAt,
      ],
    );

    return this.getEntityById(record.id).then(assertFound);
  }

  public async getEntityById(id: string) {
    const row = await this.one<DatabaseRow>('SELECT * FROM entities WHERE id = ? LIMIT 1', [id]);
    return row ? this.mapEntity(row) : null;
  }

  public async getEntityByInternalId(entityId: PrimaryId) {
    const row = await this.one<DatabaseRow>('SELECT * FROM entities WHERE entity_id = ? LIMIT 1', [
      entityId,
    ]);
    return row ? this.mapEntity(row) : null;
  }

  public async getEntityByName(
    name: string,
    includeDeleted = false,
    ownerApiKeyId?: PrimaryId,
  ): Promise<EntityRecord | null> {
    return this.getNamedRecord<EntityRecord>(
      'entities',
      name,
      includeDeleted,
      (row) => this.mapEntity(row),
      ownerApiKeyId === undefined ? {} : { ownerApiKeyId },
    );
  }

  public async listEntities(offset?: number, limit?: number) {
    return this.listTable('entities', 'entity_id', (row) => this.mapEntity(row), offset, limit);
  }

  public async listEntitiesByTagIds(tagIds: PrimaryId[]) {
    if (tagIds.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `
        SELECT e.*, et.tag_id
        FROM entities e
        JOIN entity_tags et ON et.entity_id = e.entity_id
        WHERE et.tag_id IN (${placeholders(tagIds.length)})
      `,
      tagIds,
    );

    return rows.map((row) => ({
      entity: this.mapEntity(row),
      tagId: Number(row.tag_id),
    }));
  }

  public async updateEntityRecord(record: EntityRecord): Promise<void> {
    await this.execute(
      `
        UPDATE entities
        SET owner_api_key_id = ?, name = ?, description = ?, data = ?, is_deleted = ?, updated_at = ?
        WHERE id = ?
      `,
      [
        record.ownerApiKeyId,
        record.name,
        record.description,
        JSON.stringify(record.data),
        this.booleanValue(record.isDeleted),
        record.updatedAt ?? nowIsoString(),
        record.id,
      ],
    );
  }

  public async softDeleteEntities(ids: string[]) {
    return this.softDeleteRecords('entities', ids, (id) => this.getEntityById(id));
  }

  public async createAddresses(records: AddressRecord[]) {
    for (const record of records) {
      await this.insertAddressRecord(record);
    }

    return this.createdAddressRecords(records);
  }

  private async createdAddressRecords(records: AddressRecord[]) {
    const firstRecord = firstArrayItem(records);
    if (!firstRecord) {
      return [];
    }

    return this.findAddressesByEntityNetworkAndAddresses(
      firstRecord.entityId,
      firstRecord.networkId,
      records.map((record) => record.address),
      false,
    );
  }

  private async insertAddressRecord(record: AddressRecord): Promise<void> {
    await this.execute(
      `
        INSERT INTO addresses (entity_id, network_id, owner_api_key_id, network, id, address, description, data, is_deleted, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `,
      [
        record.entityId,
        record.networkId,
        record.ownerApiKeyId,
        record.network,
        record.id,
        record.address,
        record.description,
        JSON.stringify(record.data),
        this.booleanValue(record.isDeleted),
        record.updatedAt,
        record.createdAt,
      ],
    );
  }

  public async getAddressById(id: string) {
    const row = await this.one<DatabaseRow>('SELECT * FROM addresses WHERE id = ? LIMIT 1', [id]);
    return row ? this.mapAddress(row) : null;
  }

  public async getAddressByInternalId(addressId: PrimaryId) {
    const row = await this.one<DatabaseRow>(
      'SELECT * FROM addresses WHERE address_id = ? LIMIT 1',
      [addressId],
    );
    return row ? this.mapAddress(row) : null;
  }

  public async listAddresses(offset?: number, limit?: number) {
    return this.listTable('addresses', 'address_id', (row) => this.mapAddress(row), offset, limit);
  }

  public async listTrackedAddresses(networkId: PrimaryId) {
    const rows = await this.query<DatabaseRow>(
      `
        SELECT address_id, address
        FROM addresses
        WHERE network_id = ? AND ${this.booleanCondition('is_deleted', false)}
      `,
      [networkId],
    );
    return rows.map((row) => ({
      addressId: Number(row.address_id),
      address: String(row.address),
    }));
  }

  public async listTrackedAddressesByValues(networkId: PrimaryId, addresses: string[]) {
    if (addresses.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `
        SELECT address_id, address
        FROM addresses
        WHERE
          network_id = ?
          AND address IN (${placeholders(addresses.length)})
          AND ${this.booleanCondition('is_deleted', false)}
      `,
      [networkId, ...addresses],
    );

    return rows.map((row) => ({
      addressId: Number(row.address_id),
      address: String(row.address),
    }));
  }

  public async listAddressesByValues(addresses: string[], includeDeleted = false) {
    if (addresses.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `
        SELECT * FROM addresses
        WHERE address IN (${placeholders(addresses.length)})
        ${this.notDeletedCondition('is_deleted', includeDeleted)}
      `,
      addresses,
    );
    return rows.map((row) => this.mapAddress(row));
  }

  public async findAddressesByEntityNetworkAndAddresses(
    entityId: PrimaryId,
    networkId: PrimaryId,
    addresses: string[],
    includeDeleted = false,
  ) {
    if (addresses.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `
        SELECT * FROM addresses
        WHERE entity_id = ? AND network_id = ? AND address IN (${placeholders(addresses.length)})
        ${this.notDeletedCondition('is_deleted', includeDeleted)}
      `,
      [entityId, networkId, ...addresses],
    );
    return rows.map((row) => this.mapAddress(row));
  }

  public async softDeleteAddresses(ids: string[]) {
    if (ids.length === 0) {
      return [];
    }

    return this.softDeleteExistingAddresses(await this.existingAddresses(ids));
  }

  private async softDeleteExistingAddresses(addresses: AddressRecord[]) {
    if (addresses.length === 0) {
      return [];
    }

    await this.execute(
      `UPDATE addresses SET is_deleted = ${this.booleanLiteral(true)}, updated_at = ? WHERE id IN (${placeholders(addresses.length)})`,
      [nowIsoString(), ...addresses.map((address) => address.id)],
    );

    return Promise.all(addresses.map((address) => this.getAddressById(address.id))).then(
      (updated) =>
        updated.filter((address): address is NonNullable<typeof address> => Boolean(address)),
    );
  }

  private async existingAddresses(ids: string[]): Promise<AddressRecord[]> {
    const existing = await Promise.all(ids.map((id) => this.getAddressById(id)));
    return existing.filter(isAddressRecord);
  }

  public async softDeleteAddressesByEntityIds(entityIds: PrimaryId[]): Promise<void> {
    if (entityIds.length === 0) {
      return;
    }

    await this.execute(
      `UPDATE addresses SET is_deleted = ${this.booleanLiteral(true)}, updated_at = ? WHERE entity_id IN (${placeholders(
        entityIds.length,
      )})`,
      [nowIsoString(), ...entityIds],
    );
  }

  public async softDeleteAddressesByNetworkIds(networkIds: PrimaryId[]): Promise<void> {
    if (networkIds.length === 0) {
      return;
    }

    await this.execute(
      `UPDATE addresses SET is_deleted = ${this.booleanLiteral(true)}, updated_at = ? WHERE network_id IN (${placeholders(
        networkIds.length,
      )})`,
      [nowIsoString(), ...networkIds],
    );
  }

  public async createTag(record: TagRecord) {
    await this.execute(
      `
        INSERT INTO tags (id, owner_api_key_id, name, risk_level, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
      `,
      [
        record.id,
        record.ownerApiKeyId,
        record.name,
        record.riskLevel,
        record.updatedAt,
        record.createdAt,
      ],
    );

    return this.getTagById(record.id).then(assertFound);
  }

  public async getTagById(id: string) {
    const row = await this.one<DatabaseRow>('SELECT * FROM tags WHERE id = ? LIMIT 1', [id]);
    return row ? this.mapTag(row) : null;
  }

  public async getTagByName(name: string, ownerApiKeyId?: PrimaryId) {
    const row = await this.one<DatabaseRow>(
      `
        SELECT * FROM tags
        WHERE LOWER(name) = LOWER(?)
        ${ownerTagClause(ownerApiKeyId)}
        LIMIT 1
      `,
      ownerTagParams(name, ownerApiKeyId),
    );
    return row ? this.mapTag(row) : null;
  }

  public async listTags(offset?: number, limit?: number) {
    return this.listTable('tags', 'tag_id', (row) => this.mapTag(row), offset, limit);
  }

  public async listTagsByIds(tagIds: PrimaryId[]) {
    return this.listRowsByIds('tags', 'tag_id', tagIds, (row) => this.mapTag(row));
  }

  public async updateTagRecord(record: TagRecord): Promise<void> {
    await this.execute(
      `
        UPDATE tags
        SET owner_api_key_id = ?, name = ?, risk_level = ?, updated_at = ?
        WHERE id = ?
      `,
      [
        record.ownerApiKeyId,
        record.name,
        record.riskLevel,
        record.updatedAt ?? nowIsoString(),
        record.id,
      ],
    );
  }

  public async deleteTags(ids: string[]): Promise<void> {
    if (ids.length === 0) {
      return;
    }

    await this.execute(`DELETE FROM tags WHERE id IN (${placeholders(ids.length)})`, ids);
  }

  public async listEntityTagMap(entityIds: PrimaryId[]) {
    if (entityIds.length === 0) {
      return new Map<PrimaryId, PrimaryId[]>();
    }

    const rows = await this.query<DatabaseRow>(
      `SELECT entity_id, tag_id FROM entity_tags WHERE entity_id IN (${placeholders(entityIds.length)})`,
      entityIds,
    );

    return entityTagMap(rows);
  }

  public async replaceEntityTags(entityId: PrimaryId, tagIds: PrimaryId[]): Promise<void> {
    await this.execute('DELETE FROM entity_tags WHERE entity_id = ?', [entityId]);
    for (const tagId of tagIds) {
      await this.execute(
        'INSERT INTO entity_tags (entity_id, tag_id, created_at) VALUES (?, ?, ?)',
        [entityId, tagId, nowIsoString()],
      );
    }
  }

  public async getJsonValue<T>(key: string): Promise<T | null> {
    const row = await this.one<{ value: string }>(
      'SELECT value FROM configs WHERE key = ? LIMIT 1',
      [key],
    );
    return row ? safeJsonParse<T | null>(row.value, null) : null;
  }

  public async setJsonValue<T>(key: string, value: T): Promise<void> {
    const exists = await this.getJsonValue<T>(key);
    if (exists === null) {
      await this.execute(
        'INSERT INTO configs (key, value, updated_at, created_at) VALUES (?, ?, ?, ?)',
        [key, JSON.stringify(value), nowIsoString(), nowIsoString()],
      );
      return;
    }

    await this.execute('UPDATE configs SET value = ?, updated_at = ? WHERE key = ?', [
      JSON.stringify(value),
      nowIsoString(),
      key,
    ]);
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
    await this.execute('DELETE FROM configs WHERE key LIKE ?', [`${prefix}%`]);
  }

  public async markNewlyAddedAddress(networkId: PrimaryId, addressId: PrimaryId): Promise<void> {
    await this.markPendingRelinkSeed(networkId, addressId);
  }

  public async markPendingRelinkSeed(networkId: PrimaryId, addressId: PrimaryId): Promise<void> {
    await this.setJsonValue(configKeyNewlyAddedAddress(networkId, addressId), addressId);
  }

  public async clearPendingRelinkSeed(networkId: PrimaryId, addressId: PrimaryId): Promise<void> {
    await this.execute('DELETE FROM configs WHERE key = ?', [
      configKeyNewlyAddedAddress(networkId, addressId),
    ]);
  }

  public async getTrackedAddress(networkId: PrimaryId, addressId: PrimaryId) {
    const row = await this.one<DatabaseRow>(
      `
        SELECT address_id, address
        FROM addresses
        WHERE network_id = ? AND address_id = ? AND ${this.booleanCondition('is_deleted', false)}
        LIMIT 1
      `,
      [networkId, addressId],
    );
    return row
      ? {
          addressId: Number(row.address_id),
          address: String(row.address),
        }
      : null;
  }

  public async listPendingRelinkSeeds(networkId: PrimaryId) {
    const rows = await this.query<DatabaseRow>(
      'SELECT key FROM configs WHERE key LIKE ? ORDER BY key ASC',
      [`newly_added_address_n${networkId}_a%`],
    );
    const trackedAddresses = await Promise.all(
      rows
        .map((row) => parsePendingRelinkAddressId(String(row.key), networkId))
        .filter((addressId): addressId is PrimaryId => addressId !== null)
        .map((addressId) => this.getTrackedAddress(networkId, addressId)),
    );

    return trackedAddresses.filter((address): address is NonNullable<typeof address> =>
      Boolean(address),
    );
  }

  public async getBalancesByAddresses(addresses: string[]) {
    const coreRows = await this.queryByAddresses(
      addresses,
      (count) => `
        SELECT network_id, address, asset_address, balance
        FROM core_balances
        WHERE address IN (${placeholders(count)})
        ORDER BY network_id ASC, address ASC, asset_address ASC
      `,
      (row) => ({
        networkId: Number(row.network_id),
        address: String(row.address),
        assetAddress: String(row.asset_address),
        balance: String(row.balance),
      }),
    );
    const projectionRows = await this.queryByAddresses(
      addresses,
      (count) => `
        SELECT network_id, address, asset_address, balance, as_of_block_height
        FROM projection_balances_current
        WHERE address IN (${placeholders(count)})
        ORDER BY network_id ASC, address ASC, asset_address ASC
      `,
      (row) => ({
        networkId: Number(row.network_id),
        address: String(row.address),
        assetAddress: String(row.asset_address),
        balance: String(row.balance),
      }),
    );
    return dedupeRecords(
      [...coreRows, ...projectionRows],
      (row) => `${row.networkId}:${row.address}:${row.assetAddress}`,
    );
  }

  public async getTokensByAddresses(addresses: string[]) {
    return this.queryByAddresses(
      addresses,
      (count) => `
        SELECT network_id, id, name, symbol, address, decimals
        FROM tokens
        WHERE address IN (${placeholders(count)})
        ORDER BY network_id ASC, address ASC
      `,
      (row) => ({
        networkId: Number(row.network_id),
        id: String(row.id),
        name: String(row.name),
        symbol: String(row.symbol),
        address: String(row.address),
        decimals: Number(row.decimals),
      }),
    );
  }

  public async getDistinctLinksByAddresses(addresses: string[]) {
    return this.queryByAddresses(
      addresses,
      (count) => `
        SELECT DISTINCT network_id, source_address, to_address, hop_count
        FROM projection_source_links_current
        WHERE to_address IN (${placeholders(count)})
        ORDER BY network_id ASC, source_address ASC, to_address ASC
      `,
      (row) => ({
        networkId: Number(row.network_id),
        fromAddress: String(row.source_address),
        toAddress: String(row.to_address),
        transferCount: Number(row.hop_count),
      }),
    );
  }

  public async getBalanceSnapshots(
    networkId: PrimaryId,
    keys: Array<{
      address: string;
      assetAddress: string;
    }>,
  ): Promise<Map<string, ProjectionBalanceSnapshot>> {
    if (keys.length === 0) {
      return new Map();
    }

    const rows = (
      await Promise.all(
        chunkArray(keys, 250).map((chunk) => {
          const conditions = chunk.map(() => '(address = ? AND asset_address = ?)').join(' OR ');
          return this.query<DatabaseRow>(
            `
              SELECT network_id, address, asset_address, balance, as_of_block_height
              FROM projection_balances_current
              WHERE network_id = ? AND (${conditions})
            `,
            [networkId, ...chunk.flatMap((key) => [key.address, key.assetAddress])],
          );
        }),
      )
    ).flat();

    return new Map(
      rows.map((row) => {
        const snapshot: ProjectionBalanceSnapshot = {
          networkId: Number(row.network_id),
          address: String(row.address),
          assetAddress: String(row.asset_address),
          balance: String(row.balance),
          asOfBlockHeight: Number(row.as_of_block_height),
        };
        return [projectionBalanceSnapshotKey(snapshot.address, snapshot.assetAddress), snapshot];
      }),
    );
  }

  public async getDirectLinkSnapshots(
    networkId: PrimaryId,
    keys: Array<{
      assetAddress: string;
      fromAddress: string;
      toAddress: string;
    }>,
  ): Promise<Map<string, DirectLinkRecord>> {
    if (keys.length === 0) {
      return new Map();
    }

    const rows = (
      await Promise.all(
        chunkArray(keys, 200).map((chunk) => {
          const conditions = chunk
            .map(() => '(from_address = ? AND to_address = ? AND asset_address = ?)')
            .join(' OR ');
          return this.query<DatabaseRow>(
            `
              SELECT
                network_id,
                from_address,
                to_address,
                asset_address,
                transfer_count,
                total_amount_base,
                first_seen_block_height,
                last_seen_block_height
              FROM projection_direct_links_current
              WHERE network_id = ? AND (${conditions})
            `,
            [
              networkId,
              ...chunk.flatMap((key) => [key.fromAddress, key.toAddress, key.assetAddress]),
            ],
          );
        }),
      )
    ).flat();

    return new Map(
      rows.map((row) => {
        const snapshot = this.mapDirectLinkRecord(row);
        return [
          projectionDirectLinkSnapshotKey(
            snapshot.fromAddress,
            snapshot.toAddress,
            snapshot.assetAddress,
          ),
          snapshot,
        ];
      }),
    );
  }

  public async getProjectionBootstrapTail(networkId: PrimaryId): Promise<number | null> {
    return this.getJsonValue<number>(configKeyProjectionBootstrapTail(networkId));
  }

  public async getCurrentAddressSummary(
    networkId: PrimaryId,
    address: string,
  ): Promise<{
    balance: string;
    utxoCount: number;
  } | null> {
    const [balance, utxoCount] = await Promise.all([
      this.getCurrentNativeBalance(networkId, address),
      this.countCurrentSpendableUtxos(networkId, address),
    ]);
    return currentAddressSummary(balance, utxoCount);
  }

  private async getCurrentNativeBalance(networkId: PrimaryId, address: string): Promise<string> {
    const coreBalance = await this.getCoreNativeBalance(networkId, address);
    if (coreBalance !== null) {
      return coreBalance;
    }

    return nativeBalanceOrZero(await this.getProjectionNativeBalance(networkId, address));
  }

  private async getCoreNativeBalance(
    networkId: PrimaryId,
    address: string,
  ): Promise<string | null> {
    const row = await this.one<DatabaseRow>(
      `
        SELECT balance
        FROM core_balances
        WHERE network_id = ? AND address = ? AND asset_address = ''
        LIMIT 1
      `,
      [networkId, address],
    );
    return rowValueString(row?.balance);
  }

  private async getProjectionNativeBalance(
    networkId: PrimaryId,
    address: string,
  ): Promise<string | null> {
    const row = await this.one<DatabaseRow>(
      `
        SELECT balance
        FROM projection_balances_current
        WHERE network_id = ? AND address = ? AND asset_address = ''
        LIMIT 1
      `,
      [networkId, address],
    );

    return rowValueString(row?.balance);
  }

  private async countCurrentSpendableUtxos(networkId: PrimaryId, address: string): Promise<number> {
    return (
      (await this.countCoreSpendableUtxos(networkId, address)) ??
      (await this.countProjectionSpendableUtxos(networkId, address))
    );
  }

  private async countCoreSpendableUtxos(
    networkId: PrimaryId,
    address: string,
  ): Promise<number | null> {
    const row = await this.one<DatabaseRow>(
      `
        SELECT utxo_count
        FROM core_balances
        WHERE network_id = ? AND address = ? AND asset_address = ''
        LIMIT 1
      `,
      [networkId, address],
    );
    return rowValueNumber(row?.utxo_count);
  }

  private async countProjectionSpendableUtxos(
    networkId: PrimaryId,
    address: string,
  ): Promise<number> {
    const row = await this.one<DatabaseRow>(
      `
        SELECT COUNT(*) AS utxo_count
        FROM projection_utxo_outputs_current
        WHERE
          network_id = ?
          AND address = ?
          AND ${this.booleanCondition('is_spendable', true)}
          AND spent_by_txid IS NULL
      `,
      [networkId, address],
    );

    return numberValueOrZero(row, 'utxo_count');
  }

  public async getUtxoOutputs(
    networkId: PrimaryId,
    outputKeys: string[],
  ): Promise<Map<string, ProjectionUtxoOutput>> {
    if (outputKeys.length === 0) {
      return new Map();
    }

    const coreOutputs = await this.getCoreUtxoOutputs(networkId, outputKeys);
    return this.mergeMissingProjectionUtxoOutputs(networkId, outputKeys, coreOutputs);
  }

  private async mergeMissingProjectionUtxoOutputs(
    networkId: PrimaryId,
    outputKeys: string[],
    coreOutputs: Map<string, ProjectionUtxoOutput>,
  ): Promise<Map<string, ProjectionUtxoOutput>> {
    const missingOutputKeys = outputKeys.filter((outputKey) => !coreOutputs.has(outputKey));
    if (missingOutputKeys.length === 0) {
      return coreOutputs;
    }

    const projectionOutputs = await this.listProjectionUtxoOutputs(networkId, missingOutputKeys);
    return new Map([...coreOutputs, ...projectionOutputs]);
  }

  public async listAddressUtxos(
    networkId: PrimaryId,
    address: string,
    offset = 0,
    limit?: number,
  ): Promise<ProjectionUtxoOutput[]> {
    if (await this.hasCoreProcessingStarted(networkId)) {
      const rows = await this.listCoreSpendableUtxoRows(networkId, address, offset, limit);
      return rows.map((row) => this.mapProjectionUtxoOutput(row));
    }

    const rows = await this.listSpendableUtxoRows(networkId, address, offset, limit);
    return rows.map((row) => this.mapProjectionUtxoOutput(row));
  }

  private async listCoreSpendableUtxoRows(
    networkId: PrimaryId,
    address: string,
    offset: number,
    limit?: number,
  ): Promise<DatabaseRow[]> {
    return this.query<DatabaseRow>(
      `
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
          value_base,
          is_coinbase,
          is_spendable,
          spent_by_txid,
          spent_in_block,
          spent_input_index
        FROM core_utxos
        WHERE
          network_id = ?
          AND address = ?
          AND ${this.booleanCondition('is_spendable', true)}
          AND spent_by_txid IS NULL
        ORDER BY block_height DESC, tx_index DESC, vout ASC
        ${sqlLimitClause(limit)}
        ${sqlOffsetClause(offset)}
      `,
      [networkId, address, ...sqlPaginationParams(offset, limit)],
    );
  }

  private async hasCoreProcessingStarted(networkId: PrimaryId): Promise<boolean> {
    const state = await this.getCoreIndexerState(networkId);
    return Boolean(state && state.processTail >= 0);
  }

  private async listProjectionUtxoOutputs(
    networkId: PrimaryId,
    outputKeys: string[],
  ): Promise<Map<string, ProjectionUtxoOutput>> {
    const rows = (
      await Promise.all(
        chunkArray(outputKeys, 1_000).map((chunk) =>
          this.query<DatabaseRow>(
            `
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
                value_base,
                is_coinbase,
                is_spendable,
                spent_by_txid,
                spent_in_block,
                spent_input_index
              FROM projection_utxo_outputs_current
              WHERE network_id = ? AND output_key IN (${placeholders(chunk.length)})
            `,
            [networkId, ...chunk],
          ),
        ),
      )
    ).flat();

    return new Map(
      rows.map((row) => {
        const output = this.mapProjectionUtxoOutput(row);
        return [output.outputKey, output];
      }),
    );
  }

  private async listSpendableUtxoRows(
    networkId: PrimaryId,
    address: string,
    offset: number,
    limit?: number,
  ): Promise<DatabaseRow[]> {
    return this.query<DatabaseRow>(
      `
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
          value_base,
          is_coinbase,
          is_spendable,
          spent_by_txid,
          spent_in_block,
          spent_input_index
        FROM projection_utxo_outputs_current
        WHERE
          network_id = ?
          AND address = ?
          AND ${this.booleanCondition('is_spendable', true)}
          AND spent_by_txid IS NULL
        ORDER BY block_height DESC, tx_index DESC, vout ASC
        ${sqlLimitClause(limit)}
        ${sqlOffsetClause(offset)}
      `,
      [networkId, address, ...sqlPaginationParams(offset, limit)],
    );
  }

  public async hasAppliedBlock(
    networkId: PrimaryId,
    blockHeight: number,
    blockHash: string,
  ): Promise<boolean> {
    const row = await this.one<DatabaseRow>(
      `
        SELECT 1 AS present
        FROM projection_applied_blocks
        WHERE network_id = ? AND block_height = ? AND block_hash = ?
        LIMIT 1
      `,
      [networkId, blockHeight, blockHash],
    );
    if (row) {
      return true;
    }

    return hasBootstrapAppliedBlock(blockHeight, await this.getProjectionBootstrapTail(networkId));
  }

  public async listAppliedBlockSet(
    networkId: PrimaryId,
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>> {
    const identities = await this.listAppliedBlockIdentities(
      'projection_applied_blocks',
      networkId,
      blocks,
    );
    for (const identity of bootstrapBlockIdentities(
      networkId,
      blocks,
      await this.getProjectionBootstrapTail(networkId),
    )) {
      identities.add(identity);
    }

    return identities;
  }

  public async applyDirectLinkDeltasWindow(batches: ProjectionDirectLinkBatch[]): Promise<void> {
    const window = await resolvePendingProjectionWindow(batches, (networkId, blocks) =>
      this.listDirectLinkAppliedBlockSet(networkId, blocks),
    );
    if (window === null) {
      return;
    }

    const directLinks = await this.buildProjectionDirectLinks(
      window.networkId,
      window.pendingBatches,
    );

    const timestamp = nowIsoString();
    await this.withTransaction(async (executor) => {
      await this.upsertProjectionDirectLinks(directLinks, timestamp, executor);
      await this.insertProjectionDirectLinkAppliedBlocks(
        toProjectionAppliedBlocks(window.pendingBatches),
        timestamp,
        executor,
      );
    });
  }

  private async listDirectLinkAppliedBlockSet(
    networkId: PrimaryId,
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>> {
    return this.listAppliedBlockIdentities(
      'projection_direct_link_applied_blocks',
      networkId,
      blocks,
    );
  }

  public async hasProjectionState(networkId: PrimaryId): Promise<boolean> {
    const bootstrapTail = await this.getProjectionBootstrapTail(networkId);
    if (bootstrapTail !== null) {
      return true;
    }

    const row = await this.one<DatabaseRow>(
      `
        SELECT 1 AS present
        FROM projection_applied_blocks
        WHERE network_id = ?
        LIMIT 1
      `,
      [networkId],
    );

    return Boolean(row);
  }

  public async clearProjectionBootstrapState(networkId: PrimaryId): Promise<void> {
    await this.withTransaction(async (executor) => {
      await this.executeWithExecutor(
        executor,
        'DELETE FROM projection_utxo_outputs_current WHERE network_id = ?',
        [networkId],
      );
      await this.executeWithExecutor(
        executor,
        'DELETE FROM projection_balances_current WHERE network_id = ?',
        [networkId],
      );
      await this.executeWithExecutor(
        executor,
        'DELETE FROM projection_applied_blocks WHERE network_id = ?',
        [networkId],
      );
    });
  }

  public async upsertProjectionBootstrapUtxoOutputs(rows: ProjectionUtxoOutput[]): Promise<void> {
    if (rows.length === 0) {
      return;
    }

    const timestamp = nowIsoString();
    await this.withTransaction(async (executor) => {
      await this.upsertProjectionUtxoOutputs(rows, timestamp, executor);
    });
  }

  public async upsertProjectionBootstrapBalances(rows: ProjectionBalanceSnapshot[]): Promise<void> {
    if (rows.length === 0) {
      return;
    }

    const timestamp = nowIsoString();
    await this.withTransaction(async (executor) => {
      await this.upsertProjectionBalances(rows, timestamp, executor);
    });
  }

  public async finalizeProjectionBootstrap(
    networkId: PrimaryId,
    processTail: number,
  ): Promise<void> {
    await this.setJsonValue(configKeyProjectionBootstrapTail(networkId), processTail);
  }

  public async importProjectionStateSnapshot(
    networkId: PrimaryId,
    snapshot: ProjectionStateBootstrapSnapshot,
    processTail: number,
  ): Promise<void> {
    const timestamp = nowIsoString();
    await this.withTransaction(async (executor) => {
      await this.executeWithExecutor(
        executor,
        'DELETE FROM projection_utxo_outputs_current WHERE network_id = ?',
        [networkId],
      );
      await this.executeWithExecutor(
        executor,
        'DELETE FROM projection_balances_current WHERE network_id = ?',
        [networkId],
      );
      await this.executeWithExecutor(
        executor,
        'DELETE FROM projection_direct_links_current WHERE network_id = ?',
        [networkId],
      );
      await this.executeWithExecutor(
        executor,
        'DELETE FROM projection_source_links_current WHERE network_id = ?',
        [networkId],
      );
      await this.executeWithExecutor(
        executor,
        'DELETE FROM projection_direct_link_applied_blocks WHERE network_id = ?',
        [networkId],
      );
      await this.executeWithExecutor(
        executor,
        'DELETE FROM projection_applied_blocks WHERE network_id = ?',
        [networkId],
      );
      await this.upsertProjectionUtxoOutputs(snapshot.utxoOutputs, timestamp, executor);
      await this.upsertProjectionBalances(snapshot.balances, timestamp, executor);
      await this.upsertProjectionDirectLinks(snapshot.directLinks, timestamp, executor);
      await this.insertProjectionSourceLinks(snapshot.sourceLinks, timestamp, executor);
      await this.insertProjectionAppliedBlocks(snapshot.appliedBlocks, timestamp, executor);
    });
    await this.setJsonValue(configKeyProjectionBootstrapTail(networkId), processTail);
  }

  public async listDirectLinksFromAddresses(networkId: PrimaryId, fromAddresses: string[]) {
    if (fromAddresses.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `
        SELECT
          network_id,
          from_address,
          to_address,
          asset_address,
          transfer_count,
          total_amount_base,
          first_seen_block_height,
          last_seen_block_height
        FROM projection_direct_links_current
        WHERE network_id = ? AND from_address IN (${placeholders(fromAddresses.length)})
        ORDER BY from_address ASC, to_address ASC, asset_address ASC
      `,
      [networkId, ...fromAddresses],
    );

    return rows.map((row) => this.mapDirectLinkRecord(row));
  }

  public async listSourceSeedIdsReachingAddresses(
    networkId: PrimaryId,
    addresses: string[],
  ): Promise<PrimaryId[]> {
    if (addresses.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `
        SELECT DISTINCT source_address_id
        FROM projection_source_links_current
        WHERE network_id = ? AND to_address IN (${placeholders(addresses.length)})
      `,
      [networkId, ...addresses],
    );

    return rows.map((row) => Number(row.source_address_id));
  }

  public async replaceSourceLinks(
    networkId: PrimaryId,
    sourceAddressId: PrimaryId,
    rows: SourceLinkRecord[],
  ): Promise<void> {
    const timestamp = nowIsoString();
    await this.withTransaction(async (executor) => {
      await this.executeWithExecutor(
        executor,
        'DELETE FROM projection_source_links_current WHERE network_id = ? AND source_address_id = ?',
        [networkId, sourceAddressId],
      );
      await this.insertProjectionSourceLinks(rows, timestamp, executor);
    });
  }

  public async applyProjectionWindow(batches: BlockProjectionBatch[]): Promise<void> {
    const window = await this.resolvePendingBlockProjectionWindow(batches);
    if (window === null) {
      return;
    }

    const { nextBalances, nextOutputs } = await buildProjectionStateChanges<{
      address: string;
      assetAddress: string;
    }>({
      batches: window.pendingBatches,
      keyForMovement: (movement) =>
        projectionBalanceSnapshotKey(movement.address, movement.assetAddress),
      loadBalances: (keys) => this.getBalanceSnapshots(window.networkId, keys),
      loadOutputs: (networkId, outputKeys) => this.getUtxoOutputs(networkId, outputKeys),
      networkId: window.networkId,
      toSnapshotKey: (key) => key,
    });

    const directLinks = await this.buildProjectionDirectLinks(
      window.networkId,
      window.pendingBatches,
    );

    const timestamp = nowIsoString();
    await this.withTransaction(async (executor) => {
      await this.upsertProjectionUtxoOutputs([...nextOutputs.values()], timestamp, executor);
      await this.upsertProjectionBalances([...nextBalances.values()], timestamp, executor);
      await this.upsertProjectionDirectLinks(directLinks, timestamp, executor);
      await this.insertProjectionAppliedBlocks(
        toProjectionAppliedBlocks(window.pendingBatches),
        timestamp,
        executor,
      );
    });
  }

  private resolvePendingBlockProjectionWindow(batches: BlockProjectionBatch[]) {
    return resolvePendingProjectionWindow(batches, (networkId, blocks) =>
      this.listAppliedBlockSet(networkId, blocks),
    );
  }

  private async buildProjectionDirectLinks(
    networkId: PrimaryId,
    batches: Array<{ directLinkDeltas: DirectLinkDelta[] }>,
  ): Promise<DirectLinkRecord[]> {
    const directLinkKeys = collectProjectionDirectLinkSnapshotKeys(batches).map(
      parseProjectionDirectLinkSnapshotKey,
    );
    const currentDirectLinks = await this.getDirectLinkSnapshots(networkId, directLinkKeys);
    const nextDirectLinks = new Map<string, DirectLinkRecord>();
    applyDirectLinkDeltasToSnapshots({
      currentDirectLinks,
      directLinkDeltas: batches.flatMap((batch) => batch.directLinkDeltas),
      keyForDelta: (delta) =>
        projectionDirectLinkSnapshotKey(delta.fromAddress, delta.toAddress, delta.assetAddress),
      nextDirectLinks,
    });
    return [...nextDirectLinks.values()];
  }

  public async getActiveNetworkById(id: string) {
    const network = await this.getNetworkById(id);
    return activeNetworkOrNull(network);
  }

  public async getActiveNetworksByInternalIds(networkIds: PrimaryId[]) {
    const networks = await Promise.all(
      networkIds.map((networkId) => this.getNetworkByInternalId(networkId)),
    );
    return networks.filter((network): network is NonNullable<typeof network> =>
      Boolean(network && !network.isDeleted),
    );
  }

  public async listTagsByEntityIds(entityIds: PrimaryId[]) {
    if (entityIds.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `
        SELECT t.id, t.name, t.risk_level, et.entity_id
        FROM tags t
        JOIN entity_tags et ON et.tag_id = t.tag_id
        WHERE et.entity_id IN (${placeholders(entityIds.length)})
      `,
      entityIds,
    );

    return rows.map((row) => ({
      entityId: Number(row.entity_id),
      id: String(row.id),
      name: String(row.name),
      riskLevel: parseRiskLevel(String(row.risk_level)),
    }));
  }

  public async listNetworksByInternalIds(networkIds: PrimaryId[]) {
    if (networkIds.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `
        SELECT network_id, id, name, chain_id
        FROM networks
        WHERE network_id IN (${placeholders(networkIds.length)}) AND ${this.booleanCondition('is_deleted', false)}
      `,
      networkIds,
    );

    return rows.map((row) => ({
      networkId: Number(row.network_id),
      id: String(row.id),
      name: String(row.name),
      chainId: Number(row.chain_id),
    }));
  }

  public async listActiveNetworks() {
    const rows = await this.query<DatabaseRow>(
      `SELECT * FROM networks WHERE ${this.booleanCondition('is_deleted', false)} ORDER BY network_id ASC`,
    );

    return rows.map((row) => this.mapActiveNetwork(row));
  }

  public async getCoreIndexerState(networkId: PrimaryId): Promise<CoreIndexerState | null> {
    const row = await this.one<DatabaseRow>(
      'SELECT * FROM core_indexer_state WHERE network_id = ? LIMIT 1',
      [networkId],
    );
    return row ? this.mapCoreIndexerState(row) : null;
  }

  public async canReadDogecoinHistory(networkId: PrimaryId): Promise<boolean> {
    return (await this.getJsonValue<boolean>(configKeyDogecoinHistoryReady(networkId))) !== false;
  }

  public async upsertCoreIndexerState(input: CoreIndexerStateUpdate): Promise<CoreIndexerState> {
    const current = await this.getCoreIndexerState(input.networkId);
    const timestamp = nowIsoString();
    const row = coreIndexerStateRow(input, current, timestamp);

    await this.upsertCoreIndexerStateRow(row, timestamp);
    return row;
  }

  private async upsertCoreIndexerStateRow(row: CoreIndexerState, timestamp: string): Promise<void> {
    if (this.client.kind === 'mysql') {
      await this.upsertMysqlCoreIndexerStateRow(row, timestamp);
      return;
    }

    await this.upsertStandardCoreIndexerStateRow(row, timestamp);
  }

  private async upsertMysqlCoreIndexerStateRow(
    row: CoreIndexerState,
    timestamp: string,
  ): Promise<void> {
    await this.execute(
      `
        INSERT INTO core_indexer_state (
          network_id, stage, sync_tail, process_tail, online_tip, last_error, updated_at, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          stage = VALUES(stage),
          sync_tail = VALUES(sync_tail),
          process_tail = VALUES(process_tail),
          online_tip = VALUES(online_tip),
          last_error = VALUES(last_error),
          updated_at = VALUES(updated_at)
      `,
      coreIndexerStateParams(row, timestamp),
    );
  }

  private async upsertStandardCoreIndexerStateRow(
    row: CoreIndexerState,
    timestamp: string,
  ): Promise<void> {
    await this.execute(
      `
        INSERT INTO core_indexer_state (
          network_id, stage, sync_tail, process_tail, online_tip, last_error, updated_at, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (network_id) DO UPDATE SET
          stage = excluded.stage,
          sync_tail = excluded.sync_tail,
          process_tail = excluded.process_tail,
          online_tip = excluded.online_tip,
          last_error = excluded.last_error,
          updated_at = excluded.updated_at
      `,
      coreIndexerStateParams(row, timestamp),
    );
  }

  public async setCoreIndexerStage(networkId: PrimaryId, stage: CoreIndexerStage): Promise<void> {
    await this.upsertCoreIndexerState({ networkId, stage });
  }

  public async setCoreIndexerError(networkId: PrimaryId, error: string | null): Promise<void> {
    await this.upsertCoreIndexerState({ networkId, lastError: error });
  }

  public async upsertCoreBlock(record: CoreBlockRecord): Promise<void> {
    const timestamp = nowIsoString();
    if (this.client.kind === 'mysql') {
      await this.execute(
        `
          INSERT INTO core_blocks (
            network_id, block_height, block_hash, previous_block_hash, block_time, tx_count,
            raw_storage_key, fetched_at, processed_at, updated_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
            block_hash = VALUES(block_hash),
            previous_block_hash = VALUES(previous_block_hash),
            block_time = VALUES(block_time),
            tx_count = VALUES(tx_count),
            raw_storage_key = VALUES(raw_storage_key),
            fetched_at = VALUES(fetched_at),
            processed_at = COALESCE(core_blocks.processed_at, VALUES(processed_at)),
            updated_at = VALUES(updated_at)
        `,
        [
          record.networkId,
          record.blockHeight,
          record.blockHash,
          record.previousBlockHash,
          record.blockTime,
          record.txCount,
          record.rawStorageKey,
          record.fetchedAt,
          record.processedAt,
          timestamp,
        ],
      );
      return;
    }

    await this.execute(
      `
        INSERT INTO core_blocks (
          network_id, block_height, block_hash, previous_block_hash, block_time, tx_count,
          raw_storage_key, fetched_at, processed_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (network_id, block_height) DO UPDATE SET
          block_hash = excluded.block_hash,
          previous_block_hash = excluded.previous_block_hash,
          block_time = excluded.block_time,
          tx_count = excluded.tx_count,
          raw_storage_key = excluded.raw_storage_key,
          fetched_at = excluded.fetched_at,
          processed_at = COALESCE(core_blocks.processed_at, excluded.processed_at),
          updated_at = excluded.updated_at
      `,
      [
        record.networkId,
        record.blockHeight,
        record.blockHash,
        record.previousBlockHash,
        record.blockTime,
        record.txCount,
        record.rawStorageKey,
        record.fetchedAt,
        record.processedAt,
        timestamp,
      ],
    );
  }

  public async listCoreBackfillBenchmarkRanges(
    networkId: PrimaryId,
    input: { blocks: number; ranges: number },
  ): Promise<Array<{ end: number; start: number; txCount: number }>> {
    const rows = await this.query<DatabaseRow>(
      `
        SELECT block_height, tx_count
        FROM core_blocks
        WHERE network_id = ?
        ORDER BY tx_count DESC, block_height ASC
        LIMIT ?
      `,
      [networkId, input.ranges],
    );
    const halfWindow = Math.floor(input.blocks / 2);
    return rows.map((row) => {
      const center = Number(row.block_height);
      const start = Math.max(0, center - halfWindow);
      return {
        start,
        end: start + input.blocks - 1,
        txCount: Number(row.tx_count),
      };
    });
  }

  public async getCoreUtxoOutputs(
    networkId: PrimaryId,
    outputKeys: string[],
  ): Promise<Map<string, ProjectionUtxoOutput>> {
    return this.getCoreUtxoOutputsWithExecutor(this.client, networkId, outputKeys);
  }

  public async applyCoreDogecoinBlock(
    input: CoreDogecoinBlockApplication,
    context?: CoreDogecoinApplyContext,
  ): Promise<CoreDogecoinApplyResult> {
    return this.withTransaction(
      (executor) => this.applyCoreDogecoinBlockInTransaction(input, executor),
      coreApplyOptions(context),
    );
  }

  private async applyCoreDogecoinBlockInTransaction(
    input: CoreDogecoinBlockApplication,
    executor: SupportedExecutor,
  ): Promise<CoreDogecoinApplyResult> {
    await this.acquireCoreNetworkLock(executor, input.networkId);
    try {
      return await this.applyCoreDogecoinBlockWithLock(input, executor);
    } finally {
      await this.releaseCoreNetworkLock(executor, input.networkId);
    }
  }

  private async applyCoreDogecoinBlockWithLock(
    input: CoreDogecoinBlockApplication,
    executor: SupportedExecutor,
  ): Promise<CoreDogecoinApplyResult> {
    const replayResult = await this.getCoreBlockReplayResult(input, executor);
    if (replayResult) {
      return replayResult;
    }

    await this.upsertCoreBlockWithExecutor(input, executor);
    const mutation = await this.prepareCoreUtxoMutation(input, executor);
    await this.persistCoreBlockApplication(input, mutation, executor);
    return { applied: true, processTail: input.blockHeight };
  }

  private async getCoreBlockReplayResult(
    input: CoreDogecoinBlockApplication,
    executor: SupportedExecutor,
  ): Promise<CoreDogecoinApplyResult | null> {
    const existing = await this.oneWithExecutor<DatabaseRow>(
      executor,
      'SELECT block_hash FROM core_processed_blocks WHERE network_id = ? AND block_height = ? LIMIT 1',
      [input.networkId, input.blockHeight],
    );
    if (!existing) {
      return null;
    }

    return coreBlockReplayResult(input, String(existing.block_hash));
  }

  private async prepareCoreUtxoMutation(
    input: CoreDogecoinBlockApplication,
    executor: SupportedExecutor,
  ): Promise<CoreUtxoMutation> {
    const mutation = createCoreUtxoMutation();
    appendCoreCreatedOutputs(mutation, input.utxoCreates);
    const currentOutputs = await this.getCoreUtxoOutputsWithExecutor(
      executor,
      input.networkId,
      input.utxoSpends.map((spend) => spend.outputKey),
    );
    appendCoreSpentOutputs(mutation, input.utxoSpends, currentOutputs);
    return mutation;
  }

  private async persistCoreBlockApplication(
    input: CoreDogecoinBlockApplication,
    mutation: CoreUtxoMutation,
    executor: SupportedExecutor,
  ): Promise<void> {
    const timestamp = nowIsoString();
    await this.upsertCoreUtxoOutputs([...mutation.nextOutputs.values()], timestamp, executor);
    await this.recomputeCoreBalances(
      input.networkId,
      [...mutation.affectedAddresses],
      input.blockHeight,
      timestamp,
      executor,
    );
    await this.insertCoreBlockUndo(input, timestamp, executor);
    await this.insertCoreProcessedBlock(input, timestamp, executor);
    await this.markCoreBlockProcessed(input, timestamp, executor);
  }

  private async markCoreBlockProcessed(
    input: CoreDogecoinBlockApplication,
    timestamp: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    await this.executeWithExecutor(
      executor,
      'UPDATE core_blocks SET processed_at = ?, updated_at = ? WHERE network_id = ? AND block_height = ?',
      [timestamp, timestamp, input.networkId, input.blockHeight],
    );
  }

  public async applyCoreDogecoinWindow(
    input: CoreDogecoinBlockApplication[],
    context?: CoreDogecoinApplyContext,
  ): Promise<CoreDogecoinApplyResult> {
    let result = initialCoreWindowApplyResult(input);

    for (const application of input) {
      result = mergeCoreApplyResult(
        result,
        await this.applyCoreDogecoinBlock(application, context),
      );
    }

    return result;
  }

  public async materializeCoreDogecoinCurrentState(
    _networkId: PrimaryId,
    _asOfBlockHeight: number,
    _context?: CoreDogecoinApplyContext,
  ): Promise<void> {}

  private mapProjectionUtxoOutput(row: DatabaseRow): ProjectionUtxoOutput {
    return {
      networkId: Number(row.network_id),
      blockHeight: Number(row.block_height),
      blockHash: String(row.block_hash),
      blockTime: Number(row.block_time),
      txid: String(row.txid),
      txIndex: Number(row.tx_index),
      vout: Number(row.vout),
      outputKey: String(row.output_key),
      address: String(row.address),
      scriptType: String(row.script_type),
      valueBase: String(row.value_base),
      isCoinbase: toBoolean(row.is_coinbase),
      isSpendable: toBoolean(row.is_spendable),
      spentByTxid: nullableString(row.spent_by_txid),
      spentInBlock: nullableNumber(row.spent_in_block),
      spentInputIndex: nullableNumber(row.spent_input_index),
    };
  }

  private async upsertProjectionUtxoOutputs(
    outputs: ProjectionUtxoOutput[],
    timestamp: string,
    executor: SupportedExecutor = this.client,
  ): Promise<void> {
    for (const chunk of chunkArray(outputs, this.bulkChunkSize(executor.kind))) {
      await this.upsertProjectionUtxoOutputChunk(chunk, timestamp, executor);
    }
  }

  private async upsertProjectionUtxoOutputChunk(
    chunk: ProjectionUtxoOutput[],
    timestamp: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    await this.upsertUtxoOutputChunk(
      'projection_utxo_outputs_current',
      chunk,
      timestamp,
      executor,
      projectionUtxoUpdateClauses,
    );
  }

  private async upsertProjectionBalances(
    balances: ProjectionBalanceSnapshot[],
    timestamp: string,
    executor: SupportedExecutor = this.client,
  ): Promise<void> {
    for (const chunk of chunkArray(balances, this.bulkChunkSize(executor.kind))) {
      await this.upsertProjectionBalanceChunk(chunk, timestamp, executor);
    }
  }

  private async upsertProjectionBalanceChunk(
    chunk: ProjectionBalanceSnapshot[],
    timestamp: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    const params = projectionBalanceParams(chunk, timestamp);
    const values = multiRowPlaceholders(chunk.length, 6);
    if (executor.kind === 'mysql') {
      await this.executeWithExecutor(
        executor,
        `
          INSERT INTO projection_balances_current (
            network_id, address, asset_address, balance, as_of_block_height, updated_at
          )
          VALUES ${values}
          ON DUPLICATE KEY UPDATE
            balance = VALUES(balance),
            as_of_block_height = VALUES(as_of_block_height),
            updated_at = VALUES(updated_at)
        `,
        params,
      );
      return;
    }

    await this.executeWithExecutor(
      executor,
      `
        INSERT INTO projection_balances_current (
          network_id, address, asset_address, balance, as_of_block_height, updated_at
        )
        VALUES ${values}
        ON CONFLICT (network_id, address, asset_address) DO UPDATE SET
          balance = excluded.balance,
          as_of_block_height = excluded.as_of_block_height,
          updated_at = excluded.updated_at
      `,
      params,
    );
  }

  private async upsertProjectionDirectLinks(
    links: DirectLinkRecord[],
    timestamp: string,
    executor: SupportedExecutor = this.client,
  ): Promise<void> {
    for (const chunk of chunkArray(links, this.bulkChunkSize(executor.kind))) {
      await this.upsertProjectionDirectLinkChunk(chunk, timestamp, executor);
    }
  }

  private async upsertProjectionDirectLinkChunk(
    chunk: DirectLinkRecord[],
    timestamp: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    const params = projectionDirectLinkParams(chunk, timestamp);
    const values = multiRowPlaceholders(chunk.length, 9);
    if (executor.kind === 'mysql') {
      await this.executeWithExecutor(
        executor,
        `
          INSERT INTO projection_direct_links_current (
            network_id, from_address, to_address, asset_address, transfer_count,
            total_amount_base, first_seen_block_height, last_seen_block_height, updated_at
          )
          VALUES ${values}
          ON DUPLICATE KEY UPDATE
            transfer_count = VALUES(transfer_count),
            total_amount_base = VALUES(total_amount_base),
            first_seen_block_height = VALUES(first_seen_block_height),
            last_seen_block_height = VALUES(last_seen_block_height),
            updated_at = VALUES(updated_at)
        `,
        params,
      );
      return;
    }

    await this.executeWithExecutor(
      executor,
      `
        INSERT INTO projection_direct_links_current (
          network_id, from_address, to_address, asset_address, transfer_count,
          total_amount_base, first_seen_block_height, last_seen_block_height, updated_at
        )
        VALUES ${values}
        ON CONFLICT (network_id, from_address, to_address, asset_address) DO UPDATE SET
          transfer_count = excluded.transfer_count,
          total_amount_base = excluded.total_amount_base,
          first_seen_block_height = excluded.first_seen_block_height,
          last_seen_block_height = excluded.last_seen_block_height,
          updated_at = excluded.updated_at
      `,
      params,
    );
  }

  private async insertProjectionSourceLinks(
    rows: SourceLinkRecord[],
    timestamp: string,
    executor: SupportedExecutor = this.client,
  ): Promise<void> {
    for (const chunk of chunkArray(rows, this.bulkChunkSize(executor.kind))) {
      await this.executeWithExecutor(
        executor,
        `
          INSERT INTO projection_source_links_current (
            network_id, source_address_id, source_address, to_address, hop_count,
            path_transfer_count, path_addresses, first_seen_block_height, last_seen_block_height,
            updated_at, created_at
          )
          VALUES ${multiRowPlaceholders(chunk.length, 11)}
        `,
        projectionSourceLinkParams(chunk, timestamp),
      );
    }
  }

  private async insertProjectionAppliedBlocks(
    blocks: ProjectionAppliedBlock[],
    timestamp: string,
    executor: SupportedExecutor = this.client,
  ): Promise<void> {
    await this.insertProjectionAppliedBlockRows(
      'projection_applied_blocks',
      blocks,
      timestamp,
      executor,
    );
  }

  private async insertProjectionDirectLinkAppliedBlocks(
    blocks: ProjectionAppliedBlock[],
    timestamp: string,
    executor: SupportedExecutor = this.client,
  ): Promise<void> {
    await this.insertProjectionAppliedBlockRows(
      'projection_direct_link_applied_blocks',
      blocks,
      timestamp,
      executor,
    );
  }

  private async insertProjectionAppliedBlockRows(
    table: 'projection_applied_blocks' | 'projection_direct_link_applied_blocks',
    blocks: ProjectionAppliedBlock[],
    timestamp: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    for (const chunk of chunkArray(blocks, this.bulkChunkSize(executor.kind))) {
      await this.insertProjectionAppliedBlockChunk(table, chunk, timestamp, executor);
    }
  }

  private async insertProjectionAppliedBlockChunk(
    table: 'projection_applied_blocks' | 'projection_direct_link_applied_blocks',
    chunk: ProjectionAppliedBlock[],
    timestamp: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    const params = projectionAppliedBlockParams(chunk, timestamp);
    const values = multiRowPlaceholders(chunk.length, 5);
    if (executor.kind === 'mysql') {
      await this.executeWithExecutor(
        executor,
        `
          INSERT INTO ${table} (
            network_id, block_height, block_hash, updated_at, created_at
          )
          VALUES ${values}
          ON DUPLICATE KEY UPDATE
            updated_at = VALUES(updated_at)
        `,
        params,
      );
      return;
    }

    await this.executeWithExecutor(
      executor,
      `
        INSERT INTO ${table} (
          network_id, block_height, block_hash, updated_at, created_at
        )
        VALUES ${values}
        ON CONFLICT (network_id, block_height, block_hash) DO UPDATE SET
          updated_at = excluded.updated_at
      `,
      params,
    );
  }

  private async upsertCoreBlockWithExecutor(
    input: CoreDogecoinBlockApplication,
    executor: SupportedExecutor,
  ): Promise<void> {
    const timestamp = nowIsoString();
    const params = [
      input.networkId,
      input.blockHeight,
      input.blockHash,
      input.previousBlockHash,
      input.blockTime,
      input.txCount,
      input.rawStorageKey,
      timestamp,
      null,
      timestamp,
    ];

    if (executor.kind === 'mysql') {
      await this.executeWithExecutor(
        executor,
        `
          INSERT INTO core_blocks (
            network_id, block_height, block_hash, previous_block_hash, block_time, tx_count,
            raw_storage_key, fetched_at, processed_at, updated_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON DUPLICATE KEY UPDATE
            block_hash = VALUES(block_hash),
            previous_block_hash = VALUES(previous_block_hash),
            block_time = VALUES(block_time),
            tx_count = VALUES(tx_count),
            raw_storage_key = VALUES(raw_storage_key),
            updated_at = VALUES(updated_at)
        `,
        params,
      );
      return;
    }

    await this.executeWithExecutor(
      executor,
      `
        INSERT INTO core_blocks (
          network_id, block_height, block_hash, previous_block_hash, block_time, tx_count,
          raw_storage_key, fetched_at, processed_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (network_id, block_height) DO UPDATE SET
          block_hash = excluded.block_hash,
          previous_block_hash = excluded.previous_block_hash,
          block_time = excluded.block_time,
          tx_count = excluded.tx_count,
          raw_storage_key = excluded.raw_storage_key,
          updated_at = excluded.updated_at
      `,
      params,
    );
  }

  private async upsertCoreUtxoOutputs(
    outputs: ProjectionUtxoOutput[],
    timestamp: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    for (const chunk of chunkArray(outputs, this.bulkChunkSize(executor.kind))) {
      await this.upsertCoreUtxoOutputChunk(chunk, timestamp, executor);
    }
  }

  private async upsertCoreUtxoOutputChunk(
    chunk: ProjectionUtxoOutput[],
    timestamp: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    await this.upsertUtxoOutputChunk(
      'core_utxos',
      chunk,
      timestamp,
      executor,
      coreUtxoUpdateClauses,
    );
  }

  private async upsertUtxoOutputChunk(
    table: 'core_utxos' | 'projection_utxo_outputs_current',
    chunk: ProjectionUtxoOutput[],
    timestamp: string,
    executor: SupportedExecutor,
    updateClauses: UtxoUpdateClauses,
  ): Promise<void> {
    const params = projectionUtxoOutputParams(chunk, timestamp, (value) =>
      this.booleanValue(value),
    );
    const values = multiRowPlaceholders(chunk.length, 17);
    if (executor.kind === 'mysql') {
      await this.executeWithExecutor(
        executor,
        `
          INSERT INTO ${table} (${utxoOutputColumns})
          VALUES ${values}
          ON DUPLICATE KEY UPDATE ${updateClauses.mysql}
        `,
        params,
      );
      return;
    }

    await this.executeWithExecutor(
      executor,
      `
        INSERT INTO ${table} (${utxoOutputColumns})
        VALUES ${values}
        ON CONFLICT (network_id, output_key) DO UPDATE SET ${updateClauses.standard}
      `,
      params,
    );
  }

  private async recomputeCoreBalances(
    networkId: PrimaryId,
    addresses: string[],
    asOfBlockHeight: number,
    timestamp: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    if (addresses.length === 0) {
      return;
    }

    const balances = initialCoreBalanceAccumulators(addresses);
    await this.loadCoreBalanceAccumulators(networkId, addresses, balances, executor);
    await this.upsertCoreBalanceRows(
      coreBalanceRows(networkId, balances, asOfBlockHeight),
      timestamp,
      executor,
    );
  }

  private async loadCoreBalanceAccumulators(
    networkId: PrimaryId,
    addresses: string[],
    balances: Map<string, CoreBalanceAccumulator>,
    executor: SupportedExecutor,
  ): Promise<void> {
    for (const chunk of chunkArray(addresses, 500)) {
      const rows = await this.queryWithExecutor<DatabaseRow>(
        executor,
        `
          SELECT address, value_base
          FROM core_utxos
          WHERE
            network_id = ?
            AND address IN (${placeholders(chunk.length)})
            AND ${this.booleanCondition('is_spendable', true)}
            AND spent_by_txid IS NULL
        `,
        [networkId, ...chunk],
      );
      appendCoreBalanceRows(balances, rows);
    }
  }

  private async upsertCoreBalanceRows(
    rows: CoreBalanceRow[],
    timestamp: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    for (const chunk of chunkArray(rows, this.bulkChunkSize(executor.kind))) {
      await this.upsertCoreBalanceChunk(chunk, timestamp, executor);
    }
  }

  private async upsertCoreBalanceChunk(
    chunk: CoreBalanceRow[],
    timestamp: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    const params = coreBalanceParams(chunk, timestamp);
    const values = multiRowPlaceholders(chunk.length, 7);
    if (executor.kind === 'mysql') {
      await this.executeWithExecutor(
        executor,
        `
          INSERT INTO core_balances (
            network_id, address, asset_address, balance, utxo_count, as_of_block_height, updated_at
          )
          VALUES ${values}
          ON DUPLICATE KEY UPDATE
            balance = VALUES(balance),
            utxo_count = VALUES(utxo_count),
            as_of_block_height = VALUES(as_of_block_height),
            updated_at = VALUES(updated_at)
        `,
        params,
      );
      return;
    }

    await this.executeWithExecutor(
      executor,
      `
        INSERT INTO core_balances (
          network_id, address, asset_address, balance, utxo_count, as_of_block_height, updated_at
        )
        VALUES ${values}
        ON CONFLICT (network_id, address, asset_address) DO UPDATE SET
          balance = excluded.balance,
          utxo_count = excluded.utxo_count,
          as_of_block_height = excluded.as_of_block_height,
          updated_at = excluded.updated_at
      `,
      params,
    );
  }

  private async insertCoreProcessedBlock(
    input: CoreDogecoinBlockApplication,
    timestamp: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    if (executor.kind === 'mysql') {
      await this.executeWithExecutor(
        executor,
        `
          INSERT IGNORE INTO core_processed_blocks (
            network_id, block_height, block_hash, processed_at, created_at
          )
          VALUES (?, ?, ?, ?, ?)
        `,
        [input.networkId, input.blockHeight, input.blockHash, timestamp, timestamp],
      );
    } else {
      await this.executeWithExecutor(
        executor,
        `
          INSERT INTO core_processed_blocks (
            network_id, block_height, block_hash, processed_at, created_at
          )
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT (network_id, block_height) DO NOTHING
        `,
        [input.networkId, input.blockHeight, input.blockHash, timestamp, timestamp],
      );
    }

    await this.assertCoreIdentityHash(
      executor,
      'core_processed_blocks',
      input.networkId,
      input.blockHeight,
      input.blockHash,
    );
  }

  private async insertCoreBlockUndo(
    input: CoreDogecoinBlockApplication,
    timestamp: string,
    executor: SupportedExecutor,
  ): Promise<void> {
    const payload = JSON.stringify({
      createdOutputKeys: input.utxoCreates.map((output) => output.outputKey),
      spentOutputs: input.utxoSpends.map((spend) => ({
        outputKey: spend.outputKey,
        spentByTxid: spend.spentByTxid,
        spentInBlock: spend.spentInBlock,
        spentInputIndex: spend.spentInputIndex,
      })),
    });

    if (executor.kind === 'mysql') {
      await this.executeWithExecutor(
        executor,
        `
          INSERT IGNORE INTO core_block_undo (
            network_id, block_height, block_hash, undo_json, created_at
          )
          VALUES (?, ?, ?, ?, ?)
        `,
        [input.networkId, input.blockHeight, input.blockHash, payload, timestamp],
      );
    } else {
      await this.executeWithExecutor(
        executor,
        `
          INSERT INTO core_block_undo (
            network_id, block_height, block_hash, undo_json, created_at
          )
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT (network_id, block_height) DO NOTHING
        `,
        [input.networkId, input.blockHeight, input.blockHash, payload, timestamp],
      );
    }

    await this.assertCoreIdentityHash(
      executor,
      'core_block_undo',
      input.networkId,
      input.blockHeight,
      input.blockHash,
    );
  }

  private async assertCoreIdentityHash(
    executor: SupportedExecutor,
    table: 'core_block_undo' | 'core_processed_blocks',
    networkId: PrimaryId,
    blockHeight: number,
    blockHash: string,
  ): Promise<void> {
    const row = await this.oneWithExecutor<DatabaseRow>(
      executor,
      `SELECT block_hash FROM ${table} WHERE network_id = ? AND block_height = ? LIMIT 1`,
      [networkId, blockHeight],
    );

    assertMatchingCoreIdentityHash(table, networkId, blockHeight, blockHash, storedCoreHash(row));
  }

  private async acquireCoreNetworkLock(
    executor: SupportedExecutor,
    networkId: PrimaryId,
  ): Promise<void> {
    if (executor.kind === 'postgres') {
      await this.acquirePostgresCoreNetworkLock(executor, networkId);
      return;
    }

    await this.acquireNonPostgresCoreNetworkLock(executor, networkId);
  }

  private async acquireNonPostgresCoreNetworkLock(
    executor: Exclude<SupportedExecutor, { kind: 'postgres' }>,
    networkId: PrimaryId,
  ): Promise<void> {
    if (executor.kind === 'mysql') {
      await this.acquireMysqlCoreNetworkLock(executor, networkId);
    }
  }

  private async acquirePostgresCoreNetworkLock(
    executor: Extract<SupportedExecutor, { kind: 'postgres' }>,
    networkId: PrimaryId,
  ): Promise<void> {
    await this.executeWithExecutor(executor, 'SELECT pg_advisory_xact_lock(1868853095, ?)', [
      networkId,
    ]);
  }

  private async acquireMysqlCoreNetworkLock(
    executor: Extract<SupportedExecutor, { kind: 'mysql' }>,
    networkId: PrimaryId,
  ): Promise<void> {
    const [row] = await this.queryWithExecutor<DatabaseRow>(
      executor,
      'SELECT GET_LOCK(?, 30) AS locked',
      [`onlydoge-core-network-${networkId}`],
    );
    assertMysqlCoreNetworkLock(row, networkId);
  }

  private async releaseCoreNetworkLock(
    executor: SupportedExecutor,
    networkId: PrimaryId,
  ): Promise<void> {
    if (executor.kind === 'mysql') {
      await this.executeWithExecutor(executor, 'SELECT RELEASE_LOCK(?)', [
        `onlydoge-core-network-${networkId}`,
      ]);
    }
  }

  private async getCoreUtxoOutputsWithExecutor(
    executor: SupportedExecutor,
    networkId: PrimaryId,
    outputKeys: string[],
  ): Promise<Map<string, ProjectionUtxoOutput>> {
    if (outputKeys.length === 0) {
      return new Map();
    }

    const rows = (
      await Promise.all(
        chunkArray(outputKeys, 1_000).map((chunk) =>
          this.queryWithExecutor<DatabaseRow>(
            executor,
            `
              SELECT
                network_id, block_height, block_hash, block_time, txid, tx_index, vout,
                output_key, address, script_type, value_base, is_coinbase, is_spendable,
                spent_by_txid, spent_in_block, spent_input_index
              FROM core_utxos
              WHERE network_id = ? AND output_key IN (${placeholders(chunk.length)})
            `,
            [networkId, ...chunk],
          ),
        ),
      )
    ).flat();

    return new Map(
      rows.map((row) => {
        const output = this.mapProjectionUtxoOutput(row);
        return [output.outputKey, output];
      }),
    );
  }

  private async query<T extends DatabaseRow>(
    statement: string,
    params: SqlValue[] = [],
  ): Promise<T[]> {
    return this.queryWithExecutor<T>(this.client, statement, params);
  }

  private async softDeleteRecords<T extends { id: string }>(
    table: 'entities' | 'networks',
    ids: string[],
    loadById: (id: string) => Promise<T | null>,
  ): Promise<T[]> {
    if (ids.length === 0) {
      return [];
    }

    const records: Array<T | null> = await Promise.all(ids.map((id) => loadById(id)));
    return this.softDeleteExistingRecords(table, records.filter(isPresentRecord), loadById);
  }

  private async softDeleteExistingRecords<T extends { id: string }>(
    table: 'entities' | 'networks',
    existing: T[],
    loadById: (id: string) => Promise<T | null>,
  ): Promise<T[]> {
    if (existing.length === 0) {
      return [];
    }

    await this.execute(
      `UPDATE ${table} SET is_deleted = ${this.booleanLiteral(true)}, updated_at = ? WHERE id IN (${placeholders(existing.length)})`,
      [nowIsoString(), ...existing.map((record) => record.id)],
    );

    const updated: Array<T | null> = await Promise.all(
      existing.map((record) => loadById(record.id)),
    );
    return updated.filter(isPresentRecord);
  }

  private async listTable<T>(
    table: string,
    orderBy: string,
    mapRow: (row: DatabaseRow) => T,
    offset?: number,
    limit?: number,
  ): Promise<T[]> {
    const rows = await this.query<DatabaseRow>(
      `
        SELECT * FROM ${table}
        ORDER BY ${orderBy} ASC
        ${sqlLimitClause(limit)}
        ${sqlNullableOffsetClause(offset)}
      `,
      sqlNullablePaginationParams(offset, limit),
    );

    return rows.map(mapRow);
  }

  private async listRowsByIds<T>(
    table: string,
    column: string,
    values: PrimaryId[],
    mapRow: (row: DatabaseRow) => T,
  ): Promise<T[]> {
    if (values.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(
      `SELECT * FROM ${table} WHERE ${column} IN (${placeholders(values.length)})`,
      values,
    );
    return rows.map(mapRow);
  }

  private async getNamedRecord<T>(
    table: string,
    name: string,
    includeDeleted: boolean,
    mapRow: (row: DatabaseRow) => T,
    options: { ownerApiKeyId?: PrimaryId } = {},
  ): Promise<T | null> {
    const filter = this.namedRecordFilter(includeDeleted, options.ownerApiKeyId);
    const row = await this.one<DatabaseRow>(
      `
        SELECT * FROM ${table}
        WHERE LOWER(name) = LOWER(?)
        ${filter.conditions}
        LIMIT 1
      `,
      [name, ...filter.params],
    );
    return mapNullableRow(row, mapRow);
  }

  private namedRecordFilter(
    includeDeleted: boolean,
    ownerApiKeyId: PrimaryId | undefined,
  ): { conditions: string; params: SqlValue[] } {
    const conditions: string[] = [];
    const params: SqlValue[] = [];

    addOptionalCondition(conditions, params, 'AND owner_api_key_id = ?', ownerApiKeyId);
    if (!includeDeleted) {
      conditions.push(`AND ${this.booleanCondition('is_deleted', false)}`);
    }

    return {
      conditions: conditions.join('\n'),
      params,
    };
  }

  private async queryByAddresses<T>(
    addresses: string[],
    buildStatement: (placeholderCount: number) => string,
    mapRow: (row: DatabaseRow) => T,
  ): Promise<T[]> {
    if (addresses.length === 0) {
      return [];
    }

    const rows = await this.query<DatabaseRow>(buildStatement(addresses.length), addresses);
    return rows.map(mapRow);
  }

  private async listAppliedBlockIdentities(
    table: string,
    networkId: PrimaryId,
    blocks: Array<{ blockHash: string; blockHeight: number }>,
  ): Promise<Set<string>> {
    if (blocks.length === 0) {
      return new Set();
    }

    const conditions = blocks.map(() => '(block_height = ? AND block_hash = ?)').join(' OR ');
    const rows = await this.query<DatabaseRow>(
      `
        SELECT block_height, block_hash
        FROM ${table}
        WHERE network_id = ? AND (${conditions})
      `,
      [networkId, ...blocks.flatMap((block) => [block.blockHeight, block.blockHash])],
    );

    return new Set(
      rows.map((row) =>
        projectionBlockIdentity(networkId, Number(row.block_height), String(row.block_hash)),
      ),
    );
  }

  private async one<T extends DatabaseRow>(
    statement: string,
    params: SqlValue[] = [],
  ): Promise<T | null> {
    const rows = await this.queryWithExecutor<T>(this.client, statement, params);
    return rows[0] ?? null;
  }

  private async oneWithExecutor<T extends DatabaseRow>(
    executor: SupportedExecutor,
    statement: string,
    params: SqlValue[] = [],
  ): Promise<T | null> {
    const rows = await this.queryWithExecutor<T>(executor, statement, params);
    return rows[0] ?? null;
  }

  private async execute(statement: string, params: SqlValue[] = []): Promise<void> {
    await this.executeWithExecutor(this.client, statement, params);
  }

  private async withTransaction<T>(
    work: (executor: SupportedExecutor) => Promise<T>,
    options: { statementTimeoutMs?: number } = {},
  ): Promise<T> {
    const client = this.client;
    return {
      sqlite: () => this.withSqliteTransaction(work),
      postgres: () =>
        this.withPostgresTransaction(
          client as Extract<SupportedClient, { kind: 'postgres' }>,
          work,
          options,
        ),
      mysql: () =>
        this.withMysqlTransaction(client as Extract<SupportedClient, { kind: 'mysql' }>, work),
    }[client.kind]();
  }

  private async withSqliteTransaction<T>(
    work: (executor: SupportedExecutor) => Promise<T>,
  ): Promise<T> {
    await this.executeWithExecutor(this.client, 'BEGIN IMMEDIATE');
    try {
      const result = await work(this.client);
      await this.executeWithExecutor(this.client, 'COMMIT');
      return result;
    } catch (error) {
      await this.executeWithExecutor(this.client, 'ROLLBACK');
      throw error;
    }
  }

  private async withPostgresTransaction<T>(
    clientPool: Extract<SupportedClient, { kind: 'postgres' }>,
    work: (executor: SupportedExecutor) => Promise<T>,
    options: { statementTimeoutMs?: number } = {},
  ): Promise<T> {
    const client = await clientPool.raw.connect();
    const executor: SupportedExecutor = { kind: 'postgres', raw: client };
    try {
      await this.executeWithExecutor(executor, 'BEGIN');
      await this.setPostgresStatementTimeout(executor, options.statementTimeoutMs);
      const result = await work(executor);
      await this.executeWithExecutor(executor, 'COMMIT');
      return result;
    } catch (error) {
      await this.executeWithExecutor(executor, 'ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  private async setPostgresStatementTimeout(
    executor: SupportedExecutor,
    statementTimeoutMs: number | undefined,
  ): Promise<void> {
    if (!statementTimeoutMs) {
      return;
    }

    await this.executeWithExecutor(
      executor,
      `SET LOCAL statement_timeout = ${Math.trunc(statementTimeoutMs)}`,
    );
  }

  private async withMysqlTransaction<T>(
    mysqlPool: Extract<SupportedClient, { kind: 'mysql' }>,
    work: (executor: SupportedExecutor) => Promise<T>,
  ): Promise<T> {
    const connection = await mysqlPool.raw.getConnection();
    const executor: SupportedExecutor = { kind: 'mysql', raw: connection };
    try {
      await connection.beginTransaction();
      const result = await work(executor);
      await connection.commit();
      return result;
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  }

  private async queryWithExecutor<T extends DatabaseRow>(
    executor: SupportedExecutor,
    statement: string,
    params: SqlValue[] = [],
  ): Promise<T[]> {
    const compiled = compileQuery(executor.kind, statement);

    if (executor.kind === 'sqlite') {
      return this.querySqliteWithExecutor<T>(executor, compiled, params);
    }

    return this.queryNonSqliteWithExecutor<T>(executor, compiled, params);
  }

  private async querySqliteWithExecutor<T extends DatabaseRow>(
    executor: Extract<SupportedExecutor, { kind: 'sqlite' }>,
    compiled: string,
    params: SqlValue[],
  ): Promise<T[]> {
    const result = await executor.raw.execute({ sql: compiled, args: params });
    return sqliteResultRows<T>(result.rows);
  }

  private async queryNonSqliteWithExecutor<T extends DatabaseRow>(
    executor: Exclude<SupportedExecutor, { kind: 'sqlite' }>,
    compiled: string,
    params: SqlValue[],
  ): Promise<T[]> {
    if (executor.kind === 'postgres') {
      const result = await executor.raw.query(compiled, params);
      return result.rows as T[];
    }

    const [rows] = await executor.raw.query(compiled, params);
    return rows as T[];
  }

  private async executeWithExecutor(
    executor: SupportedExecutor,
    statement: string,
    params: SqlValue[] = [],
  ): Promise<void> {
    const compiled = compileQuery(executor.kind, statement);

    if (executor.kind === 'sqlite') {
      await executor.raw.execute({ sql: compiled, args: params });
      return;
    }

    await this.executeNonSqliteWithExecutor(executor, compiled, params);
  }

  private async executeNonSqliteWithExecutor(
    executor: Exclude<SupportedExecutor, { kind: 'sqlite' }>,
    compiled: string,
    params: SqlValue[],
  ): Promise<void> {
    if (executor.kind === 'postgres') {
      await executor.raw.query(compiled, params);
      return;
    }

    await executor.raw.query(compiled, params);
  }

  private bulkChunkSize(kind: SupportedExecutor['kind']): number {
    return kind === 'sqlite' ? 200 : 500;
  }

  private booleanLiteral(value: boolean): string {
    if (this.client.kind === 'sqlite') {
      return sqliteBooleanLiteral(value);
    }

    return sqlBooleanLiteral(value);
  }

  private booleanCondition(column: string, value: boolean): string {
    return `${column} = ${this.booleanLiteral(value)}`;
  }

  private notDeletedCondition(column: string, includeDeleted: boolean): string {
    return includeDeleted ? '' : `AND ${this.booleanCondition(column, false)}`;
  }

  private booleanValue(value: boolean): SqlValue {
    if (this.client.kind === 'sqlite') {
      return sqliteBooleanValue(value);
    }

    return value;
  }

  private async migrate(): Promise<void> {
    for (const statement of migrationStatements(this.client.kind)) {
      await this.execute(statement);
    }

    await this.ensureApiKeySchema();
    await this.ensureOwnerScopedLabelSchema();
    await this.ensureNetworksZmqColumn();
  }

  private async ensureApiKeySchema(): Promise<void> {
    await this.dropApiKeySecretColumn();
    await this.ensureApiKeyRoleColumn();
    await this.ensureInitialAdminApiKey();
    await this.ensureApiKeyHashUniqueIndex();
  }

  private async ensureApiKeyRoleColumn(): Promise<void> {
    if (this.client.kind === 'sqlite') {
      return this.ensureSqliteApiKeyRoleColumn();
    }

    return this.ensureNonSqliteApiKeyRoleColumn();
  }

  private async ensureNonSqliteApiKeyRoleColumn(): Promise<void> {
    if (this.client.kind === 'postgres') {
      return this.ensurePostgresApiKeyRoleColumn();
    }

    await this.ensureMysqlApiKeyRoleColumn();
  }

  private async ensureSqliteApiKeyRoleColumn(): Promise<void> {
    const columns = await this.query<DatabaseRow>('PRAGMA table_info(api_keys)');
    if (hasNamedColumn(columns, 'name', 'role')) {
      return;
    }

    await this.execute('ALTER TABLE api_keys ADD COLUMN role TEXT NULL');
  }

  private async ensurePostgresApiKeyRoleColumn(): Promise<void> {
    await this.execute('ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS role TEXT NULL');
  }

  private async ensureMysqlApiKeyRoleColumn(): Promise<void> {
    if (!(await this.hasMysqlColumn('api_keys', 'role'))) {
      await this.execute('ALTER TABLE api_keys ADD COLUMN role VARCHAR(32) NULL');
    }
  }

  private async ensureInitialAdminApiKey(): Promise<void> {
    await this.execute("UPDATE api_keys SET role = 'member' WHERE role IS NULL");
    const firstKey = await this.firstApiKeyForInitialAdmin();
    if (!firstKey) {
      return;
    }

    await this.promoteInitialAdminApiKey(Number(firstKey.api_key_id));
  }

  private async firstApiKeyForInitialAdmin(): Promise<DatabaseRow | null> {
    const activeKey = await this.one<DatabaseRow>(
      `SELECT api_key_id FROM api_keys WHERE ${this.booleanCondition('is_active', true)} ORDER BY api_key_id ASC LIMIT 1`,
    );
    if (activeKey) {
      return activeKey;
    }

    return this.one<DatabaseRow>('SELECT api_key_id FROM api_keys ORDER BY api_key_id ASC LIMIT 1');
  }

  private async promoteInitialAdminApiKey(apiKeyId: PrimaryId): Promise<void> {
    const activeAdminCount = await this.countActiveAdminApiKeys();
    if (activeAdminCount !== 0) {
      return;
    }

    await this.execute("UPDATE api_keys SET role = 'admin' WHERE api_key_id = ?", [apiKeyId]);
  }

  private async ensureOwnerScopedLabelSchema(): Promise<void> {
    if (this.client.kind === 'sqlite') {
      await this.ensureSqliteOwnerScopedLabelSchema();
      return;
    }

    await this.ensureNonSqliteOwnerScopedLabelSchema();
  }

  private async ensureNonSqliteOwnerScopedLabelSchema(): Promise<void> {
    if (this.client.kind === 'postgres') {
      await this.ensurePostgresOwnerScopedLabelSchema();
      return;
    }

    await this.ensureMysqlOwnerScopedLabelSchema();
  }

  private async ensureSqliteOwnerScopedLabelSchema(): Promise<void> {
    await this.rebuildSqliteEntityTableIfNeeded();
    await this.rebuildSqliteTagTableIfNeeded();
    await this.ensureSqliteColumn('addresses', 'owner_api_key_id', 'INTEGER NULL');
    await this.backfillOwnerApiKeyIds();
  }

  private async ensurePostgresOwnerScopedLabelSchema(): Promise<void> {
    await this.execute(
      'ALTER TABLE entities ADD COLUMN IF NOT EXISTS owner_api_key_id BIGINT NULL',
    );
    await this.execute('ALTER TABLE tags ADD COLUMN IF NOT EXISTS owner_api_key_id BIGINT NULL');
    await this.execute(
      'ALTER TABLE addresses ADD COLUMN IF NOT EXISTS owner_api_key_id BIGINT NULL',
    );
    await this.backfillOwnerApiKeyIds();
    await this.execute('ALTER TABLE entities DROP CONSTRAINT IF EXISTS entities_name_key');
    await this.execute('ALTER TABLE tags DROP CONSTRAINT IF EXISTS tags_name_key');
    await this.execute(
      'CREATE UNIQUE INDEX IF NOT EXISTS uq_entities_owner_name ON entities (owner_api_key_id, name)',
    );
    await this.execute(
      'CREATE UNIQUE INDEX IF NOT EXISTS uq_tags_owner_name ON tags (owner_api_key_id, name)',
    );
  }

  private async ensureMysqlOwnerScopedLabelSchema(): Promise<void> {
    await this.ensureMysqlOwnerApiKeyColumn('entities');
    await this.ensureMysqlOwnerApiKeyColumn('tags');
    await this.ensureMysqlOwnerApiKeyColumn('addresses');
    await this.backfillOwnerApiKeyIds();
    await this.dropMysqlIndexIfExists('entities', 'name');
    await this.dropMysqlIndexIfExists('tags', 'name');
    await this.createMysqlIndexIfMissing(
      'entities',
      'uq_entities_owner_name',
      'ALTER TABLE entities ADD UNIQUE KEY uq_entities_owner_name (owner_api_key_id, name)',
    );
    await this.createMysqlIndexIfMissing(
      'tags',
      'uq_tags_owner_name',
      'ALTER TABLE tags ADD UNIQUE KEY uq_tags_owner_name (owner_api_key_id, name)',
    );
  }

  private async ensureMysqlOwnerApiKeyColumn(
    table: 'addresses' | 'entities' | 'tags',
  ): Promise<void> {
    if (!(await this.hasMysqlColumn(table, 'owner_api_key_id'))) {
      await this.execute(`ALTER TABLE ${table} ADD COLUMN owner_api_key_id BIGINT NULL`);
    }
  }

  private async rebuildSqliteEntityTableIfNeeded(): Promise<void> {
    const sql = await this.getSqliteCreateTableSql('entities');
    if (!sql.includes('name TEXT NULL UNIQUE')) {
      await this.ensureSqliteColumn('entities', 'owner_api_key_id', 'INTEGER NULL');
      return;
    }

    await this.execute(`
      CREATE TABLE entities_next (
        entity_id INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT NOT NULL UNIQUE,
        owner_api_key_id INTEGER NULL,
        name TEXT NULL,
        description TEXT NOT NULL,
        data TEXT NOT NULL,
        is_deleted INTEGER NOT NULL,
        updated_at TEXT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(owner_api_key_id, name)
      )
    `);
    await this.execute(`
      INSERT INTO entities_next (
        entity_id, id, owner_api_key_id, name, description, data, is_deleted, updated_at, created_at
      )
      SELECT entity_id, id, NULL, name, description, data, is_deleted, updated_at, created_at
      FROM entities
    `);
    await this.execute('DROP TABLE entities');
    await this.execute('ALTER TABLE entities_next RENAME TO entities');
  }

  private async rebuildSqliteTagTableIfNeeded(): Promise<void> {
    const sql = await this.getSqliteCreateTableSql('tags');
    if (!sql.includes('name TEXT NOT NULL UNIQUE')) {
      await this.ensureSqliteColumn('tags', 'owner_api_key_id', 'INTEGER NULL');
      return;
    }

    await this.execute(`
      CREATE TABLE tags_next (
        tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
        id TEXT NOT NULL UNIQUE,
        owner_api_key_id INTEGER NULL,
        name TEXT NOT NULL,
        risk_level TEXT NOT NULL,
        updated_at TEXT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(owner_api_key_id, name)
      )
    `);
    await this.execute(`
      INSERT INTO tags_next (
        tag_id, id, owner_api_key_id, name, risk_level, updated_at, created_at
      )
      SELECT tag_id, id, NULL, name, risk_level, updated_at, created_at
      FROM tags
    `);
    await this.execute('DROP TABLE tags');
    await this.execute('ALTER TABLE tags_next RENAME TO tags');
  }

  private async getSqliteCreateTableSql(table: string): Promise<string> {
    const row = await this.one<DatabaseRow>(
      "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
      [table],
    );
    return stringValueOrEmpty(row, 'sql');
  }

  private async ensureSqliteColumn(
    table: string,
    column: string,
    definition: string,
  ): Promise<void> {
    const columns = await this.query<DatabaseRow>(`PRAGMA table_info(${table})`);
    if (!hasNamedColumn(columns, 'name', column)) {
      await this.execute(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
    }
  }

  private async backfillOwnerApiKeyIds(): Promise<void> {
    const firstKey = await this.one<DatabaseRow>(
      'SELECT api_key_id FROM api_keys ORDER BY api_key_id ASC LIMIT 1',
    );
    if (!firstKey) {
      return;
    }

    const firstApiKeyId = Number(firstKey.api_key_id);
    await this.execute('UPDATE entities SET owner_api_key_id = ? WHERE owner_api_key_id IS NULL', [
      firstApiKeyId,
    ]);
    await this.execute('UPDATE tags SET owner_api_key_id = ? WHERE owner_api_key_id IS NULL', [
      firstApiKeyId,
    ]);
    await this.execute(`
      UPDATE addresses
      SET owner_api_key_id = (
        SELECT e.owner_api_key_id FROM entities e WHERE e.entity_id = addresses.entity_id
      )
      WHERE owner_api_key_id IS NULL
    `);
    await this.execute('UPDATE addresses SET owner_api_key_id = ? WHERE owner_api_key_id IS NULL', [
      firstApiKeyId,
    ]);
  }

  private async hasMysqlColumn(table: string, column: string): Promise<boolean> {
    const columns = await this.query<DatabaseRow>(
      `
        SELECT COLUMN_NAME AS column_name
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?
      `,
      [table, column],
    );
    return columns.length > 0;
  }

  private async dropMysqlIndexIfExists(table: string, indexName: string): Promise<void> {
    if (await this.hasMysqlIndex(table, indexName)) {
      await this.execute(`ALTER TABLE ${table} DROP INDEX ${indexName}`);
    }
  }

  private async createMysqlIndexIfMissing(
    table: string,
    indexName: string,
    statement: string,
  ): Promise<void> {
    if (!(await this.hasMysqlIndex(table, indexName))) {
      await this.execute(statement);
    }
  }

  private async hasMysqlIndex(table: string, indexName: string): Promise<boolean> {
    const indexes = await this.query<DatabaseRow>(
      `
        SELECT INDEX_NAME AS index_name
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?
      `,
      [table, indexName],
    );
    return indexes.length > 0;
  }

  private async dropApiKeySecretColumn(): Promise<void> {
    if (this.client.kind === 'sqlite') {
      await this.dropSqliteApiKeySecretColumn();
      return;
    }

    await this.dropNonSqliteApiKeySecretColumn();
  }

  private async dropNonSqliteApiKeySecretColumn(): Promise<void> {
    if (this.client.kind === 'postgres') {
      await this.execute('ALTER TABLE api_keys DROP COLUMN IF EXISTS secret_key');
      return;
    }

    await this.dropMysqlApiKeySecretColumn();
  }

  private async dropSqliteApiKeySecretColumn(): Promise<void> {
    const columns = await this.query<DatabaseRow>('PRAGMA table_info(api_keys)');
    if (hasNamedColumn(columns, 'name', 'secret_key')) {
      await this.execute('ALTER TABLE api_keys DROP COLUMN secret_key');
    }
  }

  private async dropMysqlApiKeySecretColumn(): Promise<void> {
    const columns = await this.query<DatabaseRow>(
      `
        SELECT COLUMN_NAME AS column_name
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'api_keys'
      `,
    );
    if (hasNamedColumn(columns, 'column_name', 'secret_key')) {
      await this.execute('ALTER TABLE api_keys DROP COLUMN secret_key');
    }
  }

  private async ensureApiKeyHashUniqueIndex(): Promise<void> {
    if (supportsCreateIndexIfNotExists(this.client.kind)) {
      await this.execute(
        'CREATE UNIQUE INDEX IF NOT EXISTS uq_api_keys_secret_key_hash ON api_keys (secret_key_hash)',
      );
      return;
    }

    await this.ensureMysqlApiKeyHashUniqueIndex();
  }

  private async ensureMysqlApiKeyHashUniqueIndex(): Promise<void> {
    const indexes = await this.query<DatabaseRow>(
      `
        SELECT INDEX_NAME AS index_name
        FROM INFORMATION_SCHEMA.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = 'api_keys'
          AND INDEX_NAME = 'uq_api_keys_secret_key_hash'
      `,
    );
    if (indexes.length === 0) {
      await this.execute(
        'ALTER TABLE api_keys ADD UNIQUE KEY uq_api_keys_secret_key_hash (secret_key_hash(64))',
      );
    }
  }

  private async ensureNetworksZmqColumn(): Promise<void> {
    if (this.client.kind === 'sqlite') {
      await this.ensureSqliteNetworksZmqColumn();
      return;
    }

    await this.ensureNonSqliteNetworksZmqColumn();
  }

  private async ensureNonSqliteNetworksZmqColumn(): Promise<void> {
    if (this.client.kind === 'postgres') {
      await this.execute(
        'ALTER TABLE networks ADD COLUMN IF NOT EXISTS zmq_block_endpoint TEXT NULL',
      );
      return;
    }

    await this.ensureMysqlNetworksZmqColumn();
  }

  private async ensureSqliteNetworksZmqColumn(): Promise<void> {
    const columns = await this.query<DatabaseRow>('PRAGMA table_info(networks)');
    if (!hasNamedColumn(columns, 'name', 'zmq_block_endpoint')) {
      await this.execute('ALTER TABLE networks ADD COLUMN zmq_block_endpoint TEXT NULL');
    }
  }

  private async ensureMysqlNetworksZmqColumn(): Promise<void> {
    const columns = await this.query<DatabaseRow>(
      `
        SELECT COLUMN_NAME AS column_name
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'networks'
      `,
    );
    if (!hasNamedColumn(columns, 'column_name', 'zmq_block_endpoint')) {
      await this.execute('ALTER TABLE networks ADD COLUMN zmq_block_endpoint TEXT NULL');
    }
  }

  private mapApiKey(row: DatabaseRow): ApiKeyRecord {
    return {
      apiKeyId: Number(row.api_key_id),
      id: String(row.id),
      role: apiKeyRole(row.role),
      secretKeyHash: String(row.secret_key_hash),
      isActive: toBoolean(row.is_active),
      updatedAt: nullableString(row.updated_at),
      createdAt: String(row.created_at),
    };
  }

  private mapAuditEvent(row: DatabaseRow): AuditEventRecord {
    return {
      auditEventId: Number(row.audit_event_id),
      id: String(row.id),
      actorApiKeyId: Number(row.actor_api_key_id),
      actorApiKey: String(row.actor_api_key),
      actorRole: String(row.actor_role) === 'admin' ? 'admin' : 'member',
      method: String(row.method),
      path: String(row.path),
      route: String(row.route),
      operation: String(row.operation),
      resourceType: String(row.resource_type),
      resourceIds: auditResourceIds(row.resource_ids),
      ownerApiKeyId: nullableNumber(row.owner_api_key_id),
      ownerApiKey: nullableString(row.owner_api_key),
      statusCode: Number(row.status_code),
      outcome: mapAuditOutcome(sqlValueOrDefault(row.outcome, 'failure')),
      error: nullableString(row.error),
      requestId: String(row.request_id),
      ip: nullableString(row.ip),
      userAgent: nullableString(row.user_agent),
      createdAt: String(row.created_at),
    };
  }

  private mapNetwork(row: DatabaseRow): NetworkRecord {
    return {
      networkId: Number(row.network_id),
      id: String(row.id),
      name: String(row.name),
      architecture: parseChainFamily(String(row.architecture)),
      chainId: Number(row.chain_id),
      blockTime: Number(row.block_time),
      rpcEndpoint: String(row.rpc_endpoint),
      rps: Number(row.rps),
      zmqBlockEndpoint: nullableString(row.zmq_block_endpoint),
      isDeleted: toBoolean(row.is_deleted),
      updatedAt: row.updated_at ? String(row.updated_at) : null,
      createdAt: String(row.created_at),
    };
  }

  private mapActiveNetwork(row: DatabaseRow) {
    const network = this.mapNetwork(row);
    return {
      networkId: network.networkId,
      id: network.id,
      name: network.name,
      architecture: network.architecture,
      chainId: network.chainId,
      blockTime: network.blockTime,
      rpcEndpoint: network.rpcEndpoint,
      rps: network.rps,
      zmqBlockEndpoint: network.zmqBlockEndpoint,
    };
  }

  private mapCoreIndexerState(row: DatabaseRow): CoreIndexerState {
    return {
      networkId: Number(row.network_id),
      stage: String(row.stage) as CoreIndexerStage,
      syncTail: Number(row.sync_tail),
      processTail: Number(row.process_tail),
      onlineTip: Number(row.online_tip),
      lastError: nullableString(row.last_error),
      updatedAt: String(row.updated_at),
    };
  }

  private mapToken(row: DatabaseRow): TokenRecord {
    return {
      tokenId: Number(row.token_id),
      networkId: Number(row.network_id),
      id: String(row.id),
      name: String(row.name),
      symbol: String(row.symbol),
      address: String(row.address),
      decimals: Number(row.decimals),
      updatedAt: row.updated_at ? String(row.updated_at) : null,
      createdAt: String(row.created_at),
    };
  }

  private mapEntity(row: DatabaseRow): EntityRecord {
    return {
      entityId: Number(row.entity_id),
      id: String(row.id),
      name: row.name === null ? null : String(row.name),
      ownerApiKeyId: Number(row.owner_api_key_id),
      ...this.mapLabelMetadata(row),
    };
  }

  private mapAddress(row: DatabaseRow): AddressRecord {
    return {
      addressId: Number(row.address_id),
      entityId: Number(row.entity_id),
      networkId: Number(row.network_id),
      ownerApiKeyId: Number(row.owner_api_key_id),
      id: String(row.id),
      network: String(row.network),
      address: String(row.address),
      ...this.mapLabelMetadata(row),
    };
  }

  private mapLabelMetadata(row: DatabaseRow) {
    return {
      description: String(row.description),
      data: safeJsonParse<Record<string, unknown>>(sqlValueStringOrDefault(row.data, '{}'), {}),
      isDeleted: toBoolean(row.is_deleted),
      updatedAt: nullableString(row.updated_at),
      createdAt: String(row.created_at),
    };
  }

  private mapTag(row: DatabaseRow): TagRecord {
    return {
      tagId: Number(row.tag_id),
      id: String(row.id),
      ownerApiKeyId: Number(row.owner_api_key_id),
      name: String(row.name),
      riskLevel: parseRiskLevel(String(row.risk_level)),
      updatedAt: nullableString(row.updated_at),
      createdAt: String(row.created_at),
    };
  }

  private mapDirectLinkRecord(row: DatabaseRow): DirectLinkRecord {
    return {
      networkId: Number(row.network_id),
      fromAddress: String(row.from_address),
      toAddress: String(row.to_address),
      assetAddress: String(row.asset_address),
      transferCount: Number(row.transfer_count),
      totalAmountBase: String(row.total_amount_base),
      firstSeenBlockHeight: Number(row.first_seen_block_height),
      lastSeenBlockHeight: Number(row.last_seen_block_height),
    };
  }
}

function rowValueString(value: SqlValue | undefined): string | null {
  return value ? String(value) : null;
}

function rowValueNumber(value: SqlValue | undefined): number | null {
  return value === undefined ? null : Number(value);
}

function nativeBalanceOrZero(value: string | null): string {
  if (value === null) {
    return '0';
  }

  return value;
}

function numberValueOrZero(row: DatabaseRow | null, key: string): number {
  if (!row) {
    return 0;
  }

  return sqlNumberOrZero(row[key]);
}

function sqlNumberOrZero(value: SqlValue | undefined): number {
  if (value == null) {
    return 0;
  }

  return Number(value);
}

function stringValueOrEmpty(row: DatabaseRow | null, key: string): string {
  if (!row) {
    return '';
  }

  return sqlValueStringOrDefault(row[key], '');
}

function sqlValueOrDefault(value: SqlValue | undefined, fallback: SqlValue): SqlValue {
  if (value == null) {
    return fallback;
  }

  return value;
}

function sqlValueStringOrDefault(value: SqlValue | undefined, fallback: string): string {
  return String(sqlValueOrDefault(value, fallback));
}

function postgresPoolOptions(settings: DatabaseSettings) {
  return settings.ssl
    ? { connectionString: settings.location, ssl: settings.ssl }
    : { connectionString: settings.location };
}

function countRowValue(row: { count: number | string } | null): number {
  if (!row) {
    return 0;
  }

  return Number(row.count);
}

function addAuditResourceIdCondition(
  conditions: string[],
  params: SqlValue[],
  resourceId: string | undefined,
): void {
  if (!resourceId) {
    return;
  }

  conditions.push('resource_ids LIKE ?');
  params.push(`%"${resourceId}"%`);
}

function whereClause(conditions: string[]): string {
  return conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
}

function firstArrayItem<T>(values: T[]): T | undefined {
  return values[0];
}

function isAddressRecord(address: AddressRecord | null): address is AddressRecord {
  return address !== null;
}

function ownerTagClause(ownerApiKeyId: PrimaryId | undefined): string {
  return ownerApiKeyId === undefined ? '' : 'AND owner_api_key_id = ?';
}

function ownerTagParams(name: string, ownerApiKeyId: PrimaryId | undefined): SqlValue[] {
  return ownerApiKeyId === undefined ? [name] : [name, ownerApiKeyId];
}

function addEntityTagMapRow(result: Map<PrimaryId, PrimaryId[]>, row: DatabaseRow): void {
  const entityId = Number(row.entity_id);
  const tagIds = result.get(entityId) ?? [];
  tagIds.push(Number(row.tag_id));
  result.set(entityId, tagIds);
}

function entityTagMap(rows: DatabaseRow[]): Map<PrimaryId, PrimaryId[]> {
  const result = new Map<PrimaryId, PrimaryId[]>();
  for (const row of rows) {
    addEntityTagMapRow(result, row);
  }
  return result;
}

function hasBootstrapAppliedBlock(blockHeight: number, bootstrapTail: number | null): boolean {
  if (bootstrapTail === null) {
    return false;
  }

  return blockHeight <= bootstrapTail;
}

function bootstrapBlockIdentities(
  networkId: PrimaryId,
  blocks: Array<{ blockHash: string; blockHeight: number }>,
  bootstrapTail: number | null,
): string[] {
  if (bootstrapTail === null) {
    return [];
  }

  return blocks
    .filter((block) => block.blockHeight <= bootstrapTail)
    .map((block) => projectionBlockIdentity(networkId, block.blockHeight, block.blockHash));
}

function activeNetworkOrNull<T extends { isDeleted: boolean }>(network: T | null): T | null {
  if (!network) {
    return null;
  }

  return nonDeletedRecordOrNull(network);
}

function nonDeletedRecordOrNull<T extends { isDeleted: boolean }>(record: T): T | null {
  return record.isDeleted ? null : record;
}

function coreApplyOptions(context: CoreDogecoinApplyContext | undefined): {
  statementTimeoutMs?: number;
} {
  const statementTimeoutMs = coreStatementTimeout(context);
  return statementTimeoutMs ? { statementTimeoutMs } : {};
}

function coreStatementTimeout(context: CoreDogecoinApplyContext | undefined): number | undefined {
  if (!context) {
    return undefined;
  }

  return context.statementTimeoutMs;
}

function coreBlockReplayResult(
  input: CoreDogecoinBlockApplication,
  existingHash: string,
): CoreDogecoinApplyResult {
  if (existingHash !== input.blockHash) {
    throw new Error(coreBlockHashMismatchMessage(input, existingHash));
  }

  return { applied: false, processTail: input.blockHeight };
}

function coreBlockHashMismatchMessage(
  input: CoreDogecoinBlockApplication,
  existingHash: string,
): string {
  return `core block hash mismatch network=${input.networkId} height=${input.blockHeight} existing=${existingHash} next=${input.blockHash}`;
}

function projectionUtxoOutputParams(
  outputs: ProjectionUtxoOutput[],
  timestamp: string,
  booleanValue: (value: boolean) => SqlValue,
): SqlValue[] {
  return outputs.flatMap((output) => [
    output.networkId,
    output.outputKey,
    output.blockHeight,
    output.blockHash,
    output.blockTime,
    output.txid,
    output.txIndex,
    output.vout,
    output.address,
    output.scriptType,
    output.valueBase,
    booleanValue(output.isCoinbase),
    booleanValue(output.isSpendable),
    output.spentByTxid,
    output.spentInBlock,
    output.spentInputIndex,
    timestamp,
  ]);
}

function projectionBalanceParams(
  balances: ProjectionBalanceSnapshot[],
  timestamp: string,
): SqlValue[] {
  return balances.flatMap((balance) => [
    balance.networkId,
    balance.address,
    balance.assetAddress,
    balance.balance,
    balance.asOfBlockHeight,
    timestamp,
  ]);
}

function projectionDirectLinkParams(links: DirectLinkRecord[], timestamp: string): SqlValue[] {
  return links.flatMap((link) => [
    link.networkId,
    link.fromAddress,
    link.toAddress,
    link.assetAddress,
    link.transferCount,
    link.totalAmountBase,
    link.firstSeenBlockHeight,
    link.lastSeenBlockHeight,
    timestamp,
  ]);
}

function projectionSourceLinkParams(rows: SourceLinkRecord[], timestamp: string): SqlValue[] {
  return rows.flatMap((row) => [
    row.networkId,
    row.sourceAddressId,
    row.sourceAddress,
    row.toAddress,
    row.hopCount,
    row.pathTransferCount,
    JSON.stringify(row.pathAddresses),
    row.firstSeenBlockHeight,
    row.lastSeenBlockHeight,
    timestamp,
    timestamp,
  ]);
}

function projectionAppliedBlockParams(
  blocks: ProjectionAppliedBlock[],
  timestamp: string,
): SqlValue[] {
  return blocks.flatMap((block) => [
    block.networkId,
    block.blockHeight,
    block.blockHash,
    timestamp,
    timestamp,
  ]);
}

function storedCoreHash(row: DatabaseRow | null): string | null {
  if (!row) {
    return null;
  }

  return String(row.block_hash);
}

function assertMatchingCoreIdentityHash(
  table: 'core_block_undo' | 'core_processed_blocks',
  networkId: PrimaryId,
  blockHeight: number,
  blockHash: string,
  storedHash: string | null,
): void {
  if (storedHash !== blockHash) {
    throw new Error(
      `core block hash mismatch table=${table} network=${networkId} height=${blockHeight} existing=${displayCoreHash(storedHash)} next=${blockHash}`,
    );
  }
}

function displayCoreHash(hash: string | null): string {
  if (hash === null) {
    return 'missing';
  }

  return hash;
}

function sqliteResultRows<T extends DatabaseRow>(rows: unknown): T[] {
  if (!Array.isArray(rows)) {
    return [];
  }

  return rows as T[];
}

function sqliteBooleanLiteral(value: boolean): string {
  return value ? '1' : '0';
}

function sqlBooleanLiteral(value: boolean): string {
  return value ? 'TRUE' : 'FALSE';
}

function sqliteBooleanValue(value: boolean): SqlValue {
  return value ? 1 : 0;
}

function migrationStatements(kind: SupportedExecutor['kind']): string[] {
  return {
    sqlite: sqliteMigrations,
    postgres: postgresMigrations,
    mysql: mysqlMigrations,
  }[kind];
}

function supportsCreateIndexIfNotExists(kind: SupportedExecutor['kind']): boolean {
  return kind !== 'mysql';
}

function isPresentRecord<T>(record: T | null): record is T {
  return record !== null;
}

function apiKeyRole(value: SqlValue | undefined): ApiKeyRecord['role'] {
  return String(value) === 'admin' ? 'admin' : 'member';
}

function auditResourceIds(value: SqlValue | undefined): string[] {
  return safeJsonParse<string[]>(sqlValueStringOrDefault(value, '[]'), []);
}

function coreIndexerStateRow(
  input: CoreIndexerStateUpdate,
  current: CoreIndexerState | null,
  timestamp: string,
): CoreIndexerState {
  const currentValues = currentCoreIndexerStateValues(current);
  return {
    networkId: input.networkId,
    stage: coreIndexerStage(input.stage, currentValues.stage),
    syncTail: coreIndexerNumber(input.syncTail, currentValues.syncTail, -1),
    processTail: coreIndexerNumber(input.processTail, currentValues.processTail, -1),
    onlineTip: coreIndexerNumber(input.onlineTip, currentValues.onlineTip, -1),
    lastError: coreIndexerLastError(input, current),
    updatedAt: timestamp,
  };
}

function currentCoreIndexerStateValues(
  current: CoreIndexerState | null,
): Partial<CoreIndexerState> {
  if (!current) {
    return {};
  }

  return current;
}

function coreIndexerStage(
  nextStage: CoreIndexerStage | undefined,
  currentStage: CoreIndexerStage | undefined,
): CoreIndexerStage {
  return coreIndexerValue(nextStage, currentStage, 'sync_backfill');
}

function coreIndexerNumber(
  nextValue: number | undefined,
  currentValue: number | undefined,
  fallback: number,
): number {
  return coreIndexerValue(nextValue, currentValue, fallback);
}

function coreIndexerValue<T>(
  nextValue: T | undefined,
  currentValue: T | undefined,
  fallback: T,
): T {
  if (nextValue !== undefined) {
    return nextValue;
  }

  return currentValueOrFallback(currentValue, fallback);
}

function currentValueOrFallback<T>(currentValue: T | undefined, fallback: T): T {
  if (currentValue !== undefined) {
    return currentValue;
  }

  return fallback;
}

function coreIndexerLastError(
  input: CoreIndexerStateUpdate,
  current: CoreIndexerState | null,
): string | null {
  if (input.lastError !== undefined) {
    return input.lastError;
  }

  return currentCoreIndexerLastError(current);
}

function currentCoreIndexerLastError(current: CoreIndexerState | null): string | null {
  if (!current) {
    return null;
  }

  return current.lastError;
}

function coreIndexerStateParams(row: CoreIndexerState, timestamp: string): SqlValue[] {
  return [
    row.networkId,
    row.stage,
    row.syncTail,
    row.processTail,
    row.onlineTip,
    row.lastError,
    timestamp,
    timestamp,
  ];
}

function createCoreUtxoMutation(): CoreUtxoMutation {
  return {
    affectedAddresses: new Set<string>(),
    nextOutputs: new Map<string, ProjectionUtxoOutput>(),
  };
}

function appendCoreCreatedOutputs(
  mutation: CoreUtxoMutation,
  outputs: ProjectionUtxoOutput[],
): void {
  for (const output of outputs) {
    mutation.nextOutputs.set(output.outputKey, { ...output });
    rememberSpendableAddress(mutation.affectedAddresses, output);
  }
}

function appendCoreSpentOutputs(
  mutation: CoreUtxoMutation,
  spends: CoreDogecoinSpend[],
  currentOutputs: Map<string, ProjectionUtxoOutput>,
): void {
  for (const spend of spends) {
    appendCoreSpentOutput(mutation, spend, currentOutputs);
  }
}

function appendCoreSpentOutput(
  mutation: CoreUtxoMutation,
  spend: CoreDogecoinSpend,
  currentOutputs: Map<string, ProjectionUtxoOutput>,
): void {
  const current = mutation.nextOutputs.get(spend.outputKey) ?? currentOutputs.get(spend.outputKey);
  assertCoreSpendableOutput(current, spend);
  mutation.nextOutputs.set(spend.outputKey, spentCoreOutput(current, spend));
  rememberSpendableAddress(mutation.affectedAddresses, current);
}

function assertCoreSpendableOutput(
  current: ProjectionUtxoOutput | undefined,
  spend: CoreDogecoinSpend,
): asserts current is ProjectionUtxoOutput {
  assertCoreOutputExists(current, spend);
  assertCoreOutputNotConflicting(current, spend);
}

function assertCoreOutputExists(
  current: ProjectionUtxoOutput | undefined,
  spend: CoreDogecoinSpend,
): asserts current is ProjectionUtxoOutput {
  if (!current) {
    throw new Error(`missing core utxo output: ${spend.outputKey}`);
  }
}

function assertCoreOutputNotConflicting(
  current: ProjectionUtxoOutput,
  spend: CoreDogecoinSpend,
): void {
  if (isConflictingCoreSpend(current, spend)) {
    throw new Error(`core utxo output already spent: ${spend.outputKey} by ${current.spentByTxid}`);
  }
}

function isSameCoreSpend(output: ProjectionUtxoOutput, spend: CoreDogecoinSpend): boolean {
  return [
    output.spentByTxid === spend.spentByTxid,
    output.spentInBlock === spend.spentInBlock,
    output.spentInputIndex === spend.spentInputIndex,
  ].every(Boolean);
}

function isConflictingCoreSpend(output: ProjectionUtxoOutput, spend: CoreDogecoinSpend): boolean {
  return [Boolean(output.spentByTxid), !isSameCoreSpend(output, spend)].every(Boolean);
}

function spentCoreOutput(
  current: ProjectionUtxoOutput,
  spend: CoreDogecoinSpend,
): ProjectionUtxoOutput {
  return {
    ...current,
    spentByTxid: spend.spentByTxid,
    spentInBlock: spend.spentInBlock,
    spentInputIndex: spend.spentInputIndex,
  };
}

function rememberSpendableAddress(addresses: Set<string>, output: ProjectionUtxoOutput): void {
  if (!isRememberedSpendableAddress(output)) {
    return;
  }

  addresses.add(output.address);
}

function isRememberedSpendableAddress(output: ProjectionUtxoOutput): boolean {
  return [output.isSpendable, Boolean(output.address)].every(Boolean);
}

function initialCoreWindowApplyResult(
  input: CoreDogecoinBlockApplication[],
): CoreDogecoinApplyResult {
  return {
    applied: false,
    processTail: lastCoreApplicationHeight(input),
  };
}

function lastCoreApplicationHeight(input: CoreDogecoinBlockApplication[]): number {
  const lastApplication = input.at(-1);
  if (!lastApplication) {
    return -1;
  }

  return lastApplication.blockHeight;
}

function mergeCoreApplyResult(
  current: CoreDogecoinApplyResult,
  next: CoreDogecoinApplyResult,
): CoreDogecoinApplyResult {
  return {
    applied: current.applied || next.applied,
    processTail: next.processTail,
  };
}

function initialCoreBalanceAccumulators(addresses: string[]): Map<string, CoreBalanceAccumulator> {
  return new Map(addresses.map((address) => [address, { balance: 0n, utxoCount: 0 }]));
}

function appendCoreBalanceRows(
  balances: Map<string, CoreBalanceAccumulator>,
  rows: DatabaseRow[],
): void {
  for (const row of rows) {
    appendCoreBalanceRow(balances, row);
  }
}

function appendCoreBalanceRow(
  balances: Map<string, CoreBalanceAccumulator>,
  row: DatabaseRow,
): void {
  const address = String(row.address);
  const current = balances.get(address) ?? { balance: 0n, utxoCount: 0 };
  current.balance += parseAmountBase(String(row.value_base));
  current.utxoCount += 1;
  balances.set(address, current);
}

function coreBalanceRows(
  networkId: PrimaryId,
  balances: Map<string, CoreBalanceAccumulator>,
  asOfBlockHeight: number,
): CoreBalanceRow[] {
  return [...balances.entries()].map(([address, balance]) => ({
    networkId,
    address,
    assetAddress: '',
    balance: formatAmountBase(balance.balance),
    utxoCount: balance.utxoCount,
    asOfBlockHeight,
  }));
}

function coreBalanceParams(rows: CoreBalanceRow[], timestamp: string): SqlValue[] {
  return rows.flatMap((row) => [
    row.networkId,
    row.address,
    row.assetAddress,
    row.balance,
    row.utxoCount,
    row.asOfBlockHeight,
    timestamp,
  ]);
}

function assertMysqlCoreNetworkLock(row: DatabaseRow | undefined, networkId: PrimaryId): void {
  if (mysqlLockValue(row) !== 1) {
    throw new Error(`timed out acquiring core network lock network=${networkId}`);
  }
}

function mysqlLockValue(row: DatabaseRow | undefined): number {
  if (!row) {
    return 0;
  }

  return Number(row.locked);
}

function hasNamedColumn(rows: DatabaseRow[], key: string, name: string): boolean {
  return rows.some((row) => String(row[key]) === name);
}

function mapNullableRow<T>(row: DatabaseRow | null, mapRow: (row: DatabaseRow) => T): T | null {
  if (!row) {
    return null;
  }

  return mapRow(row);
}

function addOptionalCondition(
  conditions: string[],
  params: SqlValue[],
  condition: string,
  value: SqlValue | undefined,
): void {
  if (value === undefined) {
    return;
  }

  conditions.push(condition);
  params.push(value);
}

function createAuditEventId(): string {
  return `aud_${randomUUID().replaceAll('-', '').slice(0, 24)}`;
}

function mapAuditOutcome(value: SqlValue): AuditEventRecord['outcome'] {
  if (isAuditOutcome(value)) {
    return value;
  }

  return 'failure';
}

function isAuditOutcome(value: SqlValue): value is AuditEventRecord['outcome'] {
  return typeof value === 'string' && auditOutcomeValues.has(value as AuditEventRecord['outcome']);
}

function assertFound<T>(value: T | null): T {
  if (!value) {
    throw new Error('Expected record to exist');
  }

  return value;
}

function placeholders(count: number): string {
  return Array.from({ length: count }, () => '?').join(', ');
}

function multiRowPlaceholders(rowCount: number, valueCount: number): string {
  return Array.from({ length: rowCount }, () => `(${placeholders(valueCount)})`).join(', ');
}

function chunkArray<T>(values: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

function dedupeRecords<T>(rows: T[], keyFor: (row: T) => string): T[] {
  const seen = new Set<string>();
  const deduped: T[] = [];
  for (const row of rows) {
    appendUniqueRecord(deduped, seen, row, keyFor(row));
  }
  return deduped;
}

function appendUniqueRecord<T>(deduped: T[], seen: Set<string>, row: T, key: string): void {
  if (seen.has(key)) {
    return;
  }

  seen.add(key);
  deduped.push(row);
}

function parsePendingRelinkAddressId(key: string, networkId: PrimaryId): PrimaryId | null {
  const match = key.match(new RegExp(`^newly_added_address_n${networkId}_a(\\d+)$`, 'u'));
  const addressId = firstMatchGroup(match);
  if (addressId === null) {
    return null;
  }

  return Number(addressId);
}

function firstMatchGroup(match: RegExpMatchArray | null): string | null {
  if (!match) {
    return null;
  }

  return nonEmptyMatchGroup(match[1]);
}

function nonEmptyMatchGroup(value: string | undefined): string | null {
  if (!value) {
    return null;
  }

  return value;
}
const sqliteMigrations = [
  `CREATE TABLE IF NOT EXISTS configs (
      config_id INTEGER PRIMARY KEY AUTOINCREMENT,
      key TEXT NOT NULL UNIQUE,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS api_keys (
      api_key_id INTEGER PRIMARY KEY AUTOINCREMENT,
      id TEXT NOT NULL UNIQUE,
      secret_key_hash TEXT NOT NULL,
      is_active INTEGER NOT NULL,
      role TEXT NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS audit_events (
      audit_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
      id TEXT NOT NULL UNIQUE,
      actor_api_key_id INTEGER NOT NULL,
      actor_api_key TEXT NOT NULL,
      actor_role TEXT NOT NULL,
      method TEXT NOT NULL,
      path TEXT NOT NULL,
      route TEXT NOT NULL,
      operation TEXT NOT NULL,
      resource_type TEXT NOT NULL,
      resource_ids TEXT NOT NULL,
      owner_api_key_id INTEGER NULL,
      owner_api_key TEXT NULL,
      status_code INTEGER NOT NULL,
      outcome TEXT NOT NULL,
      error TEXT NULL,
      request_id TEXT NOT NULL,
      ip TEXT NULL,
      user_agent TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS networks (
      network_id INTEGER PRIMARY KEY AUTOINCREMENT,
      id TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL UNIQUE,
      architecture TEXT NOT NULL,
      chain_id INTEGER NOT NULL,
      block_time INTEGER NOT NULL,
      rpc_endpoint TEXT NOT NULL,
      rps INTEGER NOT NULL,
      zmq_block_endpoint TEXT NULL,
      is_deleted INTEGER NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS entities (
      entity_id INTEGER PRIMARY KEY AUTOINCREMENT,
      id TEXT NOT NULL UNIQUE,
      owner_api_key_id INTEGER NULL,
      name TEXT NULL,
      description TEXT NOT NULL,
      data TEXT NOT NULL,
      is_deleted INTEGER NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(owner_api_key_id, name)
    )`,
  `CREATE TABLE IF NOT EXISTS addresses (
      address_id INTEGER PRIMARY KEY AUTOINCREMENT,
      entity_id INTEGER NOT NULL,
      network_id INTEGER NOT NULL,
      owner_api_key_id INTEGER NULL,
      network TEXT NOT NULL,
      id TEXT NOT NULL UNIQUE,
      address TEXT NOT NULL,
      description TEXT NOT NULL,
      data TEXT NOT NULL,
      is_deleted INTEGER NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(entity_id, network_id, address)
    )`,
  `CREATE TABLE IF NOT EXISTS tags (
      tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
      id TEXT NOT NULL UNIQUE,
      owner_api_key_id INTEGER NULL,
      name TEXT NOT NULL,
      risk_level TEXT NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(owner_api_key_id, name)
    )`,
  `CREATE TABLE IF NOT EXISTS entity_tags (
      entity_id INTEGER NOT NULL,
      tag_id INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (entity_id, tag_id)
    )`,
  `CREATE TABLE IF NOT EXISTS tokens (
      token_id INTEGER PRIMARY KEY AUTOINCREMENT,
      network_id INTEGER NOT NULL,
      id TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      symbol TEXT NOT NULL,
      address TEXT NOT NULL,
      decimals INTEGER NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(network_id, address)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_applied_blocks (
      network_id INTEGER NOT NULL,
      block_height INTEGER NOT NULL,
      block_hash TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height, block_hash)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_direct_link_applied_blocks (
      network_id INTEGER NOT NULL,
      block_height INTEGER NOT NULL,
      block_hash TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height, block_hash)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_utxo_outputs_current (
      network_id INTEGER NOT NULL,
      output_key TEXT NOT NULL,
      block_height INTEGER NOT NULL,
      block_hash TEXT NOT NULL,
      block_time INTEGER NOT NULL,
      txid TEXT NOT NULL,
      tx_index INTEGER NOT NULL,
      vout INTEGER NOT NULL,
      address TEXT NOT NULL,
      script_type TEXT NOT NULL,
      value_base TEXT NOT NULL,
      is_coinbase INTEGER NOT NULL,
      is_spendable INTEGER NOT NULL,
      spent_by_txid TEXT NULL,
      spent_in_block INTEGER NULL,
      spent_input_index INTEGER NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, output_key)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_balances_current (
      network_id INTEGER NOT NULL,
      address TEXT NOT NULL,
      asset_address TEXT NOT NULL,
      balance TEXT NOT NULL,
      as_of_block_height INTEGER NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, address, asset_address)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_direct_links_current (
      network_id INTEGER NOT NULL,
      from_address TEXT NOT NULL,
      to_address TEXT NOT NULL,
      asset_address TEXT NOT NULL,
      transfer_count INTEGER NOT NULL,
      total_amount_base TEXT NOT NULL,
      first_seen_block_height INTEGER NOT NULL,
      last_seen_block_height INTEGER NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, from_address, to_address, asset_address)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_source_links_current (
      network_id INTEGER NOT NULL,
      source_address_id INTEGER NOT NULL,
      source_address TEXT NOT NULL,
      to_address TEXT NOT NULL,
      hop_count INTEGER NOT NULL,
      path_transfer_count INTEGER NOT NULL,
      path_addresses TEXT NOT NULL,
      first_seen_block_height INTEGER NOT NULL,
      last_seen_block_height INTEGER NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, source_address_id, to_address)
    )`,
  `CREATE TABLE IF NOT EXISTS core_blocks (
      network_id INTEGER NOT NULL,
      block_height INTEGER NOT NULL,
      block_hash TEXT NOT NULL,
      previous_block_hash TEXT NULL,
      block_time INTEGER NOT NULL,
      tx_count INTEGER NOT NULL,
      raw_storage_key TEXT NOT NULL,
      fetched_at TEXT NOT NULL,
      processed_at TEXT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height)
    )`,
  `CREATE TABLE IF NOT EXISTS core_utxos (
      network_id INTEGER NOT NULL,
      output_key TEXT NOT NULL,
      block_height INTEGER NOT NULL,
      block_hash TEXT NOT NULL,
      block_time INTEGER NOT NULL,
      txid TEXT NOT NULL,
      tx_index INTEGER NOT NULL,
      vout INTEGER NOT NULL,
      address TEXT NOT NULL,
      script_type TEXT NOT NULL,
      value_base TEXT NOT NULL,
      is_coinbase INTEGER NOT NULL,
      is_spendable INTEGER NOT NULL,
      spent_by_txid TEXT NULL,
      spent_in_block INTEGER NULL,
      spent_input_index INTEGER NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, output_key)
    )`,
  `CREATE TABLE IF NOT EXISTS core_balances (
      network_id INTEGER NOT NULL,
      address TEXT NOT NULL,
      asset_address TEXT NOT NULL,
      balance TEXT NOT NULL,
      utxo_count INTEGER NOT NULL,
      as_of_block_height INTEGER NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, address, asset_address)
    )`,
  `CREATE TABLE IF NOT EXISTS core_processed_blocks (
      network_id INTEGER NOT NULL,
      block_height INTEGER NOT NULL,
      block_hash TEXT NOT NULL,
      processed_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height)
    )`,
  `CREATE TABLE IF NOT EXISTS core_block_undo (
      network_id INTEGER NOT NULL,
      block_height INTEGER NOT NULL,
      block_hash TEXT NOT NULL,
      undo_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height)
    )`,
  `CREATE TABLE IF NOT EXISTS core_indexer_state (
      network_id INTEGER PRIMARY KEY,
      stage TEXT NOT NULL,
      sync_tail INTEGER NOT NULL,
      process_tail INTEGER NOT NULL,
      online_tip INTEGER NOT NULL,
      last_error TEXT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE INDEX IF NOT EXISTS idx_core_utxos_address_spend
    ON core_utxos (network_id, address, is_spendable, spent_by_txid)`,
  `CREATE INDEX IF NOT EXISTS idx_projection_balances_current_address
    ON projection_balances_current (address)`,
  `CREATE INDEX IF NOT EXISTS idx_projection_direct_links_current_from
    ON projection_direct_links_current (network_id, from_address)`,
  `CREATE INDEX IF NOT EXISTS idx_projection_source_links_current_to
    ON projection_source_links_current (network_id, to_address)`,
];

const postgresMigrations = [
  `CREATE TABLE IF NOT EXISTS configs (
      config_id BIGSERIAL PRIMARY KEY,
      key TEXT NOT NULL UNIQUE,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS api_keys (
      api_key_id BIGSERIAL PRIMARY KEY,
      id TEXT NOT NULL UNIQUE,
      secret_key_hash TEXT NOT NULL,
      is_active BOOLEAN NOT NULL,
      role TEXT NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS audit_events (
      audit_event_id BIGSERIAL PRIMARY KEY,
      id TEXT NOT NULL UNIQUE,
      actor_api_key_id BIGINT NOT NULL,
      actor_api_key TEXT NOT NULL,
      actor_role TEXT NOT NULL,
      method TEXT NOT NULL,
      path TEXT NOT NULL,
      route TEXT NOT NULL,
      operation TEXT NOT NULL,
      resource_type TEXT NOT NULL,
      resource_ids TEXT NOT NULL,
      owner_api_key_id BIGINT NULL,
      owner_api_key TEXT NULL,
      status_code INTEGER NOT NULL,
      outcome TEXT NOT NULL,
      error TEXT NULL,
      request_id TEXT NOT NULL,
      ip TEXT NULL,
      user_agent TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS networks (
      network_id BIGSERIAL PRIMARY KEY,
      id TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL UNIQUE,
      architecture TEXT NOT NULL,
      chain_id BIGINT NOT NULL,
      block_time BIGINT NOT NULL,
      rpc_endpoint TEXT NOT NULL,
      rps INTEGER NOT NULL,
      zmq_block_endpoint TEXT NULL,
      is_deleted BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS entities (
      entity_id BIGSERIAL PRIMARY KEY,
      id TEXT NOT NULL UNIQUE,
      owner_api_key_id BIGINT NULL,
      name TEXT NULL,
      description TEXT NOT NULL,
      data TEXT NOT NULL,
      is_deleted BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(owner_api_key_id, name)
    )`,
  `CREATE TABLE IF NOT EXISTS addresses (
      address_id BIGSERIAL PRIMARY KEY,
      entity_id BIGINT NOT NULL,
      network_id BIGINT NOT NULL,
      owner_api_key_id BIGINT NULL,
      network TEXT NOT NULL,
      id TEXT NOT NULL UNIQUE,
      address TEXT NOT NULL,
      description TEXT NOT NULL,
      data TEXT NOT NULL,
      is_deleted BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(entity_id, network_id, address)
    )`,
  `CREATE TABLE IF NOT EXISTS tags (
      tag_id BIGSERIAL PRIMARY KEY,
      id TEXT NOT NULL UNIQUE,
      owner_api_key_id BIGINT NULL,
      name TEXT NOT NULL,
      risk_level TEXT NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(owner_api_key_id, name)
    )`,
  `CREATE TABLE IF NOT EXISTS entity_tags (
      entity_id BIGINT NOT NULL,
      tag_id BIGINT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (entity_id, tag_id)
    )`,
  `CREATE TABLE IF NOT EXISTS tokens (
      token_id BIGSERIAL PRIMARY KEY,
      network_id BIGINT NOT NULL,
      id TEXT NOT NULL UNIQUE,
      name TEXT NOT NULL,
      symbol TEXT NOT NULL,
      address TEXT NOT NULL,
      decimals INTEGER NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE(network_id, address)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_applied_blocks (
      network_id BIGINT NOT NULL,
      block_height BIGINT NOT NULL,
      block_hash TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height, block_hash)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_direct_link_applied_blocks (
      network_id BIGINT NOT NULL,
      block_height BIGINT NOT NULL,
      block_hash TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height, block_hash)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_utxo_outputs_current (
      network_id BIGINT NOT NULL,
      output_key TEXT NOT NULL,
      block_height BIGINT NOT NULL,
      block_hash TEXT NOT NULL,
      block_time BIGINT NOT NULL,
      txid TEXT NOT NULL,
      tx_index BIGINT NOT NULL,
      vout BIGINT NOT NULL,
      address TEXT NOT NULL,
      script_type TEXT NOT NULL,
      value_base TEXT NOT NULL,
      is_coinbase BOOLEAN NOT NULL,
      is_spendable BOOLEAN NOT NULL,
      spent_by_txid TEXT NULL,
      spent_in_block BIGINT NULL,
      spent_input_index BIGINT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, output_key)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_balances_current (
      network_id BIGINT NOT NULL,
      address TEXT NOT NULL,
      asset_address TEXT NOT NULL,
      balance TEXT NOT NULL,
      as_of_block_height BIGINT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, address, asset_address)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_direct_links_current (
      network_id BIGINT NOT NULL,
      from_address TEXT NOT NULL,
      to_address TEXT NOT NULL,
      asset_address TEXT NOT NULL,
      transfer_count BIGINT NOT NULL,
      total_amount_base TEXT NOT NULL,
      first_seen_block_height BIGINT NOT NULL,
      last_seen_block_height BIGINT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, from_address, to_address, asset_address)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_source_links_current (
      network_id BIGINT NOT NULL,
      source_address_id BIGINT NOT NULL,
      source_address TEXT NOT NULL,
      to_address TEXT NOT NULL,
      hop_count BIGINT NOT NULL,
      path_transfer_count BIGINT NOT NULL,
      path_addresses TEXT NOT NULL,
      first_seen_block_height BIGINT NOT NULL,
      last_seen_block_height BIGINT NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, source_address_id, to_address)
    )`,
  `CREATE TABLE IF NOT EXISTS core_blocks (
      network_id BIGINT NOT NULL,
      block_height BIGINT NOT NULL,
      block_hash TEXT NOT NULL,
      previous_block_hash TEXT NULL,
      block_time BIGINT NOT NULL,
      tx_count BIGINT NOT NULL,
      raw_storage_key TEXT NOT NULL,
      fetched_at TEXT NOT NULL,
      processed_at TEXT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height)
    )`,
  `CREATE TABLE IF NOT EXISTS core_utxos (
      network_id BIGINT NOT NULL,
      output_key TEXT NOT NULL,
      block_height BIGINT NOT NULL,
      block_hash TEXT NOT NULL,
      block_time BIGINT NOT NULL,
      txid TEXT NOT NULL,
      tx_index BIGINT NOT NULL,
      vout BIGINT NOT NULL,
      address TEXT NOT NULL,
      script_type TEXT NOT NULL,
      value_base TEXT NOT NULL,
      is_coinbase BOOLEAN NOT NULL,
      is_spendable BOOLEAN NOT NULL,
      spent_by_txid TEXT NULL,
      spent_in_block BIGINT NULL,
      spent_input_index BIGINT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, output_key)
    )`,
  `CREATE TABLE IF NOT EXISTS core_balances (
      network_id BIGINT NOT NULL,
      address TEXT NOT NULL,
      asset_address TEXT NOT NULL,
      balance TEXT NOT NULL,
      utxo_count BIGINT NOT NULL,
      as_of_block_height BIGINT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, address, asset_address)
    )`,
  `CREATE TABLE IF NOT EXISTS core_processed_blocks (
      network_id BIGINT NOT NULL,
      block_height BIGINT NOT NULL,
      block_hash TEXT NOT NULL,
      processed_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height)
    )`,
  `CREATE TABLE IF NOT EXISTS core_block_undo (
      network_id BIGINT NOT NULL,
      block_height BIGINT NOT NULL,
      block_hash TEXT NOT NULL,
      undo_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height)
    )`,
  `CREATE TABLE IF NOT EXISTS core_indexer_state (
      network_id BIGINT PRIMARY KEY,
      stage TEXT NOT NULL,
      sync_tail BIGINT NOT NULL,
      process_tail BIGINT NOT NULL,
      online_tip BIGINT NOT NULL,
      last_error TEXT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE INDEX IF NOT EXISTS idx_core_utxos_address_spend
    ON core_utxos (network_id, address, is_spendable, spent_by_txid)`,
  `CREATE INDEX IF NOT EXISTS idx_projection_balances_current_address
    ON projection_balances_current (address)`,
  `CREATE INDEX IF NOT EXISTS idx_projection_direct_links_current_from
    ON projection_direct_links_current (network_id, from_address)`,
  `CREATE INDEX IF NOT EXISTS idx_projection_source_links_current_to
    ON projection_source_links_current (network_id, to_address)`,
];

const mysqlMigrations = [
  `CREATE TABLE IF NOT EXISTS configs (
      config_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      key TEXT NOT NULL,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      UNIQUE KEY uq_configs_key (key(255))
    )`,
  `CREATE TABLE IF NOT EXISTS api_keys (
      api_key_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      id VARCHAR(255) NOT NULL UNIQUE,
      secret_key_hash VARCHAR(64) NOT NULL,
      is_active BOOLEAN NOT NULL,
      role VARCHAR(32) NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE KEY uq_api_keys_secret_key_hash (secret_key_hash)
    )`,
  `CREATE TABLE IF NOT EXISTS audit_events (
      audit_event_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      id VARCHAR(255) NOT NULL UNIQUE,
      actor_api_key_id BIGINT NOT NULL,
      actor_api_key VARCHAR(255) NOT NULL,
      actor_role VARCHAR(32) NOT NULL,
      method VARCHAR(16) NOT NULL,
      path TEXT NOT NULL,
      route TEXT NOT NULL,
      operation VARCHAR(64) NOT NULL,
      resource_type VARCHAR(64) NOT NULL,
      resource_ids JSON NOT NULL,
      owner_api_key_id BIGINT NULL,
      owner_api_key VARCHAR(255) NULL,
      status_code INTEGER NOT NULL,
      outcome VARCHAR(32) NOT NULL,
      error TEXT NULL,
      request_id VARCHAR(255) NOT NULL,
      ip TEXT NULL,
      user_agent TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS networks (
      network_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      id VARCHAR(255) NOT NULL UNIQUE,
      name VARCHAR(255) NOT NULL UNIQUE,
      architecture VARCHAR(64) NOT NULL,
      chain_id BIGINT NOT NULL,
      block_time BIGINT NOT NULL,
      rpc_endpoint TEXT NOT NULL,
      rps INTEGER NOT NULL,
      zmq_block_endpoint TEXT NULL,
      is_deleted BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS entities (
      entity_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      id VARCHAR(255) NOT NULL UNIQUE,
      owner_api_key_id BIGINT NULL,
      name VARCHAR(255) NULL,
      description TEXT NOT NULL,
      data JSON NOT NULL,
      is_deleted BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE KEY uq_entities_owner_name (owner_api_key_id, name)
    )`,
  `CREATE TABLE IF NOT EXISTS addresses (
      address_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      entity_id BIGINT NOT NULL,
      network_id BIGINT NOT NULL,
      owner_api_key_id BIGINT NULL,
      network VARCHAR(255) NOT NULL,
      id VARCHAR(255) NOT NULL UNIQUE,
      address TEXT NOT NULL,
      description TEXT NOT NULL,
      data JSON NOT NULL,
      is_deleted BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE KEY uq_addresses_entity_network_address (entity_id, network_id, address(255))
    )`,
  `CREATE TABLE IF NOT EXISTS tags (
      tag_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      id VARCHAR(255) NOT NULL UNIQUE,
      owner_api_key_id BIGINT NULL,
      name VARCHAR(255) NOT NULL,
      risk_level VARCHAR(32) NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE KEY uq_tags_owner_name (owner_api_key_id, name)
    )`,
  `CREATE TABLE IF NOT EXISTS entity_tags (
      entity_id BIGINT NOT NULL,
      tag_id BIGINT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (entity_id, tag_id)
    )`,
  `CREATE TABLE IF NOT EXISTS tokens (
      token_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      network_id BIGINT NOT NULL,
      id VARCHAR(255) NOT NULL UNIQUE,
      name TEXT NOT NULL,
      symbol TEXT NOT NULL,
      address TEXT NOT NULL,
      decimals INTEGER NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL,
      UNIQUE KEY uq_tokens_network_address (network_id, address(255))
    )`,
  `CREATE TABLE IF NOT EXISTS projection_applied_blocks (
      network_id BIGINT NOT NULL,
      block_height BIGINT NOT NULL,
      block_hash VARCHAR(255) NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height, block_hash)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_direct_link_applied_blocks (
      network_id BIGINT NOT NULL,
      block_height BIGINT NOT NULL,
      block_hash VARCHAR(255) NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height, block_hash)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_utxo_outputs_current (
      network_id BIGINT NOT NULL,
      output_key VARCHAR(255) NOT NULL,
      block_height BIGINT NOT NULL,
      block_hash TEXT NOT NULL,
      block_time BIGINT NOT NULL,
      txid TEXT NOT NULL,
      tx_index BIGINT NOT NULL,
      vout BIGINT NOT NULL,
      address TEXT NOT NULL,
      script_type TEXT NOT NULL,
      value_base TEXT NOT NULL,
      is_coinbase BOOLEAN NOT NULL,
      is_spendable BOOLEAN NOT NULL,
      spent_by_txid TEXT NULL,
      spent_in_block BIGINT NULL,
      spent_input_index BIGINT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, output_key)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_balances_current (
      network_id BIGINT NOT NULL,
      address VARCHAR(255) NOT NULL,
      asset_address VARCHAR(255) NOT NULL,
      balance TEXT NOT NULL,
      as_of_block_height BIGINT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, address, asset_address)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_direct_links_current (
      network_id BIGINT NOT NULL,
      from_address VARCHAR(255) NOT NULL,
      to_address VARCHAR(255) NOT NULL,
      asset_address VARCHAR(255) NOT NULL,
      transfer_count BIGINT NOT NULL,
      total_amount_base TEXT NOT NULL,
      first_seen_block_height BIGINT NOT NULL,
      last_seen_block_height BIGINT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, from_address, to_address, asset_address)
    )`,
  `CREATE TABLE IF NOT EXISTS projection_source_links_current (
      network_id BIGINT NOT NULL,
      source_address_id BIGINT NOT NULL,
      source_address VARCHAR(255) NOT NULL,
      to_address VARCHAR(255) NOT NULL,
      hop_count BIGINT NOT NULL,
      path_transfer_count BIGINT NOT NULL,
      path_addresses JSON NOT NULL,
      first_seen_block_height BIGINT NOT NULL,
      last_seen_block_height BIGINT NOT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, source_address_id, to_address)
    )`,
  `CREATE TABLE IF NOT EXISTS core_blocks (
      network_id BIGINT NOT NULL,
      block_height BIGINT NOT NULL,
      block_hash VARCHAR(255) NOT NULL,
      previous_block_hash VARCHAR(255) NULL,
      block_time BIGINT NOT NULL,
      tx_count BIGINT NOT NULL,
      raw_storage_key TEXT NOT NULL,
      fetched_at TEXT NOT NULL,
      processed_at TEXT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height)
    )`,
  `CREATE TABLE IF NOT EXISTS core_utxos (
      network_id BIGINT NOT NULL,
      output_key VARCHAR(255) NOT NULL,
      block_height BIGINT NOT NULL,
      block_hash VARCHAR(255) NOT NULL,
      block_time BIGINT NOT NULL,
      txid VARCHAR(255) NOT NULL,
      tx_index BIGINT NOT NULL,
      vout BIGINT NOT NULL,
      address VARCHAR(255) NOT NULL,
      script_type TEXT NOT NULL,
      value_base TEXT NOT NULL,
      is_coinbase BOOLEAN NOT NULL,
      is_spendable BOOLEAN NOT NULL,
      spent_by_txid VARCHAR(255) NULL,
      spent_in_block BIGINT NULL,
      spent_input_index BIGINT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, output_key),
      KEY idx_core_utxos_address_spend (network_id, address, is_spendable, spent_by_txid)
    )`,
  `CREATE TABLE IF NOT EXISTS core_balances (
      network_id BIGINT NOT NULL,
      address VARCHAR(255) NOT NULL,
      asset_address VARCHAR(255) NOT NULL,
      balance TEXT NOT NULL,
      utxo_count BIGINT NOT NULL,
      as_of_block_height BIGINT NOT NULL,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (network_id, address, asset_address)
    )`,
  `CREATE TABLE IF NOT EXISTS core_processed_blocks (
      network_id BIGINT NOT NULL,
      block_height BIGINT NOT NULL,
      block_hash VARCHAR(255) NOT NULL,
      processed_at TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height)
    )`,
  `CREATE TABLE IF NOT EXISTS core_block_undo (
      network_id BIGINT NOT NULL,
      block_height BIGINT NOT NULL,
      block_hash VARCHAR(255) NOT NULL,
      undo_json JSON NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (network_id, block_height)
    )`,
  `CREATE TABLE IF NOT EXISTS core_indexer_state (
      network_id BIGINT PRIMARY KEY,
      stage VARCHAR(32) NOT NULL,
      sync_tail BIGINT NOT NULL,
      process_tail BIGINT NOT NULL,
      online_tip BIGINT NOT NULL,
      last_error TEXT NULL,
      updated_at TEXT NOT NULL,
      created_at TEXT NOT NULL
    )`,
];
