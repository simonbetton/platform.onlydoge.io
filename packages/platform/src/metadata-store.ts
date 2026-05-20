import { mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { createClient as createLibsqlClient, type Client as LibsqlClient } from '@libsql/client';
import type { ApiKeyRecord, ApiKeyRepository } from '@onlydoge/access-control';
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

export class RelationalMetadataStore
  implements
    ApiKeyRepository,
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
    if (settings.driver === 'sqlite') {
      const path = settings.location.replace(/^file:/u, '');
      await mkdir(dirname(path), { recursive: true });
      const raw = createLibsqlClient({ url: settings.location });
      drizzleLibsql(raw);

      const store = new RelationalMetadataStore({ kind: 'sqlite', raw });
      await store.migrate();
      return store;
    }

    if (settings.driver === 'postgres') {
      const raw = new Pool({
        connectionString: settings.location,
        ...(settings.ssl ? { ssl: settings.ssl } : {}),
      });
      drizzlePg(raw);

      const store = new RelationalMetadataStore({ kind: 'postgres', raw });
      await store.migrate();
      return store;
    }

    const raw = mysql.createPool(settings.location);
    drizzleMysql(raw);

    const store = new RelationalMetadataStore({ kind: 'mysql', raw });
    await store.migrate();
    return store;
  }

  public async countApiKeys(): Promise<number> {
    const row = await this.one<{ count: number | string }>(
      'SELECT COUNT(*) AS count FROM api_keys',
    );
    return Number(row?.count ?? 0);
  }

  public async createApiKey(record: {
    createdAt: string;
    id: string;
    isActive: boolean;
    secretKeyHash: string;
    secretKeyPlaintext: string | null;
    updatedAt: string | null;
  }) {
    await this.execute(
      `
        INSERT INTO api_keys (id, secret_key, secret_key_hash, is_active, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
      `,
      [
        record.id,
        record.secretKeyPlaintext,
        record.secretKeyHash,
        this.booleanValue(record.isActive),
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

  public async listApiKeys(offset?: number, limit?: number) {
    return this.listTable('api_keys', 'api_key_id', (row) => this.mapApiKey(row), offset, limit);
  }

  public async updateApiKey(record: {
    id: string;
    isActive: boolean;
    secretKeyHash: string;
    secretKeyPlaintext: string | null;
    updatedAt: string | null;
  }): Promise<void> {
    await this.execute(
      `
        UPDATE api_keys
        SET secret_key = ?, secret_key_hash = ?, is_active = ?, updated_at = ?
        WHERE id = ?
      `,
      [
        record.secretKeyPlaintext,
        record.secretKeyHash,
        this.booleanValue(record.isActive),
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
        ${includeDeleted ? '' : `AND ${this.booleanCondition('is_deleted', false)}`}
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
        INSERT INTO entities (id, name, description, data, is_deleted, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `,
      [
        record.id,
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

  public async getEntityByName(name: string, includeDeleted = false) {
    return this.getNamedRecord('entities', name, includeDeleted, (row) => this.mapEntity(row));
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
        SET name = ?, description = ?, data = ?, is_deleted = ?, updated_at = ?
        WHERE id = ?
      `,
      [
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
      await this.execute(
        `
          INSERT INTO addresses (entity_id, network_id, network, id, address, description, data, is_deleted, updated_at, created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        [
          record.entityId,
          record.networkId,
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

    if (records.length === 0) {
      return [];
    }

    const [firstRecord] = records;
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
        ${includeDeleted ? '' : `AND ${this.booleanCondition('is_deleted', false)}`}
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
        ${includeDeleted ? '' : `AND ${this.booleanCondition('is_deleted', false)}`}
      `,
      [entityId, networkId, ...addresses],
    );
    return rows.map((row) => this.mapAddress(row));
  }

  public async softDeleteAddresses(ids: string[]) {
    if (ids.length === 0) {
      return [];
    }

    const existing = await Promise.all(ids.map((id) => this.getAddressById(id)));
    const addresses = existing.filter((address): address is NonNullable<typeof address> =>
      Boolean(address),
    );
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
        INSERT INTO tags (id, name, risk_level, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?)
      `,
      [record.id, record.name, record.riskLevel, record.updatedAt, record.createdAt],
    );

    return this.getTagById(record.id).then(assertFound);
  }

  public async getTagById(id: string) {
    const row = await this.one<DatabaseRow>('SELECT * FROM tags WHERE id = ? LIMIT 1', [id]);
    return row ? this.mapTag(row) : null;
  }

  public async getTagByName(name: string) {
    const row = await this.one<DatabaseRow>(
      'SELECT * FROM tags WHERE LOWER(name) = LOWER(?) LIMIT 1',
      [name],
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
        SET name = ?, risk_level = ?, updated_at = ?
        WHERE id = ?
      `,
      [record.name, record.riskLevel, record.updatedAt ?? nowIsoString(), record.id],
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

    const result = new Map<PrimaryId, PrimaryId[]>();
    for (const row of rows) {
      const entityId = Number(row.entity_id);
      const tagIds = result.get(entityId) ?? [];
      tagIds.push(Number(row.tag_id));
      result.set(entityId, tagIds);
    }

    return result;
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
    const coreRow = await this.one<DatabaseRow>(
      `
        SELECT balance
        FROM core_balances
        WHERE network_id = ? AND address = ? AND asset_address = ''
        LIMIT 1
      `,
      [networkId, address],
    );
    if (coreRow?.balance) {
      return String(coreRow.balance);
    }

    const row = await this.one<DatabaseRow>(
      `
        SELECT balance
        FROM projection_balances_current
        WHERE network_id = ? AND address = ? AND asset_address = ''
        LIMIT 1
      `,
      [networkId, address],
    );

    return row?.balance ? String(row.balance) : '0';
  }

  private async countCurrentSpendableUtxos(networkId: PrimaryId, address: string): Promise<number> {
    const coreRow = await this.one<DatabaseRow>(
      `
        SELECT utxo_count
        FROM core_balances
        WHERE network_id = ? AND address = ? AND asset_address = ''
        LIMIT 1
      `,
      [networkId, address],
    );
    if (coreRow?.utxo_count !== undefined) {
      return Number(coreRow.utxo_count);
    }

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

    return Number(row?.utxo_count ?? 0);
  }

  public async getUtxoOutputs(
    networkId: PrimaryId,
    outputKeys: string[],
  ): Promise<Map<string, ProjectionUtxoOutput>> {
    if (outputKeys.length === 0) {
      return new Map();
    }

    const coreOutputs = await this.getCoreUtxoOutputs(networkId, outputKeys);
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

    const bootstrapTail = await this.getProjectionBootstrapTail(networkId);
    return bootstrapTail !== null && blockHeight <= bootstrapTail;
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
    const bootstrapTail = await this.getProjectionBootstrapTail(networkId);
    if (bootstrapTail !== null) {
      for (const block of blocks) {
        if (block.blockHeight <= bootstrapTail) {
          identities.add(projectionBlockIdentity(networkId, block.blockHeight, block.blockHash));
        }
      }
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
    return network && !network.isDeleted ? network : null;
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

  public async upsertCoreIndexerState(input: {
    lastError?: string | null;
    networkId: PrimaryId;
    onlineTip?: number;
    processTail?: number;
    stage?: CoreIndexerStage;
    syncTail?: number;
  }): Promise<CoreIndexerState> {
    const current = await this.getCoreIndexerState(input.networkId);
    const timestamp = nowIsoString();
    const row = {
      networkId: input.networkId,
      stage: input.stage ?? current?.stage ?? 'sync_backfill',
      syncTail: input.syncTail ?? current?.syncTail ?? -1,
      processTail: input.processTail ?? current?.processTail ?? -1,
      onlineTip: input.onlineTip ?? current?.onlineTip ?? -1,
      lastError: input.lastError === undefined ? (current?.lastError ?? null) : input.lastError,
      updatedAt: timestamp,
    } satisfies CoreIndexerState;

    if (this.client.kind === 'mysql') {
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
        [
          row.networkId,
          row.stage,
          row.syncTail,
          row.processTail,
          row.onlineTip,
          row.lastError,
          timestamp,
          timestamp,
        ],
      );
      return row;
    }

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
      [
        row.networkId,
        row.stage,
        row.syncTail,
        row.processTail,
        row.onlineTip,
        row.lastError,
        timestamp,
        timestamp,
      ],
    );
    return row;
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
      async (executor) => {
        await this.acquireCoreNetworkLock(executor, input.networkId);
        try {
          const existing = await this.oneWithExecutor<DatabaseRow>(
            executor,
            'SELECT block_hash FROM core_processed_blocks WHERE network_id = ? AND block_height = ? LIMIT 1',
            [input.networkId, input.blockHeight],
          );
          if (existing) {
            const existingHash = String(existing.block_hash);
            if (existingHash !== input.blockHash) {
              throw new Error(
                `core block hash mismatch network=${input.networkId} height=${input.blockHeight} existing=${existingHash} next=${input.blockHash}`,
              );
            }
            return { applied: false, processTail: input.blockHeight };
          }

          await this.upsertCoreBlockWithExecutor(input, executor);
          const currentOutputs = await this.getCoreUtxoOutputsWithExecutor(
            executor,
            input.networkId,
            input.utxoSpends.map((spend) => spend.outputKey),
          );
          const nextOutputs = new Map<string, ProjectionUtxoOutput>();
          const affectedAddresses = new Set<string>();

          for (const output of input.utxoCreates) {
            nextOutputs.set(output.outputKey, { ...output });
            if (output.isSpendable && output.address) {
              affectedAddresses.add(output.address);
            }
          }

          for (const spend of input.utxoSpends) {
            const current = nextOutputs.get(spend.outputKey) ?? currentOutputs.get(spend.outputKey);
            if (!current) {
              throw new Error(`missing core utxo output: ${spend.outputKey}`);
            }
            if (current.spentByTxid) {
              const sameSpend =
                current.spentByTxid === spend.spentByTxid &&
                current.spentInBlock === spend.spentInBlock &&
                current.spentInputIndex === spend.spentInputIndex;
              if (!sameSpend) {
                throw new Error(
                  `core utxo output already spent: ${spend.outputKey} by ${current.spentByTxid}`,
                );
              }
            }
            nextOutputs.set(spend.outputKey, {
              ...current,
              spentByTxid: spend.spentByTxid,
              spentInBlock: spend.spentInBlock,
              spentInputIndex: spend.spentInputIndex,
            });
            if (current.isSpendable && current.address) {
              affectedAddresses.add(current.address);
            }
          }

          const timestamp = nowIsoString();
          await this.upsertCoreUtxoOutputs([...nextOutputs.values()], timestamp, executor);
          await this.recomputeCoreBalances(
            input.networkId,
            [...affectedAddresses],
            input.blockHeight,
            timestamp,
            executor,
          );
          await this.insertCoreBlockUndo(input, timestamp, executor);
          await this.insertCoreProcessedBlock(input, timestamp, executor);
          await this.executeWithExecutor(
            executor,
            'UPDATE core_blocks SET processed_at = ?, updated_at = ? WHERE network_id = ? AND block_height = ?',
            [timestamp, timestamp, input.networkId, input.blockHeight],
          );
          return { applied: true, processTail: input.blockHeight };
        } finally {
          await this.releaseCoreNetworkLock(executor, input.networkId);
        }
      },
      context?.statementTimeoutMs ? { statementTimeoutMs: context.statementTimeoutMs } : {},
    );
  }

  public async applyCoreDogecoinWindow(
    input: CoreDogecoinBlockApplication[],
    context?: CoreDogecoinApplyContext,
  ): Promise<CoreDogecoinApplyResult> {
    let applied = false;
    let processTail = input.at(-1)?.blockHeight ?? -1;

    for (const application of input) {
      const result = await this.applyCoreDogecoinBlock(application, context);
      applied = applied || result.applied;
      processTail = result.processTail;
    }

    return { applied, processTail };
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
    if (outputs.length === 0) {
      return;
    }

    for (const chunk of chunkArray(outputs, this.bulkChunkSize(executor.kind))) {
      const params = chunk.flatMap((output) => [
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
        this.booleanValue(output.isCoinbase),
        this.booleanValue(output.isSpendable),
        output.spentByTxid,
        output.spentInBlock,
        output.spentInputIndex,
        timestamp,
      ]);

      const values = multiRowPlaceholders(chunk.length, 17);
      if (executor.kind === 'mysql') {
        await this.executeWithExecutor(
          executor,
          `
            INSERT INTO projection_utxo_outputs_current (
              network_id, output_key, block_height, block_hash, block_time, txid, tx_index, vout,
              address, script_type, value_base, is_coinbase, is_spendable,
              spent_by_txid, spent_in_block, spent_input_index, updated_at
            )
            VALUES ${values}
            ON DUPLICATE KEY UPDATE
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
          params,
        );
        continue;
      }

      await this.executeWithExecutor(
        executor,
        `
          INSERT INTO projection_utxo_outputs_current (
            network_id, output_key, block_height, block_hash, block_time, txid, tx_index, vout,
            address, script_type, value_base, is_coinbase, is_spendable,
            spent_by_txid, spent_in_block, spent_input_index, updated_at
          )
          VALUES ${values}
          ON CONFLICT (network_id, output_key) DO UPDATE SET
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
        params,
      );
    }
  }

  private async upsertProjectionBalances(
    balances: ProjectionBalanceSnapshot[],
    timestamp: string,
    executor: SupportedExecutor = this.client,
  ): Promise<void> {
    if (balances.length === 0) {
      return;
    }

    for (const chunk of chunkArray(balances, this.bulkChunkSize(executor.kind))) {
      const params = chunk.flatMap((balance) => [
        balance.networkId,
        balance.address,
        balance.assetAddress,
        balance.balance,
        balance.asOfBlockHeight,
        timestamp,
      ]);
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
        continue;
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
  }

  private async upsertProjectionDirectLinks(
    links: DirectLinkRecord[],
    timestamp: string,
    executor: SupportedExecutor = this.client,
  ): Promise<void> {
    if (links.length === 0) {
      return;
    }

    for (const chunk of chunkArray(links, this.bulkChunkSize(executor.kind))) {
      const params = chunk.flatMap((link) => [
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
        continue;
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
  }

  private async insertProjectionSourceLinks(
    rows: SourceLinkRecord[],
    timestamp: string,
    executor: SupportedExecutor = this.client,
  ): Promise<void> {
    if (rows.length === 0) {
      return;
    }

    for (const chunk of chunkArray(rows, this.bulkChunkSize(executor.kind))) {
      const params = chunk.flatMap((row) => [
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
        params,
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
    if (blocks.length === 0) {
      return;
    }

    for (const chunk of chunkArray(blocks, this.bulkChunkSize(executor.kind))) {
      const params = chunk.flatMap((block) => [
        block.networkId,
        block.blockHeight,
        block.blockHash,
        timestamp,
        timestamp,
      ]);
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
        continue;
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
    if (outputs.length === 0) {
      return;
    }

    for (const chunk of chunkArray(outputs, this.bulkChunkSize(executor.kind))) {
      const params = chunk.flatMap((output) => [
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
        this.booleanValue(output.isCoinbase),
        this.booleanValue(output.isSpendable),
        output.spentByTxid,
        output.spentInBlock,
        output.spentInputIndex,
        timestamp,
      ]);
      const values = multiRowPlaceholders(chunk.length, 17);

      if (executor.kind === 'mysql') {
        await this.executeWithExecutor(
          executor,
          `
            INSERT INTO core_utxos (
              network_id, output_key, block_height, block_hash, block_time, txid, tx_index, vout,
              address, script_type, value_base, is_coinbase, is_spendable, spent_by_txid,
              spent_in_block, spent_input_index, updated_at
            )
            VALUES ${values}
            ON DUPLICATE KEY UPDATE
              spent_by_txid = VALUES(spent_by_txid),
              spent_in_block = VALUES(spent_in_block),
              spent_input_index = VALUES(spent_input_index),
              updated_at = VALUES(updated_at)
          `,
          params,
        );
        continue;
      }

      await this.executeWithExecutor(
        executor,
        `
          INSERT INTO core_utxos (
            network_id, output_key, block_height, block_hash, block_time, txid, tx_index, vout,
            address, script_type, value_base, is_coinbase, is_spendable, spent_by_txid,
            spent_in_block, spent_input_index, updated_at
          )
          VALUES ${values}
          ON CONFLICT (network_id, output_key) DO UPDATE SET
            spent_by_txid = excluded.spent_by_txid,
            spent_in_block = excluded.spent_in_block,
            spent_input_index = excluded.spent_input_index,
            updated_at = excluded.updated_at
        `,
        params,
      );
    }
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

    const balances = new Map<string, { balance: bigint; utxoCount: number }>();
    for (const address of addresses) {
      balances.set(address, { balance: 0n, utxoCount: 0 });
    }

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
      for (const row of rows) {
        const address = String(row.address);
        const current = balances.get(address) ?? { balance: 0n, utxoCount: 0 };
        current.balance += parseAmountBase(String(row.value_base));
        current.utxoCount += 1;
        balances.set(address, current);
      }
    }

    const rows = [...balances.entries()].map(([address, balance]) => ({
      networkId,
      address,
      assetAddress: '',
      balance: formatAmountBase(balance.balance),
      utxoCount: balance.utxoCount,
      asOfBlockHeight,
    }));
    for (const chunk of chunkArray(rows, this.bulkChunkSize(executor.kind))) {
      const params = chunk.flatMap((row) => [
        row.networkId,
        row.address,
        row.assetAddress,
        row.balance,
        row.utxoCount,
        row.asOfBlockHeight,
        timestamp,
      ]);
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
        continue;
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

    const storedHash = row ? String(row.block_hash) : null;
    if (storedHash !== blockHash) {
      throw new Error(
        `core block hash mismatch table=${table} network=${networkId} height=${blockHeight} existing=${storedHash ?? 'missing'} next=${blockHash}`,
      );
    }
  }

  private async acquireCoreNetworkLock(
    executor: SupportedExecutor,
    networkId: PrimaryId,
  ): Promise<void> {
    if (executor.kind === 'postgres') {
      await this.executeWithExecutor(executor, 'SELECT pg_advisory_xact_lock(1868853095, ?)', [
        networkId,
      ]);
      return;
    }

    if (executor.kind === 'mysql') {
      const [row] = await this.queryWithExecutor<DatabaseRow>(
        executor,
        'SELECT GET_LOCK(?, 30) AS locked',
        [`onlydoge-core-network-${networkId}`],
      );
      if (Number(row?.locked ?? 0) !== 1) {
        throw new Error(`timed out acquiring core network lock network=${networkId}`);
      }
    }
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
    const existing = records.filter((record): record is T => record !== null);
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
    return updated.filter((record): record is T => record !== null);
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
  ): Promise<T | null> {
    const row = await this.one<DatabaseRow>(
      `
        SELECT * FROM ${table}
        WHERE LOWER(name) = LOWER(?)
        ${includeDeleted ? '' : `AND ${this.booleanCondition('is_deleted', false)}`}
        LIMIT 1
      `,
      [name],
    );
    return row ? mapRow(row) : null;
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
    if (this.client.kind === 'sqlite') {
      return this.withSqliteTransaction(work);
    }

    if (this.client.kind === 'postgres') {
      return this.withPostgresTransaction(this.client, work, options);
    }

    return this.withMysqlTransaction(this.client, work);
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
      if (options.statementTimeoutMs) {
        await this.executeWithExecutor(
          executor,
          `SET LOCAL statement_timeout = ${Math.trunc(options.statementTimeoutMs)}`,
        );
      }
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
      const result = await executor.raw.execute({ sql: compiled, args: params });
      return (result.rows ?? []) as unknown as T[];
    }

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
      return value ? '1' : '0';
    }

    return value ? 'TRUE' : 'FALSE';
  }

  private booleanCondition(column: string, value: boolean): string {
    return `${column} = ${this.booleanLiteral(value)}`;
  }

  private booleanValue(value: boolean): SqlValue {
    if (this.client.kind === 'sqlite') {
      return value ? 1 : 0;
    }

    return value;
  }

  private async migrate(): Promise<void> {
    const statements =
      this.client.kind === 'sqlite'
        ? sqliteMigrations
        : this.client.kind === 'postgres'
          ? postgresMigrations
          : mysqlMigrations;

    for (const statement of statements) {
      await this.execute(statement);
    }

    await this.ensureNetworksZmqColumn();
  }

  private async ensureNetworksZmqColumn(): Promise<void> {
    if (this.client.kind === 'sqlite') {
      const columns = await this.query<DatabaseRow>('PRAGMA table_info(networks)');
      if (!columns.some((column) => String(column.name) === 'zmq_block_endpoint')) {
        await this.execute('ALTER TABLE networks ADD COLUMN zmq_block_endpoint TEXT NULL');
      }
      return;
    }

    if (this.client.kind === 'postgres') {
      await this.execute(
        'ALTER TABLE networks ADD COLUMN IF NOT EXISTS zmq_block_endpoint TEXT NULL',
      );
      return;
    }

    const columns = await this.query<DatabaseRow>(
      `
        SELECT COLUMN_NAME AS column_name
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'networks'
      `,
    );
    if (!columns.some((column) => String(column.column_name) === 'zmq_block_endpoint')) {
      await this.execute('ALTER TABLE networks ADD COLUMN zmq_block_endpoint TEXT NULL');
    }
  }

  private mapApiKey(row: DatabaseRow): ApiKeyRecord {
    return {
      apiKeyId: Number(row.api_key_id),
      id: String(row.id),
      secretKeyPlaintext: row.secret_key ? String(row.secret_key) : null,
      secretKeyHash: String(row.secret_key_hash),
      isActive: toBoolean(row.is_active),
      updatedAt: row.updated_at ? String(row.updated_at) : null,
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
      ...this.mapLabelMetadata(row),
    };
  }

  private mapAddress(row: DatabaseRow): AddressRecord {
    return {
      addressId: Number(row.address_id),
      entityId: Number(row.entity_id),
      networkId: Number(row.network_id),
      id: String(row.id),
      network: String(row.network),
      address: String(row.address),
      ...this.mapLabelMetadata(row),
    };
  }

  private mapLabelMetadata(row: DatabaseRow) {
    return {
      description: String(row.description),
      data: safeJsonParse<Record<string, unknown>>(String(row.data ?? '{}'), {}),
      isDeleted: toBoolean(row.is_deleted),
      updatedAt: row.updated_at ? String(row.updated_at) : null,
      createdAt: String(row.created_at),
    };
  }

  private mapTag(row: DatabaseRow): TagRecord {
    return {
      tagId: Number(row.tag_id),
      id: String(row.id),
      name: String(row.name),
      riskLevel: parseRiskLevel(String(row.risk_level)),
      updatedAt: row.updated_at ? String(row.updated_at) : null,
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
  if (values.length === 0) {
    return [];
  }

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
    const key = keyFor(row);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    deduped.push(row);
  }
  return deduped;
}

function parsePendingRelinkAddressId(key: string, networkId: PrimaryId): PrimaryId | null {
  const match = key.match(new RegExp(`^newly_added_address_n${networkId}_a(\\d+)$`, 'u'));
  if (!match?.[1]) {
    return null;
  }

  return Number(match[1]);
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
      secret_key TEXT NULL,
      secret_key_hash TEXT NOT NULL,
      is_active INTEGER NOT NULL,
      updated_at TEXT NULL,
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
      name TEXT NULL UNIQUE,
      description TEXT NOT NULL,
      data TEXT NOT NULL,
      is_deleted INTEGER NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS addresses (
      address_id INTEGER PRIMARY KEY AUTOINCREMENT,
      entity_id INTEGER NOT NULL,
      network_id INTEGER NOT NULL,
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
      name TEXT NOT NULL UNIQUE,
      risk_level TEXT NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
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
      secret_key TEXT NULL,
      secret_key_hash TEXT NOT NULL,
      is_active BOOLEAN NOT NULL,
      updated_at TEXT NULL,
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
      name TEXT NULL UNIQUE,
      description TEXT NOT NULL,
      data TEXT NOT NULL,
      is_deleted BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS addresses (
      address_id BIGSERIAL PRIMARY KEY,
      entity_id BIGINT NOT NULL,
      network_id BIGINT NOT NULL,
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
      name TEXT NOT NULL UNIQUE,
      risk_level TEXT NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
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
      secret_key TEXT NULL,
      secret_key_hash TEXT NOT NULL,
      is_active BOOLEAN NOT NULL,
      updated_at TEXT NULL,
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
      name VARCHAR(255) NULL UNIQUE,
      description TEXT NOT NULL,
      data JSON NOT NULL,
      is_deleted BOOLEAN NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
    )`,
  `CREATE TABLE IF NOT EXISTS addresses (
      address_id BIGINT PRIMARY KEY AUTO_INCREMENT,
      entity_id BIGINT NOT NULL,
      network_id BIGINT NOT NULL,
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
      name VARCHAR(255) NOT NULL UNIQUE,
      risk_level VARCHAR(32) NOT NULL,
      updated_at TEXT NULL,
      created_at TEXT NOT NULL
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
