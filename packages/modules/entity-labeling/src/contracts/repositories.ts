import type { PrimaryId } from '@onlydoge/shared-kernel';

import type { AddressRecord, EntityRecord, TagRecord } from '../domain/entity';

export interface EntityRepository {
  createEntity(record: EntityRecord): Promise<EntityRecord>;
  getEntityById(id: string): Promise<EntityRecord | null>;
  getEntityByInternalId(entityId: PrimaryId): Promise<EntityRecord | null>;
  getEntityByName(name: string, includeDeleted?: boolean): Promise<EntityRecord | null>;
  listEntities(offset?: number, limit?: number): Promise<EntityRecord[]>;
  listEntitiesByIds(entityIds: PrimaryId[]): Promise<EntityRecord[]>;
  listEntitiesByTagIds(
    tagIds: PrimaryId[],
  ): Promise<Array<{ entity: EntityRecord; tagId: PrimaryId }>>;
  updateEntityRecord(record: EntityRecord): Promise<void>;
  softDeleteEntities(ids: string[]): Promise<EntityRecord[]>;
}

export interface AddressRepository {
  createAddresses(records: AddressRecord[]): Promise<AddressRecord[]>;
  getAddressById(id: string): Promise<AddressRecord | null>;
  listAddresses(offset?: number, limit?: number): Promise<AddressRecord[]>;
  listAddressesByEntityIds(entityIds: PrimaryId[]): Promise<AddressRecord[]>;
  listAddressesByNetworkIds(networkIds: PrimaryId[]): Promise<AddressRecord[]>;
  listAddressesByValues(addresses: string[], includeDeleted?: boolean): Promise<AddressRecord[]>;
  findAddressesByEntityNetworkAndAddresses(
    entityId: PrimaryId,
    networkId: PrimaryId,
    addresses: string[],
    includeDeleted?: boolean,
  ): Promise<AddressRecord[]>;
  softDeleteAddresses(ids: string[]): Promise<AddressRecord[]>;
  softDeleteAddressesByEntityIds(entityIds: PrimaryId[]): Promise<void>;
  softDeleteAddressesByNetworkIds(networkIds: PrimaryId[]): Promise<void>;
}

export interface TagRepository {
  createTag(record: TagRecord): Promise<TagRecord>;
  getTagById(id: string): Promise<TagRecord | null>;
  getTagByName(name: string): Promise<TagRecord | null>;
  listTags(offset?: number, limit?: number): Promise<TagRecord[]>;
  listTagsByIds(tagIds: PrimaryId[]): Promise<TagRecord[]>;
  updateTagRecord(record: TagRecord): Promise<void>;
  deleteTags(ids: string[]): Promise<void>;
}

export interface EntityTagRepository {
  listEntityTagMap(entityIds: PrimaryId[]): Promise<Map<PrimaryId, PrimaryId[]>>;
  replaceEntityTags(entityId: PrimaryId, tagIds: PrimaryId[]): Promise<void>;
}

export interface NetworkReader {
  getActiveNetworkById(id: string): Promise<{
    chainId: number;
    id: string;
    networkId: PrimaryId;
    rpcEndpoint: string;
    name: string;
  } | null>;
  getActiveNetworksByInternalIds(networkIds: PrimaryId[]): Promise<
    Array<{
      chainId: number;
      id: string;
      networkId: PrimaryId;
      rpcEndpoint: string;
      name: string;
    }>
  >;
}

export interface ConfigMutationPort {
  markNewlyAddedAddress(networkId: PrimaryId, addressId: PrimaryId): Promise<void>;
}
