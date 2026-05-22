import {
  ConflictError,
  NotFoundError,
  type PrimaryId,
  type RiskLevel,
  TooEarlyError,
  ValidationError,
} from '@onlydoge/shared-kernel';
import type {
  AddressRepository,
  ConfigMutationPort,
  EntityRepository,
  EntityTagRepository,
  NetworkReader,
  TagRepository,
} from '../contracts/repositories';
import {
  Address,
  type AddressRecord,
  Entity,
  type EntityRecord,
  Tag,
  type TagRecord,
  updateEntityRecord,
  updateTagRecord,
} from '../domain/entity';

function unique(values: string[]): string[] {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))];
}

export type CreateAddressRequest = {
  address: string;
  data?: Record<string, unknown>;
  description: string;
};

export class EntityLabelingService {
  public constructor(
    private readonly entities: EntityRepository,
    private readonly addresses: AddressRepository,
    private readonly tags: TagRepository,
    private readonly entityTags: EntityTagRepository,
    private readonly networks: NetworkReader,
    private readonly configs: ConfigMutationPort,
  ) {}

  public async createEntity(input: {
    data?: Record<string, unknown>;
    description: string;
    id?: string;
    name?: string | null;
    tags?: string[];
  }) {
    if (input.id && (await this.entities.getEntityById(input.id))) {
      throw new ValidationError(`invalid parameter for \`id\`: ${input.id}`);
    }

    await this.assertEntityNameAvailable(input.name);

    const tagRecords = await this.resolveTags(input.tags ?? []);
    const created = await this.entities.createEntity(Entity.create(input).record);
    await this.entityTags.replaceEntityTags(
      created.entityId,
      tagRecords.map((tag) => tag.tagId),
    );

    return this.getEntity(created.id);
  }

  public async updateEntity(
    id: string,
    input: {
      data?: Record<string, unknown>;
      description?: string;
      name?: string | null;
      tags?: string[];
    },
  ): Promise<void> {
    const entity = await this.entities.getEntityById(id);
    if (!entity || entity.isDeleted) {
      throw new NotFoundError();
    }

    await this.assertEntityNameAvailable(input.name, entity.id);

    const updated = updateEntityRecord(entity, input);
    await this.entities.updateEntityRecord(updated);

    if (input.tags) {
      const tags = await this.resolveTags(input.tags);
      await this.entityTags.replaceEntityTags(
        entity.entityId,
        tags.map((tag) => tag.tagId),
      );
    }
  }

  public async deleteEntities(ids: string[]): Promise<void> {
    const deleted = await this.entities.softDeleteEntities(ids);
    await this.addresses.softDeleteAddressesByEntityIds(deleted.map((entity) => entity.entityId));
  }

  public async listEntities(offset?: number, limit?: number) {
    const entities = (await this.entities.listEntities(offset, limit)).filter(
      (entity) => !entity.isDeleted,
    );
    const entityIds = entities.map((entity) => entity.entityId);

    const [tagIdMap, tagRecords, addressRecords] = await Promise.all([
      this.entityTags.listEntityTagMap(entityIds),
      this.loadTagsForEntityIds(entityIds),
      this.addresses.listAddressesByEntityIds(entityIds),
    ]);
    const tagMap = buildEntityTagMap(tagIdMap, tagRecords);

    const networkRecords = await this.networks.getActiveNetworksByInternalIds(
      unique(addressRecords.map((address) => String(address.networkId))).map(Number),
    );

    return {
      ...this.serializeAddressContext(addressRecords, networkRecords),
      entities: entities.map((entity) => this.serializeEntity(entity, tagMap, addressRecords)),
      tags: tagRecords.map((tag) => this.serializeTag(tag)),
    };
  }

  public async getEntity(id: string) {
    const entity = await this.entities.getEntityById(id);
    if (!entity || entity.isDeleted) {
      throw new NotFoundError();
    }

    const [tagIdMap, tagRecords, addressRecords] = await Promise.all([
      this.entityTags.listEntityTagMap([entity.entityId]),
      this.loadTagsForEntityIds([entity.entityId]),
      this.addresses.listAddressesByEntityIds([entity.entityId]),
    ]);
    const tagMap = buildEntityTagMap(tagIdMap, tagRecords);

    const networkRecords = await this.networks.getActiveNetworksByInternalIds(
      addressRecords.map((address) => address.networkId),
    );

    return {
      ...this.serializeAddressContext(addressRecords, networkRecords),
      entity: this.serializeEntity(entity, tagMap, addressRecords),
      tags: tagRecords.map((tag) => this.serializeTag(tag)),
    };
  }

  public async createAddresses(input: {
    addresses: CreateAddressRequest[];
    entity: string;
    network: string;
  }) {
    const entity = await this.requireActiveEntity(input.entity);
    const network = await this.requireActiveNetwork(input.network);
    const uniqueAddresses = this.requireUniqueAddresses(input.addresses);
    await this.assertAddressesNotPendingDeletion(
      entity.entityId,
      network.networkId,
      uniqueAddresses,
    );
    await this.assertAddressesAvailable(entity.entityId, network.networkId, uniqueAddresses);

    const created = await this.addresses.createAddresses(
      input.addresses.map((address) => createAddressRecord(address, entity, network)),
    );

    await this.markNewlyAddedAddresses(created);

    return created.map((address) => this.serializeAddress(address));
  }

  public async listAddresses(offset?: number, limit?: number) {
    const addresses = (await this.addresses.listAddresses(offset, limit)).filter(
      (address) => !address.isDeleted,
    );
    const networks = await this.networks.getActiveNetworksByInternalIds(
      addresses.map((address) => address.networkId),
    );

    return {
      addresses: addresses.map((address) => this.serializeAddress(address)),
      networks: networks.map(toNetworkSummary),
    };
  }

  public async getAddress(id: string) {
    const address = await this.addresses.getAddressById(id);
    if (!address || address.isDeleted) {
      throw new NotFoundError();
    }

    const network = await this.networks.getActiveNetworkById(address.network);
    return {
      address: this.serializeAddress(address),
      networks: network ? [toNetworkSummary(network)] : [],
    };
  }

  public async deleteAddresses(ids: string[]): Promise<void> {
    await this.addresses.softDeleteAddresses(ids);
  }

  public async createTag(input: { id?: string; name: string; riskLevel: RiskLevel }) {
    if (input.id && (await this.tags.getTagById(input.id))) {
      throw new ValidationError(`invalid parameter for \`id\`: ${input.id}`);
    }

    if (await this.tags.getTagByName(input.name)) {
      throw new ConflictError(`duplicate found at \`name\`: ${input.name}`);
    }

    const created = await this.tags.createTag(Tag.create(input).record);
    return this.serializeTag(created);
  }

  public async updateTag(
    id: string,
    input: {
      name?: string;
      riskLevel?: RiskLevel;
    },
  ): Promise<void> {
    const tag = await this.tags.getTagById(id);
    if (!tag) {
      throw new NotFoundError();
    }

    await this.assertTagNameAvailable(input.name, tag.id);

    await this.tags.updateTagRecord(updateTagRecord(tag, input));
  }

  public async listTags(offset?: number, limit?: number) {
    const tags = await this.tags.listTags(offset, limit);
    const { addresses, entities, joinedEntities, networks } = await this.loadTagRelations(tags);

    const entitiesByTag = new Map<PrimaryId, string[]>();
    for (const joined of joinedEntities) {
      const current = entitiesByTag.get(joined.tagId) ?? [];
      current.push(joined.entity.id);
      entitiesByTag.set(joined.tagId, current);
    }

    return {
      tags: tags.map((tag) => this.serializeTag(tag, entitiesByTag.get(tag.tagId) ?? [])),
      entities: this.serializeRelatedEntities(entities, addresses),
      addresses: addresses.map((address) => this.serializeAddress(address)),
      networks: networks.map(toNetworkSummary),
    };
  }

  public async getTag(id: string) {
    const tag = await this.tags.getTagById(id);
    if (!tag) {
      throw new NotFoundError();
    }

    const { addresses, entities, networks } = await this.loadTagRelations([tag]);

    return {
      tag: this.serializeTag(
        tag,
        entities.map((entity) => entity.id),
      ),
      entities: this.serializeRelatedEntities(entities, addresses),
      addresses: addresses.map((address) => this.serializeAddress(address)),
      networks: networks.map(toNetworkSummary),
    };
  }

  public async deleteTags(ids: string[]): Promise<void> {
    await this.tags.deleteTags(ids);
  }

  public async softDeleteAddressesByNetworkIds(networkIds: PrimaryId[]): Promise<void> {
    await this.addresses.softDeleteAddressesByNetworkIds(networkIds);
  }

  private async resolveTags(tagIds: string[]) {
    const resolved = await Promise.all(tagIds.map((tagId) => this.tags.getTagById(tagId)));
    if (resolved.some((tag) => !tag)) {
      throw new ValidationError(`invalid value(s) for \`tags\`: ${tagIds.join(', ')}`);
    }

    return resolved.filter((tag): tag is NonNullable<typeof tag> => Boolean(tag));
  }

  private async loadTagsForEntityIds(entityIds: PrimaryId[]) {
    const tagMap = await this.entityTags.listEntityTagMap(entityIds);
    const tagIds = [...new Set([...tagMap.values()].flat())];
    return this.tags.listTagsByIds(tagIds);
  }

  private async assertEntityNameAvailable(
    name: string | null | undefined,
    currentEntityId?: string,
  ): Promise<void> {
    if (!name) {
      return;
    }

    await this.assertDeletedEntityNameNotPending(name);
    await this.assertDuplicateEntityNameAvailable(name, currentEntityId);
  }

  private async requireActiveEntity(id: string): Promise<EntityRecord> {
    const entity = await this.entities.getEntityById(id);
    if (!entity || entity.isDeleted) {
      throw new ValidationError(`invalid parameter for \`entity\`: ${id}`);
    }

    return entity;
  }

  private async requireActiveNetwork(id: string) {
    const network = await this.networks.getActiveNetworkById(id);
    if (!network) {
      throw new ValidationError(`invalid parameter for \`network\`: ${id}`);
    }

    return network;
  }

  private requireUniqueAddresses(addresses: CreateAddressRequest[]): string[] {
    const uniqueAddresses = unique(addresses.map((address) => address.address));
    if (uniqueAddresses.length !== addresses.length) {
      throw new ValidationError('bad request: request contains duplicate addresses');
    }

    return uniqueAddresses;
  }

  private async assertAddressesNotPendingDeletion(
    entityId: PrimaryId,
    networkId: PrimaryId,
    addresses: string[],
  ): Promise<void> {
    const softDeleted = await this.addresses.findAddressesByEntityNetworkAndAddresses(
      entityId,
      networkId,
      addresses,
      true,
    );
    const pending = softDeleted.filter((address) => address.isDeleted);
    if (pending.length > 0) {
      throw new TooEarlyError(
        `too early: addresses haven't been deleted yet: ${pending
          .map((address) => address.address)
          .join(', ')}`,
      );
    }
  }

  private async assertAddressesAvailable(
    entityId: PrimaryId,
    networkId: PrimaryId,
    addresses: string[],
  ): Promise<void> {
    const duplicates = await this.addresses.findAddressesByEntityNetworkAndAddresses(
      entityId,
      networkId,
      addresses,
      false,
    );
    if (duplicates.length > 0) {
      throw new ConflictError(
        `duplicates found at \`addresses\`: ${duplicates.map((address) => address.address).join(', ')}`,
      );
    }
  }

  private async markNewlyAddedAddresses(addresses: AddressRecord[]): Promise<void> {
    await Promise.all(
      addresses.map((address) =>
        this.configs.markNewlyAddedAddress(address.networkId, address.addressId),
      ),
    );
  }

  private async assertTagNameAvailable(
    name: string | undefined,
    currentTagId: string,
  ): Promise<void> {
    if (!name) {
      return;
    }

    const duplicate = await this.tags.getTagByName(name);
    if (duplicate && duplicate.id !== currentTagId) {
      throw new ConflictError(`duplicate found at \`name\`: ${name}`);
    }
  }

  private async assertDeletedEntityNameNotPending(name: string): Promise<void> {
    const deleted = await this.entities.getEntityByName(name, true);
    if (deleted?.isDeleted) {
      throw new TooEarlyError(`too early: entity hasn't been deleted yet: ${name}`);
    }
  }

  private async assertDuplicateEntityNameAvailable(
    name: string,
    currentEntityId?: string,
  ): Promise<void> {
    const duplicate = await this.entities.getEntityByName(name);
    if (duplicate && duplicate.id !== currentEntityId) {
      throw new ConflictError(`duplicate found at \`name\`: ${name}`);
    }
  }

  private async loadTagRelations(tags: TagRecord[]) {
    const joinedEntities = await this.entities.listEntitiesByTagIds(tags.map((tag) => tag.tagId));
    const entities = joinedEntities.map((joined) => joined.entity);
    const addresses = await this.addresses.listAddressesByEntityIds(
      entities.map((entity) => entity.entityId),
    );
    const networks = await this.networks.getActiveNetworksByInternalIds(
      addresses.map((address) => address.networkId),
    );

    return { addresses, entities, joinedEntities, networks };
  }

  private serializeAddressContext(
    addresses: AddressRecord[],
    networks: Array<{ chainId: number; id: string; name: string }>,
  ) {
    return {
      addresses: addresses
        .filter((address) => !address.isDeleted)
        .map((address) => this.serializeAddress(address)),
      networks: networks.map(toNetworkSummary),
    };
  }

  private serializeRelatedEntities(entities: EntityRecord[], addresses: AddressRecord[]) {
    return entities.map((entity) =>
      this.serializeEntity(
        entity,
        new Map(),
        addresses.filter((address) => address.entityId === entity.entityId),
      ),
    );
  }

  private serializeEntity(
    entity: EntityRecord,
    tagMap: Map<PrimaryId, string[]>,
    addresses: AddressRecord[],
  ) {
    return {
      id: entity.id,
      name: entity.name,
      description: entity.description,
      data: entity.data,
      createdAt: entity.createdAt,
      tags: tagMap.get(entity.entityId) ?? [],
      addresses: addresses
        .filter((address) => address.entityId === entity.entityId)
        .map((address) => address.id),
    };
  }

  private serializeAddress(address: AddressRecord) {
    return {
      id: address.id,
      network: address.network,
      address: address.address,
      description: address.description,
      data: address.data,
      createdAt: address.createdAt,
    };
  }

  private serializeTag(tag: TagRecord, entities: string[] = []) {
    return {
      id: tag.id,
      name: tag.name,
      riskLevel: tag.riskLevel,
      createdAt: tag.createdAt,
      entities,
    };
  }
}

function toNetworkSummary(network: { chainId: number; id: string; name: string }) {
  return {
    id: network.id,
    name: network.name,
    chainId: network.chainId,
  };
}

function createAddressRecord(
  address: CreateAddressRequest,
  entity: EntityRecord,
  network: NonNullable<Awaited<ReturnType<NetworkReader['getActiveNetworkById']>>>,
): AddressRecord {
  return Address.create({
    address: address.address,
    ...(address.data ? { data: address.data } : {}),
    description: address.description,
    entityId: entity.entityId,
    network: network.id,
    networkId: network.networkId,
  }).record;
}

function buildEntityTagMap(
  tagIdMap: Map<PrimaryId, PrimaryId[]>,
  tags: TagRecord[],
): Map<PrimaryId, string[]> {
  const tagLookup = new Map(tags.map((tag) => [tag.tagId, tag.id]));
  const result = new Map<PrimaryId, string[]>();

  for (const [entityId, tagIds] of tagIdMap.entries()) {
    result.set(
      entityId,
      tagIds
        .map((tagId) => tagLookup.get(tagId))
        .filter((tagId): tagId is string => Boolean(tagId)),
    );
  }

  return result;
}
