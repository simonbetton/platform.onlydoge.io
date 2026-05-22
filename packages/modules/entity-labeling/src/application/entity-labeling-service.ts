import type { AuthenticatedApiKey } from '@onlydoge/access-control';
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

  public async createEntity(
    actor: AuthenticatedApiKey,
    input: {
      data?: Record<string, unknown>;
      description: string;
      id?: string;
      name?: string | null;
      tags?: string[];
    },
  ) {
    await this.assertEntityIdAvailable(input.id);
    await this.assertEntityNameAvailable(actor, input.name);

    const tagRecords = await this.resolveTags(actor, input.tags ?? []);
    const created = await this.entities.createEntity(
      Entity.create({ ...input, ownerApiKeyId: actor.apiKeyId }).record,
    );
    await this.entityTags.replaceEntityTags(
      created.entityId,
      tagRecords.map((tag) => tag.tagId),
    );

    return this.getEntity(actor, created.id);
  }

  public async updateEntity(
    actor: AuthenticatedApiKey,
    id: string,
    input: {
      data?: Record<string, unknown>;
      description?: string;
      name?: string | null;
      tags?: string[];
    },
  ): Promise<void> {
    const entity = await this.requireExistingEntity(id);
    this.requireWritableOwner(actor, entity);

    await this.assertEntityNameAvailable(actor, input.name, entity.id);

    const updated = updateEntityRecord(entity, input);
    await this.entities.updateEntityRecord(updated);
    await this.replaceEntityTagsIfProvided(actor, entity, input.tags);
  }

  public async deleteEntities(actor: AuthenticatedApiKey, ids: string[]): Promise<void> {
    await this.assertOwnsAllEntities(actor, ids);
    const deleted = await this.entities.softDeleteEntities(ids);
    await this.addresses.softDeleteAddressesByEntityIds(deleted.map((entity) => entity.entityId));
  }

  public async listEntities(actor: AuthenticatedApiKey, offset?: number, limit?: number) {
    const entities = (await this.entities.listEntities(offset, limit))
      .filter((entity) => !entity.isDeleted)
      .filter((entity) => this.canReadOwner(actor, entity));
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

  public async getEntity(actor: AuthenticatedApiKey, id: string) {
    const entity = await this.requireExistingEntity(id);
    this.requireReadableOwner(actor, entity);

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

  public async createAddresses(
    actor: AuthenticatedApiKey,
    input: {
      addresses: CreateAddressRequest[];
      entity: string;
      network: string;
    },
  ) {
    const entity = await this.requireActiveEntity(input.entity);
    this.requireWritableOwner(actor, entity);
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

  public async listAddresses(actor: AuthenticatedApiKey, offset?: number, limit?: number) {
    const addresses = (await this.addresses.listAddresses(offset, limit))
      .filter((address) => !address.isDeleted)
      .filter((address) => this.canReadOwner(actor, address));
    const networks = await this.networks.getActiveNetworksByInternalIds(
      addresses.map((address) => address.networkId),
    );

    return {
      addresses: addresses.map((address) => this.serializeAddress(address)),
      networks: networks.map(toNetworkSummary),
    };
  }

  public async getAddress(actor: AuthenticatedApiKey, id: string) {
    const address = await this.requireExistingAddress(id);
    this.requireReadableOwner(actor, address);

    const network = await this.networks.getActiveNetworkById(address.network);
    return {
      address: this.serializeAddress(address),
      networks: network ? [toNetworkSummary(network)] : [],
    };
  }

  public async deleteAddresses(actor: AuthenticatedApiKey, ids: string[]): Promise<void> {
    await this.assertOwnsAllAddresses(actor, ids);
    await this.addresses.softDeleteAddresses(ids);
  }

  public async createTag(
    actor: AuthenticatedApiKey,
    input: { id?: string; name: string; riskLevel: RiskLevel },
  ) {
    await this.assertTagIdAvailable(input.id);
    await this.assertNewTagNameAvailable(actor, input.name);

    const created = await this.tags.createTag(
      Tag.create({ ...input, ownerApiKeyId: actor.apiKeyId }).record,
    );
    return this.serializeTag(created);
  }

  public async updateTag(
    actor: AuthenticatedApiKey,
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
    this.requireWritableOwner(actor, tag);

    await this.assertTagNameAvailable(actor, input.name, tag.id);

    await this.tags.updateTagRecord(updateTagRecord(tag, input));
  }

  public async listTags(actor: AuthenticatedApiKey, offset?: number, limit?: number) {
    const tags = (await this.tags.listTags(offset, limit)).filter((tag) =>
      this.canReadOwner(actor, tag),
    );
    const { addresses, entities, joinedEntities, networks } = await this.loadTagRelations(tags);

    const entitiesByTag = new Map<PrimaryId, string[]>();
    for (const joined of joinedEntities) {
      addJoinedEntityByTag(entitiesByTag, joined);
    }

    return {
      tags: tags.map((tag) => this.serializeTag(tag, entitiesByTag.get(tag.tagId) ?? [])),
      entities: this.serializeRelatedEntities(entities, addresses),
      addresses: addresses.map((address) => this.serializeAddress(address)),
      networks: networks.map(toNetworkSummary),
    };
  }

  public async getTag(actor: AuthenticatedApiKey, id: string) {
    const tag = await this.tags.getTagById(id);
    if (!tag) {
      throw new NotFoundError();
    }
    this.requireReadableOwner(actor, tag);

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

  public async deleteTags(actor: AuthenticatedApiKey, ids: string[]): Promise<void> {
    await this.assertOwnsAllTags(actor, ids);
    await this.tags.deleteTags(ids);
  }

  public async softDeleteAddressesByNetworkIds(networkIds: PrimaryId[]): Promise<void> {
    await this.addresses.softDeleteAddressesByNetworkIds(networkIds);
  }

  private async resolveTags(actor: AuthenticatedApiKey, tagIds: string[]) {
    const resolved = await Promise.all(tagIds.map((tagId) => this.tags.getTagById(tagId)));
    if (resolved.some((tag) => !tag || !this.canWriteOwner(actor, tag))) {
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
    actor: AuthenticatedApiKey,
    name: string | null | undefined,
    currentEntityId?: string,
  ): Promise<void> {
    if (!name) {
      return;
    }

    await this.assertDeletedEntityNameNotPending(actor, name);
    await this.assertDuplicateEntityNameAvailable(actor, name, currentEntityId);
  }

  private async assertEntityIdAvailable(id: string | undefined): Promise<void> {
    if (!id) {
      return;
    }

    await this.assertEntityIdDoesNotExist(id);
  }

  private async assertEntityIdDoesNotExist(id: string): Promise<void> {
    if (await this.entities.getEntityById(id)) {
      throw new ValidationError(`invalid parameter for \`id\`: ${id}`);
    }
  }

  private async requireExistingEntity(id: string): Promise<EntityRecord> {
    const entity = await this.entities.getEntityById(id);
    if (!entity) {
      throw new NotFoundError();
    }

    return requireEntityNotDeleted(entity);
  }

  private async requireActiveEntity(id: string): Promise<EntityRecord> {
    const entity = await this.entities.getEntityById(id);
    if (!entity) {
      throw new ValidationError(`invalid parameter for \`entity\`: ${id}`);
    }

    return requireActiveEntityRecord(entity, id);
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
    actor: AuthenticatedApiKey,
    name: string | undefined,
    currentTagId: string,
  ): Promise<void> {
    if (!name) {
      return;
    }

    await this.assertNoTagNameConflict(actor, name, currentTagId);
  }

  private async assertDeletedEntityNameNotPending(
    actor: AuthenticatedApiKey,
    name: string,
  ): Promise<void> {
    const deleted = await this.entities.getEntityByName(name, true, actor.apiKeyId);
    assertEntityNameNotPendingDeletion(deleted, name);
  }

  private async assertDuplicateEntityNameAvailable(
    actor: AuthenticatedApiKey,
    name: string,
    currentEntityId?: string,
  ): Promise<void> {
    const duplicate = await this.entities.getEntityByName(name, false, actor.apiKeyId);
    assertNoDuplicateEntityName(duplicate, currentEntityId, name);
  }

  private canReadOwner(actor: AuthenticatedApiKey, record: { ownerApiKeyId: PrimaryId }): boolean {
    return actor.role === 'admin' || this.canWriteOwner(actor, record);
  }

  private canWriteOwner(actor: AuthenticatedApiKey, record: { ownerApiKeyId: PrimaryId }): boolean {
    return record.ownerApiKeyId === actor.apiKeyId;
  }

  private requireReadableOwner(
    actor: AuthenticatedApiKey,
    record: { ownerApiKeyId: PrimaryId },
  ): void {
    if (!this.canReadOwner(actor, record)) {
      throw new NotFoundError();
    }
  }

  private requireWritableOwner(
    actor: AuthenticatedApiKey,
    record: { ownerApiKeyId: PrimaryId },
  ): void {
    if (!this.canWriteOwner(actor, record)) {
      throw new NotFoundError();
    }
  }

  private async assertOwnsAllEntities(actor: AuthenticatedApiKey, ids: string[]): Promise<void> {
    const records = await Promise.all(ids.map((id) => this.entities.getEntityById(id)));
    if (records.some((record) => !this.canWriteActiveRecord(actor, record))) {
      throw new NotFoundError();
    }
  }

  private async assertOwnsAllAddresses(actor: AuthenticatedApiKey, ids: string[]): Promise<void> {
    const records = await Promise.all(ids.map((id) => this.addresses.getAddressById(id)));
    if (records.some((record) => !this.canWriteActiveRecord(actor, record))) {
      throw new NotFoundError();
    }
  }

  private async assertOwnsAllTags(actor: AuthenticatedApiKey, ids: string[]): Promise<void> {
    const records = await Promise.all(ids.map((id) => this.tags.getTagById(id)));
    if (records.some((record) => !record || !this.canWriteOwner(actor, record))) {
      throw new NotFoundError();
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

  private async replaceEntityTagsIfProvided(
    actor: AuthenticatedApiKey,
    entity: EntityRecord,
    tags: string[] | undefined,
  ): Promise<void> {
    if (!tags) {
      return;
    }

    await this.replaceEntityTags(actor, entity, tags);
  }

  private async replaceEntityTags(
    actor: AuthenticatedApiKey,
    entity: EntityRecord,
    tagIds: string[],
  ): Promise<void> {
    const tags = await this.resolveTags(actor, tagIds);
    await this.entityTags.replaceEntityTags(
      entity.entityId,
      tags.map((tag) => tag.tagId),
    );
  }

  private async requireExistingAddress(id: string): Promise<AddressRecord> {
    const address = await this.addresses.getAddressById(id);
    if (!address) {
      throw new NotFoundError();
    }

    return requireAddressNotDeleted(address);
  }

  private async assertTagIdAvailable(id: string | undefined): Promise<void> {
    if (!id) {
      return;
    }

    await this.assertTagIdDoesNotExist(id);
  }

  private async assertTagIdDoesNotExist(id: string): Promise<void> {
    if (await this.tags.getTagById(id)) {
      throw new ValidationError(`invalid parameter for \`id\`: ${id}`);
    }
  }

  private async assertNewTagNameAvailable(actor: AuthenticatedApiKey, name: string): Promise<void> {
    const duplicate = await this.tags.getTagByName(name, actor.apiKeyId);
    if (duplicate) {
      throw new ConflictError(`duplicate found at \`name\`: ${name}`);
    }
  }

  private async assertNoTagNameConflict(
    actor: AuthenticatedApiKey,
    name: string,
    currentTagId: string,
  ): Promise<void> {
    const duplicate = await this.tags.getTagByName(name, actor.apiKeyId);
    if (!duplicate) {
      return;
    }

    rejectDifferentTag(duplicate, currentTagId, name);
  }

  private canWriteActiveRecord(
    actor: AuthenticatedApiKey,
    record: ({ isDeleted: boolean; ownerApiKeyId: PrimaryId } & object) | null,
  ): boolean {
    if (!record) {
      return false;
    }

    return canWriteActiveRecord(actor, record);
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
    ownerApiKeyId: entity.ownerApiKeyId,
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

function requireEntityNotDeleted(entity: EntityRecord): EntityRecord {
  if (entity.isDeleted) {
    throw new NotFoundError();
  }

  return entity;
}

function requireActiveEntityRecord(entity: EntityRecord, id: string): EntityRecord {
  if (entity.isDeleted) {
    throw new ValidationError(`invalid parameter for \`entity\`: ${id}`);
  }

  return entity;
}

function requireAddressNotDeleted(address: AddressRecord): AddressRecord {
  if (address.isDeleted) {
    throw new NotFoundError();
  }

  return address;
}

function assertEntityNameNotPendingDeletion(entity: EntityRecord | null, name: string): void {
  if (!entity) {
    return;
  }

  assertEntityNotDeletedForName(entity, name);
}

function assertEntityNotDeletedForName(entity: EntityRecord, name: string): void {
  if (entity.isDeleted) {
    throw new TooEarlyError(`too early: entity hasn't been deleted yet: ${name}`);
  }
}

function assertNoDuplicateEntityName(
  duplicate: EntityRecord | null,
  currentEntityId: string | undefined,
  name: string,
): void {
  if (!duplicate) {
    return;
  }

  rejectDifferentEntity(duplicate, currentEntityId, name);
}

function rejectDifferentEntity(
  duplicate: EntityRecord,
  currentEntityId: string | undefined,
  name: string,
): void {
  if (duplicate.id === currentEntityId) {
    return;
  }

  throw new ConflictError(`duplicate found at \`name\`: ${name}`);
}

function rejectDifferentTag(duplicate: TagRecord, currentTagId: string, name: string): void {
  if (duplicate.id === currentTagId) {
    return;
  }

  throw new ConflictError(`duplicate found at \`name\`: ${name}`);
}

function canWriteActiveRecord(
  actor: AuthenticatedApiKey,
  record: { isDeleted: boolean; ownerApiKeyId: PrimaryId },
): boolean {
  if (record.isDeleted) {
    return false;
  }

  return record.ownerApiKeyId === actor.apiKeyId;
}

function addJoinedEntityByTag(
  entitiesByTag: Map<PrimaryId, string[]>,
  joined: { entity: EntityRecord; tagId: PrimaryId },
): void {
  const current = entitiesByTag.get(joined.tagId);
  if (current) {
    current.push(joined.entity.id);
    return;
  }

  entitiesByTag.set(joined.tagId, [joined.entity.id]);
}
