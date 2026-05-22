import {
  ExternalId,
  type PrimaryId,
  type RiskLevel,
  ValidationError,
} from '@onlydoge/shared-kernel';

export interface EntityRecord {
  entityId: PrimaryId;
  id: string;
  name: string | null;
  ownerApiKeyId: PrimaryId;
  description: string;
  data: Record<string, unknown>;
  isDeleted: boolean;
  updatedAt: string | null;
  createdAt: string;
}

export interface AddressRecord {
  addressId: PrimaryId;
  entityId: PrimaryId;
  networkId: PrimaryId;
  ownerApiKeyId: PrimaryId;
  id: string;
  network: string;
  address: string;
  description: string;
  data: Record<string, unknown>;
  isDeleted: boolean;
  updatedAt: string | null;
  createdAt: string;
}

export interface TagRecord {
  tagId: PrimaryId;
  id: string;
  ownerApiKeyId: PrimaryId;
  name: string;
  riskLevel: RiskLevel;
  updatedAt: string | null;
  createdAt: string;
}

export interface UpdateEntityInput {
  data?: Record<string, unknown>;
  description?: string;
  name?: string | null;
}

export interface UpdateTagInput {
  name?: string;
  riskLevel?: RiskLevel;
}

export class Entity {
  public readonly record: EntityRecord;

  private constructor(record: EntityRecord) {
    this.record = record;
  }

  public static create(input: {
    data?: Record<string, unknown>;
    description: string;
    id?: string;
    name?: string | null;
    ownerApiKeyId: PrimaryId;
  }): Entity {
    assertEntityDescription(input.description);

    return new Entity(entityRecordFromInput(input));
  }
}

function entityRecordFromInput(input: {
  data?: Record<string, unknown>;
  description: string;
  id?: string;
  name?: string | null;
  ownerApiKeyId: PrimaryId;
}): EntityRecord {
  return {
    entityId: 0,
    id: resolveEntityExternalId(input.id),
    name: normalizeOptionalName(input.name),
    ownerApiKeyId: input.ownerApiKeyId,
    description: input.description.trim(),
    data: input.data ?? {},
    isDeleted: false,
    updatedAt: null,
    createdAt: new Date().toISOString(),
  };
}

function assertEntityDescription(value: string): void {
  if (!value.trim()) {
    throw new ValidationError('invalid parameter for `description`: ');
  }
}

function resolveEntityExternalId(id: string | undefined): string {
  if (!id) {
    return ExternalId.create('ent').value;
  }

  return ExternalId.parse(id, 'ent').value;
}

function normalizeOptionalName(value: string | null | undefined): string | null {
  return emptyToNull(trimOptionalText(value));
}

function trimOptionalText(value: string | null | undefined): string {
  return value?.trim() ?? '';
}

function emptyToNull(value: string): string | null {
  return value || null;
}

export class Address {
  public readonly record: AddressRecord;

  private constructor(record: AddressRecord) {
    this.record = record;
  }

  public static create(input: {
    address: string;
    data?: Record<string, unknown>;
    description: string;
    entityId: PrimaryId;
    network: string;
    networkId: PrimaryId;
    ownerApiKeyId: PrimaryId;
  }): Address {
    assertAddressText(input.address);

    return new Address({
      addressId: 0,
      entityId: input.entityId,
      networkId: input.networkId,
      ownerApiKeyId: input.ownerApiKeyId,
      id: ExternalId.create('adr').value,
      network: input.network,
      address: input.address.trim(),
      description: input.description.trim(),
      data: input.data ?? {},
      isDeleted: false,
      updatedAt: null,
      createdAt: new Date().toISOString(),
    });
  }
}

function assertAddressText(value: string): void {
  if (!value.trim()) {
    throw new ValidationError('invalid parameter for `address`: ');
  }
}

export class Tag {
  public readonly record: TagRecord;

  private constructor(record: TagRecord) {
    this.record = record;
  }

  public static create(input: {
    id?: string;
    name: string;
    ownerApiKeyId: PrimaryId;
    riskLevel: RiskLevel;
  }): Tag {
    assertTagName(input.name);

    return new Tag({
      tagId: 0,
      id: resolveTagExternalId(input.id),
      ownerApiKeyId: input.ownerApiKeyId,
      name: input.name.trim(),
      riskLevel: input.riskLevel,
      updatedAt: null,
      createdAt: new Date().toISOString(),
    });
  }
}

function assertTagName(value: string): void {
  if (!value.trim()) {
    throw new ValidationError('invalid parameter for `name`: ');
  }
}

function resolveTagExternalId(id: string | undefined): string {
  if (!id) {
    return ExternalId.create('tag').value;
  }

  return ExternalId.parse(id, 'tag').value;
}

export function updateEntityRecord(record: EntityRecord, input: UpdateEntityInput): EntityRecord {
  return {
    ...record,
    name: updatedEntityName(input.name, record.name),
    description: updatedText(input.description, record.description),
    data: updatedValue(input.data, record.data),
    updatedAt: new Date().toISOString(),
  };
}

function updatedEntityName(
  value: string | null | undefined,
  fallback: string | null,
): string | null {
  return value === undefined ? fallback : normalizeOptionalName(value);
}

function updatedText(value: string | undefined, fallback: string): string {
  return value === undefined ? fallback : value.trim();
}

function updatedValue<T>(value: T | undefined, fallback: T): T {
  return value === undefined ? fallback : value;
}

export function updateTagRecord(record: TagRecord, input: UpdateTagInput): TagRecord {
  return {
    ...record,
    name: updatedText(input.name, record.name),
    riskLevel: updatedValue(input.riskLevel, record.riskLevel),
    updatedAt: new Date().toISOString(),
  };
}
