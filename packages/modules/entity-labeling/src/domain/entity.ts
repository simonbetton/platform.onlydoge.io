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
}): EntityRecord {
  return {
    entityId: 0,
    id: resolveEntityExternalId(input.id),
    name: normalizeOptionalName(input.name),
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
  return id ? ExternalId.parse(id, 'ent').value : ExternalId.create('ent').value;
}

function normalizeOptionalName(value: string | null | undefined): string | null {
  return value?.trim() || null;
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
  }): Address {
    if (!input.address.trim()) {
      throw new ValidationError('invalid parameter for `address`: ');
    }

    return new Address({
      addressId: 0,
      entityId: input.entityId,
      networkId: input.networkId,
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

export class Tag {
  public readonly record: TagRecord;

  private constructor(record: TagRecord) {
    this.record = record;
  }

  public static create(input: { id?: string; name: string; riskLevel: RiskLevel }): Tag {
    if (!input.name.trim()) {
      throw new ValidationError('invalid parameter for `name`: ');
    }

    return new Tag({
      tagId: 0,
      id: input.id ? ExternalId.parse(input.id, 'tag').value : ExternalId.create('tag').value,
      name: input.name.trim(),
      riskLevel: input.riskLevel,
      updatedAt: null,
      createdAt: new Date().toISOString(),
    });
  }
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
    name: input.name?.trim() ?? record.name,
    riskLevel: input.riskLevel ?? record.riskLevel,
    updatedAt: new Date().toISOString(),
  };
}
