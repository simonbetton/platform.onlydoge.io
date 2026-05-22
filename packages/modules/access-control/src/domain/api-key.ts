import { ApiSecret, ExternalId, NotFoundError, type PrimaryId } from '@onlydoge/shared-kernel';

export interface ApiKeyRecord {
  apiKeyId: PrimaryId;
  id: string;
  secretKeyPlaintext: string | null;
  secretKeyHash: string;
  isActive: boolean;
  updatedAt: string | null;
  createdAt: string;
}

export interface CreateApiKeyInput {
  id?: string;
}

export interface ApiKeyResponse {
  createdAt: string;
  id: string;
  isActive: boolean;
  key?: string;
}

export class ApiKey {
  public readonly record: ApiKeyRecord;

  private constructor(record: ApiKeyRecord) {
    this.record = record;
  }

  public static create(input: CreateApiKeyInput, nextPrimaryId = 0): ApiKey {
    const id = input.id ? ExternalId.parse(input.id, 'key').value : ExternalId.create('key').value;
    const secret = ApiSecret.generate();

    return new ApiKey({
      apiKeyId: nextPrimaryId,
      id,
      secretKeyPlaintext: secret.value,
      secretKeyHash: secret.hash,
      isActive: true,
      updatedAt: null,
      createdAt: new Date().toISOString(),
    });
  }
}

export function hideApiKeySecret(record: ApiKeyRecord): ApiKeyRecord {
  return {
    ...record,
    secretKeyPlaintext: null,
    updatedAt: new Date().toISOString(),
  };
}

export function setApiKeyIsActive(record: ApiKeyRecord, isActive: boolean): ApiKeyRecord {
  return {
    ...record,
    isActive,
    updatedAt: new Date().toISOString(),
  };
}

export function apiKeyToResponse(
  record: ApiKeyRecord,
  options: { includeSecret?: boolean } = {},
): ApiKeyResponse {
  return {
    id: record.id,
    isActive: record.isActive,
    createdAt: record.createdAt,
    ...(options.includeSecret && record.secretKeyPlaintext
      ? { key: record.secretKeyPlaintext }
      : {}),
  };
}

export function requireApiKey(record: ApiKeyRecord | null): ApiKeyRecord {
  if (!record) {
    throw new NotFoundError();
  }

  return record;
}
