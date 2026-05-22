import {
  ApiSecret,
  ExternalId,
  NotFoundError,
  type PrimaryId,
  ValidationError,
} from '@onlydoge/shared-kernel';

export type ApiKeyRole = 'admin' | 'member';

const apiKeyRoles = new Set<ApiKeyRole>(['admin', 'member']);

export interface ApiKeyRecord {
  apiKeyId: PrimaryId;
  id: string;
  role: ApiKeyRole;
  secretKeyHash: string;
  isActive: boolean;
  updatedAt: string | null;
  createdAt: string;
}

export interface CreateApiKeyInput {
  id?: string;
  role?: ApiKeyRole;
}

export interface ApiKeyResponse {
  createdAt: string;
  id: string;
  isActive: boolean;
  key?: string;
  role: ApiKeyRole;
}

export class ApiKey {
  public readonly record: ApiKeyRecord;
  public readonly apiToken: string;

  private constructor(record: ApiKeyRecord, apiToken: string) {
    this.record = record;
    this.apiToken = apiToken;
  }

  public static create(input: CreateApiKeyInput, nextPrimaryId = 0): ApiKey {
    const secret = ApiSecret.generate();

    return new ApiKey(
      {
        apiKeyId: nextPrimaryId,
        id: createApiKeyId(input.id),
        role: parseApiKeyRole(input.role ?? 'member'),
        secretKeyHash: secret.hash,
        isActive: true,
        updatedAt: null,
        createdAt: new Date().toISOString(),
      },
      secret.value,
    );
  }
}

function createApiKeyId(id: string | undefined): string {
  if (!id) {
    return ExternalId.create('key').value;
  }

  return ExternalId.parse(id, 'key').value;
}

export function setApiKeyIsActive(record: ApiKeyRecord, isActive: boolean): ApiKeyRecord {
  return {
    ...record,
    isActive,
    updatedAt: new Date().toISOString(),
  };
}

export function setApiKeyRole(record: ApiKeyRecord, role: ApiKeyRole): ApiKeyRecord {
  return {
    ...record,
    role: parseApiKeyRole(role),
    updatedAt: new Date().toISOString(),
  };
}

export function apiKeyToResponse(
  record: ApiKeyRecord,
  options: { apiToken?: string } = {},
): ApiKeyResponse {
  return {
    id: record.id,
    isActive: record.isActive,
    role: record.role,
    createdAt: record.createdAt,
    ...(options.apiToken ? { key: options.apiToken } : {}),
  };
}

export function requireApiKey(record: ApiKeyRecord | null): ApiKeyRecord {
  if (!record) {
    throw new NotFoundError();
  }

  return record;
}

export function parseApiKeyRole(value: string): ApiKeyRole {
  const normalized = value.trim().toLowerCase();
  if (apiKeyRoles.has(normalized as ApiKeyRole)) {
    return normalized as ApiKeyRole;
  }

  throw new ValidationError(`invalid parameter for \`role\`: ${value}`);
}
