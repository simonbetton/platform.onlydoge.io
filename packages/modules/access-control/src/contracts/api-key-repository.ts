import type { PrimaryId } from '@onlydoge/shared-kernel';
import type { ApiKeyRecord } from '../domain/api-key';

export type BootstrapApiKeyResult = { created: true; record: ApiKeyRecord } | { created: false };

export interface ApiKeyRepository {
  countActiveAdminApiKeys(): Promise<number>;
  countApiKeys(): Promise<number>;
  createBootstrapApiKey(record: ApiKeyRecord): Promise<BootstrapApiKeyResult>;
  createApiKey(record: ApiKeyRecord): Promise<ApiKeyRecord>;
  getApiKeyByHash(secretKeyHash: string): Promise<ApiKeyRecord | null>;
  getApiKeyById(id: string): Promise<ApiKeyRecord | null>;
  getApiKeyByInternalId(apiKeyId: PrimaryId): Promise<ApiKeyRecord | null>;
  listApiKeys(offset?: number, limit?: number): Promise<ApiKeyRecord[]>;
  updateApiKey(record: ApiKeyRecord): Promise<void>;
  deleteApiKeys(ids: string[]): Promise<void>;
}
