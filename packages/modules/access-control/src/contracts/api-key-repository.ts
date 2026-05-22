import type { PrimaryId } from '@onlydoge/shared-kernel';
import type { ApiKeyRecord } from '../domain/api-key';

export interface ApiKeyRepository {
  countActiveAdminApiKeys(): Promise<number>;
  countApiKeys(): Promise<number>;
  createApiKey(record: ApiKeyRecord): Promise<ApiKeyRecord>;
  getApiKeyByHash(secretKeyHash: string): Promise<ApiKeyRecord | null>;
  getApiKeyById(id: string): Promise<ApiKeyRecord | null>;
  getApiKeyByInternalId(apiKeyId: PrimaryId): Promise<ApiKeyRecord | null>;
  listApiKeys(offset?: number, limit?: number): Promise<ApiKeyRecord[]>;
  updateApiKey(record: ApiKeyRecord): Promise<void>;
  deleteApiKeys(ids: string[]): Promise<void>;
}
