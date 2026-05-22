import type { ApiKeyRecord } from '../domain/api-key';

export interface ApiKeyRepository {
  countApiKeys(): Promise<number>;
  createApiKey(record: ApiKeyRecord): Promise<ApiKeyRecord>;
  getApiKeyByHash(secretKeyHash: string): Promise<ApiKeyRecord | null>;
  getApiKeyById(id: string): Promise<ApiKeyRecord | null>;
  listApiKeys(offset?: number, limit?: number): Promise<ApiKeyRecord[]>;
  updateApiKey(record: ApiKeyRecord): Promise<void>;
  deleteApiKeys(ids: string[]): Promise<void>;
}
