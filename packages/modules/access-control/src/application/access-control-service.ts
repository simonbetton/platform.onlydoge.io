import { ApiSecret, ExternalId, UnauthorizedError, ValidationError } from '@onlydoge/shared-kernel';
import type { ApiKeyRepository } from '../contracts/api-key-repository';
import {
  ApiKey,
  type ApiKeyResponse,
  apiKeyToResponse,
  type CreateApiKeyInput,
  hideApiKeySecret,
  requireApiKey,
  setApiKeyIsActive,
} from '../domain/api-key';

export class AccessControlService {
  public constructor(private readonly apiKeys: ApiKeyRepository) {}

  public async hasConfiguredKeys(): Promise<boolean> {
    return (await this.apiKeys.countApiKeys()) > 0;
  }

  public async createKey(input: CreateApiKeyInput): Promise<ApiKeyResponse> {
    if (input.id) {
      ExternalId.parse(input.id, 'key');
      const existing = await this.apiKeys.getApiKeyById(input.id);
      if (existing) {
        throw new ValidationError(`invalid parameter for \`id\`: ${input.id}`);
      }
    }

    const entity = ApiKey.create(input);
    const created = await this.apiKeys.createApiKey(entity.record);

    return apiKeyToResponse(created, { includeSecret: true });
  }

  public async listKeys(
    offset?: number,
    limit?: number,
  ): Promise<{
    keys: ApiKeyResponse[];
  }> {
    return {
      keys: (await this.apiKeys.listApiKeys(offset, limit)).map((key) => apiKeyToResponse(key)),
    };
  }

  public async getKey(id: string): Promise<{ key: ApiKeyResponse }> {
    return {
      key: apiKeyToResponse(requireApiKey(await this.apiKeys.getApiKeyById(id))),
    };
  }

  public async updateKey(
    id: string,
    input: {
      isActive?: boolean;
    },
  ): Promise<void> {
    const current = requireApiKey(await this.apiKeys.getApiKeyById(id));
    const updated =
      typeof input.isActive === 'boolean' ? setApiKeyIsActive(current, input.isActive) : current;

    if (updated !== current) {
      await this.apiKeys.updateApiKey(updated);
    }
  }

  public async deleteKeys(ids: string[]): Promise<void> {
    await this.apiKeys.deleteApiKeys(ids);
  }

  public async authenticate(apiToken: string | null | undefined): Promise<void> {
    if (!(await this.hasConfiguredKeys())) {
      return;
    }

    const secretHash = ApiSecret.hashFromToken(requireApiTokenHeader(apiToken));
    const record = await this.apiKeys.getApiKeyByHash(secretHash);
    assertApiKeyAuthenticates(record);
    await this.hidePlaintextSecret(record);
  }

  private async hidePlaintextSecret(record: Parameters<typeof hideApiKeySecret>[0]): Promise<void> {
    if (record.secretKeyPlaintext) {
      await this.apiKeys.updateApiKey(hideApiKeySecret(record));
    }
  }
}

function requireApiTokenHeader(apiToken: string | null | undefined): string {
  const token = apiToken?.trim();
  if (!token) {
    throw new UnauthorizedError();
  }

  return token;
}

function assertApiKeyAuthenticates(
  record: Awaited<ReturnType<ApiKeyRepository['getApiKeyByHash']>>,
): asserts record is NonNullable<typeof record> {
  if (!record || !record.isActive) {
    throw new UnauthorizedError();
  }
}
