import {
  ApiSecret,
  defaultPageLimit,
  ExternalId,
  ForbiddenError,
  maxPageLimit,
  maxPageOffset,
  NotFoundError,
  type PrimaryId,
  parseBoundedNonNegativeInteger,
  UnauthorizedError,
  ValidationError,
} from '@onlydoge/shared-kernel';
import type { ApiKeyRepository } from '../contracts/api-key-repository';
import type { AuditEventRepository } from '../contracts/audit-event-repository';
import {
  ApiKey,
  type ApiKeyResponse,
  type ApiKeyRole,
  apiKeyToResponse,
  type CreateApiKeyInput,
  parseApiKeyRole,
  requireApiKey,
  setApiKeyIsActive,
  setApiKeyRole,
} from '../domain/api-key';
import {
  type AuditEventFilters,
  type AuditEventResponse,
  auditEventToResponse,
} from '../domain/audit-event';
import { type ApiKeyRateLimitResult, InMemoryApiKeyRateLimiter } from './api-key-rate-limiter';

export interface AuthenticatedApiKey {
  apiKeyId: PrimaryId;
  id: string;
  role: ApiKeyRole;
}

type ApiKeyAdminState = {
  isActive: boolean;
  role: ApiKeyRole;
};
type StoredApiKeyRecord = Parameters<ApiKeyRepository['updateApiKey']>[0];

export class AccessControlService {
  public constructor(
    private readonly apiKeys: ApiKeyRepository & AuditEventRepository,
    private readonly rateLimiter = new InMemoryApiKeyRateLimiter(),
  ) {}

  public async hasConfiguredKeys(): Promise<boolean> {
    return (await this.apiKeys.countApiKeys()) > 0;
  }

  public async createKey(
    input: CreateApiKeyInput,
    actor?: AuthenticatedApiKey,
  ): Promise<ApiKeyResponse> {
    if (!actor) {
      return this.createBootstrapKey(input);
    }

    const hasConfiguredKeys = await this.hasConfiguredKeys();
    this.assertCanCreateKey(hasConfiguredKeys, actor);
    await this.assertApiKeyIdAvailable(input.id);

    const entity = ApiKey.create({
      ...input,
      role: createApiKeyRole(input, hasConfiguredKeys),
    });
    const created = await this.apiKeys.createApiKey(entity.record);

    return apiKeyToResponse(created, { apiToken: entity.apiToken });
  }

  private async createBootstrapKey(input: CreateApiKeyInput): Promise<ApiKeyResponse> {
    await this.assertApiKeyIdAvailable(input.id);
    const entity = ApiKey.create({ ...input, role: 'admin' });
    const result = await this.apiKeys.createBootstrapApiKey(entity.record);
    if (!result.created) {
      throw new UnauthorizedError();
    }

    return apiKeyToResponse(result.record, { apiToken: entity.apiToken });
  }

  public async listKeys(
    actor: AuthenticatedApiKey,
    offset?: number,
    limit?: number,
  ): Promise<{
    keys: ApiKeyResponse[];
  }> {
    requireAdminApiKey(actor);
    const page = boundedPage(offset, limit);

    return {
      keys: (await this.apiKeys.listApiKeys(page.offset, page.limit)).map((key) =>
        apiKeyToResponse(key),
      ),
    };
  }

  public async getKey(actor: AuthenticatedApiKey, id: string): Promise<{ key: ApiKeyResponse }> {
    requireAdminApiKey(actor);

    return {
      key: apiKeyToResponse(requireApiKey(await this.apiKeys.getApiKeyById(id))),
    };
  }

  public async updateKey(
    actor: AuthenticatedApiKey,
    id: string,
    input: {
      isActive?: boolean;
      role?: ApiKeyRole;
    },
  ): Promise<void> {
    requireAdminApiKey(actor);

    const current = requireApiKey(await this.apiKeys.getApiKeyById(id));
    const updated = applyApiKeyUpdate(current, input);

    await this.assertDoesNotRemoveLastActiveAdmin(current, updated);
    await this.updateApiKeyIfChanged(current, updated);
  }

  public async deleteKeys(actor: AuthenticatedApiKey, ids: string[]): Promise<void> {
    requireAdminApiKey(actor);
    await this.assertCanDeleteKeys(ids);
    await this.apiKeys.deleteApiKeys(ids);
  }

  public async authenticate(
    apiToken: string | null | undefined,
  ): Promise<AuthenticatedApiKey | null> {
    if (!(await this.hasConfiguredKeys())) {
      return null;
    }

    const secretHash = ApiSecret.hashFromToken(requireApiTokenHeader(apiToken));
    const record = await this.apiKeys.getApiKeyByHash(secretHash);
    assertApiKeyAuthenticates(record);

    return {
      apiKeyId: record.apiKeyId,
      id: record.id,
      role: record.role,
    };
  }

  public consumeRateLimit(apiKey: AuthenticatedApiKey): ApiKeyRateLimitResult {
    return this.rateLimiter.consume(String(apiKey.apiKeyId));
  }

  public async deleteExpiredAuditEvents(retentionDays: number): Promise<void> {
    const cutoff = new Date(Date.now() - retentionDays * 24 * 60 * 60 * 1000).toISOString();
    await this.apiKeys.deleteAuditEventsBefore(cutoff);
  }

  public async listAuditEvents(
    actor: AuthenticatedApiKey,
    filters: AuditEventFilters,
  ): Promise<{ events: AuditEventResponse[] }> {
    const page = boundedPage(filters.offset, filters.limit);
    const boundedFilters = { ...filters, ...page };
    const scopedFilters =
      actor.role === 'admin'
        ? boundedFilters
        : { ...boundedFilters, actorApiKeyId: actor.apiKeyId };

    return {
      events: (await this.apiKeys.listAuditEvents(scopedFilters)).map(auditEventToResponse),
    };
  }

  private assertCanCreateKey(
    hasConfiguredKeys: boolean,
    actor: AuthenticatedApiKey | undefined,
  ): void {
    if (hasConfiguredKeys) {
      requireAdminApiKey(actor);
    }
  }

  private async assertApiKeyIdAvailable(id: string | undefined): Promise<void> {
    if (!id) {
      return;
    }

    ExternalId.parse(id, 'key');
    await this.assertApiKeyIdDoesNotExist(id);
  }

  private async assertApiKeyIdDoesNotExist(id: string): Promise<void> {
    const existing = await this.apiKeys.getApiKeyById(id);
    if (existing) {
      throw new ValidationError(`invalid parameter for \`id\`: ${id}`);
    }
  }

  private async assertDoesNotRemoveLastActiveAdmin(
    current: ApiKeyAdminState,
    updated: ApiKeyAdminState,
  ): Promise<void> {
    if (!removesActiveAdminApiKey(current, updated)) {
      return;
    }

    await this.assertHasAnotherActiveAdminApiKey();
  }

  private async assertHasAnotherActiveAdminApiKey(): Promise<void> {
    const activeAdminCount = await this.apiKeys.countActiveAdminApiKeys();
    if (activeAdminCount > 1) {
      return;
    }

    throw new ValidationError('cannot remove the last active admin API key');
  }

  private async assertCanDeleteKeys(ids: string[]): Promise<void> {
    const uniqueIds = [...new Set(ids)];
    const records = await Promise.all(uniqueIds.map((id) => this.apiKeys.getApiKeyById(id)));
    assertAllApiKeysFound(records);
    await this.assertDeleteKeepsActiveAdmin(records);
  }

  private async assertDeleteKeepsActiveAdmin(
    records: Array<ApiKeyAdminState | null>,
  ): Promise<void> {
    const deletingActiveAdminCount = countActiveAdminApiKeys(records);
    const activeAdminCount = await this.apiKeys.countActiveAdminApiKeys();
    if (removesLastActiveAdminApiKey(deletingActiveAdminCount, activeAdminCount)) {
      throw new ValidationError('cannot remove the last active admin API key');
    }
  }

  private async updateApiKeyIfChanged(
    current: StoredApiKeyRecord,
    updated: StoredApiKeyRecord,
  ): Promise<void> {
    if (updated !== current) {
      await this.apiKeys.updateApiKey(updated);
    }
  }
}

function boundedPage(
  offset: number | undefined,
  limit: number | undefined,
): { limit: number; offset: number } {
  return {
    offset:
      parseBoundedNonNegativeInteger(offset, {
        defaultValue: 0,
        field: 'offset',
        maximum: maxPageOffset,
      }) ?? 0,
    limit:
      parseBoundedNonNegativeInteger(limit, {
        defaultValue: defaultPageLimit,
        field: 'limit',
        maximum: maxPageLimit,
      }) ?? defaultPageLimit,
  };
}

function applyApiKeyUpdate(
  current: Parameters<typeof setApiKeyRole>[0],
  input: { isActive?: boolean; role?: ApiKeyRole },
): Parameters<typeof setApiKeyRole>[0] {
  return applyApiKeyActiveState(applyApiKeyRole(current, input.role), input.isActive);
}

function applyApiKeyRole(
  current: Parameters<typeof setApiKeyRole>[0],
  role: ApiKeyRole | undefined,
): Parameters<typeof setApiKeyRole>[0] {
  return role ? setApiKeyRole(current, role) : current;
}

function applyApiKeyActiveState(
  current: Parameters<typeof setApiKeyRole>[0],
  isActive: boolean | undefined,
): Parameters<typeof setApiKeyRole>[0] {
  return typeof isActive === 'boolean' ? setApiKeyIsActive(current, isActive) : current;
}

function createApiKeyRole(input: CreateApiKeyInput, hasConfiguredKeys: boolean): ApiKeyRole {
  if (!hasConfiguredKeys) {
    return 'admin';
  }

  return parseApiKeyRole(createConfiguredKeyRole(input.role));
}

function createConfiguredKeyRole(role: ApiKeyRole | undefined): ApiKeyRole {
  return role ?? 'member';
}

function removesActiveAdminApiKey(current: ApiKeyAdminState, updated: ApiKeyAdminState): boolean {
  return [isActiveAdminApiKey(current), !isActiveAdminApiKey(updated)].every(Boolean);
}

function isActiveAdminApiKey(record: ApiKeyAdminState): boolean {
  return [record.isActive, record.role === 'admin'].every(Boolean);
}

function countActiveAdminApiKeys(records: Array<ApiKeyAdminState | null>): number {
  return records.filter(isActiveAdminApiKeyRecord).length;
}

function isActiveAdminApiKeyRecord(record: ApiKeyAdminState | null): boolean {
  return record !== null && isActiveAdminApiKey(record);
}

function removesLastActiveAdminApiKey(deletingCount: number, activeCount: number): boolean {
  return [deletingCount > 0, activeCount - deletingCount <= 0].every(Boolean);
}

function assertAllApiKeysFound(records: Array<ApiKeyAdminState | null>): void {
  if (records.some(isMissingApiKey)) {
    throw new NotFoundError();
  }
}

function isMissingApiKey(record: ApiKeyAdminState | null): boolean {
  return record === null;
}

export function requireAdminApiKey(
  actor: AuthenticatedApiKey | null | undefined,
): asserts actor is AuthenticatedApiKey {
  requireAuthenticatedApiKey(actor);
  requireAdminRole(actor);
}

function requireAuthenticatedApiKey(
  actor: AuthenticatedApiKey | null | undefined,
): asserts actor is AuthenticatedApiKey {
  if (!actor) {
    throw new UnauthorizedError();
  }
}

function requireAdminRole(actor: AuthenticatedApiKey): void {
  if (actor.role !== 'admin') {
    throw new ForbiddenError();
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
  if (!isAuthenticatingApiKey(record)) {
    throw new UnauthorizedError();
  }
}

function isAuthenticatingApiKey(
  record: Awaited<ReturnType<ApiKeyRepository['getApiKeyByHash']>>,
): record is NonNullable<typeof record> {
  return record?.isActive === true;
}
