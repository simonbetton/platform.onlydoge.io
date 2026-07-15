import {
  defaultPageLimit,
  InfrastructureError,
  maxPageLimit,
  maxPageOffset,
  parseBoundedNonNegativeInteger,
  UnauthorizedError,
  ValidationError,
} from '@onlydoge/shared-kernel';
import { Elysia, t } from 'elysia';

import type {
  AccessControlService,
  AuthenticatedApiKey,
} from '../application/access-control-service';
import type { ApiKeyRateLimitResult } from '../application/api-key-rate-limiter';
import type { AuditEventFilters, AuditEventOutcome } from '../domain/audit-event';

export type AuthenticatedApiKeyResolver = (request: Request) => AuthenticatedApiKey;
export type OptionalAuthenticatedApiKeyResolver = (
  request: Request,
) => AuthenticatedApiKey | undefined;

type AuditEventQuery = {
  actor?: string;
  from?: string;
  limit?: string;
  method?: string;
  offset?: string;
  outcome?: AuditEventOutcome;
  resourceId?: string;
  resourceType?: string;
  statusCode?: string;
  to?: string;
};

type AuditEventStringFilterKey = Extract<
  keyof AuditEventFilters,
  'actor' | 'from' | 'method' | 'resourceId' | 'resourceType' | 'to'
>;
type AuditEventNumericFilterKey = Extract<
  keyof AuditEventFilters,
  'limit' | 'offset' | 'statusCode'
>;

const auditEventStringFilterKeys: AuditEventStringFilterKey[] = [
  'actor',
  'from',
  'method',
  'resourceId',
  'resourceType',
  'to',
];
const auditEventNumericFilterKeys: AuditEventNumericFilterKey[] = ['limit', 'offset', 'statusCode'];

function parseNumericFilter(value: string | undefined, field: string): number | undefined {
  if (value === undefined) {
    return undefined;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new ValidationError(`invalid parameter for \`${field}\`: ${value}`);
  }

  return parsed;
}

export const apiTokenSecuritySchemeName = 'ApiTokenAuth';
const apiTokenSecurityRequirement: Record<typeof apiTokenSecuritySchemeName, []> = {
  [apiTokenSecuritySchemeName]: [],
};
export const protectedOperationDetail = {
  security: [apiTokenSecurityRequirement],
};

export function protectedRouteDetail<T extends object>(detail?: T) {
  return {
    ...protectedOperationDetail,
    ...(detail ?? {}),
  };
}

const accessControlTag = 'Access Control';

const offsetQuerySchema = t.Optional(
  t.String({
    description: `Zero-based number of records to skip. Maximum ${maxPageOffset}.`,
    examples: ['0', '25'],
  }),
);
const limitQuerySchema = t.Optional(
  t.String({
    description: `Maximum number of records to return. Defaults to ${defaultPageLimit}; maximum ${maxPageLimit}. Values above the maximum return a validation error.`,
    examples: ['25', '100'],
  }),
);

const paginationQuerySchema = t.Object({
  offset: offsetQuerySchema,
  limit: limitQuerySchema,
});

const auditQuerySchema = t.Object({
  actor: t.Optional(t.String({ description: 'Actor API key id.', examples: ['key_operator'] })),
  from: t.Optional(
    t.String({
      description: 'Inclusive lower bound for event creation time as ISO-8601.',
    }),
  ),
  to: t.Optional(
    t.String({
      description: 'Exclusive upper bound for event creation time as ISO-8601.',
    }),
  ),
  method: t.Optional(t.String({ description: 'HTTP method.', examples: ['POST'] })),
  outcome: t.Optional(
    t.Union(
      [t.Literal('success'), t.Literal('failure'), t.Literal('denied'), t.Literal('rate_limited')],
      {
        description: 'Audit event outcome.',
      },
    ),
  ),
  resourceId: t.Optional(t.String({ description: 'Resource id recorded for the request.' })),
  resourceType: t.Optional(t.String({ description: 'Resource type recorded for the request.' })),
  statusCode: t.Optional(t.String({ description: 'HTTP status code.', examples: ['403'] })),
  offset: offsetQuerySchema,
  limit: limitQuerySchema,
});

export function buildAccessControlHttp(
  service: AccessControlService,
  resolveAuthenticatedApiKey: AuthenticatedApiKeyResolver,
  resolveOptionalAuthenticatedApiKey: OptionalAuthenticatedApiKeyResolver,
) {
  return new Elysia()
    .use(
      new Elysia({ prefix: '/v1/keys' })
        .post(
          '/',
          async ({ body, request }) =>
            service.createKey(body, resolveOptionalAuthenticatedApiKey(request)),
          {
            detail: {
              tags: [accessControlTag],
              summary: 'Create API key',
              description:
                'Creates an API key and returns its API token once in the response `key` field. OnlyDoge stores only a token hash, so the returned API token cannot be recovered later. The first key can be created without authentication as an admin key; after one key exists this route requires an admin API key.',
            },
            body: t.Object({
              id: t.Optional(
                t.String({
                  description: 'Optional key id. If omitted, OnlyDoge generates a `key_` id.',
                  examples: ['key_operator'],
                }),
              ),
              role: t.Optional(
                t.Union([t.Literal('admin'), t.Literal('member')], {
                  description: 'API key role. Defaults to `member` after bootstrap.',
                  examples: ['member'],
                }),
              ),
            }),
          },
        )
        .get(
          '/',
          async ({ query, request }) => {
            const offset = parseBoundedNonNegativeInteger(query.offset, {
              defaultValue: 0,
              field: 'offset',
              maximum: maxPageOffset,
            });
            const limit = parseBoundedNonNegativeInteger(query.limit, {
              defaultValue: defaultPageLimit,
              field: 'limit',
              maximum: maxPageLimit,
            });

            return service.listKeys(resolveAuthenticatedApiKey(request), offset, limit);
          },
          {
            detail: protectedRouteDetail({
              tags: [accessControlTag],
              summary: 'List API keys',
              description:
                'Lists API key metadata. API tokens are only returned once in the `POST /v1/keys` response `key` field and are omitted here.',
            }),
            query: paginationQuerySchema,
          },
        )
        .get(
          '/:id',
          async ({ params, request }) =>
            service.getKey(resolveAuthenticatedApiKey(request), params.id),
          {
            detail: protectedRouteDetail({
              tags: [accessControlTag],
              summary: 'Get API key',
              description: 'Returns metadata for one API key without exposing its API token.',
            }),
            params: t.Object({
              id: t.String({
                description: 'API key id.',
                examples: ['key_operator'],
              }),
            }),
          },
        )
        .put(
          '/:id',
          async ({ params, body, request }) => {
            await service.updateKey(resolveAuthenticatedApiKey(request), params.id, body);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedRouteDetail({
              tags: [accessControlTag],
              summary: 'Update API key',
              description:
                'Activates or deactivates an API key. Deactivated keys can no longer authenticate protected routes.',
              responses: {
                204: {
                  description: 'API key updated.',
                },
              },
            }),
            params: t.Object({
              id: t.String({
                description: 'API key id.',
                examples: ['key_operator'],
              }),
            }),
            body: t.Object({
              isActive: t.Optional(
                t.Boolean({
                  description: 'Whether this key can authenticate requests.',
                  examples: [false],
                }),
              ),
              role: t.Optional(
                t.Union([t.Literal('admin'), t.Literal('member')], {
                  description: 'API key role.',
                  examples: ['member'],
                }),
              ),
            }),
          },
        )
        .delete(
          '/',
          async ({ body, request }) => {
            await service.deleteKeys(resolveAuthenticatedApiKey(request), [...body.keys]);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedRouteDetail({
              tags: [accessControlTag],
              summary: 'Delete API keys',
              description: 'Deletes one or more API keys by id.',
              responses: {
                204: {
                  description: 'API keys deleted.',
                },
              },
            }),
            body: t.Object({
              keys: t.Array(
                t.String({
                  description: 'API key id.',
                  examples: ['key_operator'],
                }),
                {
                  description: 'API key ids to delete.',
                },
              ),
            }),
          },
        ),
    )
    .use(
      new Elysia({ prefix: '/v1/audit' }).get(
        '/events',
        async ({ query, request }) =>
          service.listAuditEvents(
            resolveAuthenticatedApiKey(request),
            auditEventFiltersFromQuery(query),
          ),
        {
          detail: protectedRouteDetail({
            tags: [accessControlTag],
            summary: 'List audit events',
            description:
              'Lists audit events. Admin API keys can read all events; member API keys can read only their own actor events.',
          }),
          query: auditQuerySchema,
        },
      ),
    );
}

function auditEventFiltersFromQuery(query: AuditEventQuery): AuditEventFilters {
  return {
    ...auditEventStringFiltersFromQuery(query),
    ...auditEventOutcomeFilterFromQuery(query),
    ...auditEventNumericFiltersFromQuery(query),
  };
}

function auditEventStringFiltersFromQuery(query: AuditEventQuery): AuditEventFilters {
  const filters: AuditEventFilters = {};

  for (const key of auditEventStringFilterKeys) {
    assignAuditEventStringFilter(filters, key, query[key]);
  }

  return filters;
}

function assignAuditEventStringFilter(
  filters: AuditEventFilters,
  key: AuditEventStringFilterKey,
  value: string | undefined,
): void {
  if (!value) {
    return;
  }

  filters[key] = value;
}

function auditEventOutcomeFilterFromQuery(query: AuditEventQuery): AuditEventFilters {
  if (!query.outcome) {
    return {};
  }

  return { outcome: query.outcome };
}

function auditEventNumericFiltersFromQuery(query: AuditEventQuery): AuditEventFilters {
  const filters: AuditEventFilters = {};

  for (const key of auditEventNumericFilterKeys) {
    const value =
      key === 'limit'
        ? parseBoundedNonNegativeInteger(query[key], {
            defaultValue: defaultPageLimit,
            field: key,
            maximum: maxPageLimit,
          })
        : key === 'offset'
          ? parseBoundedNonNegativeInteger(query[key], {
              defaultValue: 0,
              field: key,
              maximum: maxPageOffset,
            })
          : parseNumericFilter(query[key], key);
    assignAuditEventNumericFilter(filters, key, value);
  }

  return filters;
}

function assignAuditEventNumericFilter(
  filters: AuditEventFilters,
  key: AuditEventNumericFilterKey,
  value: number | undefined,
): void {
  if (value === undefined) {
    return;
  }

  filters[key] = value;
}

export interface ApiTokenAuthResult {
  authenticatedKey?: AuthenticatedApiKey;
  rateLimit?: ApiKeyRateLimitResult;
}

export async function enforceApiTokenAuth(
  service: AccessControlService,
  method: string,
  path: string,
  apiTokenHeader: string | null,
): Promise<ApiTokenAuthResult> {
  const normalizedPath = normalizeAuthPath(path);

  if (isPublicRoute(normalizedPath)) {
    return {};
  }

  return enforceProtectedApiTokenAuth(service, method, normalizedPath, apiTokenHeader);
}

async function enforceProtectedApiTokenAuth(
  service: AccessControlService,
  method: string,
  normalizedPath: string,
  apiTokenHeader: string | null,
): Promise<ApiTokenAuthResult> {
  const hasConfiguredKeys = await service.hasConfiguredKeys();
  if (isBootstrapKeyRoute(method, normalizedPath, hasConfiguredKeys)) {
    return {};
  }

  assertHasConfiguredApiKeys(hasConfiguredKeys);

  const authenticatedKey = await authenticateOrThrow(service, apiTokenHeader);

  return {
    authenticatedKey,
    rateLimit: service.consumeRateLimit(authenticatedKey),
  };
}

function normalizeAuthPath(path: string): string {
  if (path === '/') {
    return path;
  }

  return stripTrailingSlash(path);
}

function stripTrailingSlash(path: string): string {
  return path.endsWith('/') ? path.slice(0, -1) : path;
}

function isPublicRoute(path: string): boolean {
  if (path === '/up') {
    return true;
  }

  return publicRoutePrefixes.some((prefix) => path.startsWith(prefix));
}

const publicRoutePrefixes = ['/v1/heartbeat', '/openapi'];

function isBootstrapKeyRoute(method: string, path: string, hasConfiguredKeys: boolean): boolean {
  return [method.toUpperCase() === 'POST', path === '/v1/keys', !hasConfiguredKeys].every(Boolean);
}

function assertHasConfiguredApiKeys(hasConfiguredKeys: boolean): void {
  if (!hasConfiguredKeys) {
    throw new UnauthorizedError();
  }
}

async function authenticateOrThrow(
  service: AccessControlService,
  apiTokenHeader: string | null,
): Promise<AuthenticatedApiKey> {
  try {
    const authenticatedKey = await service.authenticate(apiTokenHeader);
    return requireAuthenticatedKey(authenticatedKey);
  } catch (error) {
    throw authenticationError(error);
  }
}

function requireAuthenticatedKey(
  authenticatedKey: AuthenticatedApiKey | null,
): AuthenticatedApiKey {
  if (!authenticatedKey) {
    throw new UnauthorizedError();
  }

  return authenticatedKey;
}

function authenticationError(error: unknown): Error {
  if (error instanceof UnauthorizedError) {
    return error;
  }

  return new InfrastructureError('authentication store unavailable', {
    cause: error,
  });
}

export function apiKeyRateLimitHeaders(result: ApiKeyRateLimitResult): Record<string, string> {
  return {
    'RateLimit-Limit': String(result.limit),
    'RateLimit-Remaining': String(result.remaining),
    'RateLimit-Reset': String(result.resetSeconds),
    ...apiKeyRetryAfterHeader(result),
  };
}

function apiKeyRetryAfterHeader(result: ApiKeyRateLimitResult): Record<string, string> {
  if (!shouldSendRetryAfter(result)) {
    return {};
  }

  return { 'Retry-After': String(result.retryAfterSeconds) };
}

function shouldSendRetryAfter(result: ApiKeyRateLimitResult): boolean {
  return [!result.allowed, Boolean(result.retryAfterSeconds)].every(Boolean);
}

export function apiKeyRateLimitExceededResponse(result: ApiKeyRateLimitResult): Response {
  return Response.json(
    {
      error: 'rate limit exceeded',
    },
    {
      status: 429,
      headers: {
        ...apiKeyRateLimitHeaders(result),
        'cache-control': 'no-store',
      },
    },
  );
}
