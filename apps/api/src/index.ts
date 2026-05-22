import { openapi } from '@elysiajs/openapi';
import {
  apiTokenSecuritySchemeName,
  buildAccessControlHttp,
  enforceApiTokenAuth,
} from '@onlydoge/access-control';
import { buildEntityLabelingHttp } from '@onlydoge/entity-labeling';
import { buildExplorerQueryHttp } from '@onlydoge/explorer-query';
import { buildInvestigationQueryHttp } from '@onlydoge/investigation-query';
import { buildNetworkCatalogHttp } from '@onlydoge/network-catalog';
import type { Runtime } from '@onlydoge/platform';
import {
  InfrastructureError,
  maskRpcEndpointAuth,
  OnlyDogeError,
  RpcEndpoint,
} from '@onlydoge/shared-kernel';
import { Elysia } from 'elysia';

export function buildApiApp(runtime: Runtime) {
  return new Elysia()
    .get('/up', () => handleUp(runtime))
    .onBeforeHandle(async ({ request }) => {
      await enforceApiTokenAuth(
        runtime.accessControl,
        request.method,
        new URL(request.url).pathname,
        request.headers.get('x-api-token'),
      );
    })
    .onAfterHandle((context) => handleAfterHandle(context))
    .onError((context) => handleApiError(context))
    .use(buildAccessControlHttp(runtime.accessControl))
    .use(buildNetworkCatalogHttp(runtime.networkCatalog))
    .use(buildEntityLabelingHttp(runtime.entityLabeling))
    .use(buildExplorerQueryHttp(runtime.explorerQuery))
    .use(buildInvestigationQueryHttp(runtime.investigationQuery))
    .use(
      openapi({
        path: '/openapi',
        specPath: '/openapi/json',
        provider: 'scalar',
        exclude: {
          paths: ['/up'],
        },
        documentation: {
          info: {
            title: 'OnlyDoge API',
            version: '0.1.0',
            description:
              'Dogecoin-first investigation and explorer API. Use `/v1/keys` to bootstrap an API token, then send it in the `x-api-token` header for protected `/v1` routes.',
          },
          servers: [
            {
              url: '/',
              description: 'Current API origin',
            },
          ],
          tags: [
            {
              name: 'Access Control',
              description: 'Bootstrap, inspect, rotate, deactivate, and delete API keys.',
            },
            {
              name: 'Network Catalog',
              description: 'Configure indexed Dogecoin networks and their token metadata.',
            },
            {
              name: 'Entity Labeling',
              description: 'Maintain entities, labels, tagged addresses, and risk tags.',
            },
            {
              name: 'Explorer',
              description:
                'Read indexed Dogecoin blocks, transactions, addresses, UTXOs, and search results.',
            },
            {
              name: 'Investigation',
              description: 'Run address/entity lookups and inspect indexer health signals.',
            },
            {
              name: 'Health',
              description: 'Public runtime health checks.',
            },
          ],
          components: {
            schemas: {
              ErrorResponse: {
                type: 'object',
                required: ['error'],
                properties: {
                  error: {
                    type: 'string',
                    description: 'Human-readable error message.',
                    example: 'not found',
                  },
                },
              },
            },
            securitySchemes: {
              [apiTokenSecuritySchemeName]: {
                type: 'apiKey',
                in: 'header',
                name: 'x-api-token',
                description:
                  'OnlyDoge API token returned once by `POST /v1/keys`. The first key can be created without authentication; subsequent protected requests require this header.',
              },
            },
          },
        },
      }),
    );
}

export async function startApiServer(runtime: Runtime) {
  const app = buildApiApp(runtime);
  return app.listen({
    hostname: runtime.settings.ip,
    port: runtime.settings.port,
  });
}

type AfterHandleContext = {
  path: string;
  request: Request;
  response: unknown;
  set: {
    headers: Record<string, string | number>;
    status?: unknown;
  };
};

type ErrorContext = {
  code: number | string;
  error: unknown;
  path: string;
  request: Request;
  set: {
    status?: number | string;
  };
};

async function handleUp(runtime: Runtime): Promise<Response> {
  await runtime.investigationQuery.heartbeat();
  return new Response('ok', {
    headers: {
      'cache-control': 'no-store',
      'content-type': 'text/plain; charset=utf-8',
    },
  });
}

function handleAfterHandle({ path, request, response, set }: AfterHandleContext): unknown {
  const requestPath = path || new URL(request.url).pathname;
  const status = responseStatus(response, set.status);
  const policy = resolveCachePolicy(request.method, requestPath, status);
  if (!policy) {
    return;
  }

  return applyCachePolicy(requestPath, response, status, policy, set);
}

function handleApiError(context: ErrorContext): { error: string } {
  const route = `${context.request.method} ${context.path || new URL(context.request.url).pathname}`;
  for (const handler of knownErrorHandlers) {
    const result = handler(context, route);
    if (result) {
      return result;
    }
  }

  return handleUnhandledError(context, route);
}

type KnownErrorHandler = (context: ErrorContext, route: string) => { error: string } | null;

const knownErrorHandlers: KnownErrorHandler[] = [
  handleInfrastructureError,
  handleOnlyDogeError,
  handleValidationError,
  handleNotFoundError,
];

function handleInfrastructureError(
  { code, error, set }: ErrorContext,
  route: string,
): { error: string } | null {
  if (!(error instanceof InfrastructureError)) {
    return null;
  }

  console.error('[onlydoge] infrastructure error', {
    route,
    code,
    message: maskLoggedErrorMessage(error.message),
    ...(error.cause ? { cause: describeErrorCause(error.cause) } : {}),
  });
  set.status = error.statusCode;
  return { error: error.message };
}

function handleOnlyDogeError({ error, set }: ErrorContext): { error: string } | null {
  if (!(error instanceof OnlyDogeError)) {
    return null;
  }

  set.status = error.statusCode;
  return { error: error.message };
}

function handleValidationError({ code, error, set }: ErrorContext): { error: string } | null {
  if (code !== 'VALIDATION') {
    return null;
  }

  set.status = validationStatus(error);
  return { error: describeValidationError(error) };
}

function handleNotFoundError({ code, set }: ErrorContext, route: string): { error: string } | null {
  if (code !== 'NOT_FOUND') {
    return null;
  }

  console.error(`[onlydoge] not found route=${route}`);
  set.status = 404;
  return { error: 'not found' };
}

function handleUnhandledError(
  { code, error, set }: ErrorContext,
  route: string,
): { error: string } {
  console.error(`[onlydoge] unhandled error route=${route} code=${code}`, error);
  set.status = 500;
  return { error: 'rekt' };
}

function responseStatus(response: unknown, setStatus: unknown): number {
  if (response instanceof Response) {
    return response.status;
  }

  return typeof setStatus === 'number' ? setStatus : 200;
}

function validationStatus(error: unknown): number {
  return readValidationStatus(error) ?? 422;
}

function readValidationStatus(error: unknown): number | null {
  if (!hasStatusProperty(error)) {
    return null;
  }

  const status = Reflect.get(error, 'status');
  return typeof status === 'number' ? status : null;
}

function hasStatusProperty(error: unknown): error is object {
  return typeof error === 'object' && error !== null && 'status' in error;
}

function maskLoggedErrorMessage(message: string): string {
  return message.replace(/`(https?:\/\/[^`]+)`/gu, (_match, endpoint) => {
    try {
      return `\`${maskRpcEndpointAuth(RpcEndpoint.parse(endpoint))}\``;
    } catch {
      return `\`${endpoint}\``;
    }
  });
}

function describeErrorCause(cause: unknown): string {
  if (cause instanceof Error) {
    return `${cause.name}: ${cause.message}`;
  }

  return String(cause);
}

function describeValidationError(error: unknown): string {
  if (!error || typeof error !== 'object') {
    return 'invalid request';
  }

  const details = error as Record<string, unknown>;
  return firstNonEmptyString(details.summary, details.message) ?? 'invalid request';
}

function firstNonEmptyString(...values: unknown[]): string | null {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) {
      return value;
    }
  }

  return null;
}

function resolveCachePolicy(
  method: string,
  path: string,
  status: number,
): { cacheControl: string; vary?: string } | null {
  if (status >= 400 || !isCacheableMethod(method)) {
    return {
      cacheControl: 'no-store',
    };
  }

  return matchedCachePolicy(normalizeCachePath(path));
}

function matchedCachePolicy(path: string): CachePolicy {
  return cachePolicyRules.find((rule) => rule.matches(path))?.policy ?? noStorePolicy;
}

function normalizeCachePath(path: string): string {
  return path !== '/' && path.endsWith('/') ? path.slice(0, -1) : path;
}

function applyCachePolicy(
  path: string,
  response: unknown,
  status: number,
  policy: { cacheControl: string; vary?: string },
  set: { headers: Record<string, string | number> },
): unknown {
  const headers = new Headers(response instanceof Response ? response.headers : undefined);
  headers.set('cache-control', policy.cacheControl);
  applyVaryHeader(headers, policy);

  for (const handler of cacheResponseHandlers) {
    const result = handler({ path, response, status, headers });
    if (result.handled) {
      return result.value;
    }
  }

  applyPolicyToSetHeaders(set.headers, policy);
  return response;
}

type CacheResponseHandlerInput = {
  headers: Headers;
  path: string;
  response: unknown;
  status: number;
};

type CacheResponseHandlerResult =
  | {
      handled: false;
    }
  | {
      handled: true;
      value: unknown;
    };

const cacheResponseHandlers: Array<
  (input: CacheResponseHandlerInput) => CacheResponseHandlerResult
> = [responseCacheResult, textCacheResult, nullCacheResult, jsonCacheResult];

function responseCacheResult({
  response,
  headers,
}: CacheResponseHandlerInput): CacheResponseHandlerResult {
  if (response instanceof Response) {
    return { handled: true, value: responseWithPolicy(response, headers) };
  }

  return { handled: false };
}

function textCacheResult({
  path,
  response,
  status,
  headers,
}: CacheResponseHandlerInput): CacheResponseHandlerResult {
  if (typeof response === 'string') {
    return {
      handled: true,
      value: textResponseWithPolicy(path, response, status, headers),
    };
  }

  return { handled: false };
}

function nullCacheResult({
  response,
  status,
  headers,
}: CacheResponseHandlerInput): CacheResponseHandlerResult {
  if (response === null) {
    return { handled: true, value: new Response(null, { status, headers }) };
  }

  return { handled: false };
}

function jsonCacheResult({
  response,
  status,
  headers,
}: CacheResponseHandlerInput): CacheResponseHandlerResult {
  if (!isPlainJsonBody(response)) {
    return { handled: false };
  }

  return {
    handled: true,
    value: Response.json(response, {
      status,
      headers,
    }),
  };
}

function applyVaryHeader(headers: Headers, policy: CachePolicy): void {
  if (policy.vary) {
    headers.set('vary', policy.vary);
  }
}

function responseWithPolicy(response: Response, headers: Headers): Response {
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

function textResponseWithPolicy(
  path: string,
  response: string,
  status: number,
  headers: Headers,
): Response {
  headers.set('content-type', textContentType(path));
  return new Response(response, {
    status,
    headers,
  });
}

function textContentType(path: string): string {
  if (path.startsWith('/openapi') && !path.endsWith('/json')) {
    return 'text/html; charset=utf-8';
  }

  return 'text/plain; charset=utf-8';
}

type CachePolicy = { cacheControl: string; vary?: string };

const noStorePolicy: CachePolicy = {
  cacheControl: 'no-store',
};

const privateFastPolicy: CachePolicy = {
  cacheControl: 'private, max-age=5, stale-while-revalidate=15',
  vary: 'x-api-token',
};

const cachePolicyRules: Array<{
  matches(path: string): boolean;
  policy: CachePolicy;
}> = [
  {
    matches: (path) => path === '/up' || path.startsWith('/v1/heartbeat'),
    policy: noStorePolicy,
  },
  {
    matches: (path) => path.startsWith('/v1/explorer/search'),
    policy: privateFastPolicy,
  },
  {
    matches: (path) => path.startsWith('/v1/explorer/addresses'),
    policy: {
      cacheControl: 'private, max-age=15, stale-while-revalidate=60',
      vary: 'x-api-token',
    },
  },
  {
    matches: (path) =>
      hasAnyPrefix(path, [
        '/v1/explorer/blocks',
        '/v1/explorer/transactions',
        '/v1/explorer/networks',
      ]),
    policy: {
      cacheControl: 'private, max-age=30, stale-while-revalidate=120',
      vary: 'x-api-token',
    },
  },
  {
    matches: (path) => path.startsWith('/openapi'),
    policy: {
      cacheControl: 'public, max-age=300, stale-while-revalidate=3600',
    },
  },
  {
    matches: (path) => path === '/v1/keys' || path.startsWith('/v1/keys/'),
    policy: {
      cacheControl: 'no-store',
      vary: 'x-api-token',
    },
  },
  {
    matches: (path) => path.startsWith('/v1/stats'),
    policy: privateFastPolicy,
  },
  {
    matches: (path) => path.startsWith('/v1/info'),
    policy: {
      cacheControl: 'private, max-age=15, stale-while-revalidate=30',
      vary: 'x-api-token',
    },
  },
  {
    matches: (path) =>
      hasAnyPrefix(path, [
        '/v1/networks',
        '/v1/tokens',
        '/v1/entities',
        '/v1/addresses',
        '/v1/tags',
      ]),
    policy: {
      cacheControl: 'private, max-age=30, stale-while-revalidate=60',
      vary: 'x-api-token',
    },
  },
];

function isCacheableMethod(method: string): boolean {
  const normalizedMethod = method.toUpperCase();
  return normalizedMethod === 'GET' || normalizedMethod === 'HEAD';
}

function hasAnyPrefix(path: string, prefixes: string[]): boolean {
  return prefixes.some((prefix) => path.startsWith(prefix));
}

function applyPolicyToSetHeaders(
  headers: Record<string, string | number>,
  policy: CachePolicy,
): void {
  headers['cache-control'] = policy.cacheControl;
  if (policy.vary) {
    headers.vary = policy.vary;
  }
}

function isPlainJsonBody(value: unknown): value is Record<string, unknown> | unknown[] {
  if (Array.isArray(value)) {
    return true;
  }

  if (!value || typeof value !== 'object') {
    return false;
  }

  return Object.getPrototypeOf(value) === Object.prototype;
}
