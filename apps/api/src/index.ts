import { randomUUID } from 'node:crypto';
import { openapi } from '@elysiajs/openapi';
import {
  type AuthenticatedApiKey,
  apiKeyRateLimitExceededResponse,
  apiKeyRateLimitHeaders,
  apiTokenSecuritySchemeName,
  buildAccessControlHttp,
  type CreateAuditEventInput,
  enforceApiTokenAuth,
} from '@onlydoge/access-control';
import { buildAnalyticsQueryHttp } from '@onlydoge/analytics-query';
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

const auditOutcomeByStatusCode: Partial<Record<number, CreateAuditEventInput['outcome']>> = {
  401: 'denied',
  403: 'denied',
  429: 'rate_limited',
};

const ownerScopedAuditResourceTypes = new Set(['entity', 'address_label', 'tag']);
const idRouteCollections = new Set(['keys', 'networks', 'tokens', 'entities', 'addresses', 'tags']);
const auditOperationByMethod: Record<string, string> = {
  DELETE: 'delete',
  PATCH: 'update',
  POST: 'create',
  PUT: 'update',
};

export function buildApiApp(runtime: Runtime) {
  const requestAuth = new WeakMap<Request, AuthenticatedApiKey>();
  const auditedRequests = new WeakSet<Request>();
  const resolveAuthenticatedApiKey = (request: Request): AuthenticatedApiKey => {
    const actor = requestAuth.get(request);
    if (!actor) {
      throw new InfrastructureError('authenticated API key missing from request context');
    }
    return actor;
  };
  const resolveOptionalAuthenticatedApiKey = (request: Request): AuthenticatedApiKey | undefined =>
    requestAuth.get(request);
  const recordAudit = async (
    request: Request,
    statusCode: number,
    outcome: CreateAuditEventInput['outcome'],
    error: string | null,
  ): Promise<void> => {
    const actor = unauditedRequestActor(request, requestAuth, auditedRequests);
    if (!actor) {
      return;
    }

    auditedRequests.add(request);
    await recordAuditEvent(runtime, actor, request, statusCode, outcome, error);
  };

  return new Elysia()
    .get('/up', () => handleUp(runtime))
    .onBeforeHandle((context) => handleBeforeHandle(context, runtime, requestAuth, recordAudit))
    .onAfterHandle((context) => handleAfterHandle(context, recordAudit))
    .onError(async (context) => {
      const response = handleApiError(context);
      await recordAudit(
        context.request,
        responseStatus(response, context.set.status),
        auditOutcome(responseStatus(response, context.set.status)),
        context.error instanceof Error ? context.error.name : String(context.code),
      );
      return response;
    })
    .use(
      buildAccessControlHttp(
        runtime.accessControl,
        resolveAuthenticatedApiKey,
        resolveOptionalAuthenticatedApiKey,
      ),
    )
    .use(buildNetworkCatalogHttp(runtime.networkCatalog, resolveAuthenticatedApiKey))
    .use(buildEntityLabelingHttp(runtime.entityLabeling, resolveAuthenticatedApiKey))
    .use(buildAnalyticsQueryHttp(runtime.analyticsQuery, resolveAuthenticatedApiKey))
    .use(buildExplorerQueryHttp(runtime.explorerQuery, resolveAuthenticatedApiKey))
    .use(buildInvestigationQueryHttp(runtime.investigationQuery, resolveAuthenticatedApiKey))
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
              description: 'Bootstrap, inspect, deactivate, and delete API keys.',
            },
            {
              name: 'Network Catalog',
              description: 'Configure indexed Dogecoin networks and token currency metadata.',
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
              name: 'Analytics',
              description:
                'Run guarded AI-generated ClickHouse SQL against curated Dogecoin analytics facts.',
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
                  'OnlyDoge API token returned once by `POST /v1/keys`. Only a token hash is stored, so the token cannot be recovered later. The first key can be created without authentication; subsequent protected requests require this header.',
              },
            },
          },
        },
      }),
    );
}

function unauditedRequestActor(
  request: Request,
  requestAuth: WeakMap<Request, AuthenticatedApiKey>,
  auditedRequests: WeakSet<Request>,
): AuthenticatedApiKey | null {
  return auditedRequestActor(requestAuth.get(request), isAuditedRequest(request, auditedRequests));
}

function auditedRequestActor(
  actor: AuthenticatedApiKey | undefined,
  audited: boolean,
): AuthenticatedApiKey | null {
  if (audited) {
    return null;
  }

  return actorOrNull(actor);
}

function actorOrNull(actor: AuthenticatedApiKey | undefined): AuthenticatedApiKey | null {
  if (!actor) {
    return null;
  }

  return actor;
}

function isAuditedRequest(request: Request, auditedRequests: WeakSet<Request>): boolean {
  return auditedRequests.has(request);
}

async function handleBeforeHandle(
  { request, set }: { request: Request; set: { headers: Record<string, string | number> } },
  runtime: Runtime,
  requestAuth: WeakMap<Request, AuthenticatedApiKey>,
  recordAudit: AuditRecorder,
): Promise<unknown> {
  const auth = await enforceApiTokenAuth(
    runtime.accessControl,
    request.method,
    new URL(request.url).pathname,
    request.headers.get('x-api-token'),
  );
  if (auth.authenticatedKey) {
    requestAuth.set(request, auth.authenticatedKey);
  }

  return handleRateLimit(request, set, auth.rateLimit, recordAudit);
}

async function handleRateLimit(
  request: Request,
  set: { headers: Record<string, string | number> },
  rateLimit: ApiRateLimitResult,
  recordAudit: AuditRecorder,
): Promise<unknown> {
  if (!rateLimit) {
    return undefined;
  }

  applyHeaders(set.headers, apiKeyRateLimitHeaders(rateLimit));
  return handleRateLimitDecision(request, rateLimit, recordAudit);
}

async function handleRateLimitDecision(
  request: Request,
  rateLimit: Parameters<typeof apiKeyRateLimitExceededResponse>[0],
  recordAudit: AuditRecorder,
): Promise<unknown> {
  if (rateLimit.allowed) {
    return undefined;
  }

  await recordAudit(request, 429, 'rate_limited', 'rate limit exceeded');
  return apiKeyRateLimitExceededResponse(rateLimit);
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

type AuditRecorder = (
  request: Request,
  statusCode: number,
  outcome: CreateAuditEventInput['outcome'],
  error: string | null,
) => Promise<void>;

type ApiRateLimitResult = Awaited<ReturnType<typeof enforceApiTokenAuth>>['rateLimit'];

type ErrorContext = {
  code: number | string;
  error: unknown;
  path: string;
  request: Request;
  set: {
    headers: Record<string, string | number>;
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

async function handleAfterHandle(
  { path, request, response, set }: AfterHandleContext,
  recordAudit: AuditRecorder,
): Promise<unknown> {
  const requestPath = requestPathname(path, request);
  const status = responseStatus(response, set.status);
  await recordAudit(request, status, auditOutcome(status), null);
  const policy = resolveCachePolicy(request.method, requestPath, status);
  if (!policy) {
    return;
  }

  return applyCachePolicy(requestPath, response, status, policy, set);
}

function handleApiError(context: ErrorContext): { error: string } {
  context.set.headers['cache-control'] = 'no-store';
  const route = errorRoute(context);
  return knownApiError(context, route) ?? handleUnhandledError(context, route);
}

type KnownErrorHandler = (context: ErrorContext, route: string) => { error: string } | null;

const knownErrorHandlers: KnownErrorHandler[] = [
  handleInfrastructureError,
  handleOnlyDogeError,
  handleValidationError,
  handleNotFoundError,
];

function requestPathname(path: string, request: Request): string {
  if (path) {
    return path;
  }

  return new URL(request.url).pathname;
}

function errorRoute(context: ErrorContext): string {
  return `${context.request.method} ${requestPathname(context.path, context.request)}`;
}

function knownApiError(context: ErrorContext, route: string): { error: string } | null {
  return knownErrorHandlers.map((handler) => handler(context, route)).find(isKnownError) ?? null;
}

function isKnownError(result: { error: string } | null): result is { error: string } {
  return result !== null;
}

function handleInfrastructureError(
  { code, error, set }: ErrorContext,
  route: string,
): { error: string } | null {
  if (!(error instanceof InfrastructureError)) {
    return null;
  }

  const message = maskRpcEndpointAuthInErrorMessage(error.message);
  console.error('[onlydoge] infrastructure error', {
    route,
    code,
    message,
    ...errorCauseLog(error.cause),
  });
  set.status = error.statusCode;
  return { error: message };
}

function errorCauseLog(cause: unknown): { cause?: string } {
  if (!cause) {
    return {};
  }

  return { cause: describeErrorCause(cause) };
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

  return setStatusCode(setStatus);
}

function setStatusCode(setStatus: unknown): number {
  if (typeof setStatus === 'number') {
    return setStatus;
  }

  return 200;
}

function auditEventInput(
  actor: AuthenticatedApiKey,
  request: Request,
  statusCode: number,
  outcome: CreateAuditEventInput['outcome'],
  error: string | null,
): CreateAuditEventInput {
  const url = new URL(request.url);
  const descriptor = auditDescriptor(request.method, url.pathname, actor);
  return {
    actorApiKeyId: actor.apiKeyId,
    actorApiKey: actor.id,
    actorRole: actor.role,
    method: request.method.toUpperCase(),
    path: url.pathname,
    route: descriptor.route,
    operation: descriptor.operation,
    resourceType: descriptor.resourceType,
    resourceIds: descriptor.resourceIds,
    ownerApiKeyId: descriptor.ownerApiKeyId,
    ownerApiKey: descriptor.ownerApiKey,
    statusCode,
    outcome,
    error,
    requestId: requestId(request),
    ip: clientIp(request),
    userAgent: request.headers.get('user-agent'),
    createdAt: new Date().toISOString(),
  };
}

function requestId(request: Request): string {
  const header = request.headers.get('x-request-id')?.trim();
  if (header) {
    return header;
  }

  return randomUUID();
}

async function recordAuditEvent(
  runtime: Runtime,
  actor: AuthenticatedApiKey,
  request: Request,
  statusCode: number,
  outcome: CreateAuditEventInput['outcome'],
  error: string | null,
): Promise<void> {
  try {
    await runtime.metadata.createAuditEvent(
      auditEventInput(actor, request, statusCode, outcome, error),
    );
  } catch (auditError) {
    console.error('[onlydoge] audit event write failed', auditError);
  }
}

function auditOutcome(statusCode: number): CreateAuditEventInput['outcome'] {
  return auditOutcomeByStatusCode[statusCode] ?? auditDefaultOutcome(statusCode);
}

function auditDefaultOutcome(statusCode: number): CreateAuditEventInput['outcome'] {
  return statusCode >= 400 ? 'failure' : 'success';
}

function clientIp(request: Request): string | null {
  const forwarded = forwardedClientIp(request);
  if (forwarded) {
    return forwarded;
  }

  return realClientIp(request);
}

function realClientIp(request: Request): string | null {
  return request.headers.get('x-real-ip') ?? null;
}

function forwardedClientIp(request: Request): string | null {
  const forwarded = request.headers.get('x-forwarded-for')?.split(',').at(0)?.trim();
  if (!forwarded) {
    return null;
  }

  return forwarded;
}

function auditDescriptor(
  method: string,
  path: string,
  actor: AuthenticatedApiKey,
): Pick<
  CreateAuditEventInput,
  'operation' | 'ownerApiKey' | 'ownerApiKeyId' | 'resourceIds' | 'resourceType' | 'route'
> {
  const segments = path.split('/').filter(Boolean);
  const resource = auditResource(segments);
  const route = auditRoute(segments);
  const owner = auditOwner(actor, method, resource.type);

  return {
    route,
    operation: auditOperation(method),
    resourceType: resource.type,
    resourceIds: auditResourceIds(resource),
    ownerApiKeyId: owner.ownerApiKeyId,
    ownerApiKey: owner.ownerApiKey,
  };
}

function auditResourceIds(resource: { id?: string }): string[] {
  return resource.id ? [resource.id] : [];
}

function auditOwner(
  actor: AuthenticatedApiKey,
  method: string,
  resourceType: string,
): Pick<CreateAuditEventInput, 'ownerApiKey' | 'ownerApiKeyId'> {
  if (!isActorOwnedAuditResource(actor, method, resourceType)) {
    return { ownerApiKeyId: null, ownerApiKey: null };
  }

  return { ownerApiKeyId: actor.apiKeyId, ownerApiKey: actor.id };
}

function isActorOwnedAuditResource(
  actor: AuthenticatedApiKey,
  method: string,
  resourceType: string,
): boolean {
  return [ownerScopedAuditResourceTypes.has(resourceType), !isAdminRead(actor, method)].every(
    Boolean,
  );
}

function isAdminRead(actor: AuthenticatedApiKey, method: string): boolean {
  return [actor.role === 'admin', method.toUpperCase() === 'GET'].every(Boolean);
}

function auditResource(segments: string[]): { id?: string; type: string } {
  const [, collection, id, child] = segments;
  const typed = auditResourceType(collection, id);
  if (typed) {
    return typed;
  }

  return specialAuditResource(collection, id, child);
}

function auditResourceType(
  collection: string | undefined,
  id: string | undefined,
): { id?: string; type: string } | null {
  const type = auditResourceTypes[collection ?? ''];
  return typedAuditResourceOrNull(type, id);
}

function typedAuditResourceOrNull(
  type: string | undefined,
  id: string | undefined,
): { id?: string; type: string } | null {
  if (!type) {
    return null;
  }

  return typedAuditResource(type, id);
}

function typedAuditResource(type: string, id: string | undefined): { id?: string; type: string } {
  return { type, ...auditResourceId(id) };
}

function specialAuditResource(
  collection: string | undefined,
  id: string | undefined,
  child: string | undefined,
): { type: string } {
  if (collection === 'explorer') {
    return explorerAuditResource(id, child);
  }

  return namedSpecialAuditResource(collection);
}

function explorerAuditResource(
  id: string | undefined,
  child: string | undefined,
): { type: string } {
  if (child) {
    return { type: `explorer_${id}` };
  }

  return { type: 'explorer' };
}

function namedSpecialAuditResource(collection: string | undefined): { type: string } {
  const type = specialAuditResourceTypes[specialAuditResourceKey(collection)];
  if (type) {
    return { type };
  }

  return fallbackAuditResource(collection);
}

function fallbackAuditResource(collection: string | undefined): { type: string } {
  if (!collection) {
    return { type: 'unknown' };
  }

  return { type: collection };
}

function specialAuditResourceKey(collection: string | undefined): string {
  if (!collection) {
    return '';
  }

  return collection;
}

const auditResourceTypes: Record<string, string> = {
  keys: 'api_key',
  audit: 'audit_event',
  networks: 'network',
  tokens: 'token',
  entities: 'entity',
  addresses: 'address_label',
  tags: 'tag',
};

const specialAuditResourceTypes: Record<string, string> = {
  info: 'investigation',
  stats: 'stats',
};

function auditResourceId(id: string | undefined): { id?: string } {
  if (!id) {
    return {};
  }

  return { id };
}

function auditRoute(segments: string[]): string {
  const normalized = [...segments];
  applyIdRouteParam(normalized);
  applyExplorerRouteParams(normalized);
  return `/${normalized.join('/')}`;
}

function applyIdRouteParam(segments: string[]): void {
  if (!hasIdRouteParam(segments)) {
    return;
  }

  segments[2] = ':id';
}

function hasIdRouteParam(segments: string[]): boolean {
  return [idRouteCollections.has(segments[1] ?? ''), Boolean(segments[2])].every(Boolean);
}

function applyExplorerRouteParams(segments: string[]): void {
  if (segments[1] !== 'explorer') {
    return;
  }

  applyExplorerReferenceParam(segments);
  applyExplorerAddressParam(segments);
}

function applyExplorerReferenceParam(segments: string[]): void {
  if (!segments[3]) {
    return;
  }

  segments[3] = explorerReferenceParam(segments[2]);
}

function explorerReferenceParam(collection: string | undefined): string {
  if (collection === 'blocks') {
    return ':ref';
  }

  return `:${explorerResourceName(collection)}`;
}

function applyExplorerAddressParam(segments: string[]): void {
  if (!hasExplorerAddressChildParam(segments)) {
    return;
  }

  segments[3] = ':address';
}

function explorerResourceName(collection: string | undefined): string {
  if (!collection) {
    return 'id';
  }

  return explorerCollectionResourceName(collection);
}

function explorerCollectionResourceName(collection: string): string {
  return collection.slice(0, -1) || 'id';
}

function hasExplorerAddressChildParam(segments: string[]): boolean {
  return [segments[2] === 'addresses', Boolean(segments[4])].every(Boolean);
}

function auditOperation(method: string): string {
  return auditOperationByMethod[method.toUpperCase()] ?? 'read';
}

function validationStatus(error: unknown): number {
  return readValidationStatus(error) ?? 422;
}

function readValidationStatus(error: unknown): number | null {
  if (!hasStatusProperty(error)) {
    return null;
  }

  return numberOrNull(Reflect.get(error, 'status'));
}

function hasStatusProperty(error: unknown): error is object {
  return [typeof error === 'object', error !== null, 'status' in Object(error)].every(Boolean);
}

function maskRpcEndpointAuthInErrorMessage(message: string): string {
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
  if (!isObjectLike(error)) {
    return 'invalid request';
  }

  return objectValidationError(error);
}

function objectValidationError(error: object): string {
  const details = error as Record<string, unknown>;
  return firstNonEmptyString(details.summary, details.message) ?? 'invalid request';
}

function firstNonEmptyString(...values: unknown[]): string | null {
  return values.map(nonEmptyStringOrNull).find((value) => value !== null) ?? null;
}

function resolveCachePolicy(
  method: string,
  path: string,
  status: number,
): { cacheControl: string; vary?: string } | null {
  if (shouldDisableCache(method, status)) {
    return {
      cacheControl: 'no-store',
    };
  }

  return matchedCachePolicy(normalizeCachePath(path));
}

function matchedCachePolicy(path: string): CachePolicy {
  return matchedPolicyOrDefault(cachePolicyRules.find((rule) => rule.matches(path)));
}

function normalizeCachePath(path: string): string {
  if (!hasTrailingCacheSlash(path)) {
    return path;
  }

  return path.slice(0, -1);
}

function applyCachePolicy(
  path: string,
  response: unknown,
  status: number,
  policy: { cacheControl: string; vary?: string },
  set: { headers: Record<string, string | number> },
): unknown {
  const headers = responseHeaders(response, set.headers);
  applyPolicyHeaders(headers, policy);

  const result = cacheResponseResult({ path, response, status, headers });
  if (result.handled) {
    return result.value;
  }

  applyPolicyToSetHeaders(set.headers, policy);
  return response;
}

function responseHeaders(response: unknown, setHeaders: Record<string, string | number>): Headers {
  const headers = new Headers(response instanceof Response ? response.headers : undefined);
  applyHeaders(headers, setHeaders);
  return headers;
}

function applyHeaders(
  target: Headers | Record<string, string | number>,
  headers: Record<string, string | number>,
): void {
  for (const [name, value] of Object.entries(headers)) {
    applyHeader(target, name, value);
  }
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

function shouldDisableCache(method: string, status: number): boolean {
  return [status >= 400, !isCacheableMethod(method)].includes(true);
}

function matchedPolicyOrDefault(
  rule: { matches(path: string): boolean; policy: CachePolicy } | undefined,
): CachePolicy {
  if (!rule) {
    return noStorePolicy;
  }

  return rule.policy;
}

function hasTrailingCacheSlash(path: string): boolean {
  return [path !== '/', path.endsWith('/')].every(Boolean);
}

function applyPolicyHeaders(headers: Headers, policy: CachePolicy): void {
  headers.set('cache-control', policy.cacheControl);
  applyVaryHeader(headers, policy);
}

function cacheResponseResult(input: CacheResponseHandlerInput): CacheResponseHandlerResult {
  return (
    cacheResponseHandlers.map((handler) => handler(input)).find(isHandledCacheResult) ?? {
      handled: false,
    }
  );
}

function isHandledCacheResult(
  result: CacheResponseHandlerResult,
): result is Extract<CacheResponseHandlerResult, { handled: true }> {
  return result.handled;
}

function applyHeader(
  target: Headers | Record<string, string | number>,
  name: string,
  value: string | number,
): void {
  if (target instanceof Headers) {
    target.set(name, String(value));
    return;
  }

  target[name] = value;
}

function numberOrNull(value: unknown): number | null {
  if (typeof value !== 'number') {
    return null;
  }

  return value;
}

function isObjectLike(value: unknown): value is object {
  return [typeof value === 'object', value !== null].every(Boolean);
}

function nonEmptyStringOrNull(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  return trimmedStringOrNull(value);
}

function trimmedStringOrNull(value: string): string | null {
  if (!value.trim()) {
    return null;
  }

  return value;
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
  if (isOpenApiHtmlPath(path)) {
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
    matches: (path) => path.startsWith('/v1/explorer/mempool'),
    policy: {
      cacheControl: 'no-store',
      vary: 'x-api-token',
    },
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

function isOpenApiHtmlPath(path: string): boolean {
  return [path.startsWith('/openapi'), !path.endsWith('/json')].every(Boolean);
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

  return isPlainObjectBody(value);
}

function isPlainObjectBody(value: unknown): value is Record<string, unknown> {
  if (!isObjectLike(value)) {
    return false;
  }

  return Object.getPrototypeOf(value) === Object.prototype;
}
