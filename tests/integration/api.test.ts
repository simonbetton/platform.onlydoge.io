import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createClient as createLibsqlClient } from '@libsql/client';
import { AccessControlService, InMemoryApiKeyRateLimiter } from '@onlydoge/access-control';
import { buildApiApp } from '@onlydoge/api';
import { configKeyDogecoinHistoryReady } from '@onlydoge/indexing-pipeline';
import { createRuntime } from '@onlydoge/platform';
import { ApiSecret } from '@onlydoge/shared-kernel';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { dogecoinFixture } from '../fixtures/dogecoin';
import {
  createAuthenticatedTestApp,
  createTestApp,
  installRpcMock,
  readObjectArray as readObjectArrayField,
  request,
  requireNumber as requireNumberField,
  requireObject,
  requireString as requireStringField,
  runIndexerUntilProcessed,
} from '../helpers';

describe('api integration', () => {
  let restoreFetch: ReturnType<typeof installRpcMock>;

  beforeEach(() => {
    restoreFetch = installRpcMock();
  });

  afterEach(() => {
    restoreFetch.mockRestore();
  });

  it('serves heartbeat and openapi', async () => {
    const ctx = await createTestApp();

    const up = await request(ctx.app, '/up');
    expect(up.status).toBe(200);
    expect(up.headers.get('cache-control')).toBe('no-store');

    const heartbeat = await request(ctx.app, '/v1/heartbeat');
    expect(heartbeat.status).toBe(204);
    expect(heartbeat.headers.get('cache-control')).toBe('no-store');

    const openapi = await request(ctx.app, '/openapi/json');
    expect(openapi.status).toBe(200);
    expect(openapi.headers.get('cache-control')).toBe(
      'public, max-age=300, stale-while-revalidate=3600',
    );
    const document = await openapi.json();
    expect(document).toMatchSnapshot();

    const scalar = await request(ctx.app, '/openapi');
    expect(scalar.status).toBe(200);
    expect(scalar.headers.get('content-type')).toContain('text/html');

    await ctx.cleanup();
  });

  it('returns a 404 envelope with a request id for missing paths', async () => {
    const ctx = await createTestApp();

    const response = await request(ctx.app, '/missing-route');

    expect(response.status).toBe(404);
    expect(response.headers.get('cache-control')).toBe('no-store');
    expect(String(response.headers.get('x-request-id'))).toMatch(/^[\w.-]{1,128}$/u);
    expect(await response.json()).toEqual({
      error: 'not found',
    });

    await ctx.cleanup();
  });

  it('enforces auth after the first key is created and does not expose the API token after creation', async () => {
    const ctx = await createTestApp();

    const deniedBeforeBootstrap = await request(ctx.app, '/v1/explorer/blocks');
    expect(deniedBeforeBootstrap.status).toBe(401);
    expect(deniedBeforeBootstrap.headers.get('cache-control')).toBe('no-store');

    const deniedBeforeBootstrapWithSlash = await request(ctx.app, '/v1/explorer/blocks/');
    expect(deniedBeforeBootstrapWithSlash.status).toBe(401);

    const deniedKeyListBeforeBootstrap = await request(ctx.app, '/v1/keys');
    expect(deniedKeyListBeforeBootstrap.status).toBe(401);

    const created = await request(ctx.app, '/v1/keys/', {
      method: 'POST',
      body: {},
    });
    expect(created.status).toBe(200);
    const key = await readJsonObject(created);
    const keyId = requireStringField(key, 'id');
    const apiToken = requireStringField(key, 'key');
    expect(requireStringField(key, 'role')).toBe('admin');
    expect(apiToken).toMatch(/^sk_/u);

    const fetchedBeforeUse = await request(ctx.app, `/v1/keys/${keyId}`, {
      headers: {
        'x-api-token': apiToken,
      },
    });
    const fetchedBeforeUseBody = await readJsonObject(fetchedBeforeUse);
    const fetchedBeforeUseKey = readObjectField(fetchedBeforeUseBody, 'key');
    expect(requireStringField(fetchedBeforeUseKey, 'id')).toBe(keyId);
    expect(fetchedBeforeUseKey.key).toBeUndefined();

    const listedBeforeUse = await request(ctx.app, '/v1/keys', {
      headers: {
        'x-api-token': apiToken,
      },
    });
    const listedBeforeUseBody = await readJsonObject(listedBeforeUse);
    const listedBeforeUseKeys = readObjectArrayField(listedBeforeUseBody, 'keys');
    expect(listedBeforeUseKeys).toHaveLength(1);
    expect(listedBeforeUseKeys[0]?.key).toBeUndefined();

    const denied = await request(ctx.app, '/v1/explorer/blocks');
    expect(denied.status).toBe(401);

    const deniedWithSlash = await request(ctx.app, '/v1/explorer/blocks/');
    expect(deniedWithSlash.status).toBe(401);

    const deniedExplorer = await request(ctx.app, '/v1/explorer/search?q=test');
    expect(deniedExplorer.status).toBe(401);

    const deniedInvalidToken = await request(ctx.app, '/v1/explorer/blocks', {
      headers: {
        'x-api-token': 'sk_invalid',
      },
    });
    expect(deniedInvalidToken.status).toBe(401);
    expect(deniedInvalidToken.headers.get('cache-control')).toBe('no-store');
    expect(await readJsonObject(deniedInvalidToken)).toEqual({
      error: 'unauthorized',
    });

    const heartbeat = await request(ctx.app, '/v1/heartbeat');
    expect(heartbeat.status).toBe(204);

    const allowed = await request(ctx.app, '/v1/keys', {
      headers: {
        'x-api-token': apiToken,
      },
    });
    expect(allowed.status).toBe(200);

    const fetched = await request(ctx.app, `/v1/keys/${keyId}`, {
      headers: {
        'x-api-token': apiToken,
      },
    });
    const fetchedBody = await readJsonObject(fetched);
    const fetchedKey = readObjectField(fetchedBody, 'key');
    expect(requireStringField(fetchedKey, 'id')).toBe(keyId);
    expect(fetchedKey.key).toBeUndefined();

    await ctx.cleanup();
  });

  it('creates exactly one bootstrap admin across concurrent HTTP requests', async () => {
    const ctx = await createTestApp();

    try {
      const responses = await Promise.all(
        Array.from({ length: 8 }, (_, index) =>
          request(ctx.app, '/v1/keys/', {
            method: 'POST',
            body: { id: `key_concurrent_bootstrap_${index}` },
          }),
        ),
      );
      const payloads = await Promise.all(responses.map(readJsonObject));
      const successfulIndexes = responses
        .map((response, index) => (response.status === 200 ? index : -1))
        .filter((index) => index >= 0);

      expect(successfulIndexes).toHaveLength(1);
      expect(responses.filter((response) => response.status === 401)).toHaveLength(7);
      for (const [index, payload] of payloads.entries()) {
        if (successfulIndexes.includes(index)) {
          expect(requireStringField(payload, 'role')).toBe('admin');
          expect(requireStringField(payload, 'key')).toMatch(/^sk_/u);
        } else {
          expect(payload).toEqual({ error: 'unauthorized' });
          expect(payload.key).toBeUndefined();
        }
      }

      await expect(ctx.runtime.metadata.countApiKeys()).resolves.toBe(1);
      await expect(ctx.runtime.metadata.countActiveAdminApiKeys()).resolves.toBe(1);
    } finally {
      await ctx.cleanup();
    }
  });

  it('restricts admin routes to admin API keys and records active-key denials', async () => {
    const ctx = await createTestApp();

    const adminKey = await request(ctx.app, '/v1/keys/', {
      method: 'POST',
      body: {},
    }).then(readJsonObject);
    const adminId = requireStringField(adminKey, 'id');
    const adminToken = requireStringField(adminKey, 'key');
    const adminHeaders = { 'x-api-token': adminToken };

    const memberKey = await request(ctx.app, '/v1/keys/', {
      method: 'POST',
      headers: adminHeaders,
      body: {},
    }).then(readJsonObject);
    const memberToken = requireStringField(memberKey, 'key');
    const memberHeaders = { 'x-api-token': memberToken };
    expect(requireStringField(memberKey, 'role')).toBe('member');

    const deniedKeyList = await request(ctx.app, '/v1/keys/', {
      headers: memberHeaders,
    });
    expect(deniedKeyList.status).toBe(403);

    for (const path of [
      '/v1/keys?limit=501',
      '/v1/keys?offset=100001',
      '/v1/audit/events?limit=-1',
      '/v1/audit/events?offset=not-an-integer',
    ]) {
      const invalidPage = await request(ctx.app, path, {
        headers: adminHeaders,
      });
      expect(invalidPage.status).toBe(400);
    }

    const deniedKeyCreate = await request(ctx.app, '/v1/keys/', {
      method: 'POST',
      headers: memberHeaders,
      body: {
        id: 'key_denied',
      },
    });
    expect(deniedKeyCreate.status).toBe(403);

    const lastAdminDeactivate = await request(ctx.app, `/v1/keys/${adminId}`, {
      method: 'PUT',
      headers: adminHeaders,
      body: { isActive: false },
    });
    expect(lastAdminDeactivate.status).toBe(400);
    expect(await readJsonObject(lastAdminDeactivate)).toEqual({
      error: 'cannot remove the last active admin API key',
    });

    await request(ctx.app, '/v1/keys/', {
      method: 'POST',
      headers: {
        'x-api-token': 'sk_invalid',
      },
      body: {},
    });

    const audit = await request(
      ctx.app,
      `/v1/audit/events?actor=${encodeURIComponent(requireStringField(memberKey, 'id'))}`,
      { headers: adminHeaders },
    ).then(readJsonObject);
    const events = readObjectArrayField(audit, 'events');
    expect(events.some((event) => requireNumberField(event, 'statusCode') === 403)).toBe(true);
    expect(
      events.some(
        (event) => readObjectField(event, 'actor').id === requireStringField(memberKey, 'id'),
      ),
    ).toBe(true);

    const invalidTokenAudit = await request(ctx.app, '/v1/audit/events?actor=key_missing', {
      headers: adminHeaders,
    }).then(readJsonObject);
    expect(readObjectArrayField(invalidTokenAudit, 'events')).toEqual([]);

    await ctx.cleanup();
  });

  it('removes legacy network, token, label, and investigation routes', async () => {
    const { ctx, headers } = await createAuthenticatedTestApp();

    const removedRoutes = [
      '/v1/networks',
      '/v1/tokens',
      '/v1/entities',
      '/v1/addresses',
      '/v1/tags',
      '/v1/info?q=DTestAddress123',
      '/v1/stats/',
      '/v1/explorer/networks',
    ];

    for (const path of removedRoutes) {
      const response = await request(ctx.app, path, { headers });
      expect(response.status).toBe(404);
      expect(await readJsonObject(response)).toEqual({ error: 'not found' });
    }

    await ctx.cleanup();
  });

  it('returns an infrastructure error when authentication storage fails', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    const ctx = await createTestApp();

    try {
      const accessControl = new AccessControlService(ctx.runtime.metadata);
      const created = await accessControl.createKey({});
      const apiToken = created.key;
      if (!apiToken) {
        throw new TypeError('expected API token');
      }
      vi.spyOn(accessControl, 'authenticate').mockRejectedValueOnce(
        new Error('metadata query failed'),
      );
      const app = buildApiApp({
        ...ctx.runtime,
        accessControl,
      });

      const response = await request(app, '/v1/explorer/blocks', {
        headers: {
          'x-api-token': apiToken,
        },
      });

      expect(response.status).toBe(500);
      expect(response.headers.get('cache-control')).toBe('no-store');
      expect(await readJsonObject(response)).toEqual({
        error: 'authentication store unavailable',
      });
    } finally {
      consoleError.mockRestore();
      await ctx.cleanup();
    }
  });

  it('authenticates without a post-auth metadata update', async () => {
    const ctx = await createTestApp();

    try {
      const accessControl = new AccessControlService(ctx.runtime.metadata);
      const created = await accessControl.createKey({});
      const apiToken = created.key;
      if (!apiToken) {
        throw new TypeError('expected API token');
      }
      const updateApiKey = vi.spyOn(ctx.runtime.metadata, 'updateApiKey');
      const app = buildApiApp({
        ...ctx.runtime,
        accessControl,
      });

      const response = await request(app, '/v1/keys', {
        headers: {
          'x-api-token': apiToken,
        },
      });

      expect(response.status).toBe(200);
      expect(updateApiKey).not.toHaveBeenCalled();
    } finally {
      await ctx.cleanup();
    }
  });

  it('migrates legacy API key plaintext storage away', async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), 'onlydoge-legacy-api-key-'));
    const databasePath = join(tempRoot, 'onlydoge.sqlite.db');
    const databaseUrl = `sqlite://${databasePath}`;
    const previousEnv = new Map<string, string | undefined>();
    const envKeys = [
      'ONLYDOGE_DATABASE',
      'ONLYDOGE_STORAGE',
      'ONLYDOGE_WAREHOUSE',
      'ONLYDOGE_MODE',
    ] as const;
    for (const key of envKeys) {
      previousEnv.set(key, process.env[key]);
    }
    const client = createLibsqlClient({ url: `file:${databasePath}` });

    try {
      await client.execute(`
        CREATE TABLE api_keys (
          api_key_id INTEGER PRIMARY KEY AUTOINCREMENT,
          id TEXT NOT NULL UNIQUE,
          secret_key TEXT NULL,
          secret_key_hash TEXT NOT NULL,
          is_active INTEGER NOT NULL,
          updated_at TEXT NULL,
          created_at TEXT NOT NULL
        )
      `);
      const apiToken = 'sk_legacyfixture';
      const createdAt = '2026-01-01T00:00:00.000Z';
      await client.execute({
        sql: `
          INSERT INTO api_keys (id, secret_key, secret_key_hash, is_active, updated_at, created_at)
          VALUES (?, ?, ?, ?, ?, ?)
        `,
        args: ['key_legacy', apiToken, ApiSecret.hashFromToken(apiToken), 1, null, createdAt],
      });

      process.env.ONLYDOGE_DATABASE = databaseUrl;
      process.env.ONLYDOGE_STORAGE = `file://${tempRoot}/storage`;
      process.env.ONLYDOGE_WAREHOUSE = `${tempRoot}/warehouse.json`;
      process.env.ONLYDOGE_MODE = 'both';

      const runtime = await createRuntime({
        mode: 'both',
        ip: '127.0.0.1',
        port: 2277,
      });
      const columns = await client.execute('PRAGMA table_info(api_keys)');
      const columnNames = columns.rows.map((row) => String(row.name));
      expect(columnNames).not.toContain('secret_key');

      const indexes = await client.execute('PRAGMA index_list(api_keys)');
      const hashIndex = indexes.rows.find(
        (row) => String(row.name) === 'uq_api_keys_secret_key_hash',
      );
      expect(hashIndex).toBeDefined();
      expect(Number(hashIndex?.unique)).toBe(1);
      await expect(runtime.accessControl.authenticate(apiToken)).resolves.toMatchObject({
        id: 'key_legacy',
      });
    } finally {
      for (const [key, value] of previousEnv.entries()) {
        if (value === undefined) {
          delete process.env[key];
        } else {
          process.env[key] = value;
        }
      }
      await rm(tempRoot, { recursive: true, force: true });
    }
  });

  it('migrates legacy audit event resource ids', async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), 'onlydoge-legacy-audit-events-'));
    const databasePath = join(tempRoot, 'onlydoge.sqlite.db');
    const databaseUrl = `sqlite://${databasePath}`;
    const previousEnv = new Map<string, string | undefined>();
    const envKeys = [
      'ONLYDOGE_DATABASE',
      'ONLYDOGE_STORAGE',
      'ONLYDOGE_WAREHOUSE',
      'ONLYDOGE_MODE',
    ] as const;
    for (const key of envKeys) {
      previousEnv.set(key, process.env[key]);
    }
    const client = createLibsqlClient({ url: `file:${databasePath}` });

    try {
      await client.execute(`
        CREATE TABLE audit_events (
          audit_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
          id TEXT NOT NULL UNIQUE,
          actor_api_key_id BIGINT NOT NULL,
          actor_api_key TEXT NOT NULL,
          actor_role TEXT NOT NULL,
          owner_api_key_id BIGINT NULL,
          owner_api_key TEXT NULL,
          method TEXT NOT NULL,
          path TEXT NOT NULL,
          route TEXT NOT NULL,
          operation TEXT NOT NULL,
          resource_type TEXT NOT NULL,
          resource_ids TEXT NOT NULL,
          status_code INTEGER NOT NULL,
          outcome TEXT NOT NULL,
          error TEXT NULL,
          request_id TEXT NOT NULL,
          ip TEXT NULL,
          user_agent TEXT NULL,
          created_at TEXT NOT NULL
        )
      `);
      await client.execute({
        sql: `
          INSERT INTO audit_events (
            id, actor_api_key_id, actor_api_key, actor_role, owner_api_key_id, owner_api_key,
            method, path, route, operation, resource_type, resource_ids, status_code, outcome,
            error, request_id, ip, user_agent, created_at
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        args: [
          'evt_legacy',
          1,
          'key_legacy',
          'admin',
          null,
          null,
          'GET',
          '/v1/keys/key_legacy',
          'GET /v1/keys/:id',
          'read',
          'api_key',
          '["key_legacy"]',
          200,
          'success',
          null,
          'req_legacy',
          '127.0.0.1',
          'test',
          '2026-01-01T00:00:00.000Z',
        ],
      });

      process.env.ONLYDOGE_DATABASE = databaseUrl;
      process.env.ONLYDOGE_STORAGE = `file://${tempRoot}/storage`;
      process.env.ONLYDOGE_WAREHOUSE = `${tempRoot}/warehouse.json`;
      process.env.ONLYDOGE_MODE = 'both';

      const runtime = await createRuntime({
        mode: 'both',
        ip: '127.0.0.1',
        port: 2277,
      });
      const columns = await client.execute('PRAGMA table_info(audit_events)');
      const columnNames = columns.rows.map((row) => String(row.name));
      expect(columnNames).toContain('resource_ids_json');

      await expect(runtime.metadata.listAuditEvents({})).resolves.toEqual([
        expect.objectContaining({
          id: 'evt_legacy',
          resourceIds: ['key_legacy'],
        }),
      ]);

      await runtime.metadata.createAuditEvent({
        actorApiKeyId: 1,
        actorApiKey: 'key_legacy',
        actorRole: 'admin',
        ownerApiKeyId: null,
        ownerApiKey: null,
        method: 'POST',
        path: '/v1/keys/',
        route: 'POST /v1/keys/',
        operation: 'create',
        resourceType: 'api_key',
        resourceIds: ['key_created'],
        statusCode: 201,
        outcome: 'success',
        error: null,
        requestId: 'req_new',
        ip: '127.0.0.1',
        userAgent: 'test',
        createdAt: '2026-01-01T00:01:00.000Z',
      });
      await expect(
        runtime.metadata.listAuditEvents({ resourceId: 'key_created' }),
      ).resolves.toEqual([
        expect.objectContaining({
          resourceIds: ['key_created'],
        }),
      ]);
    } finally {
      for (const [key, value] of previousEnv.entries()) {
        if (value === undefined) {
          delete process.env[key];
        } else {
          process.env[key] = value;
        }
      }
      await rm(tempRoot, { recursive: true, force: true });
    }
  });

  it('rate limits protected requests independently per authenticated API key', async () => {
    const ctx = await createTestApp();
    const accessControl = new AccessControlService(
      ctx.runtime.metadata,
      new InMemoryApiKeyRateLimiter({
        maxRequests: 2,
        windowMs: 60_000,
      }),
    );
    const app = buildApiApp({
      ...ctx.runtime,
      accessControl,
    });
    const firstKey = await accessControl.createKey({});
    const firstToken = firstKey.key;
    if (!firstToken) {
      throw new TypeError('expected API tokens');
    }
    const firstActor = await accessControl.authenticate(firstToken);
    if (!firstActor) {
      throw new TypeError('expected authenticated API key');
    }
    const secondKey = await accessControl.createKey({ role: 'admin' }, firstActor);
    const secondToken = secondKey.key;
    if (!secondToken) {
      throw new TypeError('expected API tokens');
    }
    const firstHeaders = { 'x-api-token': firstToken };
    const secondHeaders = { 'x-api-token': secondToken };

    for (let requestNumber = 0; requestNumber < 2; requestNumber += 1) {
      const response = await request(app, '/v1/keys', {
        headers: firstHeaders,
      });
      expect(response.status).toBe(200);
    }

    const limited = await request(app, '/v1/keys', { headers: firstHeaders });
    expect(limited.status).toBe(429);
    expect(await readJsonObject(limited)).toEqual({
      error: 'rate limit exceeded',
    });
    expect(limited.headers.get('ratelimit-limit')).toBe('2');
    expect(limited.headers.get('ratelimit-remaining')).toBe('0');
    expect(Number(limited.headers.get('ratelimit-reset'))).toBeGreaterThan(0);
    expect(Number(limited.headers.get('retry-after'))).toBeGreaterThan(0);
    expect(limited.headers.get('cache-control')).toBe('no-store');

    const otherKeyResponse = await request(app, '/v1/keys', {
      headers: secondHeaders,
    });
    expect(otherKeyResponse.status).toBe(200);
    expect(otherKeyResponse.headers.get('ratelimit-limit')).toBe('2');
    expect(otherKeyResponse.headers.get('ratelimit-remaining')).toBe('1');

    await ctx.cleanup();
  });

  it('returns not found for removed write routes with authenticated requests', async () => {
    const { ctx, headers } = await createAuthenticatedTestApp();

    for (const [method, path] of [
      ['POST', '/v1/networks'],
      ['POST', '/v1/tags'],
      ['POST', '/v1/entities'],
      ['POST', '/v1/addresses'],
      ['DELETE', '/v1/tags'],
      ['DELETE', '/v1/entities'],
      ['DELETE', '/v1/addresses'],
    ] as const) {
      const response = await request(ctx.app, path, {
        method,
        headers,
        body: {},
      });
      expect(response.status).toBe(404);
      expect(await readJsonObject(response)).toEqual({ error: 'not found' });
    }

    await ctx.cleanup();
  });

  it('returns short-lived private cache headers for authenticated collection reads', async () => {
    const { ctx, headers } = await createAuthenticatedTestApp();

    const response = await request(ctx.app, '/v1/explorer/blocks', {
      headers,
    });

    expect(response.status).toBe(200);
    expect(response.headers.get('cache-control')).toBe(
      'private, max-age=30, stale-while-revalidate=120',
    );
    expect(response.headers.get('vary')).toBe('x-api-token');

    await ctx.cleanup();
  });

  it('serves authenticated explorer endpoints from indexed dogecoin data', async () => {
    const scenario = await createExplorerScenario();

    try {
      await expectExplorerSearch(scenario);
      await expectExplorerBlocks(scenario);
      await expectExplorerMempool(scenario, restoreFetch);
      await expectExplorerTransaction(scenario);
      await expectExplorerAddress(scenario);
      await expectExplorerAddressHistoryAndUtxos(scenario);
    } finally {
      await scenario.ctx.cleanup();
    }
  });

  it('returns history-not-ready responses while current UTXO reads remain available', async () => {
    const scenario = await createExplorerScenario();

    try {
      await scenario.ctx.runtime.metadata.setJsonValue(configKeyDogecoinHistoryReady(), false);

      const search = await request(scenario.ctx.app, '/v1/explorer/search?q=2', {
        headers: scenario.headers,
      });
      expect(search.status).toBe(425);
      expect(await search.json()).toEqual({
        error: 'dogecoin history index is not ready',
      });

      const block = await request(scenario.ctx.app, '/v1/explorer/blocks/2', {
        headers: scenario.headers,
      });
      expect(block.status).toBe(425);

      const history = await request(
        scenario.ctx.app,
        `/v1/explorer/addresses/${dogecoinFixture.targetAddress}/transactions`,
        { headers: scenario.headers },
      );
      expect(history.status).toBe(425);

      const utxos = await request(
        scenario.ctx.app,
        `/v1/explorer/addresses/${dogecoinFixture.targetAddress}/utxos`,
        { headers: scenario.headers },
      );
      expect(utxos.status).toBe(200);
      const [utxo] = readObjectArrayField(await readJsonObject(utxos), 'utxos');
      expect(requireStringField(utxo ?? {}, 'outputKey')).toBe('doge-tx-2:0');
    } finally {
      await scenario.ctx.cleanup();
    }
  });

  it('returns not found when removed info route is requested', async () => {
    const { ctx, headers } = await createAuthenticatedTestApp();

    const response = await request(ctx.app, '/v1/info/', {
      headers,
    });

    expect(response.status).toBe(404);
    expect(await response.json()).toEqual({
      error: 'not found',
    });

    await ctx.cleanup();
  });
});

type ExplorerScenario = Awaited<ReturnType<typeof createExplorerScenario>>;

async function createExplorerScenario() {
  const { ctx, headers } = await createAuthenticatedTestApp();

  await runIndexerUntilProcessed(ctx, 2);
  await runIndexerUntilHistoryReady(ctx);

  return { ctx, headers };
}

async function runIndexerUntilHistoryReady(
  ctx: Awaited<ReturnType<typeof createAuthenticatedTestApp>>['ctx'],
): Promise<void> {
  for (let attempt = 0; attempt < 4; attempt += 1) {
    await ctx.runtime.indexingPipeline.runOnce();
    const historyReady = await ctx.runtime.metadata.getJsonValue<boolean>(
      configKeyDogecoinHistoryReady(),
    );
    if (historyReady === true) {
      return;
    }
  }

  throw new Error('dogecoin history did not become ready');
}

async function expectExplorerSearch({ ctx, headers }: ExplorerScenario): Promise<void> {
  const searchByHeight = await request(ctx.app, '/v1/explorer/search?q=2', {
    headers,
  });
  expect(searchByHeight.status).toBe(200);
  expect(searchByHeight.headers.get('cache-control')).toBe(
    'private, max-age=5, stale-while-revalidate=15',
  );
  expect(searchByHeight.headers.get('vary')).toBe('x-api-token');
  const heightMatch = readObjectArrayField(await readJsonObject(searchByHeight), 'matches')[0];
  expect(requireStringField(heightMatch ?? {}, 'type')).toBe('block');

  const searchByTx = await request(ctx.app, '/v1/explorer/search?q=doge-tx-2', {
    headers,
  });
  const txMatch = readObjectArrayField(await readJsonObject(searchByTx), 'matches')[0];
  expect(txMatch).toMatchObject({
    type: 'transaction',
    txid: 'doge-tx-2',
    blockHeight: 2,
  });

  const searchByAddress = await request(
    ctx.app,
    `/v1/explorer/search?q=${dogecoinFixture.targetAddress}`,
    { headers },
  );
  const addressMatch = readObjectArrayField(await readJsonObject(searchByAddress), 'matches')[0];
  expect(requireStringField(addressMatch ?? {}, 'address')).toBe(dogecoinFixture.targetAddress);
}

async function expectExplorerBlocks({ ctx, headers }: ExplorerScenario): Promise<void> {
  const blocks = await request(ctx.app, '/v1/explorer/blocks', { headers });
  const [latestBlock] = readObjectArrayField(await readJsonObject(blocks), 'blocks');
  expect(requireNumberField(latestBlock ?? {}, 'height')).toBe(2);
  const latestBlockHash = requireStringField(latestBlock ?? {}, 'hash');

  const blockDetail = await request(ctx.app, '/v1/explorer/blocks/2', {
    headers,
  });
  const blockDetailBody = await readJsonObject(blockDetail);
  expect(requireNumberField(readObjectField(blockDetailBody, 'block'), 'height')).toBe(2);
  expect(requireNumberField(blockDetailBody, 'offset')).toBe(0);
  expect(requireNumberField(blockDetailBody, 'limit')).toBe(50);
  expect(requireNumberField(blockDetailBody, 'returnedCount')).toBe(1);
  expect(requireNumberField(blockDetailBody, 'totalCount')).toBe(1);
  const [blockTx] = readObjectArrayField(blockDetailBody, 'transactions');
  expect(requireStringField(blockTx ?? {}, 'txid')).toBe('doge-tx-2');

  const blockDetailByHash = await request(
    ctx.app,
    `/v1/explorer/blocks/${latestBlockHash}?offset=1&limit=1`,
    { headers },
  );
  const blockDetailByHashBody = await readJsonObject(blockDetailByHash);
  expect(requireNumberField(readObjectField(blockDetailByHashBody, 'block'), 'height')).toBe(2);
  expect(requireNumberField(blockDetailByHashBody, 'offset')).toBe(1);
  expect(requireNumberField(blockDetailByHashBody, 'limit')).toBe(1);
  expect(requireNumberField(blockDetailByHashBody, 'returnedCount')).toBe(0);
  expect(requireNumberField(blockDetailByHashBody, 'totalCount')).toBe(1);
  expect(readObjectArrayField(blockDetailByHashBody, 'transactions')).toEqual([]);

  const oversizedPage = await request(ctx.app, '/v1/explorer/blocks/2?limit=501', { headers });
  expect(oversizedPage.status).toBe(400);
}

async function expectExplorerMempool(
  { ctx, headers }: ExplorerScenario,
  fetchMock: ReturnType<typeof installRpcMock>,
): Promise<void> {
  const denied = await request(ctx.app, '/v1/explorer/mempool');
  expect(denied.status).toBe(401);

  const callsBefore = fetchMock.mock.calls.length;
  const response = await request(ctx.app, '/v1/explorer/mempool?limit=2', {
    headers,
  });

  expect(response.status).toBe(200);
  expect(response.headers.get('cache-control')).toBe('no-store');
  expect(response.headers.get('vary')).toBe('x-api-token');
  const body = await readJsonObject(response);
  expect(requireNumberField(body, 'totalCount')).toBe(3);
  expect(requireNumberField(body, 'offset')).toBe(0);
  expect(requireNumberField(body, 'limit')).toBe(2);
  expect(requireNumberField(body, 'returnedCount')).toBe(2);
  expect(requireNumberField(body, 'bytes')).toBe(750);
  expect(requireNumberField(body, 'usageBytes')).toBe(1200);
  expect(requireNumberField(body, 'maxMempoolBytes')).toBe(300_000_000);
  expect(requireStringField(body, 'mempoolMinFeeBasePerKilobyte')).toBe('2000');
  expect(requireStringField(body, 'minRelayFeeBasePerKilobyte')).toBe('1000');

  const [firstTx, secondTx] = readObjectArrayField(body, 'transactions');
  expect(requireStringField(firstTx ?? {}, 'txid')).toBe('doge-mempool-b');
  expect(requireStringField(secondTx ?? {}, 'txid')).toBe('doge-mempool-c');
  expect(requireNumberField(firstTx ?? {}, 'time')).toBe(1_700_000_320);
  expect(requireNumberField(firstTx ?? {}, 'height')).toBe(3);
  expect(requireNumberField(firstTx ?? {}, 'sizeBytes')).toBe(100);
  expect(requireStringField(firstTx ?? {}, 'feeBase')).toBe('10000');
  expect(requireStringField(firstTx ?? {}, 'modifiedFeeBase')).toBe('10000');
  expect(requireStringField(firstTx ?? {}, 'feeRateBasePerKilobyte')).toBe('100000');
  expect(requireNumberField(firstTx ?? {}, 'ancestorCount')).toBe(1);
  expect(requireNumberField(firstTx ?? {}, 'ancestorSizeBytes')).toBe(100);
  expect(requireStringField(firstTx ?? {}, 'ancestorFeesBase')).toBe('10000');
  expect(requireNumberField(firstTx ?? {}, 'descendantCount')).toBe(1);
  expect(requireNumberField(firstTx ?? {}, 'descendantSizeBytes')).toBe(100);
  expect(requireStringField(firstTx ?? {}, 'descendantFeesBase')).toBe('10000');
  expect(readStringArrayField(firstTx ?? {}, 'depends')).toEqual([]);

  const zeroLimitResponse = await request(ctx.app, '/v1/explorer/mempool?limit=0', { headers });
  expect(zeroLimitResponse.status).toBe(200);
  expect(requireNumberField(await readJsonObject(zeroLimitResponse), 'limit')).toBe(0);

  const maxLimitResponse = await request(ctx.app, '/v1/explorer/mempool?limit=500', { headers });
  expect(maxLimitResponse.status).toBe(200);
  expect(requireNumberField(await readJsonObject(maxLimitResponse), 'limit')).toBe(500);

  for (const query of ['limit=501', 'limit=-1', 'limit=1.5', 'offset=100001']) {
    const invalidPage = await request(ctx.app, `/v1/explorer/mempool?${query}`, { headers });
    expect(invalidPage.status).toBe(400);
  }

  const defaultLimitResponse = await request(ctx.app, '/v1/explorer/mempool', {
    headers,
  });
  expect(requireNumberField(await readJsonObject(defaultLimitResponse), 'limit')).toBe(50);

  await request(ctx.app, '/v1/explorer/mempool?offset=1&limit=1', { headers });
  const mempoolMethods = fetchMock.mock.calls
    .slice(callsBefore)
    .map((call) => JSON.parse(String(call[1]?.body ?? '{}')) as { method?: string })
    .map((body) => body.method)
    .filter((method) => method === 'getmempoolinfo' || method === 'getrawmempool');
  expect(mempoolMethods).toEqual(['getmempoolinfo', 'getrawmempool']);
}

async function expectExplorerTransaction({ ctx, headers }: ExplorerScenario): Promise<void> {
  const transaction = await request(ctx.app, '/v1/explorer/transactions/doge-tx-2', { headers });
  expect(transaction.status).toBe(200);
  const body = await readJsonObject(transaction);
  expect(readObjectField(body, 'transaction')).toMatchObject({
    txid: 'doge-tx-2',
    blockHeight: 2,
    totalInputBase: '4000000000',
    totalOutputBase: '3900000000',
    feeBase: '100000000',
  });
  expect(readObjectArrayField(body, 'inputs')).toEqual([
    expect.objectContaining({
      address: dogecoinFixture.intermediaryAddress,
      outputKey: 'doge-tx-1:0',
      valueBase: '4000000000',
    }),
  ]);
  expect(readObjectArrayField(body, 'outputs')).toEqual([
    expect.objectContaining({
      address: dogecoinFixture.targetAddress,
      outputKey: 'doge-tx-2:0',
      valueBase: '2500000000',
    }),
    expect.objectContaining({
      address: dogecoinFixture.intermediaryAddress,
      outputKey: 'doge-tx-2:1',
      valueBase: '1400000000',
    }),
  ]);
}

async function expectExplorerAddress({ ctx, headers }: ExplorerScenario): Promise<void> {
  const address = await request(
    ctx.app,
    `/v1/explorer/addresses/${dogecoinFixture.targetAddress}`,
    { headers },
  );
  expect(address.status).toBe(200);
  expect(address.headers.get('cache-control')).toBe(
    'private, max-age=15, stale-while-revalidate=60',
  );
  expect(address.headers.get('vary')).toBe('x-api-token');
  const addressBody = await readJsonObject(address);
  const addressSummary = readObjectField(addressBody, 'address');
  expect(requireStringField(addressSummary, 'balance')).toBe('2500000000');
}

async function expectExplorerAddressHistoryAndUtxos({
  ctx,
  headers,
}: ExplorerScenario): Promise<void> {
  const history = await request(
    ctx.app,
    `/v1/explorer/addresses/${dogecoinFixture.targetAddress}/transactions`,
    { headers },
  );
  const [historyRow] = readObjectArrayField(await readJsonObject(history), 'transactions');
  expect(historyRow).toMatchObject({
    receivedBase: '2500000000',
    sentBase: '0',
  });
  expect(readObjectField(historyRow ?? {}, 'transaction')).toMatchObject({
    txid: 'doge-tx-2',
    blockHeight: 2,
  });

  const utxos = await request(
    ctx.app,
    `/v1/explorer/addresses/${dogecoinFixture.targetAddress}/utxos`,
    { headers },
  );
  const [utxo] = readObjectArrayField(await readJsonObject(utxos), 'utxos');
  expect(requireStringField(utxo ?? {}, 'outputKey')).toBe('doge-tx-2:0');
}

async function readJsonObject(response: Response): Promise<Record<string, unknown>> {
  return requireObject(await response.json(), 'response');
}

function readObjectField(record: Record<string, unknown>, field: string): Record<string, unknown> {
  return requireObject(record[field], field);
}

function readStringArrayField(record: Record<string, unknown>, field: string): string[] {
  const value = record[field];
  if (!Array.isArray(value) || value.some((item) => typeof item !== 'string')) {
    throw new TypeError(`expected string array for ${field}`);
  }

  return [...value];
}
