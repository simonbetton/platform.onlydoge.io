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
  createDogecoinAddressBook,
  createDogecoinTestNetwork,
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

  it('logs the requested route for missing paths and returns a 404 envelope', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    const ctx = await createTestApp();

    const response = await request(ctx.app, '/missing-route');

    expect(response.status).toBe(404);
    expect(response.headers.get('cache-control')).toBe('no-store');
    expect(await response.json()).toEqual({
      error: 'not found',
    });
    expect(consoleError).toHaveBeenCalledWith('[onlydoge] not found route=GET /missing-route');

    consoleError.mockRestore();
    await ctx.cleanup();
  });

  it('enforces auth after the first key is created and does not expose the API token after creation', async () => {
    const ctx = await createTestApp();

    const deniedBeforeBootstrap = await request(ctx.app, '/v1/networks');
    expect(deniedBeforeBootstrap.status).toBe(401);
    expect(deniedBeforeBootstrap.headers.get('cache-control')).toBe('no-store');

    const deniedBeforeBootstrapWithSlash = await request(ctx.app, '/v1/networks/');
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

    const denied = await request(ctx.app, '/v1/networks');
    expect(denied.status).toBe(401);

    const deniedWithSlash = await request(ctx.app, '/v1/networks/');
    expect(deniedWithSlash.status).toBe(401);

    const deniedInfo = await request(ctx.app, '/v1/info?q=test');
    expect(deniedInfo.status).toBe(401);

    const deniedExplorer = await request(ctx.app, '/v1/explorer/networks');
    expect(deniedExplorer.status).toBe(401);

    const deniedInvalidToken = await request(ctx.app, '/v1/networks', {
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

    const allowed = await request(ctx.app, '/v1/networks', {
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

    const deniedKeyList = await request(ctx.app, '/v1/keys/', { headers: memberHeaders });
    expect(deniedKeyList.status).toBe(403);

    const deniedNetworkCreate = await request(ctx.app, '/v1/networks/', {
      method: 'POST',
      headers: memberHeaders,
      body: {
        name: 'Denied Dogecoin',
        architecture: 'dogecoin',
        chainId: 0,
        blockTime: 60,
        rpcEndpoint: 'https://doge.example/rpc',
      },
    });
    expect(deniedNetworkCreate.status).toBe(403);

    const lastAdminDeactivate = await request(ctx.app, `/v1/keys/${adminId}`, {
      method: 'PUT',
      headers: adminHeaders,
      body: { isActive: false },
    });
    expect(lastAdminDeactivate.status).toBe(400);
    expect(await readJsonObject(lastAdminDeactivate)).toEqual({
      error: 'cannot remove the last active admin API key',
    });

    await request(ctx.app, '/v1/networks/', {
      method: 'POST',
      headers: {
        'x-api-token': 'sk_invalid',
      },
      body: {
        name: 'Invalid Token Dogecoin',
        architecture: 'dogecoin',
        chainId: 1,
        blockTime: 60,
        rpcEndpoint: 'https://doge.example/rpc',
      },
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

  it('enforces owner-scoped metadata writes and filters investigation overlays', async () => {
    const { ctx, headers: adminHeaders } = await createAuthenticatedTestApp();

    const memberOne = await request(ctx.app, '/v1/keys/', {
      method: 'POST',
      headers: adminHeaders,
      body: {},
    }).then(readJsonObject);
    const memberTwo = await request(ctx.app, '/v1/keys/', {
      method: 'POST',
      headers: adminHeaders,
      body: {},
    }).then(readJsonObject);
    const memberOneHeaders = { 'x-api-token': requireStringField(memberOne, 'key') };
    const memberTwoHeaders = { 'x-api-token': requireStringField(memberTwo, 'key') };

    const network = await request(ctx.app, '/v1/networks/', {
      method: 'POST',
      headers: adminHeaders,
      body: {
        name: 'Dogecoin Mainnet',
        architecture: 'dogecoin',
        chainId: 0,
        blockTime: 60,
        rpcEndpoint: 'https://doge.example/rpc',
      },
    }).then(readJsonObject);
    const networkId = requireStringField(network, 'id');

    const memberOneTag = await request(ctx.app, '/v1/tags/', {
      method: 'POST',
      headers: memberOneHeaders,
      body: { name: 'Shared Risk', riskLevel: 'high' },
    }).then(readJsonObject);
    const memberTwoTag = await request(ctx.app, '/v1/tags/', {
      method: 'POST',
      headers: memberTwoHeaders,
      body: { name: 'Shared Risk', riskLevel: 'low' },
    }).then(readJsonObject);
    expect(requireStringField(memberTwoTag, 'name')).toBe('Shared Risk');

    const memberOneEntityPayload = await request(ctx.app, '/v1/entities/', {
      method: 'POST',
      headers: memberOneHeaders,
      body: {
        name: 'Shared Entity',
        description: 'Owned by member one',
        tags: [requireStringField(memberOneTag, 'id')],
      },
    }).then(readJsonObject);
    const memberOneEntity = readObjectField(memberOneEntityPayload, 'entity');
    const memberOneEntityId = requireStringField(memberOneEntity, 'id');

    const memberTwoEntityPayload = await request(ctx.app, '/v1/entities/', {
      method: 'POST',
      headers: memberTwoHeaders,
      body: {
        name: 'Shared Entity',
        description: 'Owned by member two',
        tags: [requireStringField(memberTwoTag, 'id')],
      },
    }).then(readJsonObject);
    const memberTwoEntityId = requireStringField(
      readObjectField(memberTwoEntityPayload, 'entity'),
      'id',
    );

    const address = 'DOnlyDogeOwnerScoped123';
    await request(ctx.app, '/v1/addresses/', {
      method: 'POST',
      headers: memberOneHeaders,
      body: {
        entity: memberOneEntityId,
        network: networkId,
        addresses: [{ address, description: 'Member one address' }],
      },
    });

    expect(
      (
        await request(ctx.app, `/v1/entities/${memberOneEntityId}`, {
          headers: memberTwoHeaders,
        })
      ).status,
    ).toBe(404);
    expect(
      (
        await request(ctx.app, `/v1/entities/${memberOneEntityId}`, {
          method: 'PUT',
          headers: adminHeaders,
          body: { description: 'Admin cannot edit another owner record' },
        })
      ).status,
    ).toBe(404);
    expect(
      (
        await request(ctx.app, `/v1/entities/${memberOneEntityId}`, {
          headers: adminHeaders,
        })
      ).status,
    ).toBe(200);

    const deniedBatch = await request(ctx.app, '/v1/entities/', {
      method: 'DELETE',
      headers: memberOneHeaders,
      body: { entities: [memberOneEntityId, memberTwoEntityId] },
    });
    expect(deniedBatch.status).toBe(404);
    expect(
      (
        await request(ctx.app, `/v1/entities/${memberOneEntityId}`, {
          headers: memberOneHeaders,
        })
      ).status,
    ).toBe(200);

    const memberTwoInfo = await request(ctx.app, `/v1/info?q=${address}`, {
      headers: memberTwoHeaders,
    }).then(readJsonObject);
    expect(readObjectArrayField(memberTwoInfo, 'entities')).toEqual([]);
    expect(readObjectArrayField(memberTwoInfo, 'tags')).toEqual([]);

    const memberOneInfo = await request(ctx.app, `/v1/info?q=${address}`, {
      headers: memberOneHeaders,
    }).then(readJsonObject);
    expect(readObjectArrayField(memberOneInfo, 'entities')).toHaveLength(1);
    expect(readObjectArrayField(memberOneInfo, 'tags')).toHaveLength(1);

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

      const response = await request(app, '/v1/networks', {
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

      const response = await request(app, '/v1/networks', {
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

      const runtime = await createRuntime({ mode: 'both', ip: '127.0.0.1', port: 2277 });
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
    const secondKey = await accessControl.createKey({}, firstActor);
    const secondToken = secondKey.key;
    if (!secondToken) {
      throw new TypeError('expected API tokens');
    }
    const firstHeaders = { 'x-api-token': firstToken };
    const secondHeaders = { 'x-api-token': secondToken };

    for (let requestNumber = 0; requestNumber < 2; requestNumber += 1) {
      const response = await request(app, '/v1/networks', { headers: firstHeaders });
      expect(response.status).toBe(200);
    }

    const limited = await request(app, '/v1/networks', { headers: firstHeaders });
    expect(limited.status).toBe(429);
    expect(await readJsonObject(limited)).toEqual({
      error: 'rate limit exceeded',
    });
    expect(limited.headers.get('ratelimit-limit')).toBe('2');
    expect(limited.headers.get('ratelimit-remaining')).toBe('0');
    expect(Number(limited.headers.get('ratelimit-reset'))).toBeGreaterThan(0);
    expect(Number(limited.headers.get('retry-after'))).toBeGreaterThan(0);
    expect(limited.headers.get('cache-control')).toBe('no-store');

    const otherKeyResponse = await request(app, '/v1/networks', { headers: secondHeaders });
    expect(otherKeyResponse.status).toBe(200);
    expect(otherKeyResponse.headers.get('ratelimit-limit')).toBe('2');
    expect(otherKeyResponse.headers.get('ratelimit-remaining')).toBe('1');

    await ctx.cleanup();
  });

  it('creates networks, tags, entities, addresses, and resolves info queries', async () => {
    const { ctx, headers } = await createAuthenticatedTestApp();

    const networkResponse = await request(ctx.app, '/v1/networks', {
      method: 'POST',
      headers,
      body: {
        name: 'Dogecoin Mainnet',
        architecture: 'dogecoin',
        chainId: 0,
        blockTime: 60,
        rpcEndpoint: 'https://user:pass@doge.example/rpc',
      },
    });
    const network = await readJsonObject(networkResponse);
    const networkId = requireStringField(network, 'id');
    expect(requireStringField(network, 'rpcEndpoint')).toContain('***');

    const tagResponse = await request(ctx.app, '/v1/tags', {
      method: 'POST',
      headers,
      body: {
        name: 'Sanctions',
        riskLevel: 'high',
      },
    });
    const tag = await readJsonObject(tagResponse);
    const tagId = requireStringField(tag, 'id');

    const entityResponse = await request(ctx.app, '/v1/entities', {
      method: 'POST',
      headers,
      body: {
        name: 'Example Entity',
        description: 'Tracked counterparty',
        tags: [tagId],
      },
    });
    const entityPayload = await readJsonObject(entityResponse);
    const entity = readObjectField(entityPayload, 'entity');
    const entityId = requireStringField(entity, 'id');

    const addressResponse = await request(ctx.app, '/v1/addresses', {
      method: 'POST',
      headers,
      body: {
        entity: entityId,
        network: networkId,
        addresses: [
          {
            address: 'DTestAddress123',
            description: 'Main wallet',
          },
        ],
      },
    });
    expect(addressResponse.status).toBe(200);

    const fetchedEntity = await request(ctx.app, `/v1/entities/${entityId}`, {
      headers,
    });
    const fetchedEntityBody = await readJsonObject(fetchedEntity);
    const fetchedEntityRecord = readObjectField(fetchedEntityBody, 'entity');
    expect(readStringArrayField(fetchedEntityRecord, 'tags')).toEqual([tagId]);
    expect(readStringArrayField(fetchedEntityRecord, 'addresses')).toHaveLength(1);

    const infoResponse = await request(ctx.app, '/v1/info?q=DTestAddress123', {
      headers,
    });
    expect(infoResponse.headers.get('cache-control')).toBe(
      'private, max-age=15, stale-while-revalidate=30',
    );
    expect(infoResponse.headers.get('vary')).toBe('x-api-token');
    const info = await readJsonObject(infoResponse);
    expect(readStringArrayField(info, 'addresses')).toEqual(['DTestAddress123']);
    const [infoEntity] = readObjectArrayField(info, 'entities');
    const [infoTag] = readObjectArrayField(info, 'tags');
    const risk = readObjectField(info, 'risk');
    if (!infoEntity || !infoTag) {
      throw new TypeError('missing info relationship records');
    }
    expect(requireStringField(infoEntity, 'id')).toBe(entityId);
    expect(requireStringField(infoTag, 'id')).toBe(tagId);
    expect(requireStringField(risk, 'level')).toBe('high');
    expect(readStringArrayField(risk, 'reasons')).toContain('entity');

    await ctx.cleanup();
  });

  it('returns short-lived private cache headers for authenticated collection reads', async () => {
    const { ctx, headers } = await createAuthenticatedTestApp();

    const response = await request(ctx.app, '/v1/networks', {
      headers,
    });

    expect(response.status).toBe(200);
    expect(response.headers.get('cache-control')).toBe(
      'private, max-age=30, stale-while-revalidate=60',
    );
    expect(response.headers.get('vary')).toBe('x-api-token');

    await ctx.cleanup();
  });

  it('serves authenticated explorer endpoints from indexed dogecoin data', async () => {
    const scenario = await createExplorerScenario();

    try {
      await expectExplorerNetworks(scenario);
      await expectExplorerSearch(scenario);
      await expectExplorerBlocks(scenario);
      await expectExplorerMempool(scenario, restoreFetch);
      await expectExplorerTransaction(scenario);
      await expectExplorerAddress(scenario);
      await expectExplorerAddressHistoryAndUtxos(scenario);
      await expectExplorerInfoAuth(scenario);
    } finally {
      await scenario.ctx.cleanup();
    }
  });

  it('returns history-not-ready responses while current UTXO reads remain available', async () => {
    const scenario = await createExplorerScenario();

    try {
      const internalNetwork =
        await scenario.ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');
      const networkId = internalNetwork?.networkId ?? 0;
      await scenario.ctx.runtime.metadata.setJsonValue(
        configKeyDogecoinHistoryReady(networkId),
        false,
      );

      const search = await request(scenario.ctx.app, '/v1/explorer/search?q=2', {
        headers: scenario.headers,
      });
      expect(search.status).toBe(425);
      expect(await search.json()).toEqual({ error: 'dogecoin history index is not ready' });

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

  it('returns a clean validation error when info is requested without q', async () => {
    const { ctx, headers } = await createAuthenticatedTestApp();

    const response = await request(ctx.app, '/v1/info/', {
      headers,
    });

    expect(response.status).toBe(400);
    expect(await response.json()).toEqual({
      error: 'missing input params',
    });

    await ctx.cleanup();
  });

  it('returns a connection error when rpc health checks fail during network creation', async () => {
    restoreFetch.mockRejectedValue(new Error('socket hang up'));
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {});
    const { ctx, headers } = await createAuthenticatedTestApp();

    const response = await request(ctx.app, '/v1/networks', {
      method: 'POST',
      headers,
      body: {
        name: 'Dogecoin Mainnet',
        architecture: 'dogecoin',
        chainId: 0,
        blockTime: 60,
        rpcEndpoint: 'https://doge.example/rpc',
      },
    });

    expect(response.status).toBe(500);
    expect(await response.json()).toEqual({
      error: 'could not connect to `https://doge.example/rpc`',
    });
    expect(consoleError).toHaveBeenCalledWith('[onlydoge] infrastructure error', {
      route: 'POST /v1/networks',
      code: 'UNKNOWN',
      message: 'could not connect to `https://doge.example/rpc`',
      cause: 'Error: socket hang up',
    });

    consoleError.mockRestore();
    await ctx.cleanup();
  });
});

type ExplorerScenario = Awaited<ReturnType<typeof createExplorerScenario>>;

async function createExplorerScenario() {
  const { actor, ctx, headers } = await createAuthenticatedTestApp();
  const network = await createDogecoinTestNetwork(ctx.runtime, actor);
  const highRiskTag = await ctx.runtime.entityLabeling.createTag(actor, {
    name: 'High Risk Source',
    riskLevel: 'high',
  });
  const { targetAddressRecord } = await createDogecoinAddressBook(ctx.runtime, actor, network.id, {
    sourceTags: [highRiskTag.id],
  });
  const internalNetwork = await ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');

  expect(targetAddressRecord?.address).toBe(dogecoinFixture.targetAddress);
  await runIndexerUntilProcessed(ctx, internalNetwork?.networkId ?? 0, 2);

  return { ctx, headers, network };
}

async function expectExplorerNetworks({ ctx, headers, network }: ExplorerScenario): Promise<void> {
  const deniedNetworks = await request(ctx.app, '/v1/explorer/networks');
  expect(deniedNetworks.status).toBe(401);

  const networks = await request(ctx.app, '/v1/explorer/networks', { headers });
  expect(networks.status).toBe(200);
  expect(networks.headers.get('cache-control')).toBe(
    'private, max-age=30, stale-while-revalidate=120',
  );
  expect(networks.headers.get('vary')).toBe('x-api-token');
  const networksBody = await readJsonObject(networks);
  const [networkSummary] = readObjectArrayField(networksBody, 'networks');
  expect(requireStringField(networkSummary ?? {}, 'id')).toBe(network.id);
  expect(requireNumberField(networkSummary ?? {}, 'syncTail')).toBe(2);
  expect(requireNumberField(networkSummary ?? {}, 'processTail')).toBe(2);
  expect(requireNumberField(networkSummary ?? {}, 'tipLagBlocks')).toBe(0);
}

async function expectExplorerSearch({ ctx, headers }: ExplorerScenario): Promise<void> {
  const searchByHeight = await request(ctx.app, '/v1/explorer/search?q=2', { headers });
  expect(searchByHeight.status).toBe(200);
  expect(searchByHeight.headers.get('cache-control')).toBe(
    'private, max-age=5, stale-while-revalidate=15',
  );
  expect(searchByHeight.headers.get('vary')).toBe('x-api-token');
  const heightMatch = readObjectArrayField(await readJsonObject(searchByHeight), 'matches')[0];
  expect(requireStringField(heightMatch ?? {}, 'type')).toBe('block');

  const searchByTx = await request(ctx.app, '/v1/explorer/search?q=doge-tx-2', { headers });
  expect(readObjectArrayField(await readJsonObject(searchByTx), 'matches')).toEqual([]);

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

  const blockDetail = await request(ctx.app, '/v1/explorer/blocks/2', { headers });
  const blockDetailBody = await readJsonObject(blockDetail);
  expect(requireNumberField(readObjectField(blockDetailBody, 'block'), 'height')).toBe(2);
  const [blockTx] = readObjectArrayField(blockDetailBody, 'transactions');
  expect(requireStringField(blockTx ?? {}, 'txid')).toBe('doge-tx-2');
}

async function expectExplorerMempool(
  { ctx, headers, network }: ExplorerScenario,
  fetchMock: ReturnType<typeof installRpcMock>,
): Promise<void> {
  const denied = await request(ctx.app, '/v1/explorer/mempool');
  expect(denied.status).toBe(401);

  const callsBefore = fetchMock.mock.calls.length;
  const response = await request(ctx.app, '/v1/explorer/mempool?limit=2', { headers });

  expect(response.status).toBe(200);
  expect(response.headers.get('cache-control')).toBe('no-store');
  expect(response.headers.get('vary')).toBe('x-api-token');
  const body = await readJsonObject(response);
  expect(requireStringField(body, 'network')).toBe(network.id);
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

  const maxLimitResponse = await request(ctx.app, '/v1/explorer/mempool?limit=999', { headers });
  expect(requireNumberField(await readJsonObject(maxLimitResponse), 'limit')).toBe(500);

  const defaultLimitResponse = await request(ctx.app, '/v1/explorer/mempool', { headers });
  expect(requireNumberField(await readJsonObject(defaultLimitResponse), 'limit')).toBe(100);

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
  expect(transaction.status).toBe(404);
  expect(await transaction.json()).toEqual({ error: 'transaction not found' });
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
  const overlay = readObjectField(addressBody, 'overlay');
  const risk = readObjectField(overlay, 'risk');
  expect(readStringArrayField(risk, 'reasons')).not.toContain('source');
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
  expect(readObjectArrayField(await readJsonObject(history), 'transactions')).toEqual([]);

  const utxos = await request(
    ctx.app,
    `/v1/explorer/addresses/${dogecoinFixture.targetAddress}/utxos`,
    { headers },
  );
  const [utxo] = readObjectArrayField(await readJsonObject(utxos), 'utxos');
  expect(requireStringField(utxo ?? {}, 'outputKey')).toBe('doge-tx-2:0');
}

async function expectExplorerInfoAuth({ ctx }: ExplorerScenario): Promise<void> {
  const deniedInfo = await request(ctx.app, `/v1/info?q=${dogecoinFixture.targetAddress}`);
  expect(deniedInfo.status).toBe(401);
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
