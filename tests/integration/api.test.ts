import { configKeyDogecoinHistoryReady } from '@onlydoge/indexing-pipeline';
import { HDKey } from '@scure/bip32';
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
    expect(await response.json()).toEqual({
      error: 'not found',
    });
    expect(consoleError).toHaveBeenCalledWith('[onlydoge] not found route=GET /missing-route');

    consoleError.mockRestore();
    await ctx.cleanup();
  });

  it('enforces auth after the first key is created and hides the secret after use', async () => {
    const ctx = await createTestApp();

    const deniedBeforeBootstrap = await request(ctx.app, '/v1/networks');
    expect(deniedBeforeBootstrap.status).toBe(401);

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

      const hdWalletBalance = await request(scenario.ctx.app, '/v1/explorer/hd-wallet/balance', {
        method: 'POST',
        headers: scenario.headers,
        body: {
          xpub: testAccountXpub(),
        },
      });
      expect(hdWalletBalance.status).toBe(425);

      const utxos = await request(
        scenario.ctx.app,
        `/v1/explorer/addresses/${dogecoinFixture.targetAddress}/utxos`,
        { headers: scenario.headers },
      );
      expect(utxos.status).toBe(200);
      const [utxo] = readObjectArrayField(await readJsonObject(utxos), 'utxos');
      expect(requireStringField(utxo ?? {}, 'outputKey')).toBe('doge-tx-2:0');
      expect(requireStringField(utxo ?? {}, 'scriptPubKey')).toBe(
        '76a914333333333333333333333333333333333333333388ac',
      );
      expect(requireNumberField(utxo ?? {}, 'confirmations')).toBe(1);
    } finally {
      await scenario.ctx.cleanup();
    }
  });

  it('serves authenticated HD wallet balance scans with one-minute private cache headers', async () => {
    const { ctx, headers } = await createAuthenticatedTestApp();
    const network = await createDogecoinTestNetwork(ctx.runtime);
    const xpub = testAccountXpub();

    try {
      const denied = await request(ctx.app, '/v1/explorer/hd-wallet/balance', {
        method: 'POST',
        body: {
          xpub,
          network: network.id,
        },
      });
      expect(denied.status).toBe(401);

      const first = await request(ctx.app, '/v1/explorer/hd-wallet/balance', {
        method: 'POST',
        headers,
        body: {
          xpub,
          network: network.id,
        },
      });
      expect(first.status).toBe(200);
      expect(first.headers.get('cache-control')).toBe('private, max-age=60');
      expect(first.headers.get('vary')).toBe('x-api-token');
      const firstBody = await readJsonObject(first);
      expect(requireStringField(firstBody, 'balanceBase')).toBe('0');
      expect(requireNumberField(firstBody, 'scannedAddressCount')).toBe(40);
      expect(requireObject(firstBody.cache, 'cache').hit).toBe(false);

      const second = await request(ctx.app, '/v1/explorer/hd-wallet/balance', {
        method: 'POST',
        headers,
        body: {
          xpub,
          network: network.id,
        },
      });
      const secondBody = await readJsonObject(second);
      expect(requireObject(secondBody.cache, 'cache').hit).toBe(true);
    } finally {
      await ctx.cleanup();
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

function testAccountXpub(): string {
  return HDKey.fromMasterSeed(Uint8Array.from({ length: 32 }, (_, index) => index + 1)).derive(
    "m/44'/3'/0'",
  ).publicExtendedKey;
}

async function createExplorerScenario() {
  const { ctx, headers } = await createAuthenticatedTestApp();
  const network = await createDogecoinTestNetwork(ctx.runtime);
  const highRiskTag = await ctx.runtime.entityLabeling.createTag({
    name: 'High Risk Source',
    riskLevel: 'high',
  });
  const { targetAddressRecord } = await createDogecoinAddressBook(ctx.runtime, network.id, {
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
  expect(readObjectArrayField(await readJsonObject(blocks), 'blocks')).toEqual([]);

  const blockDetail = await request(ctx.app, '/v1/explorer/blocks/2', { headers });
  const blockDetailBody = await readJsonObject(blockDetail);
  expect(requireNumberField(readObjectField(blockDetailBody, 'block'), 'height')).toBe(2);
  const [blockTx] = readObjectArrayField(blockDetailBody, 'transactions');
  expect(requireStringField(blockTx ?? {}, 'txid')).toBe('doge-tx-2');
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
  expect(requireStringField(utxo ?? {}, 'scriptPubKey')).toBe(
    '76a914333333333333333333333333333333333333333388ac',
  );
  expect(requireNumberField(utxo ?? {}, 'confirmations')).toBe(1);
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
