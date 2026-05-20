import { randomUUID } from 'node:crypto';
import { describe, expect, it } from 'vitest';

import {
  assertRecord,
  assertRecordArray,
  eventually,
  ignoreNotFound,
  loadProductionE2EConfig,
  ProductionApiClient,
  type ProductionE2EConfig,
  type ProductionResponse,
  productionQueryPath,
  readNumberField,
  readOptionalNumberField,
  readOptionalStringField,
  readRecordArrayField,
  readRecordField,
  readStringArrayField,
  readStringField,
  TeardownStack,
  verifyProductionImageDigest,
} from './production-client';

type EnabledProductionConfig = Extract<ProductionE2EConfig, { enabled: true }>;

const config = loadProductionE2EConfig();

if (!config.enabled) {
  describe.skip('production e2e', () => {
    it('is opt-in via ONLYDOGE_RUN_PRODUCTION_E2E=1', () => {});
  });
} else {
  describe('production e2e', () => {
    it('validates production API behavior and tears down disposable artifacts', async () => {
      await runProductionE2E(config);
    }, 180_000);
  });
}

async function runProductionE2E(config: EnabledProductionConfig): Promise<void> {
  const client = new ProductionApiClient(config);
  const teardown = new TeardownStack();
  const created: Partial<CreatedMetadata> = {};
  let ephemeralToken: string | undefined;

  try {
    await expectPublicHealth(client);
    expectProductionDigest(config);
    await expectProtectedAuth(client);

    const ephemeralKey = await createEphemeralKey(client, config.adminToken);
    ephemeralToken = ephemeralKey.token;
    teardown.add('delete ephemeral API key', async () => {
      await client.delete('/v1/keys/', {
        body: { keys: [ephemeralKey.id] },
        expectedStatus: 204,
        token: config.adminToken,
      });
    });

    await expectEphemeralKeyWorks(client, ephemeralToken);
    const network = await expectProductionState(client, ephemeralToken);

    Object.assign(
      created,
      await createDisposableMetadata(client, teardown, ephemeralToken, network),
    );
    await expectMetadataReads(client, ephemeralToken, created as CreatedMetadata);
    await expectCurrentStateExplorerReads(
      client,
      ephemeralToken,
      network,
      created as CreatedMetadata,
    );
    await expectHistoryEndpointContract(
      client,
      ephemeralToken,
      network,
      created as CreatedMetadata,
    );
  } finally {
    await teardown.run();
  }

  await expectTeardownComplete(client, config.adminToken, created, ephemeralToken);
}

type ExplorerNetwork = {
  blockHeight: number;
  id: string;
  name: string;
};

type CreatedMetadata = {
  address: string;
  addressId: string;
  entityId: string;
  tagId: string;
};

async function expectPublicHealth(client: ProductionApiClient): Promise<void> {
  await eventually(
    'production public health',
    async () => {
      const up = await client.get('/up');
      expect(up.text.trim()).toBe('ok');

      await client.get('/v1/heartbeat/', { expectedStatus: 204 });
      assertRecord((await client.get('/openapi/json')).body, 'openapi');
    },
    { timeoutMs: 60_000 },
  );
}

function expectProductionDigest(config: EnabledProductionConfig): void {
  const result = verifyProductionImageDigest(config);
  if (result.checked) {
    expect(result.containers).toEqual(
      expect.arrayContaining(['/onlydoge-onlydoge-api-1', '/onlydoge-onlydoge-indexer-1']),
    );
  }
}

async function expectProtectedAuth(client: ProductionApiClient): Promise<void> {
  await client.get('/v1/stats/', { expectedStatus: 401 });
  await client.get('/v1/explorer/networks', { expectedStatus: 401 });
}

async function createEphemeralKey(
  client: ProductionApiClient,
  adminToken: string,
): Promise<{ id: string; token: string }> {
  const created = assertRecord(
    (
      await client.post('/v1/keys/', {
        body: {},
        token: adminToken,
      })
    ).body,
    'created key',
  );
  const keyId = readStringField(created, 'id');
  const token = readStringField(created, 'key');
  expect(token).toMatch(/^sk_[A-Za-z0-9]+$/u);

  const fetched = assertRecord(
    (await client.get(`/v1/keys/${encodeURIComponent(keyId)}`, { token: adminToken })).body,
    'fetched key response',
  );
  expect(readRecordField(fetched, 'key').key).toBeUndefined();

  return { id: keyId, token };
}

async function expectEphemeralKeyWorks(
  client: ProductionApiClient,
  ephemeralToken: string,
): Promise<void> {
  await client.get('/v1/stats/', { token: ephemeralToken });
  await client.get('/v1/explorer/networks', { token: ephemeralToken });
}

async function expectProductionState(
  client: ProductionApiClient,
  token: string,
): Promise<ExplorerNetwork> {
  const stats = assertRecord((await client.get('/v1/stats/', { token })).body, 'stats');
  const statsNetwork = readRecordArrayField(stats, 'networks').find((candidate) =>
    readStringField(candidate, 'name').toLowerCase().includes('dogecoin'),
  );
  if (!statsNetwork) {
    throw new Error('Dogecoin network missing from /v1/stats/');
  }

  expect(readOptionalStringField(statsNetwork, 'stage')).toBe('online');
  expect(statsNetwork.lastError ?? null).toBeNull();
  const blockHeight = readNumberField(statsNetwork, 'blockHeight');
  const processTail = readNumberField(statsNetwork, 'processTail');
  const onlineTip = readOptionalNumberField(statsNetwork, 'onlineTip') ?? blockHeight;
  expect(onlineTip - processTail).toBeLessThanOrEqual(12);

  const networks = readRecordArrayField(
    assertRecord((await client.get('/v1/explorer/networks', { token })).body, 'explorer networks'),
    'networks',
  );
  const network = networks.find((candidate) =>
    readStringField(candidate, 'name').toLowerCase().includes('dogecoin'),
  );
  if (!network) {
    throw new Error('Dogecoin network missing from /v1/explorer/networks');
  }

  expect(readNumberField(network, 'blockHeight')).toBeGreaterThan(0);
  return {
    blockHeight: readNumberField(network, 'blockHeight'),
    id: readStringField(network, 'id'),
    name: readStringField(network, 'name'),
  };
}

async function createDisposableMetadata(
  client: ProductionApiClient,
  teardown: TeardownStack,
  token: string,
  network: ExplorerNetwork,
): Promise<CreatedMetadata> {
  const runId = randomRunId();
  const address = `DOnlyDogeE2E${runId}`;
  const tag = assertRecord(
    (
      await client.post('/v1/tags/', {
        body: {
          name: `Production E2E ${runId}`,
          riskLevel: 'low',
        },
        token,
      })
    ).body,
    'created tag',
  );
  const tagId = readStringField(tag, 'id');
  teardown.add('delete E2E tag', () =>
    ignoreNotFound(async () => {
      await client.delete('/v1/tags/', {
        body: { tags: [tagId] },
        expectedStatus: 204,
        token,
      });
    }),
  );

  const entityPayload = assertRecord(
    (
      await client.post('/v1/entities/', {
        body: {
          data: {
            purpose: 'production-e2e',
            runId,
          },
          description: `Production E2E ${runId}`,
          name: `Production E2E ${runId}`,
          tags: [tagId],
        },
        token,
      })
    ).body,
    'created entity payload',
  );
  const entityId = readStringField(readRecordField(entityPayload, 'entity'), 'id');
  teardown.add('delete E2E entity', () =>
    ignoreNotFound(async () => {
      await client.delete('/v1/entities/', {
        body: { entities: [entityId] },
        expectedStatus: 204,
        token,
      });
    }),
  );

  const addresses = assertRecordArray(
    (
      await client.post('/v1/addresses/', {
        body: {
          addresses: [
            {
              address,
              data: {
                purpose: 'production-e2e',
                runId,
              },
              description: `Production E2E ${runId}`,
            },
          ],
          entity: entityId,
          network: network.id,
        },
        token,
      })
    ).body,
    'created addresses',
  );
  const addressId = readStringField(addresses[0] ?? {}, 'id');
  teardown.add('delete E2E address', () =>
    ignoreNotFound(async () => {
      await client.delete('/v1/addresses/', {
        body: { addresses: [addressId] },
        expectedStatus: 204,
        token,
      });
    }),
  );

  return {
    address,
    addressId,
    entityId,
    tagId,
  };
}

async function expectMetadataReads(
  client: ProductionApiClient,
  token: string,
  created: CreatedMetadata,
): Promise<void> {
  const tagPayload = assertRecord(
    (await client.get(`/v1/tags/${encodeURIComponent(created.tagId)}`, { token })).body,
    'tag payload',
  );
  const tag = readRecordField(tagPayload, 'tag');
  expect(readStringField(tag, 'id')).toBe(created.tagId);
  expect(readStringField(tag, 'riskLevel')).toBe('low');

  const entityPayload = assertRecord(
    (await client.get(`/v1/entities/${encodeURIComponent(created.entityId)}`, { token })).body,
    'entity payload',
  );
  const entity = readRecordField(entityPayload, 'entity');
  expect(readStringField(entity, 'id')).toBe(created.entityId);
  expect(readStringArrayField(entity, 'tags')).toContain(created.tagId);
  expect(readStringArrayField(entity, 'addresses')).toContain(created.addressId);

  const addressPayload = assertRecord(
    (await client.get(`/v1/addresses/${encodeURIComponent(created.addressId)}`, { token })).body,
    'address payload',
  );
  const address = readRecordField(addressPayload, 'address');
  expect(readStringField(address, 'id')).toBe(created.addressId);
  expect(readStringField(address, 'address')).toBe(created.address);
}

async function expectCurrentStateExplorerReads(
  client: ProductionApiClient,
  token: string,
  network: ExplorerNetwork,
  created: CreatedMetadata,
): Promise<void> {
  const addressPath = productionQueryPath(`/v1/explorer/addresses/${created.address}`, {
    network: network.id,
  });
  const addressPayload = assertRecord(
    (await client.get(addressPath, { token })).body,
    'explorer address payload',
  );
  const address = readRecordField(addressPayload, 'address');
  expect(readStringField(address, 'network')).toBe(network.id);
  expect(readStringField(address, 'address')).toBe(created.address);
  expect(readStringField(address, 'balance')).toBe('0');

  const overlay = readRecordField(addressPayload, 'overlay');
  expect(readStringArrayField(overlay, 'addresses')).toContain(created.address);
  const [entity] = readRecordArrayField(overlay, 'entities');
  expect(readStringField(entity ?? {}, 'id')).toBe(created.entityId);
  const [tag] = readRecordArrayField(overlay, 'tags');
  expect(readStringField(tag ?? {}, 'id')).toBe(created.tagId);

  const utxoPath = productionQueryPath(`/v1/explorer/addresses/${created.address}/utxos`, {
    limit: 5,
    network: network.id,
  });
  const utxoPayload = assertRecord((await client.get(utxoPath, { token })).body, 'utxo payload');
  expect(readRecordArrayField(utxoPayload, 'utxos')).toEqual([]);

  const infoPayload = assertRecord(
    (
      await client.get(productionQueryPath('/v1/info', { q: created.address }), {
        token,
      })
    ).body,
    'info payload',
  );
  expect(readStringArrayField(infoPayload, 'addresses')).toContain(created.address);
}

async function expectHistoryEndpointContract(
  client: ProductionApiClient,
  token: string,
  network: ExplorerNetwork,
  created: CreatedMetadata,
): Promise<void> {
  const blocksPayload = assertRecord(
    (
      await client.get(
        productionQueryPath('/v1/explorer/blocks', { limit: 1, network: network.id }),
        {
          token,
        },
      )
    ).body,
    'blocks payload',
  );
  const [block] = readRecordArrayField(blocksPayload, 'blocks');
  expect(block).toBeDefined();

  const search = await client.get(
    productionQueryPath('/v1/explorer/search', { network: network.id, q: created.address }),
    {
      expectedStatus: [200, 425],
      token,
    },
  );
  if (search.status === 425) {
    expectTooEarly(search);
  } else {
    const matches = readRecordArrayField(assertRecord(search.body, 'search payload'), 'matches');
    expect(matches.some((match) => match.address === created.address)).toBe(true);
  }

  const blockHeight = readNumberField(block ?? {}, 'height');
  const blockDetail = await client.get(
    productionQueryPath(`/v1/explorer/blocks/${blockHeight}`, { network: network.id }),
    {
      expectedStatus: [200, 425],
      token,
    },
  );
  if (blockDetail.status === 425) {
    expectTooEarly(blockDetail);
    return;
  }

  const blockDetailPayload = assertRecord(blockDetail.body, 'block detail payload');
  expect(readNumberField(readRecordField(blockDetailPayload, 'block'), 'height')).toBe(blockHeight);
  readRecordArrayField(blockDetailPayload, 'transactions');
}

async function expectTeardownComplete(
  client: ProductionApiClient,
  adminToken: string,
  created: Partial<CreatedMetadata>,
  ephemeralToken: string | undefined,
): Promise<void> {
  if (created.addressId) {
    await client.get(`/v1/addresses/${encodeURIComponent(created.addressId)}`, {
      expectedStatus: 404,
      token: adminToken,
    });
  }
  if (created.entityId) {
    await client.get(`/v1/entities/${encodeURIComponent(created.entityId)}`, {
      expectedStatus: 404,
      token: adminToken,
    });
  }
  if (created.tagId) {
    await client.get(`/v1/tags/${encodeURIComponent(created.tagId)}`, {
      expectedStatus: 404,
      token: adminToken,
    });
  }
  if (ephemeralToken) {
    await client.get('/v1/stats/', {
      expectedStatus: 401,
      token: ephemeralToken,
    });
  }
}

function expectTooEarly(response: ProductionResponse): void {
  expect(readStringField(assertRecord(response.body, 'too early payload'), 'error')).toBe(
    'dogecoin history index is not ready',
  );
}

function randomRunId(): string {
  return `${Date.now().toString(36)}${randomUUID().replaceAll('-', '').slice(0, 12)}`;
}
