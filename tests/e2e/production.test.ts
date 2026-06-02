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
  readRecordArrayField,
  readRecordField,
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
    it('validates production Dogecoin explorer behavior', async () => {
      await runProductionE2E(config);
    }, 180_000);
  });
}

async function runProductionE2E(config: EnabledProductionConfig): Promise<void> {
  const client = new ProductionApiClient(config);
  const teardown = new TeardownStack();
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
    await expectRemovedRoutes(client, ephemeralToken);
    await expectExplorerReads(client, ephemeralToken);
    await expectExternalParity(client, config, ephemeralToken);
  } finally {
    await teardown.run();
  }

  if (ephemeralToken) {
    await client.get('/v1/explorer/blocks', {
      expectedStatus: 401,
      token: ephemeralToken,
    });
  }
}

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
  await client.get('/v1/explorer/blocks', { expectedStatus: 401 });
  await client.get('/v1/analytics/schema', { expectedStatus: 401 });
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
  await client.get('/v1/explorer/blocks', { token: ephemeralToken });
}

async function expectRemovedRoutes(client: ProductionApiClient, token: string): Promise<void> {
  for (const path of [
    '/v1/networks',
    '/v1/tokens',
    '/v1/entities',
    '/v1/addresses',
    '/v1/tags',
    '/v1/info?q=DRemovedRoute',
    '/v1/stats/',
    '/v1/explorer/networks',
  ]) {
    await client.get(path, { expectedStatus: 404, token });
  }
}

async function expectExplorerReads(client: ProductionApiClient, token: string): Promise<void> {
  const blocksPayload = assertRecord(
    (
      await client.get(productionQueryPath('/v1/explorer/blocks', { limit: 1 }), {
        token,
      })
    ).body,
    'blocks payload',
  );
  const [block] = readRecordArrayField(blocksPayload, 'blocks');
  expect(block).toBeDefined();
  const blockHeight = readNumberField(block ?? {}, 'height');

  const blockDetail = await client.get(`/v1/explorer/blocks/${blockHeight}`, {
    expectedStatus: [200, 425],
    token,
  });
  if (blockDetail.status === 425) {
    expectTooEarly(blockDetail);
  } else {
    const blockDetailPayload = assertRecord(blockDetail.body, 'block detail payload');
    expect(readNumberField(readRecordField(blockDetailPayload, 'block'), 'height')).toBe(
      blockHeight,
    );
  }

  const randomAddress = `DOnlyDogeE2E${randomUUID().replaceAll('-', '').slice(0, 16)}`;
  await client.get(`/v1/explorer/addresses/${randomAddress}`, { token });
  await ignoreNotFound(async () => {
    await client.get(productionQueryPath('/v1/explorer/search', { q: randomAddress }), {
      expectedStatus: [200, 425],
      token,
    });
  });
}

async function expectExternalParity(
  client: ProductionApiClient,
  config: EnabledProductionConfig,
  token: string,
): Promise<void> {
  await eventually(
    'Dogecoin external parity',
    async () => {
      await expectLatestBlockParity(client, config, token);
      await expectKnownAddressParity(client, token);
    },
    { timeoutMs: 90_000 },
  );
}

async function expectLatestBlockParity(
  client: ProductionApiClient,
  config: EnabledProductionConfig,
  token: string,
): Promise<void> {
  const [onlyDogeLatest, blockCypher] = await Promise.all([
    latestOnlyDogeBlock(client, token),
    blockCypherGet('/v1/doge/main'),
  ]);
  const chainHeight = readNumberField(blockCypher, 'height');
  const chainHash = readStringField(blockCypher, 'hash');

  expect(onlyDogeLatest.height).toBeGreaterThanOrEqual(chainHeight - config.maxParityBlockLag);
  if (onlyDogeLatest.height === chainHeight) {
    expect(onlyDogeLatest.hash).toBe(chainHash);
  }
}

async function latestOnlyDogeBlock(
  client: ProductionApiClient,
  token: string,
): Promise<{ hash: string; height: number }> {
  const blocksPayload = assertRecord(
    (
      await client.get(productionQueryPath('/v1/explorer/blocks', { limit: 1 }), {
        token,
      })
    ).body,
    'blocks payload',
  );
  const [block] = readRecordArrayField(blocksPayload, 'blocks');
  if (!block) {
    throw new Error('OnlyDoge returned no latest block');
  }

  return {
    hash: readStringField(block, 'hash'),
    height: readNumberField(block, 'height'),
  };
}

async function expectKnownAddressParity(client: ProductionApiClient, token: string): Promise<void> {
  const address = 'D8AXXiGEZeZnMKTKnC9AWB3YUU4jfMAmYU';
  const [onlyDogeAddress, onlyDogeUtxos, blockCypherBalance, blockCypherUtxos] = await Promise.all([
    onlyDogeAddressSummary(client, token, address),
    onlyDogeAddressUtxos(client, token, address),
    blockCypherGet(`/v1/doge/main/addrs/${address}/balance`),
    blockCypherGet(`/v1/doge/main/addrs/${address}?unspentOnly=true&limit=50`),
  ]);

  expect(onlyDogeAddress.balance).toBe(
    String(readNumberField(blockCypherBalance, 'final_balance')),
  );
  expect(onlyDogeAddress.receivedBase).toBe(
    String(readNumberField(blockCypherBalance, 'total_received')),
  );
  expect(onlyDogeAddress.sentBase).toBe(String(readNumberField(blockCypherBalance, 'total_sent')));
  expect(onlyDogeAddress.txCount).toBe(readNumberField(blockCypherBalance, 'final_n_tx'));

  const blockCypherOutputKeys = assertRecordArray(
    blockCypherUtxos.txrefs ?? [],
    'BlockCypher txrefs',
  )
    .map((output) => ({
      outputKey: `${readStringField(output, 'tx_hash')}:${readNumberField(output, 'tx_output_n')}`,
      valueBase: String(readNumberField(output, 'value')),
    }))
    .sort(compareOutputRefs);
  const onlyDogeOutputKeys = onlyDogeUtxos
    .map((output) => ({
      outputKey: readStringField(output, 'outputKey'),
      valueBase: readStringField(output, 'valueBase'),
    }))
    .sort(compareOutputRefs);

  expect(onlyDogeAddress.utxoCount).toBeGreaterThanOrEqual(blockCypherOutputKeys.length);
  expect(onlyDogeOutputKeys).toEqual(expect.arrayContaining(blockCypherOutputKeys));
}

async function onlyDogeAddressSummary(
  client: ProductionApiClient,
  token: string,
  address: string,
): Promise<{
  balance: string;
  receivedBase: string;
  sentBase: string;
  txCount: number;
  utxoCount: number;
}> {
  const payload = assertRecord(
    (await client.get(`/v1/explorer/addresses/${address}`, { token })).body,
    'OnlyDoge address payload',
  );
  const summary = readRecordField(payload, 'address');
  return {
    balance: readStringField(summary, 'balance'),
    receivedBase: readStringField(summary, 'receivedBase'),
    sentBase: readStringField(summary, 'sentBase'),
    txCount: readNumberField(summary, 'txCount'),
    utxoCount: readNumberField(summary, 'utxoCount'),
  };
}

async function onlyDogeAddressUtxos(
  client: ProductionApiClient,
  token: string,
  address: string,
): Promise<Array<Record<string, unknown>>> {
  const payload = assertRecord(
    (
      await client.get(
        productionQueryPath(`/v1/explorer/addresses/${address}/utxos`, { limit: 500 }),
        { token },
      )
    ).body,
    'OnlyDoge UTXO payload',
  );
  return readRecordArrayField(payload, 'utxos');
}

async function blockCypherGet(path: string): Promise<Record<string, unknown>> {
  const url = new URL(path, 'https://api.blockcypher.com');
  const response = await fetch(url, {
    headers: {
      accept: 'application/json',
    },
    signal: AbortSignal.timeout(15_000),
  });
  const body = assertRecord(await response.json(), `BlockCypher ${path}`);
  if (response.status !== 200) {
    throw new Error(`BlockCypher ${path} returned ${response.status}: ${JSON.stringify(body)}`);
  }

  return body;
}

function compareOutputRefs(
  left: { outputKey: string; valueBase: string },
  right: { outputKey: string; valueBase: string },
): number {
  return (
    left.outputKey.localeCompare(right.outputKey) || left.valueBase.localeCompare(right.valueBase)
  );
}

function expectTooEarly(response: ProductionResponse): void {
  expect(readStringField(assertRecord(response.body, 'too early payload'), 'error')).toBe(
    'dogecoin history index is not ready',
  );
}
