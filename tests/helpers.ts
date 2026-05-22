import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { buildApiApp } from '@onlydoge/api';
import { configKeyIndexerProcessTail } from '@onlydoge/indexing-pipeline';
import { createRuntime } from '@onlydoge/platform';
import { vi } from 'vitest';

import { dogecoinBlocksByHash, dogecoinFixture, dogecoinHashesByHeight } from './fixtures/dogecoin';

type EnvKey = 'ONLYDOGE_DATABASE' | 'ONLYDOGE_STORAGE' | 'ONLYDOGE_WAREHOUSE' | 'ONLYDOGE_MODE';
type RpcRequestBody = { method?: string; params?: unknown[] };
type RpcMockHandler = (body: RpcRequestBody) => Response;

const ENV_KEYS: readonly EnvKey[] = [
  'ONLYDOGE_DATABASE',
  'ONLYDOGE_STORAGE',
  'ONLYDOGE_WAREHOUSE',
  'ONLYDOGE_MODE',
];

export async function createTestApp(mode: 'both' | 'http' | 'indexer' = 'both') {
  const tempRoot = await mkdtemp(join(tmpdir(), 'onlydoge-'));
  const previousEnv = new Map<string, string | undefined>();

  for (const key of ENV_KEYS) {
    previousEnv.set(key, process.env[key]);
  }

  process.env.ONLYDOGE_DATABASE = `sqlite://${tempRoot}/onlydoge.sqlite.db`;
  process.env.ONLYDOGE_STORAGE = `file://${tempRoot}/storage`;
  process.env.ONLYDOGE_WAREHOUSE = `${tempRoot}/warehouse.json`;
  process.env.ONLYDOGE_MODE = mode;

  const runtime = await createRuntime({ mode, ip: '127.0.0.1', port: 2277 });
  const app = buildApiApp(runtime);

  return {
    app,
    runtime,
    tempRoot,
    async cleanup() {
      for (const [key, value] of previousEnv.entries()) {
        if (value === undefined) {
          delete process.env[key];
        } else {
          process.env[key] = value;
        }
      }

      await rm(tempRoot, { recursive: true, force: true });
    },
  };
}

type TestRuntime = Awaited<ReturnType<typeof createRuntime>>;
type TestActor = NonNullable<Awaited<ReturnType<TestRuntime['accessControl']['authenticate']>>>;

export async function runIndexerUntilProcessed(
  ctx: { runtime: TestRuntime },
  networkId: number,
  targetTail: number,
): Promise<void> {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    await ctx.runtime.indexingPipeline.runOnce();
    const processTail =
      (await ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerProcessTail(networkId))) ??
      -1;
    if (processTail >= targetTail) {
      return;
    }
  }

  throw new Error(`core process tail did not reach ${targetTail}`);
}

export async function createAuthenticatedTestApp(mode: 'both' | 'http' | 'indexer' = 'both') {
  const ctx = await createTestApp(mode);
  const created = await request(ctx.app, '/v1/keys/', {
    method: 'POST',
    body: {},
  });
  const payload = await created.json();
  const apiToken = readStringField(payload, 'key');
  const actor = await ctx.runtime.accessControl.authenticate(apiToken);
  if (!actor) {
    throw new TypeError('expected authenticated API key');
  }
  return {
    ctx,
    actor,
    apiToken,
    headers: {
      'x-api-token': apiToken,
    },
  };
}

export async function createTestAdminActor(runtime: TestRuntime): Promise<TestActor> {
  const created = await runtime.accessControl.createKey({});
  const apiToken = readStringField(created, 'key');
  const actor = await runtime.accessControl.authenticate(apiToken);
  if (!actor) {
    throw new TypeError('expected authenticated API key');
  }
  return actor;
}

export async function createDogecoinTestNetwork(runtime: TestRuntime, actor?: TestActor) {
  const admin = actor ?? (await createTestAdminActor(runtime));
  return runtime.networkCatalog.createNetwork(admin, {
    name: 'Dogecoin Mainnet',
    architecture: 'dogecoin',
    chainId: 0,
    blockTime: 60,
    rpcEndpoint: 'https://doge.example/rpc',
  });
}

export async function createDogecoinAddressBook(
  runtime: TestRuntime,
  actor: TestActor,
  networkId: string,
  options: { sourceTags?: string[] } = {},
) {
  const sourceEntity = await runtime.entityLabeling.createEntity(actor, {
    name: 'Source Entity',
    description: 'Known source',
    ...(options.sourceTags ? { tags: options.sourceTags } : {}),
  });
  const targetEntity = await runtime.entityLabeling.createEntity(actor, {
    name: 'Target Entity',
    description: 'Known target',
  });

  await runtime.entityLabeling.createAddresses(actor, {
    entity: sourceEntity.entity.id,
    network: networkId,
    addresses: [
      {
        address: dogecoinFixture.sourceAddress,
        description: 'Source wallet',
      },
    ],
  });
  const [targetAddressRecord] = await runtime.entityLabeling.createAddresses(actor, {
    entity: targetEntity.entity.id,
    network: networkId,
    addresses: [
      {
        address: dogecoinFixture.targetAddress,
        description: 'Target wallet',
      },
    ],
  });

  return {
    sourceEntity,
    targetAddressRecord,
    targetEntity,
  };
}

export function readObjectArray(
  record: Record<string, unknown>,
  field: string,
): Array<Record<string, unknown>> {
  const value = record[field];
  if (!Array.isArray(value)) {
    throw new TypeError(`expected array for ${field}`);
  }

  return value.map((item, index) => requireObject(item, `${field}[${index}]`));
}

export function requireObject(value: unknown, field: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`expected object for ${field}`);
  }

  return Object.fromEntries(Object.entries(value));
}

export function requireString(record: Record<string, unknown>, field: string): string {
  const key = field.split('.').at(-1) ?? field;
  const value = record[key];
  if (typeof value !== 'string') {
    throw new TypeError(`expected string for ${field}`);
  }

  return value;
}

export function requireNumber(record: Record<string, unknown>, field: string): number {
  const key = field.split('.').at(-1) ?? field;
  const value = record[key];
  if (typeof value !== 'number') {
    throw new TypeError(`expected number for ${field}`);
  }

  return value;
}

export async function request(
  app: ReturnType<typeof buildApiApp>,
  path: string,
  init?: {
    body?: unknown;
    headers?: Record<string, string>;
    method?: string;
  },
) {
  const response = await app.handle(
    new Request(`http://localhost${path}`, {
      method: init?.method ?? 'GET',
      headers: {
        ...(init?.body ? { 'content-type': 'application/json' } : {}),
        ...(init?.headers ?? {}),
      },
      ...(init?.body ? { body: JSON.stringify(init.body) } : {}),
    }),
  );

  return response;
}

export function installRpcMock() {
  return vi
    .spyOn(globalThis, 'fetch')
    .mockImplementation(async (_input: RequestInfo | URL, init?: RequestInit) => {
      const body = parseRpcRequestBody(String(init?.body ?? '{}'));
      return (rpcMockHandlers[body.method ?? ''] ?? defaultRpcMockHandler)(body);
    });
}

const rpcMockHandlers: Record<string, RpcMockHandler> = {
  getblockcount: () => Response.json({ result: dogecoinFixture.latestBlockHeight, error: null }),
  getblockhash: (body) =>
    Response.json({
      result: dogecoinHashesByHeight.get(Number(body.params?.[0] ?? -1)) ?? null,
      error: null,
    }),
  getblock: (body) =>
    Response.json({
      result: dogecoinBlocksByHash.get(String(body.params?.[0] ?? '')) ?? null,
      error: null,
    }),
  getmempoolinfo: () => Response.json({ result: dogecoinFixture.mempoolInfo, error: null }),
  getrawmempool: () => Response.json({ result: dogecoinFixture.mempoolEntries, error: null }),
};

function defaultRpcMockHandler(): Response {
  return Response.json({ result: 1, error: null });
}

function parseRpcRequestBody(value: string): RpcRequestBody {
  const parsed = JSON.parse(value);
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return {};
  }

  const method = typeof parsed.method === 'string' ? parsed.method : undefined;
  const params = Array.isArray(parsed.params) ? [...parsed.params] : undefined;

  return {
    ...(method ? { method } : {}),
    ...(params ? { params } : {}),
  };
}

function readStringField(record: unknown, field: string): string {
  if (!record || typeof record !== 'object' || Array.isArray(record)) {
    throw new TypeError('expected object response');
  }

  const value = Reflect.get(record, field);
  if (typeof value !== 'string') {
    throw new TypeError(`expected string for ${field}`);
  }

  return value;
}
