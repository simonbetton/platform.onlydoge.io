import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { buildApiApp } from '@onlydoge/api';
import { configKeyIndexerProcessTail } from '@onlydoge/indexing-pipeline';
import { createRuntime } from '@onlydoge/platform';
import { vi } from 'vitest';

import {
  dogecoinBlocksByHash,
  dogecoinFixture,
  dogecoinHashesByHeight,
  dogecoinRawBlocksByHash,
} from './fixtures/dogecoin';

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
  targetTail: number,
): Promise<void> {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    await ctx.runtime.indexingPipeline.runOnce();
    const processTail =
      (await ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerProcessTail())) ?? -1;
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

export async function prepareDogecoinTestConfig(runtime: TestRuntime, actor?: TestActor) {
  void runtime;
  void actor;
  return {
    id: 'dogecoin',
    name: 'Dogecoin',
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
      const parsed = JSON.parse(String(init?.body ?? '{}'));
      if (Array.isArray(parsed)) {
        return Response.json(
          await Promise.all(
            parsed.map(async (entry, index) => ({
              ...(await handleRpcMock(entry).json()),
              id: entry?.id ?? index,
            })),
          ),
        );
      }
      return handleRpcMock(parsed);
    });
}

function handleRpcMock(parsed: unknown): Response {
  const body = parseRpcRequestBody(parsed);
  return (rpcMockHandlers[body.method ?? ''] ?? defaultRpcMockHandler)(body);
}

const rpcMockHandlers: Record<string, RpcMockHandler> = {
  getblockcount: () => Response.json({ result: dogecoinFixture.latestBlockHeight, error: null }),
  getblockhash: (body) =>
    Response.json({
      result: dogecoinHashesByHeight.get(Number(body.params?.[0] ?? -1)) ?? null,
      error: null,
    }),
  getblock: (body) => {
    const hash = String(body.params?.[0] ?? '');
    const verbose = body.params?.[1] !== false;
    return Response.json({
      result: verbose
        ? (dogecoinBlocksByHash.get(hash) ?? null)
        : (dogecoinRawBlocksByHash.get(hash) ?? null),
      error: null,
    });
  },
  getmempoolinfo: () => Response.json({ result: dogecoinFixture.mempoolInfo, error: null }),
  getrawmempool: () => Response.json({ result: dogecoinFixture.mempoolEntries, error: null }),
};

function defaultRpcMockHandler(): Response {
  return Response.json({ result: 1, error: null });
}

function parseRpcRequestBody(parsed: unknown): RpcRequestBody {
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return {};
  }

  const record: Record<string, unknown> = Object.fromEntries(Object.entries(parsed));
  const method = typeof record.method === 'string' ? record.method : undefined;
  const params = Array.isArray(record.params) ? [...record.params] : undefined;

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
