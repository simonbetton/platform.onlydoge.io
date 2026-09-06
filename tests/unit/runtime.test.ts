import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { buildApiApp } from '@onlydoge/api';
import type { ExplorerWarehousePort } from '@onlydoge/explorer-query';
import type { ProjectionStateStorePort } from '@onlydoge/indexing-pipeline';
import {
  assertMempoolWatchTopology,
  createExplorerWarehouse,
  createRuntime,
} from '@onlydoge/platform';
import { describe, expect, it, vi } from 'vitest';

type ExplorerStateStore = Pick<
  ProjectionStateStorePort,
  'getCurrentAddressSummary' | 'getUtxoOutputs' | 'listAddressUtxos'
>;

function createExplorerWarehouseStub(
  overrides: Partial<ExplorerWarehousePort> = {},
): ExplorerWarehousePort {
  return {
    getAddressSummary: vi.fn(async () => null),
    getAppliedBlockByHash: vi.fn(async () => null),
    getCreatedUtxoOutputs: vi.fn(async () => new Map()),
    getTransactionRef: vi.fn(async () => null),
    getUtxoOutputs: vi.fn(async () => new Map()),
    listAddressTransactions: vi.fn(async () => []),
    listAddressUtxos: vi.fn(async () => []),
    listAppliedBlocks: vi.fn(async () => []),
    ...overrides,
  };
}

function createExplorerStateStoreStub(
  overrides: Partial<ExplorerStateStore> = {},
): ExplorerStateStore {
  return {
    getCurrentAddressSummary: vi.fn(async () => null),
    getUtxoOutputs: vi.fn(async () => new Map()),
    listAddressUtxos: vi.fn(async () => []),
    ...overrides,
  };
}

describe('runtime explorer warehouse wiring', () => {
  it('uses the ClickHouse fact warehouse directly for one address summary call', async () => {
    const getAddressSummary = vi.fn(async () => ({
      balance: '8',
      receivedBase: '13',
      sentBase: '5',
      txCount: 3,
      utxoCount: 2,
    }));
    const factWarehouse = createExplorerWarehouseStub({ getAddressSummary });
    const stateStore = createExplorerStateStoreStub();

    const explorerWarehouse = createExplorerWarehouse('clickhouse', stateStore, factWarehouse);

    expect(explorerWarehouse).toBe(factWarehouse);
    await expect(explorerWarehouse.getAddressSummary('DClickHouse')).resolves.toEqual({
      balance: '8',
      receivedBase: '13',
      sentBase: '5',
      txCount: 3,
      utxoCount: 2,
    });
    expect(getAddressSummary).toHaveBeenCalledTimes(1);
    expect(stateStore.getCurrentAddressSummary).not.toHaveBeenCalled();
  });

  it('keeps non-ClickHouse current and historical summaries composed', async () => {
    const getCurrentAddressSummary = vi.fn(async () => ({
      balance: '11',
      utxoCount: 4,
    }));
    const getAddressSummary = vi.fn(async () => ({
      balance: '8',
      receivedBase: '13',
      sentBase: '5',
      txCount: 3,
      utxoCount: 2,
    }));
    const stateStore = createExplorerStateStoreStub({ getCurrentAddressSummary });
    const factWarehouse = createExplorerWarehouseStub({ getAddressSummary });

    const explorerWarehouse = createExplorerWarehouse('duckdb', stateStore, factWarehouse);

    expect(explorerWarehouse).not.toBe(factWarehouse);
    await expect(explorerWarehouse.getAddressSummary('DDuckDb')).resolves.toEqual({
      balance: '11',
      receivedBase: '13',
      sentBase: '5',
      txCount: 3,
      utxoCount: 4,
    });
    expect(getCurrentAddressSummary).toHaveBeenCalledTimes(1);
    expect(getAddressSummary).toHaveBeenCalledTimes(1);
  });
});

describe('runtime mempool watch topology', () => {
  it('allows an in-process bus when HTTP and indexer share a runtime', () => {
    expect(() =>
      assertMempoolWatchTopology({
        database: { driver: 'sqlite', location: 'file:test.db' },
        isHttp: true,
        isIndexer: true,
      }),
    ).not.toThrow();
  });

  it('requires Postgres before exposing an HTTP-only watch role', () => {
    expect(() =>
      assertMempoolWatchTopology({
        database: { driver: 'sqlite', location: 'file:test.db' },
        isHttp: true,
        isIndexer: false,
      }),
    ).toThrow('split HTTP/indexer mode requires Postgres');
    expect(() =>
      assertMempoolWatchTopology({
        database: { driver: 'postgres', location: 'postgres://onlydoge@localhost/onlydoge' },
        isHttp: false,
        isIndexer: true,
      }),
    ).not.toThrow();
    expect(() =>
      assertMempoolWatchTopology({
        database: { driver: 'sqlite', location: 'file:test.db' },
        isHttp: false,
        isIndexer: true,
      }),
    ).not.toThrow();
  });
});

describe('runtime HTTP availability', () => {
  it('keeps the HTTP app up when metadata and warehouse are unreachable', async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), 'onlydoge-http-unavailable-'));
    const previousEnv = snapshotRuntimeEnv();
    process.env.ONLYDOGE_DATABASE = 'postgres://onlydoge:onlydoge@127.0.0.1:1/onlydoge';
    process.env.ONLYDOGE_STORAGE = `file://${tempRoot}/storage`;
    process.env.ONLYDOGE_WAREHOUSE = 'http://127.0.0.1:1';
    process.env.ONLYDOGE_WAREHOUSE_REQUEST_TIMEOUT_MS = '250';

    try {
      const runtime = await createRuntime({ mode: 'http', ip: '127.0.0.1', port: 0 });
      const app = buildApiApp(runtime);

      const heartbeat = await app.handle(new Request('http://localhost/v1/heartbeat'));
      expect(heartbeat.status).toBe(204);

      const status = await app.handle(new Request('http://localhost/v1/status'));
      expect(status.status).toBe(500);
      expect(await status.json()).toEqual({
        error: 'metadata database unavailable',
      });

      const keys = await app.handle(
        new Request('http://localhost/v1/keys', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: '{}',
        }),
      );
      expect(keys.status).toBe(500);
      expect(await keys.json()).toEqual({
        error: 'metadata database unavailable',
      });

      await runtime.metadata.close();
    } finally {
      restoreRuntimeEnv(previousEnv);
      await rm(tempRoot, { recursive: true, force: true });
    }
  });

  it('still fails indexer startup when metadata is unreachable', async () => {
    const tempRoot = await mkdtemp(join(tmpdir(), 'onlydoge-indexer-unavailable-'));
    const previousEnv = snapshotRuntimeEnv();
    process.env.ONLYDOGE_DATABASE = 'postgres://onlydoge:onlydoge@127.0.0.1:1/onlydoge';
    process.env.ONLYDOGE_STORAGE = `file://${tempRoot}/storage`;
    process.env.ONLYDOGE_WAREHOUSE = 'http://127.0.0.1:1';
    process.env.ONLYDOGE_WAREHOUSE_REQUEST_TIMEOUT_MS = '250';

    try {
      await expect(createRuntime({ mode: 'indexer' })).rejects.toThrow();
    } finally {
      restoreRuntimeEnv(previousEnv);
      await rm(tempRoot, { recursive: true, force: true });
    }
  });
});

const runtimeEnvKeys = [
  'ONLYDOGE_DATABASE',
  'ONLYDOGE_STORAGE',
  'ONLYDOGE_WAREHOUSE',
  'ONLYDOGE_WAREHOUSE_REQUEST_TIMEOUT_MS',
] as const;

function snapshotRuntimeEnv(): Map<string, string | undefined> {
  return new Map(runtimeEnvKeys.map((key) => [key, process.env[key]]));
}

function restoreRuntimeEnv(previousEnv: Map<string, string | undefined>): void {
  for (const [key, value] of previousEnv.entries()) {
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }
}
