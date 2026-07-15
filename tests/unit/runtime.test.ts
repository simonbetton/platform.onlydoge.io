import type { ExplorerWarehousePort } from '@onlydoge/explorer-query';
import type { ProjectionStateStorePort } from '@onlydoge/indexing-pipeline';
import { assertMempoolWatchTopology, createExplorerWarehouse } from '@onlydoge/platform';
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
