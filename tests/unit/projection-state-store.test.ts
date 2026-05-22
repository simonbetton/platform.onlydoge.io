import type {
  BlockProjectionBatch,
  DirectLinkRecord,
  ProjectionBalanceSnapshot,
  ProjectionDirectLinkBatch,
  ProjectionStateBootstrapSnapshot,
  ProjectionStateStorePort,
  ProjectionUtxoOutput,
  SourceLinkRecord,
} from '@onlydoge/indexing-pipeline';
import { MirroredProjectionStateStore } from '@onlydoge/platform';
import { describe, expect, it, vi } from 'vitest';

function createStateStoreStub(
  overrides: Partial<ProjectionStateStorePort> = {},
): ProjectionStateStorePort {
  return {
    applyDirectLinkDeltasWindow: vi.fn(async (_batches: ProjectionDirectLinkBatch[]) => {}),
    applyProjectionWindow: vi.fn(async (_batches: BlockProjectionBatch[]) => {}),
    clearProjectionBootstrapState: vi.fn(async (_networkId: number) => {}),
    finalizeProjectionBootstrap: vi.fn(async (_networkId: number, _processTail: number) => {}),
    getCurrentAddressSummary: vi.fn(async () => null),
    getBalanceSnapshots: vi.fn(
      async () => new Map<string, ProjectionBalanceSnapshot>(),
    ) as ProjectionStateStorePort['getBalanceSnapshots'],
    getBalancesByAddresses: vi.fn(
      async () => [],
    ) as ProjectionStateStorePort['getBalancesByAddresses'],
    getDirectLinkSnapshots: vi.fn(
      async () => new Map<string, DirectLinkRecord>(),
    ) as ProjectionStateStorePort['getDirectLinkSnapshots'],
    getDistinctLinksByAddresses: vi.fn(
      async () => [],
    ) as ProjectionStateStorePort['getDistinctLinksByAddresses'],
    getProjectionBootstrapTail: vi.fn(async () => null),
    getUtxoOutputs: vi.fn(async () => new Map<string, ProjectionUtxoOutput>()),
    hasAppliedBlock: vi.fn(async () => false),
    hasProjectionState: vi.fn(async () => false),
    importProjectionStateSnapshot: vi.fn(
      async (
        _networkId: number,
        _snapshot: ProjectionStateBootstrapSnapshot,
        _processTail: number,
      ) => {},
    ),
    listAddressUtxos: vi.fn(async () => []),
    listAppliedBlockSet: vi.fn(async () => new Set<string>()),
    listDirectLinksFromAddresses: vi.fn(async () => []),
    listSourceSeedIdsReachingAddresses: vi.fn(async () => []),
    replaceSourceLinks: vi.fn(
      async (_networkId: number, _sourceAddressId: number, _rows: SourceLinkRecord[]) => {},
    ),
    upsertProjectionBootstrapBalances: vi.fn(async (_rows: ProjectionBalanceSnapshot[]) => {}),
    upsertProjectionBootstrapUtxoOutputs: vi.fn(async (_rows: ProjectionUtxoOutput[]) => {}),
    ...overrides,
  };
}

describe('mirrored projection state store', () => {
  it('falls back to the warehouse for missing current outputs', async () => {
    const primary = createStateStoreStub({
      getUtxoOutputs: vi.fn(async () => new Map()),
    });
    const fallback = createStateStoreStub({
      getUtxoOutputs: vi.fn(
        async () =>
          new Map([
            [
              'tx-1:0',
              {
                networkId: 1,
                blockHeight: 10,
                blockHash: 'block-10',
                blockTime: 1_700_000_000,
                txid: 'tx-1',
                txIndex: 0,
                vout: 0,
                outputKey: 'tx-1:0',
                address: 'DTestAddress123',
                scriptType: 'pubkeyhash',
                valueBase: '100000000',
                isCoinbase: false,
                isSpendable: true,
                spentByTxid: null,
                spentInBlock: null,
                spentInputIndex: null,
              },
            ],
          ]),
      ),
    });

    const store = new MirroredProjectionStateStore(primary, fallback);
    const rows = await store.getUtxoOutputs(1, ['tx-1:0']);

    expect(rows.get('tx-1:0')).toMatchObject({
      outputKey: 'tx-1:0',
      address: 'DTestAddress123',
    });
  });

  it('falls back to warehouse applied-block checks when metadata state is empty', async () => {
    const primary = createStateStoreStub({
      hasAppliedBlock: vi.fn(async () => false),
    });
    const fallback = createStateStoreStub({
      hasAppliedBlock: vi.fn(async () => true),
    });

    const store = new MirroredProjectionStateStore(primary, fallback);

    await expect(store.hasAppliedBlock(1, 10, 'block-10')).resolves.toBe(true);
  });

  it('merges current balances from metadata and warehouse fallback', async () => {
    const primary = createStateStoreStub({
      getBalancesByAddresses: vi.fn(async () => [
        {
          networkId: 1,
          assetAddress: 'DOGE',
          balance: '200000000',
        },
      ]),
    });
    const fallback = createStateStoreStub({
      getBalancesByAddresses: vi.fn(async () => [
        {
          networkId: 1,
          assetAddress: 'DOGE',
          balance: '100000000',
        },
      ]),
    });

    const store = new MirroredProjectionStateStore(primary, fallback);
    const balances = await store.getBalancesByAddresses(['DPrimary', 'DFallback']);

    expect(balances).toEqual([
      { networkId: 1, assetAddress: 'DOGE', balance: '100000000' },
      { networkId: 1, assetAddress: 'DOGE', balance: '200000000' },
    ]);
  });
});
