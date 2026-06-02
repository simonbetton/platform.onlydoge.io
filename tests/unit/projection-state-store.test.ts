import type {
  BlockProjectionBatch,
  ProjectionBalanceSnapshot,
  ProjectionStateBootstrapSnapshot,
  ProjectionStateStorePort,
  ProjectionUtxoOutput,
} from '@onlydoge/indexing-pipeline';
import { MirroredProjectionStateStore } from '@onlydoge/platform';
import { describe, expect, it, vi } from 'vitest';

function createStateStoreStub(
  overrides: Partial<ProjectionStateStorePort> = {},
): ProjectionStateStorePort {
  return {
    applyProjectionWindow: vi.fn(async (_batches: BlockProjectionBatch[]) => {}),
    clearProjectionBootstrapState: vi.fn(async () => {}),
    finalizeProjectionBootstrap: vi.fn(async (_processTail: number) => {}),
    getCurrentAddressSummary: vi.fn(async () => null),
    getBalanceSnapshots: vi.fn(
      async () => new Map<string, ProjectionBalanceSnapshot>(),
    ) as ProjectionStateStorePort['getBalanceSnapshots'],
    getProjectionBootstrapTail: vi.fn(async () => null),
    getUtxoOutputs: vi.fn(async () => new Map<string, ProjectionUtxoOutput>()),
    hasAppliedBlock: vi.fn(async () => false),
    hasProjectionState: vi.fn(async () => false),
    importProjectionStateSnapshot: vi.fn(
      async (_snapshot: ProjectionStateBootstrapSnapshot, _processTail: number) => {},
    ),
    listAddressUtxos: vi.fn(async () => []),
    listAppliedBlockSet: vi.fn(async () => new Set<string>()),
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
    const rows = await store.getUtxoOutputs(['tx-1:0']);

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

    await expect(store.hasAppliedBlock(10, 'block-10')).resolves.toBe(true);
  });

  it('fills missing balance snapshots from fallback state', async () => {
    const primary = createStateStoreStub({
      getBalanceSnapshots: vi.fn(
        async () =>
          new Map([
            [
              'DPrimary:',
              {
                address: 'DPrimary',
                assetAddress: '',
                balance: '200000000',
                asOfBlockHeight: 2,
              },
            ],
          ]),
      ),
    });
    const fallback = createStateStoreStub({
      getBalanceSnapshots: vi.fn(
        async () =>
          new Map([
            [
              'DFallback:',
              {
                address: 'DFallback',
                assetAddress: '',
                balance: '100000000',
                asOfBlockHeight: 1,
              },
            ],
          ]),
      ),
    });

    const store = new MirroredProjectionStateStore(primary, fallback);
    const balances = await store.getBalanceSnapshots([
      { address: 'DPrimary', assetAddress: '' },
      { address: 'DFallback', assetAddress: '' },
    ]);

    expect([...balances.values()]).toEqual([
      {
        address: 'DFallback',
        assetAddress: '',
        balance: '100000000',
        asOfBlockHeight: 1,
      },
      {
        address: 'DPrimary',
        assetAddress: '',
        balance: '200000000',
        asOfBlockHeight: 2,
      },
    ]);
  });

  it('uses fallback summaries and address UTXOs when primary state has no rows', async () => {
    const fallbackUtxo: ProjectionUtxoOutput = {
      blockHeight: 1,
      blockHash: 'block-1',
      blockTime: 1_700_000_000,
      txid: 'tx-1',
      txIndex: 0,
      vout: 0,
      outputKey: 'tx-1:0',
      address: 'DFallback',
      scriptType: 'pubkeyhash',
      valueBase: '100000000',
      isCoinbase: false,
      isSpendable: true,
      spentByTxid: null,
      spentInBlock: null,
      spentInputIndex: null,
    };
    const primary = createStateStoreStub();
    const fallback = createStateStoreStub({
      getCurrentAddressSummary: vi.fn(async () => ({ balance: '100000000', utxoCount: 1 })),
      listAddressUtxos: vi.fn(async () => [fallbackUtxo]),
    });

    const store = new MirroredProjectionStateStore(primary, fallback);

    await expect(store.getCurrentAddressSummary('DFallback')).resolves.toEqual({
      balance: '100000000',
      utxoCount: 1,
    });
    await expect(store.listAddressUtxos('DFallback', 0, 50)).resolves.toEqual([fallbackUtxo]);
  });
});
