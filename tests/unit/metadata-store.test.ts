import type {
  BlockProjectionBatch,
  CoreDogecoinStateStorePort,
  ProjectionDirectLinkBatch,
  ProjectionStateStorePort,
} from '@onlydoge/indexing-pipeline';
import { describe, expect, it, vi } from 'vitest';

import { createTestApp } from '../helpers';

describe('relational metadata store', () => {
  it('rolls back projection state writes when a bulk upsert fails', async () => {
    const ctx = await createTestApp('indexer');
    const store = ctx.runtime.metadata as unknown as ProjectionStateStorePort & {
      upsertProjectionBalances: (
        balances: unknown[],
        timestamp: string,
        executor: unknown,
      ) => Promise<void>;
    };

    const original = store.upsertProjectionBalances.bind(store);
    store.upsertProjectionBalances = vi.fn(async () => {
      throw new Error('boom');
    });

    const batch: BlockProjectionBatch = {
      networkId: 7,
      blockHeight: 1,
      blockHash: 'block-1',
      blockTime: 1_700_000_001,
      utxoCreates: [
        {
          networkId: 7,
          blockHeight: 1,
          blockHash: 'block-1',
          blockTime: 1_700_000_001,
          txid: 'tx-1',
          txIndex: 0,
          vout: 0,
          outputKey: 'tx-1:0',
          address: 'DTestAddress123',
          scriptType: 'pubkeyhash',
          scriptPubKey: '76a914000000000000000000000000000000000000000088ac',
          valueBase: '100000000',
          isCoinbase: false,
          isSpendable: true,
          spentByTxid: null,
          spentInBlock: null,
          spentInputIndex: null,
        },
      ],
      utxoSpends: [],
      addressMovements: [
        {
          movementId: 'tx-1:vout:0',
          networkId: 7,
          blockHeight: 1,
          blockHash: 'block-1',
          blockTime: 1_700_000_001,
          txid: 'tx-1',
          txIndex: 0,
          entryIndex: 0,
          address: 'DTestAddress123',
          assetAddress: '',
          direction: 'credit',
          amountBase: '100000000',
          outputKey: 'tx-1:0',
          derivationMethod: 'test',
        },
      ],
      transfers: [],
      directLinkDeltas: [],
    };

    await expect(store.applyProjectionWindow([batch])).rejects.toThrow('boom');
    await expect(store.getUtxoOutputs(7, ['tx-1:0'])).resolves.toEqual(new Map());
    await expect(store.hasAppliedBlock(7, 1, 'block-1')).resolves.toBe(false);

    store.upsertProjectionBalances = original;
    await ctx.cleanup();
  });

  it('applies delayed direct-link batches idempotently', async () => {
    const ctx = await createTestApp('indexer');
    const store = ctx.runtime.metadata as unknown as ProjectionStateStorePort;

    const batch: ProjectionDirectLinkBatch = {
      networkId: 7,
      blockHeight: 10,
      blockHash: 'block-10',
      directLinkDeltas: [
        {
          networkId: 7,
          fromAddress: 'DFromAddress123',
          toAddress: 'DToAddress456',
          assetAddress: '',
          transferCount: 1,
          totalAmountBase: '2500000000',
          firstSeenBlockHeight: 10,
          lastSeenBlockHeight: 10,
        },
      ],
    };

    await store.applyDirectLinkDeltasWindow([batch]);
    await store.applyDirectLinkDeltasWindow([batch]);

    const snapshots = await store.getDirectLinkSnapshots(7, [
      {
        fromAddress: 'DFromAddress123',
        toAddress: 'DToAddress456',
        assetAddress: '',
      },
    ]);

    expect(snapshots.get('DFromAddress123:DToAddress456:')).toMatchObject({
      transferCount: 1,
      totalAmountBase: '2500000000',
      lastSeenBlockHeight: 10,
    });

    await ctx.cleanup();
  });

  it('clears only bootstrap state tables for a network', async () => {
    const ctx = await createTestApp('indexer');
    const store = ctx.runtime.metadata as unknown as ProjectionStateStorePort;

    await store.upsertProjectionBootstrapUtxoOutputs([
      {
        networkId: 7,
        blockHeight: 1,
        blockHash: 'block-1',
        blockTime: 1_700_000_001,
        txid: 'tx-1',
        txIndex: 0,
        vout: 0,
        outputKey: 'tx-1:0',
        address: 'DTestAddress123',
        scriptType: 'pubkeyhash',
        scriptPubKey: '76a914000000000000000000000000000000000000000088ac',
        valueBase: '100000000',
        isCoinbase: false,
        isSpendable: true,
        spentByTxid: null,
        spentInBlock: null,
        spentInputIndex: null,
      },
    ]);
    await store.upsertProjectionBootstrapBalances([
      {
        networkId: 7,
        address: 'DTestAddress123',
        assetAddress: '',
        balance: '100000000',
        asOfBlockHeight: 1,
      },
    ]);
    await store.applyDirectLinkDeltasWindow([
      {
        networkId: 7,
        blockHeight: 1,
        blockHash: 'block-1',
        directLinkDeltas: [
          {
            networkId: 7,
            fromAddress: 'DFromAddress123',
            toAddress: 'DToAddress456',
            assetAddress: '',
            transferCount: 1,
            totalAmountBase: '2500000000',
            firstSeenBlockHeight: 1,
            lastSeenBlockHeight: 1,
          },
        ],
      },
    ]);
    await store.applyProjectionWindow([
      {
        networkId: 7,
        blockHeight: 1,
        blockHash: 'block-1',
        blockTime: 1_700_000_001,
        utxoCreates: [],
        utxoSpends: [],
        addressMovements: [],
        transfers: [],
        directLinkDeltas: [],
      },
    ]);

    await store.clearProjectionBootstrapState(7);

    await expect(store.getUtxoOutputs(7, ['tx-1:0'])).resolves.toEqual(new Map());
    await expect(
      store.getBalanceSnapshots(7, [{ address: 'DTestAddress123', assetAddress: '' }]),
    ).resolves.toEqual(new Map());
    await expect(store.hasAppliedBlock(7, 1, 'block-1')).resolves.toBe(false);
    await expect(
      store.getDirectLinkSnapshots(7, [
        {
          fromAddress: 'DFromAddress123',
          toAddress: 'DToAddress456',
          assetAddress: '',
        },
      ]),
    ).resolves.toSatisfy(
      (snapshots) => snapshots.get('DFromAddress123:DToAddress456:')?.transferCount === 1,
    );

    await ctx.cleanup();
  });

  it('finalizes bootstrap tails without importing applied-block rows', async () => {
    const ctx = await createTestApp('indexer');
    const store = ctx.runtime.metadata as unknown as ProjectionStateStorePort;

    await store.finalizeProjectionBootstrap(7, 25);

    await expect(store.getProjectionBootstrapTail(7)).resolves.toBe(25);
    await expect(store.hasAppliedBlock(7, 20, 'block-20')).resolves.toBe(true);
    await expect(store.hasAppliedBlock(7, 30, 'block-30')).resolves.toBe(false);

    await ctx.cleanup();
  });

  it('applies core dogecoin blocks idempotently from UTXO state', async () => {
    const ctx = await createTestApp('indexer');
    const store = ctx.runtime.metadata as unknown as CoreDogecoinStateStorePort;

    const coinbase = {
      networkId: 7,
      blockHeight: 0,
      blockHash: 'block-0',
      previousBlockHash: null,
      blockTime: 1_700_000_000,
      txCount: 1,
      rawStorageKey: 'block',
      utxoSpends: [],
      utxoCreates: [
        {
          networkId: 7,
          blockHeight: 0,
          blockHash: 'block-0',
          blockTime: 1_700_000_000,
          txid: 'tx-0',
          txIndex: 0,
          vout: 0,
          outputKey: 'tx-0:0',
          address: 'DSource',
          scriptType: 'pubkeyhash',
          scriptPubKey: '76a914000000000000000000000000000000000000000088ac',
          valueBase: '10000000000',
          isCoinbase: true,
          isSpendable: true,
          spentByTxid: null,
          spentInBlock: null,
          spentInputIndex: null,
        },
      ],
    };

    await expect(store.applyCoreDogecoinBlock(coinbase)).resolves.toEqual({
      applied: true,
      processTail: 0,
    });
    await expect(store.applyCoreDogecoinBlock(coinbase)).resolves.toEqual({
      applied: false,
      processTail: 0,
    });

    await expect(
      store.applyCoreDogecoinBlock({
        networkId: 7,
        blockHeight: 1,
        blockHash: 'block-1',
        previousBlockHash: 'block-0',
        blockTime: 1_700_000_060,
        txCount: 1,
        rawStorageKey: 'block',
        utxoSpends: [
          {
            outputKey: 'tx-0:0',
            spentByTxid: 'tx-1',
            spentInBlock: 1,
            spentInputIndex: 0,
          },
        ],
        utxoCreates: [
          {
            networkId: 7,
            blockHeight: 1,
            blockHash: 'block-1',
            blockTime: 1_700_000_060,
            txid: 'tx-1',
            txIndex: 0,
            vout: 0,
            outputKey: 'tx-1:0',
            address: 'DTarget',
            scriptType: 'pubkeyhash',
            scriptPubKey: '76a914111111111111111111111111111111111111111188ac',
            valueBase: '4000000000',
            isCoinbase: false,
            isSpendable: true,
            spentByTxid: null,
            spentInBlock: null,
            spentInputIndex: null,
          },
          {
            networkId: 7,
            blockHeight: 1,
            blockHash: 'block-1',
            blockTime: 1_700_000_060,
            txid: 'tx-1',
            txIndex: 0,
            vout: 1,
            outputKey: 'tx-1:1',
            address: 'DSource',
            scriptType: 'pubkeyhash',
            scriptPubKey: '76a914222222222222222222222222222222222222222288ac',
            valueBase: '5900000000',
            isCoinbase: false,
            isSpendable: true,
            spentByTxid: null,
            spentInBlock: null,
            spentInputIndex: null,
          },
        ],
      }),
    ).resolves.toEqual({ applied: true, processTail: 1 });

    await expect(ctx.runtime.metadata.getCurrentAddressSummary(7, 'DSource')).resolves.toEqual({
      balance: '5900000000',
      utxoCount: 1,
    });
    await expect(ctx.runtime.metadata.getCurrentAddressSummary(7, 'DTarget')).resolves.toEqual({
      balance: '4000000000',
      utxoCount: 1,
    });

    await ctx.cleanup();
  });

  it('applies core dogecoin block when undo already exists from a partial replay', async () => {
    const ctx = await createTestApp('indexer');
    const store = ctx.runtime.metadata as unknown as CoreDogecoinStateStorePort;
    const metadata = ctx.runtime.metadata as unknown as {
      execute(statement: string, params?: unknown[]): Promise<void>;
    };

    const coinbase = {
      networkId: 7,
      blockHeight: 0,
      blockHash: 'block-0',
      previousBlockHash: null,
      blockTime: 1_700_000_000,
      txCount: 1,
      rawStorageKey: 'block',
      utxoSpends: [],
      utxoCreates: [
        {
          networkId: 7,
          blockHeight: 0,
          blockHash: 'block-0',
          blockTime: 1_700_000_000,
          txid: 'tx-0',
          txIndex: 0,
          vout: 0,
          outputKey: 'tx-0:0',
          address: 'DSource',
          scriptType: 'pubkeyhash',
          scriptPubKey: '76a914000000000000000000000000000000000000000088ac',
          valueBase: '10000000000',
          isCoinbase: true,
          isSpendable: true,
          spentByTxid: null,
          spentInBlock: null,
          spentInputIndex: null,
        },
      ],
    };
    const spend = {
      networkId: 7,
      blockHeight: 1,
      blockHash: 'block-1',
      previousBlockHash: 'block-0',
      blockTime: 1_700_000_060,
      txCount: 1,
      rawStorageKey: 'block',
      utxoSpends: [
        {
          outputKey: 'tx-0:0',
          spentByTxid: 'tx-1',
          spentInBlock: 1,
          spentInputIndex: 0,
          address: 'DSource',
          valueBase: '10000000000',
        },
      ],
      utxoCreates: [
        {
          networkId: 7,
          blockHeight: 1,
          blockHash: 'block-1',
          blockTime: 1_700_000_060,
          txid: 'tx-1',
          txIndex: 0,
          vout: 0,
          outputKey: 'tx-1:0',
          address: 'DTarget',
          scriptType: 'pubkeyhash',
          scriptPubKey: '76a914111111111111111111111111111111111111111188ac',
          valueBase: '9900000000',
          isCoinbase: false,
          isSpendable: true,
          spentByTxid: null,
          spentInBlock: null,
          spentInputIndex: null,
        },
      ],
    };

    await store.applyCoreDogecoinBlock(coinbase);
    await metadata.execute(
      'INSERT INTO core_block_undo (network_id, block_height, block_hash, undo_json, created_at) VALUES (?, ?, ?, ?, ?)',
      [7, 1, 'block-1', '{}', '2026-01-01T00:00:00.000Z'],
    );

    await expect(store.applyCoreDogecoinBlock(spend)).resolves.toEqual({
      applied: true,
      processTail: 1,
    });
    await expect(store.applyCoreDogecoinBlock(spend)).resolves.toEqual({
      applied: false,
      processTail: 1,
    });

    await ctx.cleanup();
  });

  it('rejects core dogecoin replay with a different block hash', async () => {
    const ctx = await createTestApp('indexer');
    const store = ctx.runtime.metadata as unknown as CoreDogecoinStateStorePort;
    const coinbase = {
      networkId: 7,
      blockHeight: 0,
      blockHash: 'block-0',
      previousBlockHash: null,
      blockTime: 1_700_000_000,
      txCount: 1,
      rawStorageKey: 'block',
      utxoSpends: [],
      utxoCreates: [],
    };

    await store.applyCoreDogecoinBlock(coinbase);
    await expect(
      store.applyCoreDogecoinBlock({
        ...coinbase,
        blockHash: 'block-0-reorg',
      }),
    ).rejects.toThrow('core block hash mismatch');

    await ctx.cleanup();
  });
});
