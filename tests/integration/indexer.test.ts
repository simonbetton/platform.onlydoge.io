import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

import {
  CoreDogecoinIndexerService,
  type CoreIndexerState,
  configKeyDogecoinCurrentStateReady,
  configKeyDogecoinHistoryReady,
  configKeyIndexerFactTail,
  configKeyIndexerProcessTail,
  configKeyIndexerStage,
  configKeyIndexerSyncTail,
  type IndexingPipelineSettings,
} from '@onlydoge/indexing-pipeline';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { dogecoinFixture } from '../fixtures/dogecoin';
import {
  createDogecoinTestNetwork,
  createTestApp,
  installRpcMock,
  runIndexerUntilProcessed,
} from '../helpers';

describe('core dogecoin indexer integration', () => {
  let restoreFetch: ReturnType<typeof installRpcMock>;

  beforeEach(() => {
    restoreFetch = installRpcMock();
  });

  afterEach(() => {
    restoreFetch.mockRestore();
  });

  it('syncs raw blocks first, then processes deterministic core UTXO state', async () => {
    const ctx = await createTestApp('indexer');
    const network = await createDogecoinTestNetwork(ctx.runtime);

    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
    const internalNetwork = await ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');
    expect(internalNetwork?.networkId).toBeDefined();
    const networkId = internalNetwork?.networkId ?? 0;

    await expect(
      ctx.runtime.metadata.getJsonValue<string>(configKeyIndexerStage(networkId)),
    ).resolves.toBe('sync_backfill');
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerSyncTail(networkId)),
    ).resolves.toBe(2);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerProcessTail(networkId)),
    ).resolves.toBe(-1);

    const snapshotPath = join(ctx.tempRoot, 'storage', String(networkId), '0', 'block.json.gz');
    const snapshot = await readFile(snapshotPath);
    expect(snapshot.byteLength).toBeGreaterThan(0);

    await runIndexerUntilProcessed(ctx, networkId, 2);

    await expect(
      ctx.runtime.metadata.getJsonValue<string>(configKeyIndexerStage(networkId)),
    ).resolves.toBe('process_backfill');
    await expect(
      ctx.runtime.metadata.getCurrentAddressSummary(networkId, dogecoinFixture.sourceAddress),
    ).resolves.toEqual({
      balance: '5900000000',
      utxoCount: 1,
    });
    await expect(
      ctx.runtime.metadata.getCurrentAddressSummary(networkId, dogecoinFixture.intermediaryAddress),
    ).resolves.toEqual({
      balance: '1400000000',
      utxoCount: 1,
    });
    await expect(
      ctx.runtime.metadata.getCurrentAddressSummary(networkId, dogecoinFixture.targetAddress),
    ).resolves.toEqual({
      balance: '2500000000',
      utxoCount: 1,
    });

    await expect(
      ctx.runtime.explorerQuery.listAddressUtxos(dogecoinFixture.targetAddress, network.id),
    ).resolves.toMatchObject({
      utxos: [
        expect.objectContaining({
          address: dogecoinFixture.targetAddress,
          outputKey: 'doge-tx-2:0',
          valueBase: '2500000000',
        }),
      ],
    });

    await ctx.cleanup();
  });

  it('reclaims a stale primary lease and resumes raw sync', async () => {
    const ctx = await createTestApp('indexer');
    await createDogecoinTestNetwork(ctx.runtime);
    await ctx.runtime.metadata.setJsonValue('primary', 'stale-instance-id');

    const networkId = await runOnceAndGetDogecoinNetworkId(ctx);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerSyncTail(networkId)),
    ).resolves.toBe(2);

    await ctx.cleanup();
  });

  it('does not start core processing until raw sync reaches the tip window', async () => {
    const ctx = await createTestApp('indexer');
    await createDogecoinTestNetwork(ctx.runtime);
    ctx.runtime.settings.indexer.coreSyncCompleteDistance = 0;
    ctx.runtime.settings.indexer.syncWindow = 1;

    const networkId = await runOnceAndGetDogecoinNetworkId(ctx);

    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerSyncTail(networkId)),
    ).resolves.toBe(0);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerProcessTail(networkId)),
    ).resolves.toBe(-1);
    await expect(
      ctx.runtime.metadata.getJsonValue<string>(configKeyIndexerStage(networkId)),
    ).resolves.toBe('sync_backfill');

    await ctx.cleanup();
  });

  it('advances through already processed blocks idempotently', async () => {
    const ctx = await createTestApp('indexer');
    await createDogecoinTestNetwork(ctx.runtime);
    const network = await ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');
    const networkId = network?.networkId ?? 0;

    await runIndexerUntilProcessed(ctx, networkId, 2);
    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerProcessTail(networkId)),
    ).resolves.toBe(2);

    await ctx.cleanup();
  });

  it('catches up current-state tail without rerunning destructive materialization', async () => {
    const values = new Map<string, unknown>([
      [configKeyDogecoinCurrentStateReady(7), true],
      [configKeyDogecoinHistoryReady(7), true],
      ['primary', null],
    ]);
    const materialize = vi.fn(async () => undefined);
    const upserts: unknown[] = [];
    const errors: string[] = [];
    const applyContexts: unknown[] = [];
    const rawBlocks = new Map<number, Record<string, unknown>>();
    let state: CoreIndexerState = {
      lastError: null as string | null,
      networkId: 7,
      onlineTip: 2,
      processTail: 2,
      stage: 'online' as const,
      syncTail: 2,
      updatedAt: new Date().toISOString(),
    };
    const service = new CoreDogecoinIndexerService(
      {
        async compareAndSwapJsonValue(key, expectedValue, nextValue) {
          if ((values.get(key) ?? null) !== expectedValue) {
            return false;
          }
          values.set(key, nextValue);
          return true;
        },
        async deleteByPrefix() {},
        async getJsonValue(key) {
          return (values.get(key) as never) ?? null;
        },
        async setJsonValue(key, value) {
          values.set(key, value);
        },
      },
      {
        async listActiveNetworks() {
          return [
            {
              architecture: 'dogecoin',
              blockTime: 60,
              id: 'net_doge',
              networkId: 7,
              rpcEndpoint: 'https://doge.example/rpc',
              rps: 10,
            },
          ];
        },
      },
      {
        async getPart<T extends Record<string, unknown>>(_networkId: number, blockHeight: number) {
          return (rawBlocks.get(blockHeight) as T | undefined) ?? null;
        },
        async putPart(_networkId, blockHeight, _part, payload) {
          rawBlocks.set(blockHeight, payload);
        },
      },
      {
        async getBlockHeight() {
          return 3;
        },
        async getBlockSnapshot() {
          return {
            block: {
              hash: 'doge-block-3',
              height: 3,
              previousblockhash: 'doge-block-2',
              time: 1_700_000_180,
              tx: [
                {
                  txid: 'doge-tx-3',
                  vin: [{ coinbase: 'coinbase' }],
                  vout: [
                    {
                      n: 0,
                      value: '10.00000000',
                      scriptPubKey: {
                        type: 'pubkeyhash',
                        addresses: ['DNewTail11111111111111111111111111'],
                      },
                    },
                  ],
                },
              ],
            },
          };
        },
      },
      {
        async applyCoreDogecoinBlock() {
          throw new Error('online ready state should not process core blocks');
        },
        async applyCoreDogecoinWindow(input, context) {
          applyContexts.push(context);
          return { applied: true, processTail: input.at(-1)?.blockHeight ?? 0 };
        },
        async getCoreIndexerState() {
          return state;
        },
        async getCoreUtxoOutputs() {
          return new Map();
        },
        materializeCoreDogecoinCurrentState: materialize,
        async setCoreIndexerError(_networkId, error) {
          errors.push(error ?? '');
        },
        async setCoreIndexerStage() {},
        async upsertCoreBlock() {},
        async upsertCoreIndexerState(input) {
          upserts.push(input);
          state = {
            ...state,
            ...input,
            lastError: input.lastError === undefined ? state.lastError : input.lastError,
            updatedAt: new Date().toISOString(),
          };
          return state;
        },
      },
      testIndexerSettings({
        coreOnlineTipDistance: 0,
        coreProcessWindow: 1,
        coreProgressWatchdogMs: 0,
        coreSyncCompleteDistance: 0,
        syncWindow: 1,
      }),
      {
        exitProcess(code): never {
          throw new Error(`unexpected process exit ${code}`);
        },
      },
    );

    await expect(service.runOnce()).resolves.toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 1));
    await expect(service.runOnce()).resolves.toBe(true);
    await expect(service.runOnce()).resolves.toBe(false);
    expect(materialize).not.toHaveBeenCalled();
    expect(errors).toEqual([]);
    expect(applyContexts).toContainEqual(
      expect.objectContaining({
        updateCurrentState: true,
        validatePrevouts: false,
      }),
    );
    expect(upserts).toContainEqual(
      expect.objectContaining({
        lastError: null,
        networkId: 7,
        onlineTip: 3,
        stage: 'online',
      }),
    );
    expect(values.get(configKeyIndexerFactTail(7))).toBe(3);
  });

  it('fails fast when a core block step exceeds the deadline', async () => {
    const values = new Map<string, unknown>();
    const errors: string[] = [];
    const service = new CoreDogecoinIndexerService(
      {
        async compareAndSwapJsonValue(key, expectedValue, nextValue) {
          if ((values.get(key) ?? null) !== expectedValue) {
            return false;
          }
          values.set(key, nextValue);
          return true;
        },
        async deleteByPrefix() {},
        async getJsonValue(key) {
          return (values.get(key) as never) ?? null;
        },
        async setJsonValue(key, value) {
          values.set(key, value);
        },
      },
      {
        async listActiveNetworks() {
          return [
            {
              architecture: 'dogecoin',
              blockTime: 60,
              id: 'net_doge',
              networkId: 7,
              rpcEndpoint: 'https://doge.example/rpc',
              rps: 10,
            },
          ];
        },
      },
      {
        async getPart() {
          return new Promise<null>(() => {});
        },
        async putPart() {},
      },
      {
        async getBlockHeight() {
          return 0;
        },
        async getBlockSnapshot() {
          return {};
        },
      },
      {
        async applyCoreDogecoinBlock() {
          return { applied: true, processTail: 0 };
        },
        async applyCoreDogecoinWindow(input) {
          return { applied: true, processTail: input.at(-1)?.blockHeight ?? 0 };
        },
        async getCoreIndexerState() {
          return {
            lastError: null,
            networkId: 7,
            onlineTip: 0,
            processTail: -1,
            stage: 'process_backfill',
            syncTail: 0,
            updatedAt: new Date().toISOString(),
          };
        },
        async getCoreUtxoOutputs() {
          return new Map();
        },
        async materializeCoreDogecoinCurrentState() {},
        async setCoreIndexerError(_networkId, error) {
          errors.push(error ?? '');
        },
        async setCoreIndexerStage() {},
        async upsertCoreBlock() {},
        async upsertCoreIndexerState(input) {
          return {
            lastError: input.lastError ?? null,
            networkId: input.networkId,
            onlineTip: input.onlineTip ?? 0,
            processTail: input.processTail ?? -1,
            stage: input.stage ?? 'process_backfill',
            syncTail: input.syncTail ?? 0,
            updatedAt: new Date().toISOString(),
          };
        },
      },
      {
        ...testIndexerSettings(),
        coreBlockTimeoutMs: 1,
      },
      {
        exitProcess(code): never {
          throw new Error(`exit ${code}`);
        },
      },
    );

    await expect(service.runOnce()).rejects.toThrow('exit 1');
    expect(errors.some((error) => error.includes('height=0'))).toBe(true);
    expect(errors.some((error) => error.includes('active_step=load_raw'))).toBe(true);
  });

  it('persists async core processing errors before surfacing them', async () => {
    const values = new Map<string, unknown>();
    const errors: string[] = [];
    const service = new CoreDogecoinIndexerService(
      {
        async compareAndSwapJsonValue(key, expectedValue, nextValue) {
          if ((values.get(key) ?? null) !== expectedValue) {
            return false;
          }
          values.set(key, nextValue);
          return true;
        },
        async deleteByPrefix() {},
        async getJsonValue(key) {
          return (values.get(key) as never) ?? null;
        },
        async setJsonValue(key, value) {
          values.set(key, value);
        },
      },
      {
        async listActiveNetworks() {
          return [
            {
              architecture: 'dogecoin',
              blockTime: 60,
              id: 'net_doge',
              networkId: 7,
              rpcEndpoint: 'https://doge.example/rpc',
              rps: 10,
            },
          ];
        },
      },
      {
        async getPart() {
          throw new Error('raw block storage get timed out key=storage/7/0/block.json.gz');
        },
        async putPart() {},
      },
      {
        async getBlockHeight() {
          return 0;
        },
        async getBlockSnapshot() {
          return {};
        },
      },
      {
        async applyCoreDogecoinBlock() {
          return { applied: true, processTail: 0 };
        },
        async applyCoreDogecoinWindow(input) {
          return { applied: true, processTail: input.at(-1)?.blockHeight ?? 0 };
        },
        async getCoreIndexerState() {
          return {
            lastError: null,
            networkId: 7,
            onlineTip: 0,
            processTail: -1,
            stage: 'process_backfill',
            syncTail: 0,
            updatedAt: new Date().toISOString(),
          };
        },
        async getCoreUtxoOutputs() {
          return new Map();
        },
        async materializeCoreDogecoinCurrentState() {},
        async setCoreIndexerError(_networkId, error) {
          errors.push(error ?? '');
        },
        async setCoreIndexerStage() {},
        async upsertCoreBlock() {},
        async upsertCoreIndexerState(input) {
          return {
            lastError: input.lastError ?? null,
            networkId: input.networkId,
            onlineTip: input.onlineTip ?? 0,
            processTail: input.processTail ?? -1,
            stage: input.stage ?? 'process_backfill',
            syncTail: input.syncTail ?? 0,
            updatedAt: new Date().toISOString(),
          };
        },
      },
      testIndexerSettings(),
    );

    await expect(service.runOnce()).rejects.toThrow('raw block storage get timed out');
    expect(errors.at(-1)).toBe('raw block storage get timed out key=storage/7/0/block.json.gz');
  });
});

async function runOnceAndGetDogecoinNetworkId(
  ctx: Awaited<ReturnType<typeof createTestApp>>,
): Promise<number> {
  await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
  const network = await ctx.runtime.metadata.getNetworkByName('Dogecoin Mainnet');
  return network?.networkId ?? 0;
}

function testIndexerSettings(
  overrides: Partial<IndexingPipelineSettings> = {},
): IndexingPipelineSettings {
  return {
    bootstrapTimeoutMs: 60_000,
    coreBlockTimeoutMs: 120_000,
    coreDbStatementTimeoutMs: 30_000,
    coreOnlineTipDistance: 6,
    coreProcessLoadConcurrency: 8,
    coreProcessWindow: 100,
    coreProgressWatchdogMs: 180_000,
    coreRawStorageTimeoutMs: 30_000,
    coreSyncCompleteDistance: 6,
    dogecoinTransferMaxEdges: 1024,
    dogecoinTransferMaxInputAddresses: 64,
    factTimeoutMs: 300_000,
    factWindow: 64,
    leaseHeartbeatIntervalMs: 5_000,
    networkConcurrency: 2,
    projectTargetMs: 30_000,
    projectTimeoutMs: 120_000,
    projectWindow: 4,
    projectWindowMax: 16,
    projectWindowMin: 2,
    relinkBacklogThreshold: 256,
    relinkBatchSize: 16,
    relinkConcurrency: 2,
    relinkFrontierBatch: 32,
    relinkTipDistance: 512,
    relinkTimeoutMs: 120_000,
    syncBacklogHighWatermark: 2048,
    syncBacklogLowWatermark: 512,
    syncConcurrency: 4,
    syncTargetMs: 15_000,
    syncTimeoutMs: 120_000,
    syncWindow: 32,
    syncWindowMax: 256,
    syncWindowMin: 32,
    ...overrides,
  };
}
