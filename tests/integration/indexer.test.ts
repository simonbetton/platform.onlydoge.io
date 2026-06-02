import { readFile } from 'node:fs/promises';
import { join } from 'node:path';

import {
  type BlockchainRpcPort,
  CoreDogecoinIndexerService,
  type CoreDogecoinIndexerSettings,
  type CoreIndexerState,
  configKeyDogecoinAnalyticsFactsReady,
  configKeyDogecoinAnalyticsFactsTail,
  configKeyDogecoinCurrentStateReady,
  configKeyDogecoinHistoryReady,
  configKeyIndexerFactTail,
  configKeyIndexerProcessTail,
  configKeyIndexerStage,
  configKeyIndexerSyncTail,
  type RawBlockStoragePort,
} from '@onlydoge/indexing-pipeline';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { dogecoinFixture } from '../fixtures/dogecoin';
import {
  createTestApp,
  installRpcMock,
  prepareDogecoinTestConfig,
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
    await prepareDogecoinTestConfig(ctx.runtime);

    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);

    await expect(ctx.runtime.metadata.getJsonValue<string>(configKeyIndexerStage())).resolves.toBe(
      'sync_backfill',
    );
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerSyncTail()),
    ).resolves.toBe(2);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerProcessTail()),
    ).resolves.toBe(-1);

    const snapshotPath = join(ctx.tempRoot, 'storage', '0', 'block.json.gz');
    const snapshot = await readFile(snapshotPath);
    expect(snapshot.byteLength).toBeGreaterThan(0);

    await runIndexerUntilProcessed(ctx, 2);

    await expect(ctx.runtime.metadata.getJsonValue<string>(configKeyIndexerStage())).resolves.toBe(
      'process_backfill',
    );
    await expect(
      ctx.runtime.explorerQuery.getAddress(dogecoinFixture.sourceAddress),
    ).resolves.toMatchObject({ address: { balance: '5900000000', utxoCount: 1 } });
    await expect(
      ctx.runtime.explorerQuery.getAddress(dogecoinFixture.intermediaryAddress),
    ).resolves.toMatchObject({ address: { balance: '1400000000', utxoCount: 1 } });
    await expect(
      ctx.runtime.explorerQuery.getAddress(dogecoinFixture.targetAddress),
    ).resolves.toMatchObject({ address: { balance: '2500000000', utxoCount: 1 } });

    await expect(
      ctx.runtime.explorerQuery.listAddressUtxos(dogecoinFixture.targetAddress),
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
    await prepareDogecoinTestConfig(ctx.runtime);
    await ctx.runtime.metadata.setJsonValue('primary', 'stale-instance-id');

    await runOnce(ctx);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerSyncTail()),
    ).resolves.toBe(2);

    await ctx.cleanup();
  });

  it('does not start core processing until raw sync reaches the tip window', async () => {
    const ctx = await createTestApp('indexer');
    await prepareDogecoinTestConfig(ctx.runtime);
    ctx.runtime.settings.indexer.coreSyncCompleteDistance = 0;
    ctx.runtime.settings.indexer.syncWindow = 1;

    await runOnce(ctx);

    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerSyncTail()),
    ).resolves.toBe(0);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerProcessTail()),
    ).resolves.toBe(-1);
    await expect(ctx.runtime.metadata.getJsonValue<string>(configKeyIndexerStage())).resolves.toBe(
      'sync_backfill',
    );

    await ctx.cleanup();
  });

  it('advances through already processed blocks idempotently', async () => {
    const ctx = await createTestApp('indexer');
    await prepareDogecoinTestConfig(ctx.runtime);
    await runIndexerUntilProcessed(ctx, 2);
    await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
    await expect(
      ctx.runtime.metadata.getJsonValue<number>(configKeyIndexerProcessTail()),
    ).resolves.toBe(2);

    await ctx.cleanup();
  });

  it('catches up current-state tail without rerunning destructive materialization', async () => {
    const values = new Map<string, unknown>([
      [configKeyDogecoinAnalyticsFactsReady(), true],
      [configKeyDogecoinCurrentStateReady(), true],
      [configKeyDogecoinHistoryReady(), true],
      ['primary', null],
    ]);
    const materialize = vi.fn(async () => undefined);
    const upserts: unknown[] = [];
    const errors: string[] = [];
    const applyContexts: unknown[] = [];
    const rawBlocks = new Map<number, Record<string, unknown>>();
    let state: CoreIndexerState = {
      lastError: null as string | null,
      onlineTip: 2,
      processTail: 2,
      stage: 'online' as const,
      syncTail: 2,
      updatedAt: new Date().toISOString(),
    };
    const service = new CoreDogecoinIndexerService(
      memoryCoordinator(values),
      dogecoinConfigReader(),
      memoryRawBlockStorage(rawBlocks),
      dogecoinTipReader(3),
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
        async setCoreIndexerError(error: string | null) {
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
        coreReprocessDepth: 1,
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
        onlineTip: 3,
        stage: 'online',
      }),
    );
    expect(values.get(configKeyIndexerFactTail())).toBe(3);
    expect(values.get(configKeyDogecoinAnalyticsFactsTail())).toBe(2);
  });

  it('refreshes and processes the online reorg window to the node tip', async () => {
    const values = new Map<string, unknown>([
      [configKeyDogecoinCurrentStateReady(), true],
      [configKeyDogecoinHistoryReady(), true],
      ['primary', null],
    ]);
    const errors: string[] = [];
    const appliedWindows: number[][] = [];
    const applyContexts: unknown[] = [];
    const rawBlocks = new Map<number, Record<string, unknown>>([[2, testDogecoinBlockSnapshot(2)]]);
    let state: CoreIndexerState = {
      lastError: null,
      onlineTip: 2,
      processTail: 2,
      stage: 'online',
      syncTail: 2,
      updatedAt: new Date().toISOString(),
    };
    const service = new CoreDogecoinIndexerService(
      memoryCoordinator(values),
      dogecoinConfigReader(),
      memoryRawBlockStorage(rawBlocks),
      dogecoinTipReader(8),
      {
        async applyCoreDogecoinBlock() {
          throw new Error('online split should not use single-block core processing');
        },
        async applyCoreDogecoinWindow(input, context) {
          appliedWindows.push(input.map((application) => application.blockHeight));
          applyContexts.push(context);
          return { applied: true, processTail: input.at(-1)?.blockHeight ?? state.processTail };
        },
        async getCoreIndexerState() {
          return state;
        },
        async getCoreUtxoOutputs() {
          return new Map();
        },
        async materializeCoreDogecoinCurrentState() {},
        async setCoreIndexerError(error: string | null) {
          errors.push(error ?? '');
        },
        async setCoreIndexerStage() {},
        async upsertCoreBlock() {},
        async upsertCoreIndexerState(input) {
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
        coreOnlineTipDistance: 6,
        coreProgressWatchdogMs: 0,
        coreReprocessDepth: 10,
        syncWindow: 10,
      }),
    );

    await expect(service.runOnce()).resolves.toBe(true);
    expect([...rawBlocks.keys()].sort((left, right) => left - right)).toEqual([
      0, 1, 2, 3, 4, 5, 6, 7, 8,
    ]);
    expect(appliedWindows).toEqual([[0, 1, 2, 3, 4, 5, 6, 7, 8]]);
    expect(applyContexts).toContainEqual(
      expect.objectContaining({
        updateCurrentState: true,
        validatePrevouts: false,
      }),
    );
    expect(errors).toEqual([]);
    expect(state).toMatchObject({
      lastError: null,
      onlineTip: 8,
      processTail: 8,
      stage: 'online',
      syncTail: 8,
    });
    expect(values.get(configKeyIndexerSyncTail())).toBe(8);
    expect(values.get(configKeyIndexerProcessTail())).toBe(8);
  });

  it('replays a stale online process tail even when raw sync already reached the tip', async () => {
    const values = new Map<string, unknown>([
      [configKeyDogecoinCurrentStateReady(), true],
      [configKeyDogecoinHistoryReady(), true],
      ['primary', null],
    ]);
    const errors: string[] = [];
    const fetchedHeights: number[] = [];
    const appliedWindows: number[][] = [];
    const appliedHashes: string[][] = [];
    const rawBlocks = new Map<number, Record<string, unknown>>(
      rangeForTest(3, 12).map((height) => [
        height,
        testDogecoinBlockSnapshot(height, 'stale-doge-block'),
      ]),
    );
    let state: CoreIndexerState = {
      lastError:
        'non-contiguous core dogecoin chain previous_height=5 previous_hash=stale-doge-block-5 next_height=6 next_previous=doge-block-5',
      onlineTip: 12,
      processTail: 5,
      stage: 'online',
      syncTail: 12,
      updatedAt: new Date().toISOString(),
    };
    const service = new CoreDogecoinIndexerService(
      memoryCoordinator(values),
      dogecoinConfigReader(),
      memoryRawBlockStorage(rawBlocks),
      dogecoinTipReader(12, (blockHeight) => fetchedHeights.push(blockHeight)),
      {
        async applyCoreDogecoinBlock() {
          throw new Error('online split should not use single-block core processing');
        },
        async applyCoreDogecoinWindow(input) {
          appliedWindows.push(input.map((application) => application.blockHeight));
          appliedHashes.push(input.map((application) => application.blockHash));
          return { applied: true, processTail: input.at(-1)?.blockHeight ?? state.processTail };
        },
        async getCoreIndexerState() {
          return state;
        },
        async getCoreUtxoOutputs() {
          return new Map();
        },
        async materializeCoreDogecoinCurrentState() {},
        async setCoreIndexerError(error: string | null) {
          errors.push(error ?? '');
        },
        async setCoreIndexerStage() {},
        async upsertCoreBlock() {},
        async upsertCoreIndexerState(input) {
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
        coreProgressWatchdogMs: 0,
        coreReprocessDepth: 3,
        syncWindow: 10,
      }),
    );

    await expect(service.runOnce()).resolves.toBe(true);
    expect([...fetchedHeights].sort((left, right) => left - right)).toEqual(rangeForTest(3, 12));
    expect(appliedWindows).toEqual([rangeForTest(3, 12)]);
    expect(appliedHashes).toEqual([rangeForTest(3, 12).map((height) => `doge-block-${height}`)]);
    expect(errors).toEqual([]);
    expect(state).toMatchObject({
      lastError: null,
      onlineTip: 12,
      processTail: 12,
      stage: 'online',
      syncTail: 12,
    });
  });

  it('fails fast when a core block step exceeds the deadline', async () => {
    const values = new Map<string, unknown>();
    const errors: string[] = [];
    const service = new CoreDogecoinIndexerService(
      memoryCoordinator(values),
      dogecoinConfigReader(),
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
        async setCoreIndexerError(error: string | null) {
          errors.push(error ?? '');
        },
        async setCoreIndexerStage() {},
        async upsertCoreBlock() {},
        async upsertCoreIndexerState(input) {
          return {
            lastError: input.lastError ?? null,
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
      memoryCoordinator(values),
      dogecoinConfigReader(),
      {
        async getPart() {
          throw new Error('raw block storage get timed out key=storage/0/block.json.gz');
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
        async setCoreIndexerError(error: string | null) {
          errors.push(error ?? '');
        },
        async setCoreIndexerStage() {},
        async upsertCoreBlock() {},
        async upsertCoreIndexerState(input) {
          return {
            lastError: input.lastError ?? null,
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
    expect(errors.at(-1)).toBe('raw block storage get timed out key=storage/0/block.json.gz');
  });
});

async function runOnce(ctx: Awaited<ReturnType<typeof createTestApp>>): Promise<void> {
  await expect(ctx.runtime.indexingPipeline.runOnce()).resolves.toBe(true);
}

function testIndexerSettings(
  overrides: Partial<CoreDogecoinIndexerSettings> = {},
): CoreDogecoinIndexerSettings {
  return {
    coreBlockTimeoutMs: 120_000,
    coreDbStatementTimeoutMs: 30_000,
    coreOnlineTipDistance: 6,
    coreProcessLoadConcurrency: 8,
    coreProcessWindow: 100,
    coreProgressWatchdogMs: 180_000,
    coreRawStorageTimeoutMs: 30_000,
    coreReprocessDepth: 10,
    coreSyncCompleteDistance: 6,
    syncConcurrency: 4,
    syncWindow: 32,
    ...overrides,
  };
}

function memoryCoordinator(values: Map<string, unknown>) {
  return {
    async compareAndSwapJsonValue(key: string, expectedValue: unknown, nextValue: unknown) {
      if ((values.get(key) ?? null) !== expectedValue) {
        return false;
      }
      values.set(key, nextValue);
      return true;
    },
    async deleteByPrefix() {},
    async getJsonValue<T>(key: string) {
      return (values.get(key) as T | undefined) ?? null;
    },
    async setJsonValue(key: string, value: unknown) {
      values.set(key, value);
    },
  };
}

function testDogecoinBlockSnapshot(
  blockHeight: number,
  hashPrefix = 'doge-block',
): Record<string, unknown> {
  return {
    block: {
      hash: `${hashPrefix}-${blockHeight}`,
      height: blockHeight,
      previousblockhash: blockHeight > 0 ? `${hashPrefix}-${blockHeight - 1}` : null,
      time: 1_700_000_000 + blockHeight * 60,
      tx: [
        {
          txid: `doge-tx-${blockHeight}`,
          vin: [{ coinbase: 'coinbase' }],
          vout: [
            {
              n: 0,
              value: '10.00000000',
              scriptPubKey: {
                addresses: [`DTestTail${String(blockHeight).padStart(26, '0')}`],
                type: 'pubkeyhash',
              },
            },
          ],
        },
      ],
    },
  };
}

function rangeForTest(start: number, end: number): number[] {
  return Array.from({ length: end - start + 1 }, (_value, index) => start + index);
}

function dogecoinConfigReader() {
  return {
    async getDogecoinConfig() {
      return {
        architecture: 'dogecoin' as const,
        blockTime: 60,
        chainId: 3,
        id: 'dogecoin',
        name: 'Dogecoin',
        rpcEndpoint: 'https://doge.example/rpc',
        rps: 10,
      };
    },
  };
}

function memoryRawBlockStorage(
  rawBlocks: Map<number, Record<string, unknown>>,
): RawBlockStoragePort {
  return {
    async getPart<T extends Record<string, unknown>>(blockHeight: number): Promise<T | null> {
      return (rawBlocks.get(blockHeight) as T | undefined) ?? null;
    },
    async putPart(
      blockHeight: number,
      _part: string,
      payload: Record<string, unknown>,
    ): Promise<void> {
      rawBlocks.set(blockHeight, payload);
    },
  };
}

function dogecoinTipReader(
  tip: number,
  onBlockSnapshot?: (blockHeight: number) => void,
): BlockchainRpcPort {
  return {
    async getBlockHeight() {
      return tip;
    },
    async getBlockSnapshot(_dogecoin, blockHeight) {
      onBlockSnapshot?.(blockHeight);
      return testDogecoinBlockSnapshot(blockHeight);
    },
  };
}
