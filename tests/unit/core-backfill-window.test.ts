import {
  type BlockchainRpcPort,
  type CoordinatorConfigPort,
  type CoreDogecoinApplyContext,
  type CoreDogecoinBlockApplication,
  CoreDogecoinIndexerService,
  type CoreDogecoinIndexerSettings,
  type CoreDogecoinStateStorePort,
  type CoreIndexerState,
  configKeyDogecoinCurrentStateMaterialization,
  configKeyDogecoinCurrentStateReady,
  configKeyDogecoinHistoryReady,
  configKeyDogecoinTransactionRefsReady,
  configKeyIndexerProcessTail,
  configKeyIndexerStage,
  coreSnapshotRowCount,
  type RawBlockStoragePort,
} from '@onlydoge/indexing-pipeline';
import { afterEach, describe, expect, it, vi } from 'vitest';

describe('core backfill windows', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('reads finalized backfill windows from the node instead of raw storage', async () => {
    const harness = backfillHarness({
      latest: 1_000,
      settings: { coreBackfillBlockSource: 'auto', coreBackfillWindowBlocks: 20 },
      state: { processTail: -1, syncTail: 49 },
    });

    await expect(harness.service.runOnce()).resolves.toBe(true);

    expect(harness.appliedWindows).toEqual([range(0, 19)]);
    expect(harness.storageReads).toEqual([]);
    expect(harness.nodeReads.flat()).toEqual(expect.arrayContaining(range(0, 19)));
    expect(harness.state().processTail).toBe(19);
  });

  it('closes a window as soon as it holds the row target', async () => {
    const harness = backfillHarness({
      latest: 1_000,
      settings: {
        coreBackfillBlockSource: 'node',
        coreBackfillWindowBlocks: 50,
        // Every test block is one coinbase input plus one output: two rows.
        coreBackfillWindowRows: 10,
      },
      state: { processTail: -1, syncTail: 99 },
    });

    await harness.service.runOnce();
    await harness.service.runOnce();

    expect(harness.appliedWindows).toEqual([range(0, 4), range(5, 9)]);
  });

  it('reads ahead of the window it is applying', async () => {
    const harness = backfillHarness({
      latest: 1_000,
      settings: {
        coreBackfillBlockSource: 'node',
        coreBackfillWindowBlocks: 4,
        syncBatchSize: 2,
        syncConcurrency: 2,
      },
      state: { processTail: -1, syncTail: 99 },
    });

    await harness.service.runOnce();

    // Window 0-3 was applied; the next window's blocks are already fetched.
    expect(harness.appliedWindows).toEqual([range(0, 3)]);
    expect(Math.max(...harness.nodeReads.flat())).toBeGreaterThanOrEqual(7);

    const readsBeforeSecondWindow = harness.nodeReads.flat().length;
    await harness.service.runOnce();
    expect(harness.appliedWindows[1]).toEqual(range(4, 7));
    // The second window cost no re-read of its own blocks.
    expect(harness.nodeReads.flat().filter((height) => height <= 7)).toHaveLength(8);
    expect(readsBeforeSecondWindow).toBeGreaterThanOrEqual(8);
  });

  it('keeps reorg-window heights on the snapshots raw sync stored', async () => {
    const harness = backfillHarness({
      latest: 12,
      settings: {
        coreBackfillBlockSource: 'auto',
        coreBackfillWindowBlocks: 100,
        coreReprocessDepth: 10,
      },
      state: { processTail: -1, syncTail: 12 },
    });

    await harness.service.runOnce();
    await harness.service.runOnce();

    // Heights 0-2 are finalized (tip 12 - depth 10); everything above is not.
    expect(harness.appliedWindows).toEqual([range(0, 2), range(3, 12)]);
    expect(harness.nodeReads.flat().sort((a, b) => a - b)).toEqual(range(0, 2));
    expect(harness.storageReads.sort((a, b) => a - b)).toEqual(range(3, 12));
  });

  it('falls back to raw storage when the node fails and retries the node later', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date('2026-09-30T00:00:00.000Z'));
    let nodeHealthy = false;
    const harness = backfillHarness({
      latest: 1_000,
      nodeFailure: () => (nodeHealthy ? null : new Error('could not connect to node')),
      settings: { coreBackfillBlockSource: 'auto', coreBackfillWindowBlocks: 5 },
      state: { processTail: -1, syncTail: 99 },
    });

    await expect(harness.service.runOnce()).resolves.toBe(true);
    expect(harness.appliedWindows).toEqual([range(0, 4)]);
    expect(harness.storageReads).toEqual(expect.arrayContaining(range(0, 4)));
    expect(harness.warnings).toContain('node block read failed; processing from raw storage');

    // While the node is cooling down, windows go straight to storage.
    nodeHealthy = true;
    const nodeReadsDuringCooldown = harness.nodeReads.length;
    await harness.service.runOnce();
    expect(harness.appliedWindows[1]).toEqual(range(5, 9));
    expect(harness.nodeReads).toHaveLength(nodeReadsDuringCooldown);

    vi.setSystemTime(new Date('2026-09-30T00:02:00.000Z'));
    harness.storageReads.length = 0;
    await harness.service.runOnce();
    expect(harness.appliedWindows[2]).toEqual(range(10, 14));
    expect(harness.nodeReads.flat()).toEqual(expect.arrayContaining(range(10, 14)));
    expect(harness.storageReads.filter((height) => height >= 10 && height <= 14)).toEqual([]);
  });

  it('does not fall back when the node is the only configured source', async () => {
    const harness = backfillHarness({
      latest: 1_000,
      nodeFailure: () => new Error('could not connect to node'),
      settings: { coreBackfillBlockSource: 'node', coreBackfillWindowBlocks: 5 },
      state: { processTail: -1, syncTail: 99 },
    });

    await expect(harness.service.runOnce()).rejects.toThrow('could not connect to node');
    expect(harness.appliedWindows).toEqual([]);
    expect(harness.storageReads).toEqual([]);
  });

  it('halves the row target after a slow apply and grows it back after fast ones', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date('2026-09-30T00:00:00.000Z'));
    let applyDurationMs = 20_000;
    const harness = backfillHarness({
      latest: 100_000,
      onApply: () => vi.setSystemTime(Date.now() + applyDurationMs),
      settings: {
        coreBackfillBlockSource: 'node',
        coreBackfillWindowBlocks: 1_000,
        coreBackfillWindowRows: 40_000,
        coreDbStatementTimeoutMs: 30_000,
        coreProgressWatchdogMs: 0,
      },
      state: { processTail: -1, syncTail: 99_999 },
    });
    const windowSizes = () => harness.appliedWindows.map((window) => window.length);

    // 40,000 rows at two rows per block would be 20,000 blocks: the block
    // limit caps the first window.
    await harness.service.runOnce();
    expect(windowSizes()).toEqual([1_000]);

    // A 20s apply used more than half of the 30s statement budget.
    applyDurationMs = 1_000;
    harness.setWindowBlocks(100_000);
    await harness.service.runOnce();
    expect(windowSizes()[1]).toBe(10_000);

    // Fast applies grow the target back by half each window, up to the limit.
    await harness.service.runOnce();
    expect(windowSizes()[2]).toBe(15_000);
    await harness.service.runOnce();
    expect(windowSizes()[3]).toBe(20_000);
    await harness.service.runOnce();
    expect(windowSizes()[4]).toBe(20_000);
  });

  it('halves the row target when an apply fails', async () => {
    let failNextApply = true;
    const harness = backfillHarness({
      latest: 100_000,
      onApply: () => {
        if (failNextApply) {
          failNextApply = false;
          throw new Error('warehouse request timed out after 30000ms');
        }
      },
      settings: {
        coreBackfillBlockSource: 'node',
        coreBackfillWindowBlocks: 100_000,
        coreBackfillWindowRows: 40_000,
      },
      state: { processTail: -1, syncTail: 99_999 },
    });

    await expect(harness.service.runOnce()).rejects.toThrow('warehouse request timed out');
    await harness.service.runOnce();

    expect(harness.appliedWindows.map((window) => window.length)).toEqual([10_000]);
  });

  it('marks transaction refs ready once processing caught up, without re-reading blocks', async () => {
    const harness = backfillHarness({
      latest: 1_000,
      settings: { coreBackfillBlockSource: 'node', coreBackfillWindowBlocks: 10 },
      state: { processTail: -1, syncTail: 19 },
    });

    await harness.service.runOnce();
    expect(harness.values.get(configKeyDogecoinTransactionRefsReady())).toBeUndefined();
    await harness.service.runOnce();
    expect(harness.state().processTail).toBe(19);
    expect(harness.values.get(configKeyDogecoinTransactionRefsReady())).toBeUndefined();

    // The next iteration sees processing level with raw sync.
    await harness.service.runOnce();
    expect(harness.values.get(configKeyDogecoinTransactionRefsReady())).toBe(true);
    expect(harness.storageReads).toEqual([]);
    expect(harness.transactionRefWrites).toBe(0);
  });

  it('publishes progress in one batched write when the coordinator supports it', async () => {
    const harness = backfillHarness({
      batchedConfigWrites: true,
      latest: 1_000,
      settings: { coreBackfillBlockSource: 'node', coreBackfillWindowBlocks: 10 },
      state: { processTail: -1, syncTail: 99 },
    });

    await harness.service.runOnce();

    // One publish before the stage runs, one after the window.
    expect(harness.batchedWrites).toHaveLength(2);
    expect(harness.batchedWrites[1]?.map(([key]) => key)).toContain(configKeyIndexerProcessTail());
    expect(harness.values.get(configKeyIndexerProcessTail())).toBe(9);
    expect(harness.singleWrites).not.toContain(configKeyIndexerProcessTail());
  });

  it('skips the lease round trip while its own lease is fresh', async () => {
    const harness = backfillHarness({
      latest: 1_000,
      settings: { coreBackfillBlockSource: 'node', coreBackfillWindowBlocks: 10 },
      state: { processTail: -1, syncTail: 99 },
    });

    await harness.service.runOnce();
    const leaseReadsAfterClaim = harness.leaseReads();
    await harness.service.runOnce();
    await harness.service.runOnce();

    expect(leaseReadsAfterClaim).toBe(1);
    expect(harness.leaseReads()).toBe(1);
    expect(harness.appliedWindows).toHaveLength(3);
  });

  it('counts the rows a snapshot produces and tolerates malformed snapshots', () => {
    expect(coreSnapshotRowCount(testSnapshot(7))).toBe(2);
    expect(
      coreSnapshotRowCount({
        block: { tx: [{ vin: [{}, {}], vout: [{}, {}, {}] }, { vout: [{}] }] },
      }),
    ).toBe(6);
    expect(coreSnapshotRowCount({})).toBe(0);
    expect(coreSnapshotRowCount({ block: { tx: 'not-a-list' } })).toBe(0);
    expect(coreSnapshotRowCount({ block: { tx: [null, 'junk', { vin: 'x' }] } })).toBe(0);
  });
});

describe('core backfill recovery and completion', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('rewinds the recorded tail to what the warehouse holds, once per start', async () => {
    const harness = backfillHarness({
      latest: 1_000,
      settings: { coreBackfillBlockSource: 'node', coreBackfillWindowBlocks: 10 },
      state: { processTail: 49, syncTail: 99 },
      warehouseTail: 29,
    });

    await harness.service.runOnce();

    // Partial rows above the warehouse tail are cleaned before it is replayed.
    expect(harness.recoveries).toHaveLength(1);
    expect(harness.recoveries[0]?.fromHeight).toBe(30);
    expect(harness.recoveries[0]?.context).toMatchObject({
      updateCurrentState: false,
      validatePrevouts: false,
    });
    expect(harness.stateUpdates[0]).toEqual({ processTail: 29 });
    expect(harness.appliedWindows).toEqual([range(30, 39)]);
    expect(harness.warnings).toContain(
      'warehouse is behind the recorded process tail; rewinding to the warehouse tail',
    );

    await harness.service.runOnce();
    expect(harness.warehouseTailReads()).toBe(1);
    expect(harness.recoveries).toHaveLength(1);
    expect(harness.appliedWindows[1]).toEqual(range(40, 49));
  });

  it('replays from the first block when the warehouse came back empty', async () => {
    const harness = backfillHarness({
      latest: 1_000,
      settings: { coreBackfillBlockSource: 'node', coreBackfillWindowBlocks: 10 },
      state: { processTail: 49, syncTail: 99 },
      warehouseTail: null,
    });

    await harness.service.runOnce();

    expect(harness.recoveries[0]?.fromHeight).toBe(0);
    expect(harness.appliedWindows).toEqual([range(0, 9)]);
  });

  it('keeps the recorded tail when the warehouse is level with it or cannot tell', async () => {
    for (const warehouseTail of [49, 60]) {
      const harness = backfillHarness({
        latest: 1_000,
        settings: { coreBackfillBlockSource: 'node', coreBackfillWindowBlocks: 10 },
        state: { processTail: 49, syncTail: 99 },
        warehouseTail,
      });

      await harness.service.runOnce();

      expect(harness.recoveries).toEqual([]);
      expect(harness.appliedWindows).toEqual([range(50, 59)]);
    }

    const unaware = backfillHarness({
      latest: 1_000,
      settings: { coreBackfillBlockSource: 'node', coreBackfillWindowBlocks: 10 },
      state: { processTail: 49, syncTail: 99 },
    });
    await unaware.service.runOnce();
    expect(unaware.recoveries).toEqual([]);
    expect(unaware.appliedWindows).toEqual([range(50, 59)]);
  });

  it('goes back to raw sync when the tip moved away and nothing is materializing', async () => {
    const harness = backfillHarness({
      latest: 500,
      settings: { coreBackfillBlockSource: 'node' },
      state: { processTail: 100, syncTail: 100 },
    });

    await harness.service.runOnce();

    expect(harness.materializations).toEqual([]);
    expect(harness.state().stage).toBe('sync_backfill');
  });

  it('finishes a checkpointed materialization even though the tip moved away', async () => {
    const harness = backfillHarness({
      latest: 500,
      settings: { coreBackfillBlockSource: 'node' },
      state: { processTail: 100, syncTail: 100 },
    });
    harness.values.set(configKeyDogecoinCurrentStateMaterialization(), {
      asOfBlockHeight: 100,
      completedRanges: 40,
      rangeCount: 258,
    });

    await harness.service.runOnce();

    expect(harness.materializations.map((call) => call.asOfBlockHeight)).toEqual([100]);
    expect(harness.state().stage).toBe('online');
    expect(harness.values.get(configKeyIndexerStage())).toBe('online');
    expect(harness.values.get(configKeyDogecoinHistoryReady())).toBe(true);
  });

  it('does not resume a checkpoint taken for another tail', async () => {
    const harness = backfillHarness({
      latest: 500,
      settings: { coreBackfillBlockSource: 'node' },
      state: { processTail: 100, syncTail: 100 },
    });
    harness.values.set(configKeyDogecoinCurrentStateMaterialization(), {
      asOfBlockHeight: 90,
      completedRanges: 40,
      rangeCount: 258,
    });

    await harness.service.runOnce();

    expect(harness.materializations).toEqual([]);
    expect(harness.state().stage).toBe('sync_backfill');
  });

  it('skips materialization when current state is already maintained', async () => {
    const harness = backfillHarness({
      latest: 102,
      settings: { coreBackfillBlockSource: 'node' },
      state: { processTail: 100, syncTail: 100 },
    });
    harness.values.set(configKeyDogecoinCurrentStateReady(), true);

    await harness.service.runOnce();

    expect(harness.materializations).toEqual([]);
    expect(harness.state().stage).toBe('online');
  });

  it('gives materialization the block budget and reports life while it runs', async () => {
    vi.useFakeTimers({ toFake: ['Date'] });
    vi.setSystemTime(new Date('2026-09-30T00:00:00.000Z'));
    const heartbeats: number[] = [];
    const harness = backfillHarness({
      latest: 102,
      onMaterialize: async (context) => {
        const heartbeatCount = () =>
          harness.stateUpdates.filter((update) => update.stage === undefined).length;

        // The first finished range refreshes the state row at once.
        await context?.materialization?.onRangeCompleted?.({ completedRanges: 1, rangeCount: 258 });
        heartbeats.push(heartbeatCount());
        // Further statements inside the same half minute only count as activity.
        vi.setSystemTime(Date.now() + 10_000);
        await context?.materialization?.onRangeCompleted?.({ completedRanges: 2, rangeCount: 258 });
        await context?.materialization?.onActivity?.();
        heartbeats.push(heartbeatCount());
        vi.setSystemTime(Date.now() + 25_000);
        await context?.materialization?.onActivity?.();
        heartbeats.push(heartbeatCount());
      },
      settings: {
        coreBackfillBlockSource: 'node',
        coreBlockTimeoutMs: 300_000,
        coreDbStatementTimeoutMs: 120_000,
      },
      state: { processTail: 100, syncTail: 100 },
    });

    await harness.service.runOnce();

    expect(harness.materializations[0]?.context?.statementTimeoutMs).toBe(300_000);
    expect(heartbeats).toEqual([1, 1, 2]);
    expect(harness.stateUpdates.find((update) => update.stage === undefined)).toEqual({
      lastError: null,
      onlineTip: 102,
    });
    expect(harness.state().stage).toBe('online');
  });
});

interface HarnessInput {
  batchedConfigWrites?: boolean;
  latest: number;
  nodeFailure?: () => Error | null;
  onApply?: () => void;
  onMaterialize?: (context: CoreDogecoinApplyContext | undefined) => Promise<void> | void;
  settings?: Partial<CoreDogecoinIndexerSettings>;
  state: { processTail: number; syncTail: number };
  /** Highest processed block the warehouse holds; leave out for a store that cannot tell. */
  warehouseTail?: number | null;
}

function backfillHarness(input: HarnessInput) {
  const values = new Map<string, unknown>();
  const appliedWindows: number[][] = [];
  const nodeReads: number[][] = [];
  const storageReads: number[] = [];
  const warnings: string[] = [];
  const batchedWrites: Array<Array<readonly [string, unknown]>> = [];
  const singleWrites: string[] = [];
  const materializations: Array<{
    asOfBlockHeight: number;
    context: CoreDogecoinApplyContext | undefined;
  }> = [];
  const recoveries: Array<{ context: CoreDogecoinApplyContext | undefined; fromHeight: number }> =
    [];
  const stateUpdates: Array<Parameters<CoreDogecoinStateStorePort['upsertCoreIndexerState']>[0]> =
    [];
  let leaseReads = 0;
  let warehouseTailReads = 0;
  let transactionRefWrites = 0;
  let state: CoreIndexerState = {
    lastError: null,
    onlineTip: input.latest,
    processTail: input.state.processTail,
    stage: 'process_backfill',
    syncTail: input.state.syncTail,
    updatedAt: new Date().toISOString(),
  };
  const settings = testSettings(input.settings);

  const configs: CoordinatorConfigPort = {
    async compareAndDeleteJsonValue(key, expectedValue) {
      if (JSON.stringify(values.get(key) ?? null) !== JSON.stringify(expectedValue)) {
        return false;
      }
      values.delete(key);
      return true;
    },
    async compareAndSwapJsonValue(key, expectedValue, nextValue) {
      if ((values.get(key) ?? null) !== expectedValue) {
        return false;
      }
      values.set(key, nextValue);
      return true;
    },
    async deleteByPrefix() {},
    async getJsonValue<T>(key: string) {
      if (key === 'primary') {
        leaseReads += 1;
      }
      return (values.get(key) as T | undefined) ?? null;
    },
    async setJsonValue(key, value) {
      singleWrites.push(key);
      values.set(key, value);
    },
    ...(input.batchedConfigWrites
      ? {
          async setJsonValues(entries: ReadonlyArray<readonly [string, unknown]>) {
            batchedWrites.push([...entries]);
            for (const [key, value] of entries) {
              values.set(key, value);
            }
          },
        }
      : {}),
  };

  const rawBlocks: RawBlockStoragePort = {
    async getPart<T extends Record<string, unknown>>(blockHeight: number): Promise<T | null> {
      storageReads.push(blockHeight);
      return testSnapshot(blockHeight) as T;
    },
    async putPart() {},
  };

  const rpc: BlockchainRpcPort = {
    async getBlockHeight() {
      return input.latest;
    },
    async getBlockSnapshot(_dogecoin, blockHeight) {
      return testSnapshot(blockHeight);
    },
    async getBlockSnapshots(_dogecoin, blockHeights) {
      const failure = input.nodeFailure?.() ?? null;
      if (failure) {
        throw failure;
      }
      nodeReads.push(blockHeights);
      return blockHeights.map(testSnapshot);
    },
  };

  const stateStore: CoreDogecoinStateStorePort = {
    async applyCoreDogecoinBlock() {
      throw new Error('window processing should not apply individual blocks');
    },
    async applyCoreDogecoinWindow(
      applications: CoreDogecoinBlockApplication[],
      _context?: CoreDogecoinApplyContext,
    ) {
      input.onApply?.();
      appliedWindows.push(applications.map((application) => application.blockHeight));
      return { applied: true, processTail: applications.at(-1)?.blockHeight ?? state.processTail };
    },
    async getCoreIndexerState() {
      return state;
    },
    async getCoreUtxoOutputs() {
      return new Map();
    },
    ...('warehouseTail' in input
      ? {
          async getCoreProcessedTail() {
            warehouseTailReads += 1;
            return input.warehouseTail;
          },
        }
      : {}),
    async materializeCoreDogecoinCurrentState(asOfBlockHeight, context) {
      materializations.push({ asOfBlockHeight, context });
      await input.onMaterialize?.(context);
    },
    async recoverCoreDogecoinWindow(fromHeight, context) {
      recoveries.push({ context, fromHeight });
    },
    async setCoreIndexerError() {},
    async setCoreIndexerStage() {},
    async upsertCoreBlock() {},
    async upsertCoreIndexerState(update) {
      stateUpdates.push(update);
      state = {
        ...state,
        ...update,
        lastError: update.lastError === undefined ? state.lastError : update.lastError,
        updatedAt: new Date().toISOString(),
      };
      return state;
    },
    async upsertTransactionRefs() {
      transactionRefWrites += 1;
    },
  };

  const service = new CoreDogecoinIndexerService(
    configs,
    {
      async getDogecoinConfig() {
        return {
          architecture: 'dogecoin' as const,
          blockTime: 60,
          id: 'dogecoin',
          rpcEndpoint: 'https://doge.example/rpc',
          rps: 10,
        };
      },
    },
    rawBlocks,
    rpc,
    stateStore,
    settings,
    {
      exitProcess(code): never {
        throw new Error(`unexpected process exit ${code}`);
      },
      logger: {
        error: () => {},
        info: () => {},
        warn: (_bindings, message) => {
          warnings.push(message);
        },
      },
    },
  );

  return {
    appliedWindows,
    batchedWrites,
    leaseReads: () => leaseReads,
    materializations,
    nodeReads,
    recoveries,
    service,
    setWindowBlocks(blocks: number) {
      settings.coreBackfillWindowBlocks = blocks;
    },
    singleWrites,
    state: () => state,
    stateUpdates,
    storageReads,
    get transactionRefWrites() {
      return transactionRefWrites;
    },
    values,
    warehouseTailReads: () => warehouseTailReads,
    warnings,
  };
}

function testSettings(
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
    leaseHeartbeatIntervalMs: 5_000,
    syncBatchSize: 16,
    syncConcurrency: 4,
    syncRetryAttempts: 1,
    syncRetryBaseDelayMs: 1,
    syncWindow: 32,
    ...overrides,
  };
}

function testSnapshot(blockHeight: number): Record<string, unknown> {
  return {
    block: {
      hash: `doge-block-${blockHeight}`,
      height: blockHeight,
      previousblockhash: blockHeight > 0 ? `doge-block-${blockHeight - 1}` : null,
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

function range(start: number, end: number): number[] {
  return Array.from({ length: end - start + 1 }, (_value, index) => start + index);
}
