import { randomUUID } from 'node:crypto';

import { noopServiceLogger, type ServiceLogger } from '@onlydoge/shared-kernel';

import type {
  BlockchainRpcPort,
  CoordinatorConfigEntry,
  CoordinatorConfigPort,
  CoreDogecoinStateStorePort,
  DogecoinConfigPort,
  RawBlockStoragePort,
} from '../contracts/ports';
import { fromDecimalUnits } from '../domain/amounts';
import {
  configKeyBlockHeight,
  configKeyCoreApplyRecovery,
  configKeyDogecoinAnalyticsFactsReady,
  configKeyDogecoinAnalyticsFactsTail,
  configKeyDogecoinCurrentStateMaterialization,
  configKeyDogecoinCurrentStateReady,
  configKeyDogecoinHistoryReady,
  configKeyDogecoinTransactionRefsReady,
  configKeyIndexerFactProgress,
  configKeyIndexerFactTail,
  configKeyIndexerFinalizedTail,
  configKeyIndexerLastActivityAt,
  configKeyIndexerProcessBlocksPerSecond,
  configKeyIndexerProcessEtaSeconds,
  configKeyIndexerProcessProgress,
  configKeyIndexerProcessTail,
  configKeyIndexerReprocessDepth,
  configKeyIndexerStage,
  configKeyIndexerSyncBlocksPerSecond,
  configKeyIndexerSyncEtaSeconds,
  configKeyIndexerSyncProgress,
  configKeyIndexerSyncTail,
  configKeyPrimary,
} from '../domain/config-keys';
import {
  type CoreApplyRecoveryMarkerV1,
  createCoreApplyRecoveryMarker,
  parseCoreApplyRecoveryMarker,
} from '../domain/core-apply-recovery';
import {
  type DogecoinTransaction,
  type DogecoinVin,
  type DogecoinVout,
  extractDogecoinOutputAddress,
  isDogecoinTransaction,
  type ParsedDogecoinBlock,
} from '../domain/dogecoin-block';
import type {
  CoreDogecoinApplyResult,
  CoreDogecoinBlockApplication,
  CoreIndexerState,
  ProjectionUtxoOutput,
} from '../domain/projection-models';
import { mapWithConcurrency, range } from './concurrency';
import { CoreBlockPrefetcher } from './core-block-prefetcher';
import type { CoreDogecoinIndexerSettings } from './core-dogecoin-indexer-settings';
import { RawBlockSyncer, type RawBlockSyncHooks, rawBlockPart } from './raw-block-sync';

interface PrimaryLease {
  heartbeatAt: string;
  instanceId: string;
}

export interface CoreDogecoinIndexerServiceOptions {
  exitProcess?: (code: number) => never;
  logger?: ServiceLogger;
}

interface DogecoinRuntimeConfig {
  architecture: 'dogecoin';
  blockTime: number;
  id: string;
  rpcEndpoint: string;
  rps: number;
  zmqBlockEndpoint?: string | null;
}

const workerIdleMs = 250;
const loopFailureBackoffMaxMs = 30_000;
const throughputSmoothing = 0.3;
/** A backfill window stops filling after this long, however few blocks it holds. */
const backfillWindowMaxFillMs = 10_000;
/** Floor for the adaptive backfill row target. */
const backfillWindowMinRows = 5_000;
/** How long `auto` keeps reading raw storage after a node read failed. */
const nodeSourceRetryDelayMs = 60_000;
/** How often a running materialization refreshes indexer state and logs progress. */
const materializationHeartbeatMs = 30_000;

interface ProgressObservation {
  observedAtMs: number;
  processTail: number;
  stage: CoreIndexerState['stage'];
  syncTail: number;
}

interface CoreBlockAttempt {
  activeStep: CoreBlockStep;
  height: number;
  startedAtMs: number;
}

interface CoreBlockMetrics {
  applyMs: number;
  applied: boolean;
  buildMs: number;
  creates: number;
  loadRawMs: number;
  spends: number;
  totalMs: number;
}

interface CoreWindowMetrics extends CoreBlockMetrics {
  blocks: number;
  end: number;
  source: CoreWindowSource;
  start: number;
}

interface CoreProcessWindowBounds {
  firstHeight: number;
  lastHeight: number;
}

type CoreWindowSource = 'node' | 'storage';

/**
 * One processing window: the height range it may cover and how its block
 * snapshots are loaded. Backfill windows may load fewer blocks than the range
 * allows (row target reached, fill deadline hit); fixed windows load all of it.
 */
interface CoreWindowPlan extends CoreProcessWindowBounds {
  load: () => Promise<Record<string, unknown>[]>;
  source: CoreWindowSource;
}

interface CoreBackfillLoader {
  prefetcher: CoreBlockPrefetcher;
  source: CoreWindowSource;
}

interface CoreWindowKeyTracker {
  createdOutputKeys: Set<string>;
  spentOutputKeys: Set<string>;
}

interface CoreTransactionEffects {
  utxoCreates: ProjectionUtxoOutput[];
  utxoSpends: CoreDogecoinBlockApplication['utxoSpends'];
}

interface CoreWindowMetricsInput {
  applications: CoreDogecoinBlockApplication[];
  applyMs: number;
  applyResult: CoreDogecoinApplyResult;
  bounds: CoreProcessWindowBounds;
  buildMs: number;
  loadRawMs: number;
  source: CoreWindowSource;
  totalStartedAt: number;
}

type CoreBlockStep = 'load_raw' | 'build_application' | 'apply_state' | 'publish_progress';

class CoreBlockTimeoutError extends Error {
  public constructor(
    public readonly step: CoreBlockStep,
    public readonly timeoutMs: number,
  ) {
    super(`core block step timed out step=${step} timeout_ms=${timeoutMs}`);
  }
}

class PrimaryLeaseLostError extends Error {
  public constructor() {
    super('core indexer primary lease lost');
  }
}

export class CoreDogecoinIndexerService {
  private readonly instanceId = randomUUID();
  private readonly logger: ServiceLogger;
  private readonly syncer: RawBlockSyncer;
  private activeBlockAttempt: CoreBlockAttempt | null = null;
  private backfillLoader: CoreBackfillLoader | null = null;
  private backfillTargetRows: number | null = null;
  private consecutiveLoopFailures = 0;
  private lastActivityAtMs = Date.now();
  private lastMaterializationHeartbeatAtMs = 0;
  private nodeSourceRetryAtMs = 0;
  private primaryLease: PrimaryLease | null = null;
  private processBlocksPerSecond: number | null = null;
  private progressObservation: ProgressObservation | null = null;
  private syncBlocksPerSecond: number | null = null;
  private transactionRefsReady = false;
  private warehouseTailReconciled = false;

  public constructor(
    private readonly configs: CoordinatorConfigPort,
    private readonly dogecoin: DogecoinConfigPort,
    private readonly rawBlocks: RawBlockStoragePort,
    private readonly rpc: BlockchainRpcPort,
    private readonly stateStore: CoreDogecoinStateStorePort,
    private readonly settings: CoreDogecoinIndexerSettings,
    private readonly options: CoreDogecoinIndexerServiceOptions = {},
  ) {
    this.logger = options.logger ?? noopServiceLogger();
    this.syncer = new RawBlockSyncer(
      rpc,
      rawBlocks,
      stateStore,
      (snapshot) => {
        const block = parseDogecoinBlockSnapshot(snapshot);
        return {
          hash: block.hash,
          height: block.height,
          previousHash: block.previousHash,
          time: block.time,
          txids: block.tx.map((transaction) => requireString(transaction.txid, 'tx.txid')),
        };
      },
      settings,
      { logger: this.logger },
    );
  }

  // fallow-ignore-next-line unused-class-member
  public async start(signal?: AbortSignal): Promise<void> {
    this.logger.info(
      { component: 'core-indexer', instanceId: this.instanceId },
      'indexer loop started',
    );
    while (shouldContinueStartLoop(signal)) {
      await this.runStartLoopIteration();
    }
  }

  public async runOnce(): Promise<boolean> {
    if (!(await this.leaseLeadership())) {
      return false;
    }

    return this.runWithPrimaryLeaseHeartbeat(() => this.runDogecoin());
  }

  private async runStartLoopIteration(): Promise<void> {
    try {
      await this.runPrimaryLoopWork();
      this.consecutiveLoopFailures = 0;
    } catch (error) {
      this.consecutiveLoopFailures += 1;
      const backoffMs = loopFailureBackoffMs(this.consecutiveLoopFailures);
      this.logger.error(
        { ...this.errorBindings(error), backoffMs, failures: this.consecutiveLoopFailures },
        'core indexer loop failed',
      );
      await sleep(backoffMs);
    }
  }

  private async runPrimaryLoopWork(): Promise<void> {
    if (!(await this.leaseLeadership())) {
      await sleep(1_000);
      return;
    }

    await this.runWithPrimaryLeaseHeartbeat(async () => {
      const didWork = await this.runDogecoin();
      if (!didWork) {
        await sleep(workerIdleMs);
      }
      return didWork;
    });
  }

  private async runDogecoin(): Promise<boolean> {
    const dogecoin = await this.dogecoin.getDogecoinConfig();
    return this.runDogecoinConfig(dogecoin);
  }

  private async runDogecoinConfig(dogecoin: DogecoinRuntimeConfig): Promise<boolean> {
    const latest = await this.rpc.getBlockHeight(dogecoin);
    this.assertPrimaryLease();

    try {
      await this.recoverPendingCoreApplyIfNeeded(dogecoin);

      const state = await this.reconcileProcessTailWithWarehouse(
        dogecoin,
        await this.ensureState(dogecoin, latest),
      );
      await this.markTransactionRefsReadyIfCaughtUp(dogecoin, state);
      await this.publishProgress(latest, state);
      await this.assertProgressWatchdog(dogecoin, latest, state);
      return await this.runDogecoinStage(dogecoin, latest, state);
    } catch (error) {
      if (error instanceof PrimaryLeaseLostError) {
        throw error;
      }
      await this.stateStore.setCoreIndexerError(formatError(error));
      throw error;
    }
  }

  private runDogecoinStage(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    const stageRunners: Record<CoreIndexerState['stage'], () => Promise<boolean>> = {
      sync_backfill: () => this.syncBackfill(dogecoin, latest, state),
      process_backfill: () => this.processBackfill(dogecoin, latest, state),
      online: () => this.online(dogecoin, latest, state),
    };
    return stageRunners[state.stage]();
  }

  private async ensureState(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
  ): Promise<CoreIndexerState> {
    const current = await this.stateStore.getCoreIndexerState();
    if (current) {
      return current;
    }

    const storedSyncTail = await this.storedSyncTail();
    const syncTail = Math.min(storedSyncTail, latest);
    const state = await this.stateStore.upsertCoreIndexerState({
      stage: 'sync_backfill',
      syncTail,
      processTail: -1,
      onlineTip: latest,
      lastError: null,
    });
    this.logger.info(
      {
        chain: dogecoin.id,
        component: 'core-indexer',
        processTail: -1,
        stage: 'sync_backfill',
        syncTail,
      },
      'core indexer initialized',
    );
    return state;
  }

  /**
   * The process tail lives in the metadata database, the processed windows in
   * the warehouse, and the two do not share a durability boundary: after a
   * hard stop (host power loss, a killed VM) the warehouse can come back
   * without the newest parts while the recorded tail still points past them.
   * Continuing from that tail would leave a permanent hole, so once per
   * process start the tail is checked against what the warehouse actually
   * holds and rewound to it, cleaning any partial rows above it first.
   */
  private async reconcileProcessTailWithWarehouse(
    dogecoin: DogecoinRuntimeConfig,
    state: CoreIndexerState,
  ): Promise<CoreIndexerState> {
    if (this.warehouseTailReconciled) {
      return state;
    }

    const processedTail = await this.stateStore.getCoreProcessedTail?.();
    this.warehouseTailReconciled = true;
    if (processedTail === undefined) {
      return state;
    }

    const warehouseTail = processedTail ?? -1;
    if (warehouseTail >= state.processTail) {
      return state;
    }

    this.logger.warn(
      {
        chain: dogecoin.id,
        component: 'core-indexer',
        phase: 'core-apply-recovery',
        processTail: state.processTail,
        warehouseTail,
      },
      'warehouse is behind the recorded process tail; rewinding to the warehouse tail',
    );
    await this.stateStore.recoverCoreDogecoinWindow(warehouseTail + 1, {
      statementTimeoutMs: this.settings.coreDbStatementTimeoutMs,
      updateCurrentState: await this.isDogecoinCurrentStateReady(),
      validatePrevouts: false,
    });
    return this.stateStore.upsertCoreIndexerState({ processTail: warehouseTail });
  }

  private async storedSyncTail(): Promise<number> {
    const value = await this.configs.getJsonValue<number>(configKeyIndexerSyncTail());
    return value ?? -1;
  }

  private async syncBackfill(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    if (shouldPromoteToProcessBackfill(state, latest, this.settings.coreSyncCompleteDistance)) {
      await this.stateStore.upsertCoreIndexerState({
        stage: 'process_backfill',
        onlineTip: latest,
      });
      await this.configs.setJsonValue(configKeyIndexerStage(), 'process_backfill');
      this.logger.info(
        {
          chain: dogecoin.id,
          component: 'core-indexer',
          latest,
          stage: 'process_backfill',
          syncTail: state.syncTail,
        },
        'core stage changed',
      );
      return true;
    }

    const end = Math.min(latest, state.syncTail + this.settings.syncWindow);
    return this.syncRawBlockWindow(dogecoin, latest, state, end, 'sync_backfill');
  }

  private async syncRawBlockWindow(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
    end: number,
    stage: CoreIndexerState['stage'],
  ): Promise<boolean> {
    const heights = range(state.syncTail + 1, end);
    return this.syncRawBlockHeights(dogecoin, latest, state, heights, end, stage);
  }

  private async syncRawBlockHeights(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
    heights: number[],
    syncTail: number,
    stage: CoreIndexerState['stage'],
  ): Promise<boolean> {
    const result = await this.storeRawBlockHeights(dogecoin, heights, {
      onCheckpoint: (frontier) => this.checkpointSyncTail(latest, state, frontier, stage),
    });
    this.assertPrimaryLease();
    this.recordSyncThroughput(result.blocks, result.elapsedMs);

    const nextState = await this.stateStore.upsertCoreIndexerState({
      stage,
      syncTail,
      onlineTip: latest,
      lastError: null,
    });
    await this.publishProgress(latest, nextState);
    this.logger.info(
      {
        blockEnd: heights.at(-1) ?? syncTail,
        blockStart: heights.at(0) ?? state.syncTail + 1,
        blocksPerSecond: roundRate(this.syncBlocksPerSecond),
        chain: dogecoin.id,
        component: 'core-indexer',
        concurrency: this.syncer.currentConcurrency,
        elapsedMs: result.elapsedMs,
        failedAttempts: result.failedAttempts,
        latest,
        remaining: Math.max(0, latest - syncTail),
        rpcMs: result.rpcMs,
        storeMs: result.storeMs,
      },
      'core synced',
    );
    return true;
  }

  private async checkpointSyncTail(
    latest: number,
    state: CoreIndexerState,
    frontier: number,
    stage: CoreIndexerState['stage'],
  ): Promise<void> {
    if (frontier <= state.syncTail) {
      return;
    }

    this.assertPrimaryLease();
    const nextState = await this.stateStore.upsertCoreIndexerState({
      stage,
      syncTail: frontier,
      onlineTip: latest,
      lastError: null,
    });
    state.syncTail = frontier;
    this.observeProgress(nextState);
  }

  private storeRawBlockHeights(
    dogecoin: DogecoinRuntimeConfig,
    heights: number[],
    hooks: Pick<RawBlockSyncHooks, 'onCheckpoint'> = {},
  ) {
    return this.syncer.sync(dogecoin, heights, {
      ...hooks,
      assertActive: () => this.assertPrimaryLease(),
      onActivity: () => this.recordActivity(),
    });
  }

  private recordActivity(): void {
    this.lastActivityAtMs = Date.now();
  }

  private recordSyncThroughput(blocks: number, elapsedMs: number): void {
    this.syncBlocksPerSecond = smoothRate(this.syncBlocksPerSecond, blocks, elapsedMs);
  }

  private recordProcessThroughput(blocks: number, elapsedMs: number): void {
    this.processBlocksPerSecond = smoothRate(this.processBlocksPerSecond, blocks, elapsedMs);
  }

  /**
   * Transaction refs are written by raw sync for every block it stores, and
   * processed transactions always resolve through the core create table. The
   * only transactions that need a ref are the ones in synced-but-unprocessed
   * blocks, so the index is complete from the first moment processing has
   * caught up with raw sync: everything older is processed, everything newer
   * is synced with refs.
   *
   * This replaces a raw-block re-read backfill that fetched every stored block
   * a second time and whose output was deleted again by each window rewind.
   */
  private async markTransactionRefsReadyIfCaughtUp(
    dogecoin: DogecoinRuntimeConfig,
    state: CoreIndexerState,
  ): Promise<void> {
    if (this.transactionRefsReady || !hasProcessedEverySyncedBlock(state)) {
      return;
    }

    const ready =
      (await this.configs.getJsonValue<boolean>(configKeyDogecoinTransactionRefsReady())) === true;
    if (!ready) {
      await this.configs.setJsonValue(configKeyDogecoinTransactionRefsReady(), true);
      this.logger.info(
        {
          chain: dogecoin.id,
          component: 'core-indexer',
          throughHeight: state.syncTail,
        },
        'transaction refs backfill complete',
      );
    }
    this.transactionRefsReady = true;
  }

  private async processBackfill(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    const currentStateReady =
      (await this.configs.getJsonValue<boolean>(configKeyDogecoinCurrentStateReady())) === true;

    if (state.processTail >= state.syncTail) {
      return this.transitionCompletedBackfill(dogecoin, latest, state, currentStateReady);
    }

    return this.processBackfillWindow(dogecoin, latest, state, currentStateReady);
  }

  private async transitionCompletedBackfill(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
    currentStateReady: boolean,
  ): Promise<boolean> {
    if (await this.shouldPromoteBackfill(latest, state, currentStateReady)) {
      await this.promoteBackfillToOnline(dogecoin, latest, state, currentStateReady);
      return true;
    }

    await this.returnBackfillToSync(dogecoin, latest, state);
    return true;
  }

  private async shouldPromoteBackfill(
    latest: number,
    state: CoreIndexerState,
    currentStateReady: boolean,
  ): Promise<boolean> {
    if (state.processTail >= latest - this.settings.coreOnlineTipDistance) {
      return true;
    }
    if (currentStateReady) {
      return false;
    }

    // A materialization that already started for this tail is finished first,
    // however far the tip moved meanwhile. Going back to sync would advance the
    // tail and throw the partial materialization away; the online stage catches
    // up the blocks that arrived in between.
    const pending = await this.configs.getJsonValue<{ asOfBlockHeight?: number }>(
      configKeyDogecoinCurrentStateMaterialization(),
    );
    return pending?.asOfBlockHeight === state.processTail;
  }

  private async promoteBackfillToOnline(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
    currentStateReady: boolean,
  ): Promise<void> {
    await this.materializeCurrentStateIfNeeded(dogecoin, latest, state, currentStateReady);
    await this.stateStore.upsertCoreIndexerState({
      stage: 'online',
      onlineTip: latest,
      lastError: null,
    });
    await Promise.all([
      this.configs.setJsonValue(configKeyIndexerStage(), 'online'),
      this.configs.setJsonValue(configKeyDogecoinHistoryReady(), true),
      this.configs.setJsonValue(configKeyIndexerFactTail(), state.processTail),
      this.configs.setJsonValue(
        configKeyIndexerFactProgress(),
        toProgress(state.processTail, latest),
      ),
    ]);
    this.logger.info(
      {
        chain: dogecoin.id,
        component: 'core-indexer',
        latest,
        processTail: state.processTail,
        stage: 'online',
      },
      'core stage changed',
    );
  }

  private async materializeCurrentStateIfNeeded(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
    currentStateReady: boolean,
  ): Promise<void> {
    if (currentStateReady) {
      return;
    }

    this.logger.info(
      {
        asOfBlockHeight: state.processTail,
        chain: dogecoin.id,
        component: 'core-indexer',
        phase: 'core-current-state-materialization',
      },
      'core current state materialization started',
    );
    await this.stateStore.materializeCoreDogecoinCurrentState(state.processTail, {
      // One materialization statement covers a whole key range, not one window.
      statementTimeoutMs: Math.max(
        this.settings.coreDbStatementTimeoutMs,
        this.settings.coreBlockTimeoutMs,
      ),
      materialization: {
        onActivity: () => this.materializationHeartbeat(dogecoin, latest, null),
        onRangeCompleted: (progress) => this.materializationHeartbeat(dogecoin, latest, progress),
      },
    });
    this.logger.info(
      {
        asOfBlockHeight: state.processTail,
        chain: dogecoin.id,
        component: 'core-indexer',
        phase: 'core-current-state-materialization',
      },
      'core current state materialization completed',
    );
  }

  /**
   * Materialization advances no tail, so without this the indexer would look
   * stalled for as long as it runs: every finished statement counts as
   * activity, and twice a minute the state row is refreshed for the health
   * check and progress is logged.
   */
  private async materializationHeartbeat(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    progress: { completedRanges: number; rangeCount: number } | null,
  ): Promise<void> {
    this.recordActivity();
    this.assertPrimaryLease();
    if (Date.now() - this.lastMaterializationHeartbeatAtMs < materializationHeartbeatMs) {
      return;
    }

    this.lastMaterializationHeartbeatAtMs = Date.now();
    await this.stateStore.upsertCoreIndexerState({ onlineTip: latest, lastError: null });
    this.logger.info(
      {
        chain: dogecoin.id,
        component: 'core-indexer',
        phase: 'core-current-state-materialization',
        ...(progress ?? {}),
      },
      'core current state materialization progress',
    );
  }

  private async returnBackfillToSync(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
  ): Promise<void> {
    await this.stateStore.upsertCoreIndexerState({
      stage: 'sync_backfill',
      onlineTip: latest,
    });
    await this.configs.setJsonValue(configKeyIndexerStage(), 'sync_backfill');
    this.logger.info(
      {
        chain: dogecoin.id,
        component: 'core-indexer',
        latest,
        processTail: state.processTail,
        reason: 'tip-advanced',
        stage: 'sync_backfill',
      },
      'core stage changed',
    );
  }

  private async processBackfillWindow(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
    currentStateReady: boolean,
    stage: CoreIndexerState['stage'] = 'process_backfill',
  ): Promise<boolean> {
    const plan = this.planBackfillWindow(dogecoin, latest, state);
    const metrics = await this.processWindow(dogecoin, plan, currentStateReady);
    this.adaptBackfillTargetRows(metrics.applyMs);
    await this.publishWindowProgress(dogecoin, latest, metrics, stage);

    this.logger.info(
      {
        blockEnd: metrics.end,
        blockStart: metrics.start,
        chain: dogecoin.id,
        component: 'core-indexer',
        latest,
        syncTail: state.syncTail,
      },
      'core processed',
    );
    return true;
  }

  /**
   * Backfill windows are sized by work, not by a fixed block count: a window
   * closes once it holds the row target, reaches the block limit, or has been
   * filling for `backfillWindowMaxFillMs`. Blocks come through a read-ahead
   * that keeps fetching while the previous window is applied.
   */
  private planBackfillWindow(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
  ): CoreWindowPlan {
    const firstHeight = state.processTail + 1;
    const source = this.backfillSource(latest, firstHeight);
    const readAheadLimit = backfillReadAheadLimit(source, latest, state.syncTail, this.settings);
    const plan: CoreWindowPlan = {
      firstHeight,
      lastHeight: Math.min(readAheadLimit, state.processTail + this.backfillWindowBlocks()),
      source,
      load: () => this.loadBackfillSnapshots(dogecoin, plan, readAheadLimit),
    };
    return plan;
  }

  private backfillWindowBlocks(): number {
    return this.settings.coreBackfillWindowBlocks ?? this.settings.coreProcessWindow;
  }

  private backfillSource(latest: number, firstHeight: number): CoreWindowSource {
    const configured = this.settings.coreBackfillBlockSource ?? 'storage';
    if (configured !== 'auto') {
      return configured;
    }
    if (Date.now() < this.nodeSourceRetryAtMs) {
      return 'storage';
    }

    // Heights inside the reorg window keep using the snapshots raw sync stored,
    // so processing and the explorer agree on the block they describe.
    return firstHeight <= finalizedNodeHeight(latest, this.settings) ? 'node' : 'storage';
  }

  private async loadBackfillSnapshots(
    dogecoin: DogecoinRuntimeConfig,
    plan: CoreWindowPlan,
    readAheadLimit: number,
  ): Promise<Record<string, unknown>[]> {
    try {
      return await this.fillBackfillWindow(dogecoin, plan, readAheadLimit);
    } catch (error) {
      if (!this.shouldFallBackToStorage(plan.source, error)) {
        throw error;
      }

      this.nodeSourceRetryAtMs = Date.now() + nodeSourceRetryDelayMs;
      this.logger.warn(
        {
          ...this.errorBindings(error),
          blockEnd: plan.lastHeight,
          blockStart: plan.firstHeight,
          chain: dogecoin.id,
          component: 'core-indexer',
          phase: 'core-process-window',
          retryNodeInMs: nodeSourceRetryDelayMs,
        },
        'node block read failed; processing from raw storage',
      );
      plan.source = 'storage';
      return this.fillBackfillWindow(dogecoin, plan, readAheadLimit);
    }
  }

  private shouldFallBackToStorage(source: CoreWindowSource, error: unknown): boolean {
    return (
      source === 'node' &&
      this.settings.coreBackfillBlockSource === 'auto' &&
      !(error instanceof PrimaryLeaseLostError)
    );
  }

  private async fillBackfillWindow(
    dogecoin: DogecoinRuntimeConfig,
    plan: CoreWindowPlan,
    readAheadLimit: number,
  ): Promise<Record<string, unknown>[]> {
    const { prefetcher } = this.backfillLoaderFor(dogecoin, plan.source);
    const targetRows = this.currentBackfillTargetRows();
    const fillDeadline = Date.now() + backfillWindowMaxFillMs;
    const snapshots: Record<string, unknown>[] = [];
    let rows = 0;

    for (let height = plan.firstHeight; height <= plan.lastHeight; height += 1) {
      const snapshot = await prefetcher.get(height, readAheadLimit);
      this.recordActivity();
      snapshots.push(snapshot);
      rows += coreSnapshotRowCount(snapshot);
      if (rows >= targetRows || Date.now() >= fillDeadline) {
        break;
      }
    }

    return snapshots;
  }

  private backfillLoaderFor(
    dogecoin: DogecoinRuntimeConfig,
    source: CoreWindowSource,
  ): CoreBackfillLoader {
    if (this.backfillLoader?.source === source) {
      return this.backfillLoader;
    }

    this.backfillLoader?.prefetcher.reset();
    this.backfillLoader = { prefetcher: this.createBackfillPrefetcher(dogecoin, source), source };
    return this.backfillLoader;
  }

  private createBackfillPrefetcher(
    dogecoin: DogecoinRuntimeConfig,
    source: CoreWindowSource,
  ): CoreBlockPrefetcher {
    const readAhead = {
      maxBufferedBlocks: this.backfillWindowBlocks() * 2,
      maxBufferedWeight: (this.settings.coreBackfillWindowRows ?? Number.MAX_SAFE_INTEGER) * 2,
      weigh: coreSnapshotRowCount,
    };
    if (source === 'node') {
      return new CoreBlockPrefetcher((heights) => this.rpc.getBlockSnapshots(dogecoin, heights), {
        ...readAhead,
        batchSize: this.settings.syncBatchSize,
        concurrency: this.settings.syncConcurrency,
      });
    }

    return new CoreBlockPrefetcher(
      (heights) => Promise.all(heights.map((height) => this.loadRawSnapshot(dogecoin, height))),
      { ...readAhead, batchSize: 1, concurrency: this.settings.coreProcessLoadConcurrency },
    );
  }

  private currentBackfillTargetRows(): number {
    const configured = this.settings.coreBackfillWindowRows;
    if (configured === undefined) {
      return Number.POSITIVE_INFINITY;
    }

    return Math.min(configured, this.backfillTargetRows ?? configured);
  }

  /**
   * Keeps window apply time well inside the statement budget on whatever
   * hardware the warehouse runs on: halve the row target when an apply used
   * more than half the budget (or failed), grow it back while applies are fast.
   */
  private adaptBackfillTargetRows(applyMs: number | null): void {
    const configured = this.settings.coreBackfillWindowRows;
    if (configured === undefined) {
      return;
    }

    const current = Math.min(configured, this.backfillTargetRows ?? configured);
    this.backfillTargetRows = nextBackfillTargetRows(
      current,
      configured,
      applyMs,
      this.settings.coreDbStatementTimeoutMs,
    );
  }

  private async processWindow(
    dogecoin: DogecoinRuntimeConfig,
    plan: CoreWindowPlan,
    updateCurrentState: boolean,
  ): Promise<CoreWindowMetrics> {
    const attempt = this.createCoreBlockAttempt(plan.lastHeight);
    this.activeBlockAttempt = attempt;

    try {
      return await this.processWindowWithAttempt(plan, updateCurrentState, attempt);
    } catch (error) {
      await this.exitForCoreBlockTimeout(error, dogecoin, attempt.height);
      throw error;
    } finally {
      this.clearActiveBlockAttempt(attempt);
    }
  }

  private fixedWindowPlan(dogecoin: DogecoinRuntimeConfig, heights: number[]): CoreWindowPlan {
    return {
      ...requireCoreProcessWindowBounds(heights),
      source: 'storage',
      load: () =>
        mapWithConcurrency(heights, this.settings.coreProcessLoadConcurrency, (height) =>
          this.loadRawSnapshot(dogecoin, height),
        ),
    };
  }

  private createCoreBlockAttempt(height: number): CoreBlockAttempt {
    return {
      activeStep: 'load_raw',
      height,
      startedAtMs: Date.now(),
    };
  }

  private async processWindowWithAttempt(
    plan: CoreWindowPlan,
    updateCurrentState: boolean,
    attempt: CoreBlockAttempt,
  ): Promise<CoreWindowMetrics> {
    const totalStartedAt = Date.now();
    const { result: snapshots, elapsedMs: loadRawMs } = await this.runCoreBlockStep(
      attempt,
      'load_raw',
      plan.load,
    );
    const { result: applications, elapsedMs: buildMs } = await this.buildWindowApplications(
      snapshots,
      attempt,
    );
    const { result: applyResult, elapsedMs: applyMs } =
      await this.applyWindowApplicationsWithRecovery(applications, updateCurrentState, attempt);

    return coreWindowMetrics({
      applications,
      applyMs,
      applyResult,
      bounds: plan,
      buildMs,
      loadRawMs,
      source: plan.source,
      totalStartedAt,
    });
  }

  private async loadRawSnapshot(
    dogecoin: DogecoinRuntimeConfig,
    height: number,
  ): Promise<Record<string, unknown>> {
    const snapshot = await this.rawBlocks.getPart<Record<string, unknown>>(height, rawBlockPart, {
      timeoutMs: this.settings.coreRawStorageTimeoutMs,
    });
    if (!snapshot) {
      throw new Error(`missing raw dogecoin block snapshot chain=${dogecoin.id} height=${height}`);
    }
    return snapshot;
  }

  private buildWindowApplications(
    snapshots: Record<string, unknown>[],
    attempt: CoreBlockAttempt,
  ): Promise<{ elapsedMs: number; result: CoreDogecoinBlockApplication[] }> {
    return this.runCoreBlockStep(attempt, 'build_application', () =>
      Promise.resolve(this.buildFastBlockApplications(snapshots)),
    );
  }

  private applyWindowApplications(
    applications: CoreDogecoinBlockApplication[],
    updateCurrentState: boolean,
    attempt: CoreBlockAttempt,
  ): Promise<{ elapsedMs: number; result: CoreDogecoinApplyResult }> {
    return this.runCoreBlockStep(attempt, 'apply_state', (abortSignal) =>
      this.stateStore.applyCoreDogecoinWindow(applications, {
        abortSignal,
        statementTimeoutMs: this.settings.coreDbStatementTimeoutMs,
        updateCurrentState,
        validatePrevouts: false,
      }),
    );
  }

  private async applyWindowApplicationsWithRecovery(
    applications: CoreDogecoinBlockApplication[],
    updateCurrentState: boolean,
    attempt: CoreBlockAttempt,
  ): Promise<{ elapsedMs: number; result: CoreDogecoinApplyResult }> {
    if (applications.length === 0) {
      return this.applyWindowApplications(applications, updateCurrentState, attempt);
    }

    const firstApplication = applications[0];
    if (!firstApplication) {
      return this.applyWindowApplications(applications, updateCurrentState, attempt);
    }

    const lastApplication = applications.at(-1) ?? firstApplication;
    const marker = createCoreApplyRecoveryMarker({
      instanceId: this.instanceId,
      startHeight: firstApplication.blockHeight,
      endHeight: lastApplication.blockHeight,
      blockHashes: applications.map((application) => application.blockHash),
      updateCurrentState,
    });
    await this.configs.setJsonValue(configKeyCoreApplyRecovery(), marker);
    this.logger.info(
      {
        action: 'marker-set',
        component: 'core-indexer',
        endHeight: marker.endHeight,
        instanceId: marker.instanceId,
        phase: 'core-apply-recovery',
        startHeight: marker.startHeight,
      },
      'core apply recovery marker set',
    );

    try {
      const applied = await this.applyWindowApplications(applications, updateCurrentState, attempt);
      await this.clearCoreApplyRecoveryMarker(marker);
      return applied;
    } catch (error) {
      this.adaptBackfillTargetRows(null);
      this.logger.error(
        {
          action: 'marker-retained',
          component: 'core-indexer',
          endHeight: marker.endHeight,
          instanceId: marker.instanceId,
          phase: 'core-apply-recovery',
          startHeight: marker.startHeight,
          ...this.errorBindings(error),
        },
        'core apply failed with recovery marker retained',
      );
      throw error;
    }
  }

  private async recoverPendingCoreApplyIfNeeded(dogecoin: DogecoinRuntimeConfig): Promise<void> {
    const rawMarker = await this.configs.getJsonValue<unknown>(configKeyCoreApplyRecovery());
    if (!rawMarker) {
      return;
    }

    const marker = parseCoreApplyRecoveryMarker(rawMarker);
    this.logger.warn(
      {
        action: 'recover',
        component: 'core-indexer',
        endHeight: marker.endHeight,
        instanceId: this.instanceId,
        markerInstanceId: marker.instanceId,
        phase: 'core-apply-recovery',
        startHeight: marker.startHeight,
      },
      'recovering pending core apply window',
    );
    await this.stateStore.recoverCoreDogecoinWindow(marker.startHeight, {
      statementTimeoutMs: this.settings.coreDbStatementTimeoutMs,
      updateCurrentState: marker.updateCurrentState,
      validatePrevouts: false,
    });
    const cleared = await this.configs.compareAndDeleteJsonValue(
      configKeyCoreApplyRecovery(),
      marker,
    );
    if (!cleared) {
      throw new Error(
        `core apply recovery marker changed during recovery chain=${dogecoin.id} window=${marker.startHeight}-${marker.endHeight}`,
      );
    }
    this.logger.info(
      {
        action: 'marker-cleared',
        component: 'core-indexer',
        endHeight: marker.endHeight,
        instanceId: this.instanceId,
        phase: 'core-apply-recovery',
        startHeight: marker.startHeight,
      },
      'core apply recovery marker cleared',
    );
  }

  private async clearCoreApplyRecoveryMarker(marker: CoreApplyRecoveryMarkerV1): Promise<void> {
    const cleared = await this.configs.compareAndDeleteJsonValue(
      configKeyCoreApplyRecovery(),
      marker,
    );
    if (!cleared) {
      throw new Error(
        `core apply recovery marker changed during apply instance=${marker.instanceId} window=${marker.startHeight}-${marker.endHeight}`,
      );
    }
    this.logger.info(
      {
        action: 'marker-cleared',
        component: 'core-indexer',
        endHeight: marker.endHeight,
        instanceId: marker.instanceId,
        phase: 'core-apply-recovery',
        startHeight: marker.startHeight,
      },
      'core apply recovery marker cleared',
    );
  }

  private clearActiveBlockAttempt(attempt: CoreBlockAttempt): void {
    if (this.activeBlockAttempt === attempt) {
      this.activeBlockAttempt = null;
    }
  }

  private buildFastBlockApplications(
    snapshots: Record<string, unknown>[],
  ): CoreDogecoinBlockApplication[] {
    return buildFastCoreDogecoinBlockApplications(snapshots);
  }

  private async publishWindowProgress(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    metrics: CoreWindowMetrics,
    stage: CoreIndexerState['stage'] = 'process_backfill',
  ): Promise<void> {
    let nextState: CoreIndexerState;
    let publishMs: number;
    try {
      const published = await this.runCoreBlockStep(
        {
          activeStep: 'publish_progress',
          height: metrics.end,
          startedAtMs: Date.now(),
        },
        'publish_progress',
        async () => {
          const state = await this.stateStore.upsertCoreIndexerState({
            stage,
            processTail: metrics.end,
            onlineTip: latest,
            lastError: null,
          });
          await this.publishProgress(latest, state);
          return state;
        },
      );
      nextState = published.result;
      publishMs = published.elapsedMs;
    } catch (error) {
      await this.exitForCoreBlockTimeout(error, dogecoin, metrics.end);
      throw error;
    }
    this.recordProcessThroughput(metrics.blocks, metrics.totalMs + publishMs);

    this.logger.info(
      {
        applied: metrics.applied,
        applyMs: metrics.applyMs,
        blockEnd: metrics.end,
        blockStart: metrics.start,
        blocks: metrics.blocks,
        blocksPerSecond: roundRate(this.processBlocksPerSecond),
        buildMs: metrics.buildMs,
        chain: dogecoin.id,
        component: 'core-indexer',
        creates: metrics.creates,
        loadRawMs: metrics.loadRawMs,
        phase: 'core-process-window',
        processTail: nextState.processTail,
        publishProgressMs: publishMs,
        source: metrics.source,
        spends: metrics.spends,
        totalMs: metrics.totalMs + publishMs,
      },
      'core process window completed',
    );
  }

  private async exitForCoreBlockTimeout(
    error: unknown,
    dogecoin: DogecoinRuntimeConfig,
    height: number,
  ): Promise<void> {
    if (!(error instanceof CoreBlockTimeoutError)) {
      return;
    }

    const message = `core block timed out chain=${dogecoin.id} height=${height} active_step=${error.step} timeout_ms=${error.timeoutMs}`;
    await this.stateStore.setCoreIndexerError(message);
    this.logger.error(
      {
        activeStep: error.step,
        chain: dogecoin.id,
        component: 'core-indexer',
        height,
        phase: 'core-process',
        timeoutMs: error.timeoutMs,
      },
      'core block timed out',
    );
    this.exitProcess(1);
  }

  private async online(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    const didWork = await this.advanceOnlineBacklog(dogecoin, latest, state);
    if (didWork) {
      return true;
    }

    if (await this.publishOnlineNoWorkIfPossible(dogecoin, latest, state)) {
      return false;
    }

    return false;
  }

  private async publishOnlineNoWorkIfPossible(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    if (await this.publishReadyOnlineStateIfCurrent(dogecoin, latest, state)) {
      return true;
    }

    return this.publishProgressIfAtLatest(latest, state);
  }

  private async isCurrentOnlineStateReady(
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    return (
      state.syncTail >= latest &&
      (await this.isDogecoinCurrentStateReady()) &&
      state.processTail >= latest
    );
  }

  private async publishReadyOnlineStateIfCurrent(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    void dogecoin;
    if (!(await this.isCurrentOnlineStateReady(latest, state))) {
      return false;
    }

    await this.publishReadyOnlineState(latest, state);
    return true;
  }

  private async publishProgressIfAtLatest(
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    if (!isCoreStateAtLatest(state, latest)) {
      return false;
    }

    await this.publishProgress(latest, state);
    return true;
  }

  private async publishReadyOnlineState(latest: number, state: CoreIndexerState): Promise<void> {
    const nextState = await this.ensureReadyOnlineState(latest, state);
    await this.publishProgress(latest, nextState);
  }

  private async ensureReadyOnlineState(
    latest: number,
    state: CoreIndexerState,
  ): Promise<CoreIndexerState> {
    if (isReadyOnlineState(state, latest)) {
      return state;
    }

    return this.stateStore.upsertCoreIndexerState({
      stage: 'online',
      onlineTip: latest,
    });
  }

  private async advanceOnlineBacklog(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    const syncEnd = Math.min(latest, state.syncTail + this.settings.syncWindow);
    const didSync = await this.syncOnlineBacklogIfNeeded(dogecoin, latest, state, syncEnd);

    const refreshed = await this.refreshedOnlineBacklogState(state, syncEnd);
    const didProcess = await this.processOnlineBacklogIfNeeded(
      dogecoin,
      latest,
      refreshed,
      didSync,
    );
    return [didSync, didProcess].includes(true);
  }

  private async syncOnlineBacklogIfNeeded(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
    syncEnd: number,
  ): Promise<boolean> {
    const shouldRefreshTail = shouldRefreshOnlineReprocessWindow(state, latest);
    if (state.syncTail >= syncEnd && !shouldRefreshTail) {
      return false;
    }

    const start = onlineRawRefreshStart(state, syncEnd, this.settings.coreReprocessDepth);
    const heights = range(start, syncEnd);
    await this.syncRawBlockHeights(dogecoin, latest, state, heights, syncEnd, 'online');
    return true;
  }

  private async refreshedOnlineBacklogState(
    state: CoreIndexerState,
    syncEnd: number,
  ): Promise<CoreIndexerState> {
    const refreshed = await this.stateStore.getCoreIndexerState();
    if (refreshed) {
      return refreshed;
    }

    return { ...state, syncTail: syncEnd };
  }

  private async processOnlineBacklogIfNeeded(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
    forceTailReprocess: boolean,
  ): Promise<boolean> {
    const processTarget = Math.min(state.syncTail, latest);
    if (state.processTail >= processTarget && !forceTailReprocess) {
      return false;
    }

    const didProcess = await this.processOnlineWindow(
      dogecoin,
      latest,
      state,
      processTarget,
      await this.isDogecoinCurrentStateReady(),
    );
    return didProcess;
  }

  private async processOnlineWindow(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
    processTarget: number,
    currentStateReady: boolean,
  ): Promise<boolean> {
    const heights = onlineProcessHeights(
      state.processTail,
      processTarget,
      this.settings.coreReprocessDepth,
      this.settings.coreProcessWindow,
    );
    await this.storeRawBlockHeights(dogecoin, heights);
    const metrics = await this.processWindow(
      dogecoin,
      this.fixedWindowPlan(dogecoin, heights),
      currentStateReady,
    );
    await this.publishWindowProgress(dogecoin, latest, metrics, 'online');

    this.logger.info(
      {
        blockEnd: metrics.end,
        blockStart: metrics.start,
        chain: dogecoin.id,
        component: 'core-indexer',
        latest,
        syncTail: state.syncTail,
      },
      'core processed',
    );
    return metrics.applied || state.processTail < metrics.end;
  }

  private async publishProgress(latest: number, state: CoreIndexerState): Promise<void> {
    const [historyReady, analyticsFactsReady] = await Promise.all([
      this.isDogecoinHistoryReady(),
      this.isDogecoinAnalyticsFactsReady(),
    ]);
    this.assertPrimaryLease();
    await this.setConfigValues(
      this.progressEntries(latest, state, { analyticsFactsReady, historyReady }),
    );
    this.observeProgress(state);
  }

  private progressEntries(
    latest: number,
    state: CoreIndexerState,
    readiness: { analyticsFactsReady: boolean; historyReady: boolean },
  ): CoordinatorConfigEntry[] {
    const entries: CoordinatorConfigEntry[] = [
      [configKeyBlockHeight(), latest],
      [configKeyIndexerStage(), state.stage],
      [configKeyIndexerSyncTail(), state.syncTail],
      [configKeyIndexerProcessTail(), state.processTail],
      [
        configKeyIndexerFinalizedTail(),
        finalizedTail(state.processTail, this.settings.coreReprocessDepth),
      ],
      [configKeyIndexerReprocessDepth(), this.settings.coreReprocessDepth],
      [configKeyIndexerSyncProgress(), toProgress(state.syncTail, latest)],
      [configKeyIndexerProcessProgress(), toProgress(state.processTail, latest)],
      [configKeyIndexerSyncBlocksPerSecond(), roundRate(this.syncBlocksPerSecond)],
      [
        configKeyIndexerSyncEtaSeconds(),
        etaSeconds(latest - state.syncTail, this.syncBlocksPerSecond),
      ],
      [configKeyIndexerProcessBlocksPerSecond(), roundRate(this.processBlocksPerSecond)],
      [
        configKeyIndexerProcessEtaSeconds(),
        etaSeconds(latest - state.processTail, this.processBlocksPerSecond),
      ],
      [configKeyIndexerLastActivityAt(), new Date(this.lastActivityAtMs).toISOString()],
    ];
    if (readiness.historyReady) {
      entries.push(
        [configKeyIndexerFactTail(), state.processTail],
        [configKeyIndexerFactProgress(), toProgress(state.processTail, latest)],
      );
    }
    if (readiness.analyticsFactsReady) {
      entries.push([
        configKeyDogecoinAnalyticsFactsTail(),
        finalizedTail(state.processTail, this.settings.coreReprocessDepth),
      ]);
    }

    return entries;
  }

  /**
   * Progress is a dozen keys rewritten every window. Adapters that can, write
   * them in one statement; the rest fall back to one write per key.
   */
  private async setConfigValues(entries: CoordinatorConfigEntry[]): Promise<void> {
    if (this.configs.setJsonValues) {
      await this.configs.setJsonValues(entries);
      return;
    }

    await Promise.all(entries.map(([key, value]) => this.configs.setJsonValue(key, value)));
  }

  private async isDogecoinHistoryReady(): Promise<boolean> {
    return (await this.configs.getJsonValue<boolean>(configKeyDogecoinHistoryReady())) === true;
  }

  private async isDogecoinAnalyticsFactsReady(): Promise<boolean> {
    return (
      (await this.configs.getJsonValue<boolean>(configKeyDogecoinAnalyticsFactsReady())) === true
    );
  }

  private async runCoreBlockStep<T>(
    attempt: CoreBlockAttempt,
    step: CoreBlockStep,
    work: (abortSignal: AbortSignal) => Promise<T>,
  ): Promise<{ elapsedMs: number; result: T }> {
    this.assertPrimaryLease();
    attempt.activeStep = step;
    const startedAt = Date.now();
    const controller = new AbortController();
    const result = await withTimeout(
      work(controller.signal),
      this.settings.coreBlockTimeoutMs,
      () => {
        const error = new CoreBlockTimeoutError(step, this.settings.coreBlockTimeoutMs);
        controller.abort(error);
        return error;
      },
    );
    this.recordActivity();
    this.assertPrimaryLease();
    return {
      elapsedMs: Date.now() - startedAt,
      result,
    };
  }

  private async assertProgressWatchdog(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
  ): Promise<void> {
    if (await this.shouldSkipProgressWatchdog(state)) {
      return;
    }

    await this.assertObservedProgressWatchdog(dogecoin, latest, state);
  }

  private async assertObservedProgressWatchdog(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
  ): Promise<void> {
    const observation = this.observeProgress(state);
    const ageMs = this.coreProgressBacklogAgeMs(state, latest, observation);
    if (isFreshProgressAge(ageMs, this.settings.coreProgressWatchdogMs)) {
      return;
    }

    await this.exitForExpiredProgressWatchdog(dogecoin, state, requireProgressAge(ageMs));
  }

  private async shouldSkipProgressWatchdog(state: CoreIndexerState): Promise<boolean> {
    if (state.stage !== 'online') {
      return false;
    }

    return this.isDogecoinCurrentStateReady();
  }

  private async isDogecoinCurrentStateReady(): Promise<boolean> {
    return (
      (await this.configs.getJsonValue<boolean>(configKeyDogecoinCurrentStateReady())) === true
    );
  }

  /**
   * Age of the last sign of life while a backlog exists. Both progress and
   * completed attempts (including failed RPC batches) count as life: a slow or
   * unreachable node is reported through `lastError`/health, not by killing
   * the process, which would only re-enqueue the same work against the node.
   */
  private coreProgressBacklogAgeMs(
    state: CoreIndexerState,
    latest: number,
    observation: ProgressObservation,
  ): number | null {
    if (!hasCoreWorkBacklog(state, latest, this.settings.coreSyncCompleteDistance)) {
      return null;
    }

    return Date.now() - Math.max(observation.observedAtMs, this.lastActivityAtMs);
  }

  private async exitForExpiredProgressWatchdog(
    dogecoin: DogecoinRuntimeConfig,
    state: CoreIndexerState,
    ageMs: number,
  ): Promise<void> {
    const activeAttempt = activeAttemptLog(this.activeBlockAttempt ?? undefined);
    const message = `core progress watchdog expired chain=${dogecoin.id} stage=${state.stage} sync_tail=${state.syncTail} process_tail=${state.processTail} age_ms=${ageMs}`;
    await this.stateStore.setCoreIndexerError(message);
    this.logger.error(
      {
        activeHeight: activeAttempt.height,
        activeStep: activeAttempt.step,
        ageMs,
        chain: dogecoin.id,
        component: 'core-indexer',
        phase: 'core-watchdog',
        processTail: state.processTail,
        stage: state.stage,
        syncTail: state.syncTail,
      },
      'core progress watchdog expired',
    );
    this.exitProcess(1);
  }

  private observeProgress(state: CoreIndexerState): ProgressObservation {
    const previous = this.progressObservation ?? undefined;
    if (isSameProgressObservation(previous, state)) {
      return previous;
    }

    return this.recordProgressObservation(state);
  }

  private recordProgressObservation(state: CoreIndexerState): ProgressObservation {
    const next = {
      observedAtMs: Date.now(),
      processTail: state.processTail,
      stage: state.stage,
      syncTail: state.syncTail,
    };
    this.progressObservation = next;
    return next;
  }

  private exitProcess(code: number): never {
    if (this.options.exitProcess) {
      return this.options.exitProcess(code);
    }

    process.exit(code);
  }

  private errorBindings(error: unknown): Record<string, unknown> {
    return {
      err: error instanceof Error ? error : new Error(formatError(error)),
    };
  }

  private async leaseLeadership(): Promise<boolean> {
    // A lease this instance renewed less than one heartbeat ago cannot have
    // been replaced (takeover needs three missed heartbeats), so loop
    // iterations shorter than the heartbeat skip the metadata round trip.
    if (this.hasFreshPrimaryLease()) {
      return true;
    }

    const current = await this.configs.getJsonValue<PrimaryLease | string>(configKeyPrimary());
    const currentLease = toPrimaryLease(current);
    if (!currentLease) {
      return this.claimPrimaryLease(current, '');
    }

    return this.leaseKnownPrimary(current, currentLease);
  }

  private hasFreshPrimaryLease(): boolean {
    const lease = this.primaryLease;
    if (!lease) {
      return false;
    }

    return Date.now() - Date.parse(lease.heartbeatAt) < this.settings.leaseHeartbeatIntervalMs;
  }

  private async leaseKnownPrimary(
    current: PrimaryLease | string | null,
    currentLease: PrimaryLease,
  ): Promise<boolean> {
    if (currentLease.instanceId === this.instanceId) {
      return this.renewPrimaryLease(currentLease);
    }

    return this.leaseCompetingPrimary(current, currentLease);
  }

  private async leaseCompetingPrimary(
    current: PrimaryLease | string | null,
    currentLease: PrimaryLease,
  ): Promise<boolean> {
    if (isFreshPrimaryLease(currentLease, this.settings.leaseHeartbeatIntervalMs)) {
      this.primaryLease = null;
      return false;
    }

    return this.claimPrimaryLease(current, ' replaced-stale-primary');
  }

  private async renewPrimaryLease(expectedLease: PrimaryLease): Promise<boolean> {
    const nextLease = createLease(this.instanceId);
    try {
      const renewed = await this.configs.compareAndSwapJsonValue(
        configKeyPrimary(),
        expectedLease,
        nextLease,
      );
      this.primaryLease = renewed ? nextLease : null;
      return renewed;
    } catch (error) {
      this.primaryLease = null;
      this.logger.error(this.errorBindings(error), 'core indexer lease renewal failed');
      return false;
    }
  }

  private async claimPrimaryLease(
    current: PrimaryLease | string | null,
    logSuffix: string,
  ): Promise<boolean> {
    const nextLease = createLease(this.instanceId);
    const claimed = await this.configs.compareAndSwapJsonValue(
      configKeyPrimary(),
      current,
      nextLease,
    );
    if (claimed) {
      this.primaryLease = nextLease;
      this.logger.info(
        { component: 'core-indexer', instanceId: this.instanceId, logSuffix },
        'core indexer claimed primary lease',
      );
    } else {
      this.primaryLease = null;
    }
    return claimed;
  }

  private async runWithPrimaryLeaseHeartbeat(work: () => Promise<boolean>): Promise<boolean> {
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let renewal: Promise<void> | undefined;

    const scheduleRenewal = () => {
      timer = setTimeout(() => {
        renewal = renew();
      }, this.settings.leaseHeartbeatIntervalMs);
    };
    const renew = async (): Promise<void> => {
      const expectedLease = this.primaryLease;
      if (stopped || !expectedLease) {
        return;
      }
      if ((await this.renewPrimaryLease(expectedLease)) && !stopped) {
        scheduleRenewal();
      }
    };

    scheduleRenewal();
    let result = false;
    try {
      result = await work();
    } catch (error) {
      if (!(error instanceof PrimaryLeaseLostError)) {
        throw error;
      }
    } finally {
      stopped = true;
      if (timer) {
        clearTimeout(timer);
      }
      await renewal;
    }
    return result && this.primaryLease !== null;
  }

  private assertPrimaryLease(): void {
    if (!this.primaryLease) {
      throw new PrimaryLeaseLostError();
    }
  }
}

function shouldContinueStartLoop(signal: AbortSignal | undefined): boolean {
  return signal?.aborted !== true;
}

function hasProcessedEverySyncedBlock(state: CoreIndexerState): boolean {
  return state.syncTail >= 0 && state.processTail >= state.syncTail;
}

/** Highest height the node is trusted for without consulting the stored snapshot. */
function finalizedNodeHeight(
  latest: number,
  settings: Pick<CoreDogecoinIndexerSettings, 'coreReprocessDepth'>,
): number {
  return latest - settings.coreReprocessDepth;
}

function backfillReadAheadLimit(
  source: CoreWindowSource,
  latest: number,
  syncTail: number,
  settings: Pick<CoreDogecoinIndexerSettings, 'coreBackfillBlockSource' | 'coreReprocessDepth'>,
): number {
  if (source === 'node' && settings.coreBackfillBlockSource === 'auto') {
    return Math.min(syncTail, finalizedNodeHeight(latest, settings));
  }

  return syncTail;
}

function nextBackfillTargetRows(
  current: number,
  configured: number,
  applyMs: number | null,
  statementBudgetMs: number,
): number {
  if (applyMs === null || applyMs > statementBudgetMs / 2) {
    return Math.max(Math.min(backfillWindowMinRows, configured), Math.floor(current / 2));
  }
  if (applyMs < statementBudgetMs / 6) {
    return Math.min(configured, Math.ceil(current * 1.5));
  }

  return current;
}

/**
 * Inputs + outputs of a block snapshot: the number of core rows it produces.
 * Used only to size windows, so malformed snapshots count as zero here and are
 * rejected later by the strict parser.
 */
export function coreSnapshotRowCount(snapshot: Record<string, unknown>): number {
  const transactions = (snapshot.block as { tx?: unknown } | undefined)?.tx;
  if (!Array.isArray(transactions)) {
    return 0;
  }

  let rows = 0;
  for (const transaction of transactions as Array<{ vin?: unknown; vout?: unknown } | null>) {
    rows += arrayLength(transaction?.vin) + arrayLength(transaction?.vout);
  }
  return rows;
}

function arrayLength(value: unknown): number {
  return Array.isArray(value) ? value.length : 0;
}

function shouldPromoteToProcessBackfill(
  state: CoreIndexerState,
  latest: number,
  coreSyncCompleteDistance: number,
): boolean {
  if (state.syncTail < 0) {
    return false;
  }

  return state.syncTail >= latest - coreSyncCompleteDistance;
}

function isFreshProgressAge(ageMs: number | null, watchdogMs: number): boolean {
  if (ageMs === null) {
    return true;
  }

  return ageMs <= watchdogMs;
}

function requireProgressAge(ageMs: number | null): number {
  if (ageMs === null) {
    throw new Error('missing progress age');
  }

  return ageMs;
}

function requireCoreProcessWindowBounds(heights: number[]): CoreProcessWindowBounds {
  const firstHeight = heights[0];
  if (firstHeight === undefined) {
    throw new Error('empty core process window');
  }

  return {
    firstHeight,
    lastHeight: lastCoreProcessWindowHeight(heights, firstHeight),
  };
}

function lastCoreProcessWindowHeight(heights: number[], firstHeight: number): number {
  const lastHeight = heights.at(-1);
  if (lastHeight === undefined) {
    return firstHeight;
  }

  return lastHeight;
}

function coreWindowMetrics(input: CoreWindowMetricsInput): CoreWindowMetrics {
  return {
    applied: input.applyResult.applied,
    applyMs: input.applyMs,
    blocks: input.applications.length,
    buildMs: input.buildMs,
    creates: countCoreCreates(input.applications),
    end: input.applyResult.processTail,
    loadRawMs: input.loadRawMs,
    source: input.source,
    spends: countCoreSpends(input.applications),
    start: coreWindowMetricStart(input.applications, input.bounds.firstHeight),
    totalMs: Date.now() - input.totalStartedAt,
  };
}

function coreWindowMetricStart(
  applications: CoreDogecoinBlockApplication[],
  firstHeight: number,
): number {
  const [application] = applications;
  if (!application) {
    return firstHeight;
  }

  return application.blockHeight;
}

function countCoreCreates(applications: CoreDogecoinBlockApplication[]): number {
  return applications.reduce((sum, application) => sum + application.utxoCreates.length, 0);
}

function countCoreSpends(applications: CoreDogecoinBlockApplication[]): number {
  return applications.reduce((sum, application) => sum + application.utxoSpends.length, 0);
}

function shouldRefreshOnlineReprocessWindow(state: CoreIndexerState, latest: number): boolean {
  return [state.syncTail < latest, state.onlineTip !== latest].includes(true);
}

function onlineRawRefreshStart(
  state: CoreIndexerState,
  syncEnd: number,
  coreReprocessDepth: number,
): number {
  return Math.max(
    0,
    Math.min(state.syncTail + 1, reprocessWindowStart(syncEnd, coreReprocessDepth)),
  );
}

function onlineProcessWindowStart(
  processTail: number,
  processTarget: number,
  coreReprocessDepth: number,
): number {
  const reprocessTip = Math.min(processTail, processTarget);
  return Math.max(
    0,
    Math.min(processTail + 1, reprocessWindowStart(reprocessTip, coreReprocessDepth)),
  );
}

function onlineProcessHeights(
  processTail: number,
  processTarget: number,
  coreReprocessDepth: number,
  coreProcessWindow: number,
): number[] {
  const start = onlineProcessWindowStart(processTail, processTarget, coreReprocessDepth);
  const end = Math.min(processTarget, processTail + coreProcessWindow);
  return range(start, end);
}

function reprocessWindowStart(tip: number, coreReprocessDepth: number): number {
  return Math.max(0, tip - coreReprocessDepth + 1);
}

function finalizedTail(processTail: number, coreReprocessDepth: number): number {
  return Math.max(-1, processTail - coreReprocessDepth);
}

function isCoreStateAtLatest(state: CoreIndexerState, latest: number): boolean {
  return state.syncTail >= latest && state.processTail >= latest;
}

function isReadyOnlineState(state: CoreIndexerState, latest: number): boolean {
  return [state.stage === 'online', state.onlineTip === latest, state.lastError === null].every(
    Boolean,
  );
}

function isSameProgressObservation(
  previous: ProgressObservation | undefined,
  state: CoreIndexerState,
): previous is ProgressObservation {
  if (!previous) {
    return false;
  }

  return hasSameProgressValues(previous, state);
}

function hasSameProgressValues(previous: ProgressObservation, state: CoreIndexerState): boolean {
  return [
    previous.stage === state.stage,
    previous.syncTail === state.syncTail,
    previous.processTail === state.processTail,
  ].every(Boolean);
}

function isFreshPrimaryLease(lease: PrimaryLease, heartbeatIntervalMs: number): boolean {
  return Date.now() - Date.parse(lease.heartbeatAt) <= heartbeatIntervalMs * 3;
}

function activeAttemptLog(attempt: CoreBlockAttempt | undefined): {
  height: number | 'none';
  step: CoreBlockStep | 'none';
} {
  if (!attempt) {
    return { height: 'none', step: 'none' };
  }

  return { height: attempt.height, step: attempt.activeStep };
}

function createLease(instanceId: string): PrimaryLease {
  return {
    instanceId,
    heartbeatAt: new Date().toISOString(),
  };
}

function toPrimaryLease(value: PrimaryLease | string | null): PrimaryLease | null {
  if (!isPrimaryLeaseCandidate(value)) {
    return null;
  }

  return primaryLeaseOrNull(value);
}

function primaryLeaseOrNull(value: PrimaryLease): PrimaryLease | null {
  if (!hasPrimaryLeaseShape(value)) {
    return null;
  }

  return value;
}

function isPrimaryLeaseCandidate(value: PrimaryLease | string | null): value is PrimaryLease {
  return Boolean(value) && typeof value !== 'string';
}

function hasPrimaryLeaseShape(value: PrimaryLease): boolean {
  return typeof value.instanceId === 'string' && typeof value.heartbeatAt === 'string';
}

function loopFailureBackoffMs(failures: number): number {
  return Math.min(loopFailureBackoffMaxMs, 1_000 * 2 ** Math.max(0, failures - 1));
}

function smoothRate(previous: number | null, blocks: number, elapsedMs: number): number | null {
  if (blocks <= 0 || elapsedMs <= 0) {
    return previous;
  }

  const sample = (blocks * 1_000) / elapsedMs;
  if (previous === null) {
    return sample;
  }

  return previous + throughputSmoothing * (sample - previous);
}

function roundRate(rate: number | null): number | null {
  if (rate === null) {
    return null;
  }

  return Math.round(rate * 100) / 100;
}

function etaSeconds(remaining: number, rate: number | null): number | null {
  if (rate === null || rate <= 0) {
    return null;
  }

  return Math.max(0, Math.round(remaining / rate));
}

function toProgress(tail: number, latest: number): number {
  if (latest < 0) {
    return 0;
  }
  return Math.max(0, Math.min(1, (tail + 1) / (latest + 1)));
}

function hasCoreWorkBacklog(
  state: CoreIndexerState,
  latest: number,
  coreSyncCompleteDistance: number,
): boolean {
  const checks: Record<CoreIndexerState['stage'], () => boolean> = {
    sync_backfill: () => state.syncTail < latest - coreSyncCompleteDistance,
    process_backfill: () => state.processTail < state.syncTail,
    online: () => [state.syncTail < latest, state.processTail < latest].includes(true),
  };
  return checks[state.stage]();
}

async function withTimeout<T>(
  work: Promise<T>,
  timeoutMs: number,
  createError: () => Error,
): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | null = null;
  try {
    return await Promise.race([
      work,
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(() => reject(createError()), timeoutMs);
      }),
    ]);
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

export function buildFastCoreDogecoinBlockApplications(
  snapshots: Record<string, unknown>[],
): CoreDogecoinBlockApplication[] {
  const tracker = createCoreWindowKeyTracker();
  return snapshots.map((snapshot) => buildCoreBlockApplication(snapshot, tracker));
}

function parseDogecoinBlockSnapshot(snapshot: Record<string, unknown>): ParsedDogecoinBlock & {
  previousHash: string | null;
} {
  const candidate = requireBlockRecord(snapshot.block);
  const hash = requireString(candidate.hash, 'block.hash');
  const height = requireNumber(candidate.height, 'block.height');

  return {
    hash,
    height,
    time: requireNumber(candidate.time, 'block.time'),
    previousHash: readPreviousBlockHash(candidate.previousblockhash),
    tx: readDogecoinTransactions(candidate.tx, height),
  };
}

function createCoreWindowKeyTracker(): CoreWindowKeyTracker {
  return {
    createdOutputKeys: new Set<string>(),
    spentOutputKeys: new Set<string>(),
  };
}

function buildCoreBlockApplication(
  snapshot: Record<string, unknown>,
  tracker: CoreWindowKeyTracker,
): CoreDogecoinBlockApplication {
  const block = parseDogecoinBlockSnapshot(snapshot);
  const effects = collectCoreBlockEffects(block, tracker);

  return {
    blockHeight: block.height,
    blockHash: block.hash,
    previousBlockHash: block.previousHash,
    blockTime: block.time,
    txCount: block.tx.length,
    rawStorageKey: rawBlockPart,
    utxoCreates: effects.utxoCreates,
    utxoSpends: effects.utxoSpends,
  };
}

function collectCoreBlockEffects(
  block: ParsedDogecoinBlock,
  tracker: CoreWindowKeyTracker,
): CoreTransactionEffects {
  const effects: CoreTransactionEffects = { utxoCreates: [], utxoSpends: [] };
  for (const [txIndex, tx] of block.tx.entries()) {
    appendTransactionEffects(effects, block, tx, txIndex, tracker);
  }
  return effects;
}

function appendTransactionEffects(
  effects: CoreTransactionEffects,
  block: ParsedDogecoinBlock,
  tx: DogecoinTransaction,
  txIndex: number,
  tracker: CoreWindowKeyTracker,
): void {
  const txid = requireString(tx.txid, 'tx.txid');
  const isCoinbase = hasCoinbaseInput(tx);
  effects.utxoSpends.push(...buildTransactionSpends(txid, block.height, tx, tracker));
  effects.utxoCreates.push(
    ...buildTransactionOutputs(block, tx, txid, txIndex, isCoinbase, tracker),
  );
}

function buildTransactionSpends(
  txid: string,
  blockHeight: number,
  tx: DogecoinTransaction,
  tracker: CoreWindowKeyTracker,
): CoreDogecoinBlockApplication['utxoSpends'] {
  const spends: CoreDogecoinBlockApplication['utxoSpends'] = [];
  for (const [inputIndex, input] of dogecoinInputs(tx).entries()) {
    appendTransactionSpend(spends, txid, blockHeight, input, inputIndex, tracker);
  }
  return spends;
}

function appendTransactionSpend(
  spends: CoreDogecoinBlockApplication['utxoSpends'],
  txid: string,
  blockHeight: number,
  input: DogecoinVin,
  inputIndex: number,
  tracker: CoreWindowKeyTracker,
): void {
  const spend = buildTransactionSpend(txid, blockHeight, input, inputIndex, tracker);
  if (spend) {
    spends.push(spend);
  }
}

function buildTransactionSpend(
  txid: string,
  blockHeight: number,
  input: DogecoinVin,
  inputIndex: number,
  tracker: CoreWindowKeyTracker,
): CoreDogecoinBlockApplication['utxoSpends'][number] | null {
  if (input.coinbase) {
    return null;
  }

  const outputKey = `${requireString(input.txid, 'vin.txid')}:${requireNumber(input.vout, 'vin.vout')}`;
  assertUniqueCoreSpend(outputKey, tracker);
  return {
    outputKey,
    spentByTxid: txid,
    spentInBlock: blockHeight,
    spentInputIndex: inputIndex,
  };
}

function buildTransactionOutputs(
  block: ParsedDogecoinBlock,
  tx: DogecoinTransaction,
  txid: string,
  txIndex: number,
  isCoinbase: boolean,
  tracker: CoreWindowKeyTracker,
): ProjectionUtxoOutput[] {
  const outputs: ProjectionUtxoOutput[] = [];
  for (const [outputIndex, output] of dogecoinOutputs(tx).entries()) {
    outputs.push(
      buildTransactionOutput(block, txid, txIndex, output, outputIndex, isCoinbase, tracker),
    );
  }
  return outputs;
}

function buildTransactionOutput(
  block: ParsedDogecoinBlock,
  txid: string,
  txIndex: number,
  output: DogecoinVout,
  outputIndex: number,
  isCoinbase: boolean,
  tracker: CoreWindowKeyTracker,
): ProjectionUtxoOutput {
  const outputKey = `${txid}:${outputIndex}`;
  assertUniqueCoreOutput(outputKey, tracker);
  const address = extractDogecoinOutputAddress(output);
  return {
    blockHeight: block.height,
    blockHash: block.hash,
    blockTime: block.time,
    txid,
    txIndex,
    vout: requireNumber(outputIndexValue(output, outputIndex), 'vout.n'),
    outputKey,
    address,
    scriptType: outputScriptType(output),
    valueBase: fromDecimalUnits(requireAmount(output.value), 8),
    isCoinbase,
    isSpendable: Boolean(address),
    spentByTxid: null,
    spentInBlock: null,
    spentInputIndex: null,
  };
}

function outputIndexValue(output: DogecoinVout, outputIndex: number): number {
  if (output.n === undefined) {
    return outputIndex;
  }

  return output.n;
}

function outputScriptType(output: DogecoinVout): string {
  return trimmedStringOrEmpty(output.scriptPubKey?.type);
}

function assertUniqueCoreSpend(outputKey: string, tracker: CoreWindowKeyTracker): void {
  if (tracker.spentOutputKeys.has(outputKey)) {
    throw new Error(`duplicate dogecoin spend in core window: ${outputKey}`);
  }
  tracker.spentOutputKeys.add(outputKey);
}

function assertUniqueCoreOutput(outputKey: string, tracker: CoreWindowKeyTracker): void {
  if (tracker.createdOutputKeys.has(outputKey)) {
    throw new Error(`duplicate dogecoin output in core window: ${outputKey}`);
  }
  tracker.createdOutputKeys.add(outputKey);
}

function dogecoinInputs(tx: DogecoinTransaction): DogecoinVin[] {
  return tx.vin ?? [];
}

function dogecoinOutputs(tx: DogecoinTransaction): DogecoinVout[] {
  return tx.vout ?? [];
}

function hasCoinbaseInput(tx: DogecoinTransaction): boolean {
  return dogecoinInputs(tx).some(isCoinbaseInput);
}

function isCoinbaseInput(input: DogecoinVin): boolean {
  return Boolean(input.coinbase);
}

function requireBlockRecord(value: unknown): Record<string, unknown> {
  if (!isPlainRecord(value)) {
    throw new Error('invalid dogecoin block snapshot');
  }
  return value;
}

function readDogecoinTransactions(value: unknown, blockHeight: number): DogecoinTransaction[] {
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error(
      `invalid dogecoin block transactions height=${blockHeight}: expected non-empty array`,
    );
  }

  const transactions: DogecoinTransaction[] = [];
  for (const [transactionIndex, transaction] of value.entries()) {
    if (!isDogecoinTransaction(transaction)) {
      throw new Error(
        `invalid dogecoin block transaction height=${blockHeight} tx_index=${transactionIndex}`,
      );
    }
    transactions.push(transaction);
  }

  return transactions;
}

function readPreviousBlockHash(value: unknown): string | null {
  if (typeof value !== 'string') {
    return null;
  }

  return nullIfEmpty(value.trim());
}

function requireString(value: unknown, label: string): string {
  if (typeof value !== 'string') {
    throw new Error(`missing ${label}`);
  }

  return requireTrimmedString(value, label);
}

function requireNumber(value: unknown, label: string): number {
  if (typeof value !== 'number') {
    throw new Error(`missing ${label}`);
  }

  return requireFiniteNumber(value, label);
}

function requireFiniteNumber(value: number, label: string): number {
  if (!Number.isFinite(value)) {
    throw new Error(`missing ${label}`);
  }

  return value;
}

function requireTrimmedString(value: string, label: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new Error(`missing ${label}`);
  }

  return trimmed;
}

function requireAmount(value: unknown): string {
  if (typeof value === 'number') {
    return value.toFixed(8);
  }

  return requireStringAmount(value);
}

function requireStringAmount(value: unknown): string {
  if (typeof value !== 'string') {
    throw new Error(`invalid dogecoin amount: ${String(value)}`);
  }

  return requireTrimmedAmount(value);
}

function requireTrimmedAmount(value: string): string {
  const trimmed = value.trim();
  if (trimmed) {
    return trimmed;
  }

  throw new Error(`invalid dogecoin amount: ${String(value)}`);
}

function trimmedStringOrEmpty(value: string | undefined): string {
  if (!value) {
    return '';
  }

  return value.trim();
}

function nullIfEmpty(value: string): string | null {
  if (value === '') {
    return null;
  }

  return value;
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return [Object(value) === value, !Array.isArray(value)].every(Boolean);
}

function formatError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
