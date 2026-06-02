import { randomUUID } from 'node:crypto';

import type {
  BlockchainRpcPort,
  CoordinatorConfigPort,
  CoreDogecoinStateStorePort,
  DogecoinConfigPort,
  RawBlockStoragePort,
} from '../contracts/ports';
import { fromDecimalUnits } from '../domain/amounts';
import {
  configKeyBlockHeight,
  configKeyDogecoinAnalyticsFactsReady,
  configKeyDogecoinAnalyticsFactsTail,
  configKeyDogecoinCurrentStateReady,
  configKeyDogecoinHistoryReady,
  configKeyIndexerFactProgress,
  configKeyIndexerFactTail,
  configKeyIndexerFinalizedTail,
  configKeyIndexerProcessProgress,
  configKeyIndexerProcessTail,
  configKeyIndexerReprocessDepth,
  configKeyIndexerStage,
  configKeyIndexerSyncProgress,
  configKeyIndexerSyncTail,
  configKeyPrimary,
} from '../domain/config-keys';
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
import type { CoreDogecoinIndexerSettings } from './core-dogecoin-indexer-settings';

interface PrimaryLease {
  heartbeatAt: string;
  instanceId: string;
}

export interface CoreDogecoinIndexerServiceOptions {
  exitProcess?: (code: number) => never;
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
const leaseTimeoutMs = 15_000;
const rawBlockPart = 'block';

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
  start: number;
}

interface CoreProcessWindowBounds {
  firstHeight: number;
  lastHeight: number;
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

export class CoreDogecoinIndexerService {
  private readonly instanceId = randomUUID();
  private activeBlockAttempt: CoreBlockAttempt | null = null;
  private progressObservation: ProgressObservation | null = null;

  public constructor(
    private readonly configs: CoordinatorConfigPort,
    private readonly dogecoin: DogecoinConfigPort,
    private readonly rawBlocks: RawBlockStoragePort,
    private readonly rpc: BlockchainRpcPort,
    private readonly stateStore: CoreDogecoinStateStorePort,
    private readonly settings: CoreDogecoinIndexerSettings,
    private readonly options: CoreDogecoinIndexerServiceOptions = {},
  ) {}

  // fallow-ignore-next-line unused-class-member
  public async start(signal?: AbortSignal): Promise<void> {
    console.info('[onlydoge] core dogecoin indexer loop started');
    while (shouldContinueStartLoop(signal)) {
      await this.runStartLoopIteration();
    }
  }

  public async runOnce(): Promise<boolean> {
    if (!(await this.leaseLeadership())) {
      return false;
    }

    return this.runDogecoin();
  }

  private async runStartLoopIteration(): Promise<void> {
    try {
      await this.runPrimaryLoopWork();
    } catch (error) {
      console.error(`[onlydoge] core indexer loop failed error=${formatError(error)}`);
      await sleep(1_000);
    }
  }

  private async runPrimaryLoopWork(): Promise<void> {
    const idleMs = await this.primaryLoopIdleMs();
    if (idleMs !== null) {
      await sleep(idleMs);
    }
  }

  private async primaryLoopIdleMs(): Promise<number | null> {
    if (!(await this.leaseLeadership())) {
      return 1_000;
    }

    return this.dogecoinWorkIdleMs();
  }

  private async dogecoinWorkIdleMs(): Promise<number | null> {
    if (await this.runOnce()) {
      return null;
    }

    return workerIdleMs;
  }

  private async runDogecoin(): Promise<boolean> {
    const dogecoin = await this.dogecoin.getDogecoinConfig();
    return this.runDogecoinConfig(dogecoin);
  }

  private async runDogecoinConfig(dogecoin: DogecoinRuntimeConfig): Promise<boolean> {
    const latest = await this.rpc.getBlockHeight(dogecoin);
    await this.configs.setJsonValue(configKeyBlockHeight(), latest);

    const state = await this.ensureState(dogecoin, latest);
    await this.publishProgress(latest, state);
    await this.assertProgressWatchdog(dogecoin, latest, state);

    try {
      return await this.runDogecoinStage(dogecoin, latest, state);
    } catch (error) {
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
    console.info(
      `[onlydoge] core indexer initialized chain=${dogecoin.id} stage=sync_backfill sync_tail=${syncTail} process_tail=-1`,
    );
    return state;
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
      console.info(
        `[onlydoge] core stage changed chain=${dogecoin.id} stage=process_backfill sync_tail=${state.syncTail} latest=${latest}`,
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
    await this.storeRawBlockHeights(dogecoin, heights);

    const nextState = await this.stateStore.upsertCoreIndexerState({
      stage,
      syncTail,
      onlineTip: latest,
      lastError: null,
    });
    await this.publishProgress(latest, nextState);
    console.info(
      `[onlydoge] core synced chain=${dogecoin.id} blocks=${heights.at(0) ?? state.syncTail + 1}-${heights.at(-1) ?? syncTail} latest=${latest}`,
    );
    return true;
  }

  private async storeRawBlockHeights(
    dogecoin: DogecoinRuntimeConfig,
    heights: number[],
  ): Promise<void> {
    await mapWithConcurrency(heights, this.settings.syncConcurrency, async (height) => {
      const snapshot = await this.rpc.getBlockSnapshot(dogecoin, height);
      await this.rawBlocks.putPart(height, rawBlockPart, snapshot, {
        timeoutMs: this.settings.coreRawStorageTimeoutMs,
      });
      const block = parseDogecoinBlockSnapshot(snapshot);
      await this.stateStore.upsertCoreBlock({
        blockHeight: block.height,
        blockHash: block.hash,
        previousBlockHash: block.previousHash,
        blockTime: block.time,
        txCount: block.tx.length,
        rawStorageKey: rawBlockPart,
        fetchedAt: new Date().toISOString(),
        processedAt: null,
      });
    });
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
    if (state.processTail >= latest - this.settings.coreOnlineTipDistance) {
      await this.promoteBackfillToOnline(dogecoin, latest, state, currentStateReady);
      return true;
    }

    await this.returnBackfillToSync(dogecoin, latest, state);
    return true;
  }

  private async promoteBackfillToOnline(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
    currentStateReady: boolean,
  ): Promise<void> {
    await this.materializeCurrentStateIfNeeded(state, currentStateReady);
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
    console.info(
      `[onlydoge] core stage changed chain=${dogecoin.id} stage=online process_tail=${state.processTail} latest=${latest}`,
    );
  }

  private async materializeCurrentStateIfNeeded(
    state: CoreIndexerState,
    currentStateReady: boolean,
  ): Promise<void> {
    if (currentStateReady) {
      return;
    }

    await this.stateStore.materializeCoreDogecoinCurrentState(state.processTail, {
      statementTimeoutMs: this.settings.coreDbStatementTimeoutMs,
    });
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
    console.info(
      `[onlydoge] core stage changed chain=${dogecoin.id} stage=sync_backfill reason=tip-advanced process_tail=${state.processTail} latest=${latest}`,
    );
  }

  private async processBackfillWindow(
    dogecoin: DogecoinRuntimeConfig,
    latest: number,
    state: CoreIndexerState,
    currentStateReady: boolean,
    stage: CoreIndexerState['stage'] = 'process_backfill',
  ): Promise<boolean> {
    const end = Math.min(state.syncTail, state.processTail + this.settings.coreProcessWindow);
    const heights = range(state.processTail + 1, end);
    const metrics = await this.processWindow(dogecoin, latest, heights, currentStateReady);
    await this.publishWindowProgress(dogecoin, latest, metrics, stage);

    console.info(
      `[onlydoge] core processed chain=${dogecoin.id} blocks=${metrics.start}-${metrics.end} sync_tail=${state.syncTail} latest=${latest}`,
    );
    return true;
  }

  private async processWindow(
    dogecoin: DogecoinRuntimeConfig,
    _latest: number,
    heights: number[],
    updateCurrentState: boolean,
  ): Promise<CoreWindowMetrics> {
    const bounds = requireCoreProcessWindowBounds(heights);
    const attempt = this.createCoreBlockAttempt(bounds.lastHeight);
    this.activeBlockAttempt = attempt;

    try {
      return await this.processWindowWithAttempt(
        dogecoin,
        heights,
        bounds,
        updateCurrentState,
        attempt,
      );
    } catch (error) {
      await this.exitForCoreBlockTimeout(error, dogecoin, attempt.height);
      throw error;
    } finally {
      this.clearActiveBlockAttempt(attempt);
    }
  }

  private createCoreBlockAttempt(height: number): CoreBlockAttempt {
    return {
      activeStep: 'load_raw',
      height,
      startedAtMs: Date.now(),
    };
  }

  private async processWindowWithAttempt(
    dogecoin: DogecoinRuntimeConfig,
    heights: number[],
    bounds: CoreProcessWindowBounds,
    updateCurrentState: boolean,
    attempt: CoreBlockAttempt,
  ): Promise<CoreWindowMetrics> {
    const totalStartedAt = Date.now();
    const { result: snapshots, elapsedMs: loadRawMs } = await this.loadRawSnapshots(
      dogecoin,
      heights,
      attempt,
    );
    const { result: applications, elapsedMs: buildMs } = await this.buildWindowApplications(
      snapshots,
      attempt,
    );
    const { result: applyResult, elapsedMs: applyMs } = await this.applyWindowApplications(
      applications,
      updateCurrentState,
      attempt,
    );

    return coreWindowMetrics({
      applications,
      applyMs,
      applyResult,
      bounds,
      buildMs,
      loadRawMs,
      totalStartedAt,
    });
  }

  private loadRawSnapshots(
    dogecoin: DogecoinRuntimeConfig,
    heights: number[],
    attempt: CoreBlockAttempt,
  ): Promise<{ elapsedMs: number; result: Record<string, unknown>[] }> {
    return this.runCoreBlockStep(attempt, 'load_raw', () =>
      mapWithConcurrency(heights, this.settings.coreProcessLoadConcurrency, (height) =>
        this.loadRawSnapshot(dogecoin, height),
      ),
    );
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

    console.info(
      `[onlydoge] phase=core-process-window chain=${dogecoin.id} blocks=${metrics.start}-${metrics.end} applied=${metrics.applied} load_raw_ms=${metrics.loadRawMs} build_ms=${metrics.buildMs} apply_ms=${metrics.applyMs} publish_progress_ms=${publishMs} total_ms=${metrics.totalMs + publishMs} creates=${metrics.creates} spends=${metrics.spends} process_tail=${nextState.processTail}`,
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
    console.error(
      `[onlydoge] phase=core-process error=timeout chain=${dogecoin.id} height=${height} active_step=${error.step} timeout_ms=${error.timeoutMs}`,
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
      lastError: null,
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
    const metrics = await this.processWindow(dogecoin, latest, heights, currentStateReady);
    await this.publishWindowProgress(dogecoin, latest, metrics, 'online');

    console.info(
      `[onlydoge] core processed chain=${dogecoin.id} blocks=${metrics.start}-${metrics.end} sync_tail=${state.syncTail} latest=${latest}`,
    );
    return metrics.applied || state.processTail < metrics.end;
  }

  private async publishProgress(latest: number, state: CoreIndexerState): Promise<void> {
    const historyReady =
      (await this.configs.getJsonValue<boolean>(configKeyDogecoinHistoryReady())) === true;
    const writes = [
      this.configs.setJsonValue(configKeyPrimary(), createLease(this.instanceId)),
      this.configs.setJsonValue(configKeyIndexerStage(), state.stage),
      this.configs.setJsonValue(configKeyIndexerSyncTail(), state.syncTail),
      this.configs.setJsonValue(configKeyIndexerProcessTail(), state.processTail),
      this.configs.setJsonValue(
        configKeyIndexerFinalizedTail(),
        finalizedTail(state.processTail, this.settings.coreReprocessDepth),
      ),
      this.configs.setJsonValue(configKeyIndexerReprocessDepth(), this.settings.coreReprocessDepth),
      this.configs.setJsonValue(configKeyIndexerSyncProgress(), toProgress(state.syncTail, latest)),
      this.configs.setJsonValue(
        configKeyIndexerProcessProgress(),
        toProgress(state.processTail, latest),
      ),
    ];
    if (historyReady) {
      writes.push(
        this.configs.setJsonValue(configKeyIndexerFactTail(), state.processTail),
        this.configs.setJsonValue(
          configKeyIndexerFactProgress(),
          toProgress(state.processTail, latest),
        ),
      );
    }
    if (await this.isDogecoinAnalyticsFactsReady()) {
      writes.push(
        this.configs.setJsonValue(
          configKeyDogecoinAnalyticsFactsTail(),
          finalizedTail(state.processTail, this.settings.coreReprocessDepth),
        ),
      );
    }

    await Promise.all(writes);
    this.observeProgress(state);
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

  private coreProgressBacklogAgeMs(
    state: CoreIndexerState,
    latest: number,
    observation: ProgressObservation,
  ): number | null {
    if (!hasCoreWorkBacklog(state, latest, this.settings.coreSyncCompleteDistance)) {
      return null;
    }

    return Date.now() - observation.observedAtMs;
  }

  private async exitForExpiredProgressWatchdog(
    dogecoin: DogecoinRuntimeConfig,
    state: CoreIndexerState,
    ageMs: number,
  ): Promise<void> {
    const activeAttempt = activeAttemptLog(this.activeBlockAttempt ?? undefined);
    const message = `core progress watchdog expired chain=${dogecoin.id} stage=${state.stage} sync_tail=${state.syncTail} process_tail=${state.processTail} age_ms=${ageMs}`;
    await this.stateStore.setCoreIndexerError(message);
    console.error(
      `[onlydoge] phase=core-watchdog error=no-progress chain=${dogecoin.id} stage=${state.stage} sync_tail=${state.syncTail} process_tail=${state.processTail} age_ms=${ageMs} active_height=${activeAttempt.height} active_step=${activeAttempt.step}`,
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

  private async leaseLeadership(): Promise<boolean> {
    const current = await this.configs.getJsonValue<PrimaryLease | string>(configKeyPrimary());
    const currentLease = toPrimaryLease(current);
    if (!currentLease) {
      return this.claimPrimaryLease(current, '');
    }

    return this.leaseKnownPrimary(current, currentLease);
  }

  private async leaseKnownPrimary(
    current: PrimaryLease | string | null,
    currentLease: PrimaryLease,
  ): Promise<boolean> {
    if (currentLease.instanceId === this.instanceId) {
      return this.refreshPrimaryLease();
    }

    return this.leaseCompetingPrimary(current, currentLease);
  }

  private async leaseCompetingPrimary(
    current: PrimaryLease | string | null,
    currentLease: PrimaryLease,
  ): Promise<boolean> {
    if (isFreshPrimaryLease(currentLease)) {
      return false;
    }

    return this.claimPrimaryLease(current, ' replaced-stale-primary');
  }

  private async refreshPrimaryLease(): Promise<boolean> {
    await this.configs.setJsonValue(configKeyPrimary(), createLease(this.instanceId));
    return true;
  }

  private async claimPrimaryLease(
    current: PrimaryLease | string | null,
    logSuffix: string,
  ): Promise<boolean> {
    const claimed = await this.configs.compareAndSwapJsonValue(
      configKeyPrimary(),
      current,
      createLease(this.instanceId),
    );
    if (claimed) {
      console.info(`[onlydoge] core indexer primary instance=${this.instanceId}${logSuffix}`);
    }
    return claimed;
  }
}

function shouldContinueStartLoop(signal: AbortSignal | undefined): boolean {
  return signal?.aborted !== true;
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

function isFreshPrimaryLease(lease: PrimaryLease): boolean {
  return Date.now() - Date.parse(lease.heartbeatAt) <= leaseTimeoutMs;
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

  return {
    hash: requireString(candidate.hash, 'block.hash'),
    height: requireNumber(candidate.height, 'block.height'),
    time: requireNumber(candidate.time, 'block.time'),
    previousHash: readPreviousBlockHash(candidate.previousblockhash),
    tx: readDogecoinTransactions(candidate.tx),
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

function readDogecoinTransactions(value: unknown): DogecoinTransaction[] {
  return Array.isArray(value) ? value.filter(isDogecoinTransaction) : [];
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
