import { randomUUID } from 'node:crypto';

import type { PrimaryId } from '@onlydoge/shared-kernel';

import type {
  BlockchainRpcPort,
  CoordinatorConfigPort,
  CoreDogecoinStateStorePort,
  IndexedNetworkPort,
  RawBlockStoragePort,
} from '../contracts/ports';
import { fromDecimalUnits } from '../domain/amounts';
import {
  configKeyBlockHeight,
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

interface CoreDogecoinNetwork {
  architecture: 'dogecoin';
  blockTime: number;
  id: string;
  networkId: PrimaryId;
  rpcEndpoint: string;
  rps: number;
  zmqBlockEndpoint?: string | null;
}

type IndexedNetwork = Awaited<ReturnType<IndexedNetworkPort['listActiveNetworks']>>[number];

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
  networkId: PrimaryId;
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
  private readonly activeBlockAttempts = new Map<PrimaryId, CoreBlockAttempt>();
  private latestLog: string | null = null;
  private readonly progressObservations = new Map<PrimaryId, ProgressObservation>();

  public constructor(
    private readonly configs: CoordinatorConfigPort,
    private readonly networks: IndexedNetworkPort,
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

    return this.runActiveDogecoinNetworks();
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

    return this.networkWorkIdleMs();
  }

  private async networkWorkIdleMs(): Promise<number | null> {
    if (await this.runOnce()) {
      return null;
    }

    return workerIdleMs;
  }

  private async runActiveDogecoinNetworks(): Promise<boolean> {
    const dogecoinNetworks = await this.listDogecoinNetworks();
    if (dogecoinNetworks.length === 0) {
      this.logOnce('[onlydoge] core indexer idle reason=no-dogecoin-networks');
      return false;
    }

    return this.runDogecoinNetworkBatch(dogecoinNetworks);
  }

  private async runDogecoinNetworkBatch(dogecoinNetworks: CoreDogecoinNetwork[]): Promise<boolean> {
    let didWork = false;
    for (const network of dogecoinNetworks) {
      didWork = didAnyNetworkWork(didWork, await this.runNetwork(network));
    }
    return didWork;
  }

  private async listDogecoinNetworks(): Promise<CoreDogecoinNetwork[]> {
    return (await this.networks.listActiveNetworks()).filter(isDogecoinNetwork);
  }

  private async runNetwork(network: CoreDogecoinNetwork): Promise<boolean> {
    const latest = await this.rpc.getBlockHeight(network);
    await this.configs.setJsonValue(configKeyBlockHeight(network.networkId), latest);

    const state = await this.ensureState(network, latest);
    await this.publishProgress(network.networkId, latest, state);
    await this.assertProgressWatchdog(network, latest, state);

    try {
      return await this.runNetworkStage(network, latest, state);
    } catch (error) {
      await this.stateStore.setCoreIndexerError(network.networkId, formatError(error));
      throw error;
    }
  }

  private runNetworkStage(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    const stageRunners: Record<CoreIndexerState['stage'], () => Promise<boolean>> = {
      sync_backfill: () => this.syncBackfill(network, latest, state),
      process_backfill: () => this.processBackfill(network, latest, state),
      online: () => this.online(network, latest, state),
    };
    return stageRunners[state.stage]();
  }

  private async ensureState(
    network: CoreDogecoinNetwork,
    latest: number,
  ): Promise<CoreIndexerState> {
    const current = await this.stateStore.getCoreIndexerState(network.networkId);
    if (current) {
      return current;
    }

    const storedSyncTail = await this.storedSyncTail(network.networkId);
    const syncTail = Math.min(storedSyncTail, latest);
    const state = await this.stateStore.upsertCoreIndexerState({
      networkId: network.networkId,
      stage: 'sync_backfill',
      syncTail,
      processTail: -1,
      onlineTip: latest,
      lastError: null,
    });
    console.info(
      `[onlydoge] core indexer initialized network=${network.id} stage=sync_backfill sync_tail=${syncTail} process_tail=-1`,
    );
    return state;
  }

  private async storedSyncTail(networkId: PrimaryId): Promise<number> {
    const value = await this.configs.getJsonValue<number>(configKeyIndexerSyncTail(networkId));
    return value ?? -1;
  }

  private async syncBackfill(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    if (shouldPromoteToProcessBackfill(state, latest, this.settings.coreSyncCompleteDistance)) {
      await this.stateStore.upsertCoreIndexerState({
        networkId: network.networkId,
        stage: 'process_backfill',
        onlineTip: latest,
      });
      await this.configs.setJsonValue(configKeyIndexerStage(network.networkId), 'process_backfill');
      console.info(
        `[onlydoge] core stage changed network=${network.id} stage=process_backfill sync_tail=${state.syncTail} latest=${latest}`,
      );
      return true;
    }

    const end = Math.min(latest, state.syncTail + this.settings.syncWindow);
    return this.syncRawBlockWindow(network, latest, state, end, 'sync_backfill');
  }

  private async syncRawBlockWindow(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
    end: number,
    stage: CoreIndexerState['stage'],
  ): Promise<boolean> {
    const heights = range(state.syncTail + 1, end);
    return this.syncRawBlockHeights(network, latest, state, heights, end, stage);
  }

  private async syncRawBlockHeights(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
    heights: number[],
    syncTail: number,
    stage: CoreIndexerState['stage'],
  ): Promise<boolean> {
    await mapWithConcurrency(heights, this.settings.syncConcurrency, async (height) => {
      const snapshot = await this.rpc.getBlockSnapshot(network, height);
      await this.rawBlocks.putPart(network.networkId, height, rawBlockPart, snapshot, {
        timeoutMs: this.settings.coreRawStorageTimeoutMs,
      });
      const block = parseDogecoinBlockSnapshot(snapshot);
      await this.stateStore.upsertCoreBlock({
        networkId: network.networkId,
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

    const nextState = await this.stateStore.upsertCoreIndexerState({
      networkId: network.networkId,
      stage,
      syncTail,
      onlineTip: latest,
      lastError: null,
    });
    await this.publishProgress(network.networkId, latest, nextState);
    console.info(
      `[onlydoge] core synced network=${network.id} blocks=${heights.at(0) ?? state.syncTail + 1}-${heights.at(-1) ?? syncTail} latest=${latest}`,
    );
    return true;
  }

  private async processBackfill(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    const currentStateReady =
      (await this.configs.getJsonValue<boolean>(
        configKeyDogecoinCurrentStateReady(network.networkId),
      )) === true;

    if (state.processTail >= state.syncTail) {
      return this.transitionCompletedBackfill(network, latest, state, currentStateReady);
    }

    return this.processBackfillWindow(network, latest, state, currentStateReady);
  }

  private async transitionCompletedBackfill(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
    currentStateReady: boolean,
  ): Promise<boolean> {
    if (state.processTail >= latest - this.settings.coreOnlineTipDistance) {
      await this.promoteBackfillToOnline(network, latest, state, currentStateReady);
      return true;
    }

    await this.returnBackfillToSync(network, latest, state);
    return true;
  }

  private async promoteBackfillToOnline(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
    currentStateReady: boolean,
  ): Promise<void> {
    await this.materializeCurrentStateIfNeeded(network, state, currentStateReady);
    await this.stateStore.upsertCoreIndexerState({
      networkId: network.networkId,
      stage: 'online',
      onlineTip: latest,
      lastError: null,
    });
    await this.configs.setJsonValue(configKeyIndexerStage(network.networkId), 'online');
    console.info(
      `[onlydoge] core stage changed network=${network.id} stage=online process_tail=${state.processTail} latest=${latest}`,
    );
  }

  private async materializeCurrentStateIfNeeded(
    network: CoreDogecoinNetwork,
    state: CoreIndexerState,
    currentStateReady: boolean,
  ): Promise<void> {
    if (currentStateReady) {
      return;
    }

    await this.stateStore.materializeCoreDogecoinCurrentState(
      network.networkId,
      state.processTail,
      {
        statementTimeoutMs: this.settings.coreDbStatementTimeoutMs,
      },
    );
  }

  private async returnBackfillToSync(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<void> {
    await this.stateStore.upsertCoreIndexerState({
      networkId: network.networkId,
      stage: 'sync_backfill',
      onlineTip: latest,
    });
    await this.configs.setJsonValue(configKeyIndexerStage(network.networkId), 'sync_backfill');
    console.info(
      `[onlydoge] core stage changed network=${network.id} stage=sync_backfill reason=tip-advanced process_tail=${state.processTail} latest=${latest}`,
    );
  }

  private async processBackfillWindow(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
    currentStateReady: boolean,
    stage: CoreIndexerState['stage'] = 'process_backfill',
  ): Promise<boolean> {
    const end = Math.min(state.syncTail, state.processTail + this.settings.coreProcessWindow);
    const heights = range(state.processTail + 1, end);
    const metrics = await this.processWindow(network, latest, heights, currentStateReady);
    await this.publishWindowProgress(network, latest, metrics, stage);

    console.info(
      `[onlydoge] core processed network=${network.id} blocks=${metrics.start}-${metrics.end} sync_tail=${state.syncTail} latest=${latest}`,
    );
    return true;
  }

  private async processWindow(
    network: CoreDogecoinNetwork,
    _latest: number,
    heights: number[],
    updateCurrentState: boolean,
  ): Promise<CoreWindowMetrics> {
    const bounds = requireCoreProcessWindowBounds(heights);
    const attempt = this.createCoreBlockAttempt(network, bounds.lastHeight);
    this.activeBlockAttempts.set(network.networkId, attempt);

    try {
      return await this.processWindowWithAttempt(
        network,
        heights,
        bounds,
        updateCurrentState,
        attempt,
      );
    } catch (error) {
      await this.exitForCoreBlockTimeout(error, network, attempt.height);
      throw error;
    } finally {
      this.clearActiveBlockAttempt(network.networkId, attempt);
    }
  }

  private createCoreBlockAttempt(network: CoreDogecoinNetwork, height: number): CoreBlockAttempt {
    return {
      activeStep: 'load_raw',
      height,
      networkId: network.networkId,
      startedAtMs: Date.now(),
    };
  }

  private async processWindowWithAttempt(
    network: CoreDogecoinNetwork,
    heights: number[],
    bounds: CoreProcessWindowBounds,
    updateCurrentState: boolean,
    attempt: CoreBlockAttempt,
  ): Promise<CoreWindowMetrics> {
    const totalStartedAt = Date.now();
    const { result: snapshots, elapsedMs: loadRawMs } = await this.loadRawSnapshots(
      network,
      heights,
      attempt,
    );
    const { result: applications, elapsedMs: buildMs } = await this.buildWindowApplications(
      network.networkId,
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
    network: CoreDogecoinNetwork,
    heights: number[],
    attempt: CoreBlockAttempt,
  ): Promise<{ elapsedMs: number; result: Record<string, unknown>[] }> {
    return this.runCoreBlockStep(attempt, 'load_raw', () =>
      mapWithConcurrency(heights, this.settings.coreProcessLoadConcurrency, (height) =>
        this.loadRawSnapshot(network, height),
      ),
    );
  }

  private async loadRawSnapshot(
    network: CoreDogecoinNetwork,
    height: number,
  ): Promise<Record<string, unknown>> {
    const snapshot = await this.rawBlocks.getPart<Record<string, unknown>>(
      network.networkId,
      height,
      rawBlockPart,
      {
        timeoutMs: this.settings.coreRawStorageTimeoutMs,
      },
    );
    if (!snapshot) {
      throw new Error(`missing raw dogecoin block snapshot network=${network.id} height=${height}`);
    }
    return snapshot;
  }

  private buildWindowApplications(
    networkId: PrimaryId,
    snapshots: Record<string, unknown>[],
    attempt: CoreBlockAttempt,
  ): Promise<{ elapsedMs: number; result: CoreDogecoinBlockApplication[] }> {
    return this.runCoreBlockStep(attempt, 'build_application', () =>
      Promise.resolve(this.buildFastBlockApplications(networkId, snapshots)),
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

  private clearActiveBlockAttempt(networkId: PrimaryId, attempt: CoreBlockAttempt): void {
    if (this.activeBlockAttempts.get(networkId) === attempt) {
      this.activeBlockAttempts.delete(networkId);
    }
  }

  private buildFastBlockApplications(
    networkId: PrimaryId,
    snapshots: Record<string, unknown>[],
  ): CoreDogecoinBlockApplication[] {
    return buildFastCoreDogecoinBlockApplications(networkId, snapshots);
  }

  private async publishWindowProgress(
    network: CoreDogecoinNetwork,
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
          networkId: network.networkId,
          startedAtMs: Date.now(),
        },
        'publish_progress',
        async () => {
          const state = await this.stateStore.upsertCoreIndexerState({
            networkId: network.networkId,
            stage,
            processTail: metrics.end,
            onlineTip: latest,
            lastError: null,
          });
          await this.publishProgress(network.networkId, latest, state);
          return state;
        },
      );
      nextState = published.result;
      publishMs = published.elapsedMs;
    } catch (error) {
      await this.exitForCoreBlockTimeout(error, network, metrics.end);
      throw error;
    }

    console.info(
      `[onlydoge] phase=core-process-window network=${network.id} blocks=${metrics.start}-${metrics.end} applied=${metrics.applied} load_raw_ms=${metrics.loadRawMs} build_ms=${metrics.buildMs} apply_ms=${metrics.applyMs} publish_progress_ms=${publishMs} total_ms=${metrics.totalMs + publishMs} creates=${metrics.creates} spends=${metrics.spends} process_tail=${nextState.processTail}`,
    );
  }

  private async exitForCoreBlockTimeout(
    error: unknown,
    network: CoreDogecoinNetwork,
    height: number,
  ): Promise<void> {
    if (!(error instanceof CoreBlockTimeoutError)) {
      return;
    }

    const message = `core block timed out network=${network.id} height=${height} active_step=${error.step} timeout_ms=${error.timeoutMs}`;
    await this.stateStore.setCoreIndexerError(network.networkId, message);
    console.error(
      `[onlydoge] phase=core-process error=timeout network=${network.id} height=${height} active_step=${error.step} timeout_ms=${error.timeoutMs}`,
    );
    this.exitProcess(1);
  }

  private async online(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    const didWork = await this.advanceOnlineBacklog(network, latest, state);
    if (didWork) {
      return true;
    }

    if (await this.publishOnlineNoWorkIfPossible(network, latest, state)) {
      return false;
    }

    return false;
  }

  private async publishOnlineNoWorkIfPossible(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    if (await this.publishReadyOnlineStateIfCurrent(network, latest, state)) {
      return true;
    }

    return this.publishProgressIfAtLatest(network, latest, state);
  }

  private async isCurrentOnlineStateReady(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    return (
      state.syncTail >= latest &&
      (await this.isDogecoinCurrentStateReady(network.networkId)) &&
      state.processTail >= latest
    );
  }

  private async publishReadyOnlineStateIfCurrent(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    if (!(await this.isCurrentOnlineStateReady(network, latest, state))) {
      return false;
    }

    await this.publishReadyOnlineState(network.networkId, latest, state);
    return true;
  }

  private async publishProgressIfAtLatest(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    if (!isCoreStateAtLatest(state, latest)) {
      return false;
    }

    await this.publishProgress(network.networkId, latest, state);
    return true;
  }

  private async publishReadyOnlineState(
    networkId: PrimaryId,
    latest: number,
    state: CoreIndexerState,
  ): Promise<void> {
    const nextState = await this.ensureReadyOnlineState(networkId, latest, state);
    await this.publishProgress(networkId, latest, nextState);
  }

  private async ensureReadyOnlineState(
    networkId: PrimaryId,
    latest: number,
    state: CoreIndexerState,
  ): Promise<CoreIndexerState> {
    if (isReadyOnlineState(state, latest)) {
      return state;
    }

    return this.stateStore.upsertCoreIndexerState({
      networkId,
      stage: 'online',
      onlineTip: latest,
      lastError: null,
    });
  }

  private async advanceOnlineBacklog(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<boolean> {
    const syncEnd = Math.min(latest, state.syncTail + this.settings.syncWindow);
    const didSync = await this.syncOnlineBacklogIfNeeded(network, latest, state, syncEnd);

    const refreshed = await this.refreshedOnlineBacklogState(network, state, syncEnd);
    const didProcess = await this.processOnlineBacklogIfNeeded(network, latest, refreshed, didSync);
    return [didSync, didProcess].includes(true);
  }

  private async syncOnlineBacklogIfNeeded(
    network: CoreDogecoinNetwork,
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
    await this.syncRawBlockHeights(network, latest, state, heights, syncEnd, 'online');
    return true;
  }

  private async refreshedOnlineBacklogState(
    network: CoreDogecoinNetwork,
    state: CoreIndexerState,
    syncEnd: number,
  ): Promise<CoreIndexerState> {
    const refreshed = await this.stateStore.getCoreIndexerState(network.networkId);
    if (refreshed) {
      return refreshed;
    }

    return { ...state, syncTail: syncEnd };
  }

  private async processOnlineBacklogIfNeeded(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
    forceTailReprocess: boolean,
  ): Promise<boolean> {
    const processTarget = Math.min(state.syncTail, latest);
    if (state.processTail >= processTarget && !forceTailReprocess) {
      return false;
    }

    const didProcess = await this.processOnlineWindow(
      network,
      latest,
      state,
      processTarget,
      await this.isDogecoinCurrentStateReady(network.networkId),
    );
    return didProcess;
  }

  private async processOnlineWindow(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
    processTarget: number,
    currentStateReady: boolean,
  ): Promise<boolean> {
    const start = onlineProcessWindowStart(
      state.processTail,
      processTarget,
      this.settings.coreReprocessDepth,
    );
    const end = Math.min(processTarget, state.processTail + this.settings.coreProcessWindow);
    const heights = range(start, end);
    const metrics = await this.processWindow(network, latest, heights, currentStateReady);
    await this.publishWindowProgress(network, latest, metrics, 'online');

    console.info(
      `[onlydoge] core processed network=${network.id} blocks=${metrics.start}-${metrics.end} sync_tail=${state.syncTail} latest=${latest}`,
    );
    return metrics.applied || state.processTail < metrics.end;
  }

  private async publishProgress(
    networkId: PrimaryId,
    latest: number,
    state: CoreIndexerState,
  ): Promise<void> {
    const historyReady =
      (await this.configs.getJsonValue<boolean>(configKeyDogecoinHistoryReady(networkId))) === true;
    const writes = [
      this.configs.setJsonValue(configKeyPrimary(), createLease(this.instanceId)),
      this.configs.setJsonValue(configKeyIndexerStage(networkId), state.stage),
      this.configs.setJsonValue(configKeyIndexerSyncTail(networkId), state.syncTail),
      this.configs.setJsonValue(configKeyIndexerProcessTail(networkId), state.processTail),
      this.configs.setJsonValue(
        configKeyIndexerFinalizedTail(networkId),
        finalizedTail(state.processTail, this.settings.coreReprocessDepth),
      ),
      this.configs.setJsonValue(
        configKeyIndexerReprocessDepth(networkId),
        this.settings.coreReprocessDepth,
      ),
      this.configs.setJsonValue(
        configKeyIndexerSyncProgress(networkId),
        toProgress(state.syncTail, latest),
      ),
      this.configs.setJsonValue(
        configKeyIndexerProcessProgress(networkId),
        toProgress(state.processTail, latest),
      ),
    ];
    if (historyReady) {
      writes.push(
        this.configs.setJsonValue(configKeyIndexerFactTail(networkId), state.processTail),
        this.configs.setJsonValue(
          configKeyIndexerFactProgress(networkId),
          toProgress(state.processTail, latest),
        ),
      );
    }

    await Promise.all(writes);
    this.observeProgress(networkId, state);
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
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<void> {
    if (await this.shouldSkipProgressWatchdog(network.networkId, state)) {
      return;
    }

    await this.assertObservedProgressWatchdog(network, latest, state);
  }

  private async assertObservedProgressWatchdog(
    network: CoreDogecoinNetwork,
    latest: number,
    state: CoreIndexerState,
  ): Promise<void> {
    const observation = this.observeProgress(network.networkId, state);
    const ageMs = this.coreProgressBacklogAgeMs(state, latest, observation);
    if (isFreshProgressAge(ageMs, this.settings.coreProgressWatchdogMs)) {
      return;
    }

    await this.exitForExpiredProgressWatchdog(network, state, requireProgressAge(ageMs));
  }

  private async shouldSkipProgressWatchdog(
    networkId: PrimaryId,
    state: CoreIndexerState,
  ): Promise<boolean> {
    if (state.stage !== 'online') {
      return false;
    }

    return this.isDogecoinCurrentStateReady(networkId);
  }

  private async isDogecoinCurrentStateReady(networkId: PrimaryId): Promise<boolean> {
    return (
      (await this.configs.getJsonValue<boolean>(configKeyDogecoinCurrentStateReady(networkId))) ===
      true
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
    network: CoreDogecoinNetwork,
    state: CoreIndexerState,
    ageMs: number,
  ): Promise<void> {
    const activeAttempt = activeAttemptLog(this.activeBlockAttempts.get(network.networkId));
    const message = `core progress watchdog expired network=${network.id} stage=${state.stage} sync_tail=${state.syncTail} process_tail=${state.processTail} age_ms=${ageMs}`;
    await this.stateStore.setCoreIndexerError(network.networkId, message);
    console.error(
      `[onlydoge] phase=core-watchdog error=no-progress network=${network.id} stage=${state.stage} sync_tail=${state.syncTail} process_tail=${state.processTail} age_ms=${ageMs} active_height=${activeAttempt.height} active_step=${activeAttempt.step}`,
    );
    this.exitProcess(1);
  }

  private observeProgress(networkId: PrimaryId, state: CoreIndexerState): ProgressObservation {
    const previous = this.progressObservations.get(networkId);
    if (isSameProgressObservation(previous, state)) {
      return previous;
    }

    return this.recordProgressObservation(networkId, state);
  }

  private recordProgressObservation(
    networkId: PrimaryId,
    state: CoreIndexerState,
  ): ProgressObservation {
    const next = {
      observedAtMs: Date.now(),
      processTail: state.processTail,
      stage: state.stage,
      syncTail: state.syncTail,
    };
    this.progressObservations.set(networkId, next);
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

  private logOnce(message: string): void {
    if (this.latestLog === message) {
      return;
    }

    this.latestLog = message;
    console.info(message);
  }
}

function isDogecoinNetwork(network: IndexedNetwork): network is CoreDogecoinNetwork {
  return network.architecture === 'dogecoin';
}

function shouldContinueStartLoop(signal: AbortSignal | undefined): boolean {
  return signal?.aborted !== true;
}

function didAnyNetworkWork(previous: boolean, next: boolean): boolean {
  return [previous, next].includes(true);
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
  return Math.max(
    0,
    Math.min(processTail + 1, reprocessWindowStart(processTarget, coreReprocessDepth)),
  );
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
  networkId: PrimaryId,
  snapshots: Record<string, unknown>[],
): CoreDogecoinBlockApplication[] {
  const tracker = createCoreWindowKeyTracker();
  return snapshots.map((snapshot) => buildCoreBlockApplication(networkId, snapshot, tracker));
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
  networkId: PrimaryId,
  snapshot: Record<string, unknown>,
  tracker: CoreWindowKeyTracker,
): CoreDogecoinBlockApplication {
  const block = parseDogecoinBlockSnapshot(snapshot);
  const effects = collectCoreBlockEffects(networkId, block, tracker);

  return {
    networkId,
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
  networkId: PrimaryId,
  block: ParsedDogecoinBlock,
  tracker: CoreWindowKeyTracker,
): CoreTransactionEffects {
  const effects: CoreTransactionEffects = { utxoCreates: [], utxoSpends: [] };
  for (const [txIndex, tx] of block.tx.entries()) {
    appendTransactionEffects(effects, networkId, block, tx, txIndex, tracker);
  }
  return effects;
}

function appendTransactionEffects(
  effects: CoreTransactionEffects,
  networkId: PrimaryId,
  block: ParsedDogecoinBlock,
  tx: DogecoinTransaction,
  txIndex: number,
  tracker: CoreWindowKeyTracker,
): void {
  const txid = requireString(tx.txid, 'tx.txid');
  const isCoinbase = hasCoinbaseInput(tx);
  effects.utxoSpends.push(...buildTransactionSpends(txid, block.height, tx, tracker));
  effects.utxoCreates.push(
    ...buildTransactionOutputs(networkId, block, tx, txid, txIndex, isCoinbase, tracker),
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
  networkId: PrimaryId,
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
      buildTransactionOutput(
        networkId,
        block,
        txid,
        txIndex,
        output,
        outputIndex,
        isCoinbase,
        tracker,
      ),
    );
  }
  return outputs;
}

function buildTransactionOutput(
  networkId: PrimaryId,
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
    networkId,
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
