import { randomUUID } from 'node:crypto';

import { nowIsoString, type PrimaryId, sleep } from '@onlydoge/shared-kernel';

import type {
  BlockchainRpcPort,
  CoordinatorConfigPort,
  IndexedNetworkPort,
  ProjectionFactWarehousePort,
  ProjectionLinkSeedPort,
  ProjectionStateStorePort,
  RawBlockStoragePort,
} from '../contracts/ports';
import {
  configKeyBlockHeight,
  configKeyDogecoinCurrentStateReady,
  configKeyIndexerFactProgress,
  configKeyIndexerFactTail,
  configKeyIndexerProcessProgress,
  configKeyIndexerProcessTail,
  configKeyIndexerSyncProgress,
  configKeyIndexerSyncTail,
  configKeyPrimary,
  configKeyProjectionBootstrapCursorBalance,
  configKeyProjectionBootstrapCursorUtxo,
  configKeyProjectionBootstrapPhase,
  configKeyProjectionBootstrapStartedAt,
  configKeyProjectionBootstrapTail,
  configKeyProjectionBootstrapTargetTail,
} from '../domain/config-keys';
import type {
  BlockProjectionBatch,
  DirectLinkRecord,
  ProjectionBalanceCursor,
  ProjectionBalanceSnapshot,
  ProjectionDirectLinkBatch,
  ProjectionFactWindow,
  ProjectionUtxoOutput,
} from '../domain/projection-models';
import {
  applyAddressMovementsToBalances,
  applyDirectLinkDeltasToSnapshots,
  buildNextProjectionUtxoOutputs,
  collectProjectionBalanceSnapshotKeys,
  collectProjectionDirectLinkSnapshotKeys,
  collectProjectionTouchedOutputKeys,
  orderProjectionBatches,
  parseProjectionBalanceSnapshotKey,
  parseProjectionDirectLinkSnapshotKey,
  projectionBalanceSnapshotKey,
  projectionBlockIdentity,
  projectionDirectLinkSnapshotKey,
  toProjectionAppliedBlocks,
} from '../domain/projection-models';
import { mapWithConcurrency, range } from './concurrency';
import { DogecoinBlockProjector } from './dogecoin-block-projector';
import {
  type AppliedSnapshotRecovery,
  adaptiveWindowLogLine,
  type BootstrapPhase,
  type BootstrapStatus,
  blockRangeWork,
  bootstrapCursorLabel,
  bootstrapRequiredMessage,
  bootstrapRetryMarker,
  bootstrapStatusPhase,
  contiguousStoredTail,
  createPhaseAttempt,
  factPhaseMetrics,
  failedPhaseExecution,
  formatError,
  formatMetrics,
  hasRequiredBootstrapTail,
  idlePhaseExecution,
  isBootstrapPhase,
  isTimeoutError,
  noPhaseWork,
  type PhaseAttempt,
  type PhaseExecutionResult,
  type PhaseName,
  type PhaseWorkResult,
  type ProjectionMode,
  phasePendingText,
  phaseRangeText,
  phaseRate,
  phaseWindowText,
  recoveredSnapshotResult,
  requiresDogecoinPrevoutBootstrap,
  shouldBackoffWindow,
  tailOrInitial,
} from './indexing-pipeline-planning';
import { SourceLinkProjector } from './source-link-projector';

export interface IndexingPipelineSettings {
  bootstrapTimeoutMs: number;
  coreBlockTimeoutMs: number;
  coreDbStatementTimeoutMs: number;
  coreOnlineTipDistance: number;
  coreProcessLoadConcurrency: number;
  coreProcessWindow: number;
  coreProgressWatchdogMs: number;
  coreRawStorageTimeoutMs: number;
  coreSyncCompleteDistance: number;
  dogecoinTransferMaxEdges: number;
  dogecoinTransferMaxInputAddresses: number;
  factTimeoutMs: number;
  factWindow: number;
  leaseHeartbeatIntervalMs: number;
  networkConcurrency: number;
  projectTargetMs: number;
  projectTimeoutMs: number;
  projectWindow: number;
  projectWindowMax: number;
  projectWindowMin: number;
  relinkBacklogThreshold: number;
  relinkBatchSize: number;
  relinkConcurrency: number;
  relinkFrontierBatch: number;
  relinkTipDistance: number;
  relinkTimeoutMs: number;
  syncBacklogHighWatermark: number;
  syncBacklogLowWatermark: number;
  syncConcurrency: number;
  syncTargetMs: number;
  syncTimeoutMs: number;
  syncWindow: number;
  syncWindowMax: number;
  syncWindowMin: number;
}

interface PrimaryLease {
  heartbeatAt: string;
  instanceId: string;
}

interface PhaseTuningState {
  successStreak: number;
  window: number;
}

interface AdaptiveWindowContext {
  maxWindow: number;
  minWindow: number;
  targetMs: number;
  tuning: PhaseTuningState;
}

interface BacklogState {
  backlog: number;
  latestBlockHeight: number;
  processTail: number;
  syncTail: number;
}

interface StartedPhaseAttempt {
  attempt: PhaseAttempt;
  startedAt: number;
  workPromise: Promise<PhaseWorkResult>;
}

interface AppliedSnapshotWindow {
  appliedBlocks: Set<string>;
  heights: number[];
  loadSnapshotsMs: number;
  orderedSnapshots: Record<string, unknown>[];
  windowEnd: number;
}

interface ProjectRecoveryOutcome {
  recoveredTail: number;
  result: PhaseWorkResult | null;
  snapshotsToProject: Record<string, unknown>[];
}

interface FactRecoveryOutcome {
  metrics: Record<string, number>;
  recoveredTail: number;
  result: PhaseWorkResult | null;
  snapshotsToPersist: Record<string, unknown>[];
}

interface TimedValue<T> {
  durationMs: number;
  value: T;
}

type IndexedNetwork = Awaited<ReturnType<IndexedNetworkPort['listActiveNetworks']>>[number];
type AppliedBlockSetReader = Pick<ProjectionStateStorePort, 'listAppliedBlockSet'>;

export type IndexingFactWarehouse = ProjectionFactWarehousePort &
  Pick<
    ProjectionStateStorePort,
    'getBalanceSnapshots' | 'getDirectLinkSnapshots' | 'getUtxoOutputs'
  >;

const bootstrapUtxoChunkSize = 1_000;
const bootstrapBalanceChunkSize = 5_000;

const defaultSettings: IndexingPipelineSettings = {
  bootstrapTimeoutMs: 60_000,
  coreBlockTimeoutMs: 120_000,
  coreDbStatementTimeoutMs: 30_000,
  coreOnlineTipDistance: 6,
  coreProcessLoadConcurrency: 8,
  coreProcessWindow: 100,
  coreProgressWatchdogMs: 180_000,
  coreRawStorageTimeoutMs: 30_000,
  coreSyncCompleteDistance: 6,
  dogecoinTransferMaxInputAddresses: 64,
  dogecoinTransferMaxEdges: 1024,
  factWindow: 64,
  factTimeoutMs: 300_000,
  leaseHeartbeatIntervalMs: 5_000,
  networkConcurrency: 2,
  syncWindow: 32,
  syncWindowMin: 32,
  syncWindowMax: 256,
  syncConcurrency: 4,
  syncTargetMs: 15_000,
  syncTimeoutMs: 120_000,
  projectWindow: 8,
  projectWindowMin: 2,
  projectWindowMax: 16,
  projectTargetMs: 30_000,
  projectTimeoutMs: 120_000,
  relinkBatchSize: 16,
  relinkConcurrency: 2,
  relinkFrontierBatch: 32,
  relinkBacklogThreshold: 256,
  relinkTipDistance: 512,
  relinkTimeoutMs: 120_000,
  syncBacklogHighWatermark: 2_048,
  syncBacklogLowWatermark: 512,
};

export interface IndexingPipelineService {
  factWarehouse: IndexingFactWarehouse;
  settings: IndexingPipelineSettings;
  warehouse: Pick<ProjectionStateStorePort, 'applyProjectionWindow'>;
  relinkNewAddress(networkId: number, addressId: number): Promise<void>;
  runOnce(): Promise<boolean>;
  start(signal?: AbortSignal): Promise<void>;
}

export function createIndexingPipelineService(
  configs: CoordinatorConfigPort,
  networks: IndexedNetworkPort,
  seeds: ProjectionLinkSeedPort,
  rawBlocks: RawBlockStoragePort,
  rpc: BlockchainRpcPort,
  projectStateStore: ProjectionStateStorePort,
  stateStore: ProjectionStateStorePort,
  factWarehouse: IndexingFactWarehouse,
  settings: IndexingPipelineSettings = defaultSettings,
): IndexingPipelineService {
  return new IndexingPipelineServiceEngine(
    configs,
    networks,
    seeds,
    rawBlocks,
    rpc,
    projectStateStore,
    stateStore,
    factWarehouse,
    settings,
  ).service;
}

class IndexingPipelineServiceEngine {
  public readonly service: IndexingPipelineService;
  private readonly warehouse: Pick<ProjectionStateStorePort, 'applyProjectionWindow'>;
  private readonly instanceId = randomUUID();
  private readonly dogecoinFactProjector: DogecoinBlockProjector;
  private readonly dogecoinStateProjector: DogecoinBlockProjector;
  private readonly sourceLinkProjector: SourceLinkProjector;
  private readonly latestHeights = new Map<number, number>();
  private readonly bootstrapRetryMarkers = new Map<number, string>();
  private readonly phaseSkipReasons = new Map<string, string>();
  private readonly projectTuning = new Map<number, PhaseTuningState>();
  private readonly syncTuning = new Map<number, PhaseTuningState>();
  private readonly syncPausedNetworks = new Set<number>();
  private readonly bootstrapInFlight = new Map<number, PhaseAttempt>();
  private readonly syncInFlight = new Map<number, PhaseAttempt>();
  private readonly factInFlight = new Map<number, PhaseAttempt>();
  private readonly projectInFlight = new Map<number, PhaseAttempt>();
  private readonly relinkInFlight = new Map<number, PhaseAttempt>();
  private lastIdleReason: string | null = null;
  private readonly leaseTimeoutMs = 15_000;
  private readonly latestHeightRefreshMs = 5_000;
  private readonly workerIdleMs = 250;

  public constructor(
    private readonly configs: CoordinatorConfigPort,
    private readonly networks: IndexedNetworkPort,
    private readonly seeds: ProjectionLinkSeedPort,
    private readonly rawBlocks: RawBlockStoragePort,
    private readonly rpc: BlockchainRpcPort,
    private readonly projectStateStore: ProjectionStateStorePort,
    private readonly stateStore: ProjectionStateStorePort,
    private readonly factWarehouse: IndexingFactWarehouse,
    private readonly settings: IndexingPipelineSettings = defaultSettings,
  ) {
    this.warehouse = projectStateStore;
    this.dogecoinStateProjector = new DogecoinBlockProjector(projectStateStore);
    this.dogecoinFactProjector = new DogecoinBlockProjector(stateStore);
    this.sourceLinkProjector = new SourceLinkProjector(stateStore);
    this.service = {
      factWarehouse: this.factWarehouse,
      settings: this.settings,
      warehouse: this.warehouse,
      relinkNewAddress: (networkId, addressId) => this.relinkNewAddress(networkId, addressId),
      runOnce: () => this.runOnce(),
      start: (signal) => this.start(signal),
    };
  }

  private async start(signal?: AbortSignal): Promise<void> {
    while (shouldContinue(signal)) {
      await this.runPrimaryWorkerSet(signal);
    }
  }

  private async runPrimaryWorkerSet(signal?: AbortSignal): Promise<void> {
    const isPrimary = await this.leaseLeadership();
    if (!isPrimary) {
      this.logIdleOnce('not-primary');
      await sleep(1_000);
      return;
    }

    try {
      await this.withLeaseHeartbeat(() => this.runWorkerLoops(signal));
    } catch (error) {
      console.error(`[onlydoge] indexer worker set failed error=${formatError(error)}`);
      await sleep(1_000);
    }
  }

  private async runOnce(): Promise<boolean> {
    const isPrimary = await this.leaseLeadership();
    if (!isPrimary) {
      this.logIdleOnce('not-primary');
      return false;
    }

    return this.withLeaseHeartbeat(async () => {
      const activeNetworks = await this.networks.listActiveNetworks();
      if (activeNetworks.length === 0) {
        this.logIdleOnce('no-active-networks');
        return false;
      }

      const latestHeights = await this.refreshNetworkHeights(activeNetworks);
      const didWorkByNetwork = await mapWithConcurrency(
        latestHeights.filter((item): item is NonNullable<typeof item> => Boolean(item)),
        this.settings.networkConcurrency,
        ({ latestBlockHeight, network }) => this.runNetworkWorkerOnce(network, latestBlockHeight),
      );

      const didWork = didWorkByNetwork.some(Boolean);
      if (!didWork) {
        this.logIdleOnce('caught-up');
      } else {
        this.lastIdleReason = null;
      }

      return didWork;
    });
  }

  private async runNetworkWorkerOnce(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);
    if (this.shouldDrainProjectionBacklog(backlog)) {
      return this.runBackloggedNetworkPhases(network, latestBlockHeight, backlog);
    }

    return this.runCaughtUpNetworkPhases(network, latestBlockHeight);
  }

  private shouldDrainProjectionBacklog(backlog: BacklogState): boolean {
    return backlog.backlog > this.settings.syncBacklogLowWatermark;
  }

  private async runBackloggedNetworkPhases(
    network: IndexedNetwork,
    latestBlockHeight: number,
    backlog: BacklogState,
  ): Promise<boolean> {
    const didWork = await this.runNetworkPhaseSequence([
      () => this.bootstrapNetworkPhase(network, latestBlockHeight),
      () => this.projectNetworkPhase(network, latestBlockHeight),
      () => this.factNetworkPhase(network, latestBlockHeight),
    ]);
    if (!this.canRunRelink(backlog)) {
      return didWork;
    }

    return (await this.relinkNetworkPhase(network, latestBlockHeight)) || didWork;
  }

  private async runCaughtUpNetworkPhases(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    const didWork = await this.runNetworkPhaseSequence([
      () => this.bootstrapNetworkPhase(network, latestBlockHeight),
      () => this.syncNetworkPhase(network, latestBlockHeight),
      () => this.projectNetworkPhase(network, latestBlockHeight),
      () => this.factNetworkPhase(network, latestBlockHeight),
    ]);
    const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);
    if (!this.canRunRelink(backlog)) {
      return didWork;
    }

    return (await this.relinkNetworkPhase(network, latestBlockHeight)) || didWork;
  }

  private async runNetworkPhaseSequence(phases: Array<() => Promise<boolean>>): Promise<boolean> {
    let didWork = false;
    for (const phase of phases) {
      didWork = (await phase()) || didWork;
    }

    return didWork;
  }

  private async relinkNewAddress(networkId: number, addressId: number): Promise<void> {
    const address = await this.seeds.getTrackedAddress(networkId, addressId);
    if (!address) {
      return;
    }

    await this.seeds.markPendingRelinkSeed(networkId, addressId);
  }

  private async runWorkerLoops(signal?: AbortSignal): Promise<void> {
    await Promise.all([
      this.runLatestHeightLoop(signal),
      this.runBootstrapLoop(signal),
      this.runSyncLoop(signal),
      this.runProjectLoop(signal),
      this.runFactLoop(signal),
      this.runRelinkLoop(signal),
    ]);
  }

  private async runLatestHeightLoop(signal?: AbortSignal): Promise<void> {
    while (shouldContinue(signal)) {
      await this.refreshLatestHeightsOnce();

      await sleep(this.latestHeightRefreshMs);
    }
  }

  private async refreshLatestHeightsOnce(): Promise<void> {
    try {
      const activeNetworks = await this.networks.listActiveNetworks();
      if (activeNetworks.length === 0) {
        this.logIdleOnce('no-active-networks');
        return;
      }

      await this.refreshNetworkHeights(activeNetworks);
    } catch (error) {
      console.error(`[onlydoge] height refresh failed error=${formatError(error)}`);
    }
  }

  private async runSyncLoop(signal?: AbortSignal): Promise<void> {
    while (!signal?.aborted) {
      const didWork = await this.runSyncWorkerTick();
      if (!didWork) {
        await sleep(this.workerIdleMs);
      }
    }
  }

  private async runBootstrapLoop(signal?: AbortSignal): Promise<void> {
    while (!signal?.aborted) {
      const didWork = await this.runBootstrapWorkerTick();
      if (!didWork) {
        await sleep(this.workerIdleMs);
      }
    }
  }

  private async runProjectLoop(signal?: AbortSignal): Promise<void> {
    while (!signal?.aborted) {
      const didWork = await this.runProjectWorkerTick();
      if (!didWork) {
        await sleep(this.workerIdleMs);
      }
    }
  }

  private async runFactLoop(signal?: AbortSignal): Promise<void> {
    while (!signal?.aborted) {
      const didWork = await this.runFactWorkerTick();
      if (!didWork) {
        await sleep(this.workerIdleMs);
      }
    }
  }

  private async runRelinkLoop(signal?: AbortSignal): Promise<void> {
    while (!signal?.aborted) {
      const didWork = await this.runRelinkWorkerTick();
      if (!didWork) {
        await sleep(this.workerIdleMs);
      }
    }
  }

  private async runSyncWorkerTick(): Promise<boolean> {
    return this.runNetworkWorkerTick(
      (network) => this.syncInFlight.has(network.networkId),
      (network, latestBlockHeight) => this.syncNetworkPhase(network, latestBlockHeight),
    );
  }

  private async runBootstrapWorkerTick(): Promise<boolean> {
    return this.runNetworkWorkerTick(
      (network) => this.bootstrapInFlight.has(network.networkId),
      (network, latestBlockHeight) => this.bootstrapNetworkPhase(network, latestBlockHeight),
    );
  }

  private async runProjectWorkerTick(): Promise<boolean> {
    return this.runNetworkWorkerTick(
      (network) =>
        this.projectInFlight.has(network.networkId) ||
        this.bootstrapInFlight.has(network.networkId),
      (network, latestBlockHeight) => this.projectNetworkPhase(network, latestBlockHeight),
    );
  }

  private async runFactWorkerTick(): Promise<boolean> {
    return this.runNetworkWorkerTick(
      (network) => this.factInFlight.has(network.networkId),
      (network, latestBlockHeight) => this.factNetworkPhase(network, latestBlockHeight),
    );
  }

  private async runRelinkWorkerTick(): Promise<boolean> {
    return this.runNetworkWorkerTick(
      (network) => this.relinkInFlight.has(network.networkId),
      async (network, latestBlockHeight) => {
        const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);
        return this.canRunRelink(backlog)
          ? this.relinkNetworkPhase(network, latestBlockHeight)
          : false;
      },
    );
  }

  private async runNetworkWorkerTick(
    isBlocked: (network: IndexedNetwork) => boolean,
    runPhase: (network: IndexedNetwork, latestBlockHeight: number) => Promise<boolean>,
  ): Promise<boolean> {
    const activeNetworks = await this.networks.listActiveNetworks();
    if (activeNetworks.length === 0) {
      return false;
    }

    const didWorkByNetwork = await mapWithConcurrency(
      activeNetworks,
      this.settings.networkConcurrency,
      async (network) => {
        if (isBlocked(network)) {
          return false;
        }

        const latestBlockHeight = await this.getOrRefreshLatestHeight(network);
        if (latestBlockHeight === null) {
          return false;
        }

        return runPhase(network, latestBlockHeight);
      },
    );

    return didWorkByNetwork.some(Boolean);
  }

  private async syncNetworkPhase(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);
    if (!this.shouldRunSync(network, backlog.backlog)) {
      return false;
    }

    const tuning = this.getSyncTuning(network.networkId);
    const result = await this.runNetworkPhase(
      network,
      'sync',
      this.settings.syncTimeoutMs,
      backlog,
      tuning.window,
      this.syncInFlight,
      (signal) => this.syncNetworkWindow(network, latestBlockHeight, tuning.window, signal),
    );
    this.recordWindowOutcome(network, 'sync', result);
    return result.didWork;
  }

  private async bootstrapNetworkPhase(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    if (await this.shouldSkipLegacyDogecoinProjection(network)) {
      return false;
    }

    const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);
    const status = await this.getBootstrapStatus(network.networkId, backlog.processTail);
    if (!status.required) {
      this.bootstrapRetryMarkers.delete(network.networkId);
      this.clearPhaseSkipReason(network.networkId, 'project-state');
      return false;
    }

    this.logBootstrapRetryIfStale(network, status, backlog.processTail);

    const result = await this.runNetworkPhase(
      network,
      'bootstrap',
      this.settings.bootstrapTimeoutMs,
      backlog,
      null,
      this.bootstrapInFlight,
      (signal) => this.bootstrapNetworkWindow(network.networkId, backlog.processTail, signal),
    );
    return result.didWork;
  }

  private logBootstrapRetryIfStale(
    network: IndexedNetwork,
    status: BootstrapStatus,
    processTail: number,
  ): void {
    const retryMarker = bootstrapRetryMarker(status, processTail);
    if (!this.shouldLogBootstrapRetry(network.networkId, status, retryMarker)) {
      return;
    }

    console.log(
      `[onlydoge] phase=bootstrap network=${network.id} retrying table_phase=${status.phase ?? 'utxos'} cursor=${bootstrapCursorLabel(status)} target_tail=${status.targetTail ?? processTail}`,
    );
    this.bootstrapRetryMarkers.set(network.networkId, retryMarker);
  }

  private shouldLogBootstrapRetry(
    networkId: number,
    status: BootstrapStatus,
    retryMarker: string,
  ): boolean {
    if (this.bootstrapInFlight.has(networkId) || status.startedAtMs === null) {
      return false;
    }

    return (
      Date.now() - status.startedAtMs >= this.settings.bootstrapTimeoutMs &&
      this.bootstrapRetryMarkers.get(networkId) !== retryMarker
    );
  }

  private async projectNetworkPhase(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    if (await this.shouldSkipLegacyDogecoinProjection(network)) {
      return false;
    }

    const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);
    const bootstrapStatus = await this.getBootstrapStatus(network.networkId, backlog.processTail);
    if (this.isProjectBlockedByBootstrap(network.networkId, bootstrapStatus)) {
      this.logProjectBootstrapPending(network, bootstrapStatus, backlog.processTail);
      return false;
    }

    this.clearPhaseSkipReason(network.networkId, 'project-state');
    const tuning = this.getProjectTuning(network.networkId);
    const result = await this.runNetworkPhase(
      network,
      'project-state',
      this.settings.projectTimeoutMs,
      backlog,
      tuning.window,
      this.projectInFlight,
      (signal) => this.projectNetworkWindow(network, latestBlockHeight, tuning.window, signal),
    );
    this.recordWindowOutcome(network, 'project-state', result);
    return result.didWork;
  }

  private isProjectBlockedByBootstrap(
    networkId: number,
    bootstrapStatus: BootstrapStatus,
  ): boolean {
    return bootstrapStatus.required || this.bootstrapInFlight.has(networkId);
  }

  private logProjectBootstrapPending(
    network: IndexedNetwork,
    bootstrapStatus: BootstrapStatus,
    processTail: number,
  ): void {
    this.logPhaseSkipOnce(network, 'project-state', 'bootstrap-pending', {
      bootstrap_tail: bootstrapStatus.bootstrapTail ?? -1,
      bootstrap_phase: bootstrapStatus.phase ?? 'pending',
      bootstrap_cursor: bootstrapCursorLabel(bootstrapStatus),
      target_tail: bootstrapStatus.targetTail ?? processTail,
    });
  }

  private async factNetworkPhase(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    if (await this.shouldSkipLegacyDogecoinProjection(network)) {
      return false;
    }

    const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);
    const result = await this.runNetworkPhase(
      network,
      'facts',
      this.settings.factTimeoutMs,
      backlog,
      this.settings.factWindow,
      this.factInFlight,
      (signal) =>
        this.factNetworkWindow(network, latestBlockHeight, this.settings.factWindow, signal),
    );
    return result.didWork;
  }

  private async relinkNetworkPhase(
    network: IndexedNetwork,
    latestBlockHeight: number,
  ): Promise<boolean> {
    if (await this.shouldSkipLegacyDogecoinProjection(network)) {
      return false;
    }

    const backlog = await this.readBacklogState(network.networkId, latestBlockHeight);
    const result = await this.runNetworkPhase(
      network,
      'relink',
      this.settings.relinkTimeoutMs,
      backlog,
      null,
      this.relinkInFlight,
      (signal) => this.processPendingRelinkSeeds(network.networkId, signal),
    );
    return result.didWork;
  }

  private async leaseLeadership(): Promise<boolean> {
    const current = await this.configs.getJsonValue<PrimaryLease | string>(configKeyPrimary());
    const nextLease = this.createLease();

    if (!current) {
      return this.claimPrimaryLease(null, nextLease, 'fresh');
    }

    const lease = this.toPrimaryLease(current);
    if (isOwnLease(lease, this.instanceId)) {
      await this.configs.setJsonValue(configKeyPrimary(), nextLease);
      return true;
    }

    if (this.isLeaseExpired(lease)) {
      return this.claimPrimaryLease(current, nextLease, 'stale');
    }

    return false;
  }

  private async shouldSkipLegacyDogecoinProjection(network: IndexedNetwork): Promise<boolean> {
    return (
      network.architecture === 'dogecoin' &&
      (await this.configs.getJsonValue<boolean>(
        configKeyDogecoinCurrentStateReady(network.networkId),
      )) === true
    );
  }

  private async claimPrimaryLease(
    expected: PrimaryLease | string | null,
    nextLease: PrimaryLease,
    reason: 'fresh' | 'stale',
  ): Promise<boolean> {
    const claimed = await this.configs.compareAndSwapJsonValue(
      configKeyPrimary(),
      expected,
      nextLease,
    );
    if (claimed) {
      console.log(primaryLeaseClaimMessage(this.instanceId, reason));
    }

    return claimed;
  }

  private async refreshLeadership(): Promise<void> {
    await this.configs.setJsonValue(configKeyPrimary(), this.createLease());
  }

  private async withLeaseHeartbeat<T>(work: () => Promise<T>): Promise<T> {
    const interval = setInterval(() => {
      void this.refreshLeadership().catch((error) => {
        console.error(`[onlydoge] primary heartbeat failed error=${formatError(error)}`);
      });
    }, this.settings.leaseHeartbeatIntervalMs);

    interval.unref?.();

    try {
      return await work();
    } finally {
      clearInterval(interval);
    }
  }

  private async refreshNetworkHeights(networks: IndexedNetwork[]) {
    const latestHeights = await mapWithConcurrency(
      networks,
      this.settings.networkConcurrency,
      async (network) => {
        const latestBlockHeight = await this.refreshNetworkHeight(network);
        return latestBlockHeight === null ? null : { latestBlockHeight, network };
      },
    );

    return latestHeights;
  }

  private async getOrRefreshLatestHeight(network: IndexedNetwork): Promise<number | null> {
    const cached = this.latestHeights.get(network.networkId);
    if (cached !== undefined) {
      return cached;
    }

    return this.refreshNetworkHeight(network);
  }

  private async refreshNetworkHeight(network: IndexedNetwork): Promise<number | null> {
    try {
      const latestBlockHeight = await this.rpc.getBlockHeight(network);
      this.latestHeights.set(network.networkId, latestBlockHeight);
      await this.configs.setJsonValue(configKeyBlockHeight(network.networkId), latestBlockHeight);
      return latestBlockHeight;
    } catch (error) {
      console.error(
        `[onlydoge] sync height failed network=${network.id} error=${formatError(error)}`,
      );
      return null;
    }
  }

  private async readSyncTail(networkId: PrimaryId): Promise<number> {
    return (await this.configs.getJsonValue<number>(configKeyIndexerSyncTail(networkId))) ?? -1;
  }

  private async readProcessTail(networkId: PrimaryId): Promise<number> {
    return tailOrInitial(
      await this.configs.getJsonValue<number>(configKeyIndexerProcessTail(networkId)),
    );
  }

  private async runNetworkPhase(
    network: IndexedNetwork,
    phase: PhaseName,
    timeoutMs: number,
    backlog: BacklogState,
    window: number | null,
    inFlight: Map<number, PhaseAttempt>,
    work: (signal: AbortSignal) => Promise<PhaseWorkResult>,
  ): Promise<PhaseExecutionResult> {
    if (inFlight.has(network.networkId)) {
      return idlePhaseExecution();
    }

    const started = this.startPhaseAttempt(network, phase, inFlight, work);

    try {
      return await this.completePhaseAttempt(network, phase, backlog, window, timeoutMs, started);
    } catch (error) {
      return this.failPhaseAttempt(network, phase, backlog, window, inFlight, started, error);
    }
  }

  private startPhaseAttempt(
    network: IndexedNetwork,
    phase: PhaseName,
    inFlight: Map<number, PhaseAttempt>,
    work: (signal: AbortSignal) => Promise<PhaseWorkResult>,
  ): StartedPhaseAttempt {
    const startedAt = Date.now();
    const attempt = createPhaseAttempt(startedAt);
    const workPromise = work(attempt.controller.signal);
    attempt.promise = this.trackPhaseAttempt(network, phase, inFlight, attempt, workPromise);
    inFlight.set(network.networkId, attempt);
    return { attempt, startedAt, workPromise };
  }

  private trackPhaseAttempt(
    network: IndexedNetwork,
    phase: PhaseName,
    inFlight: Map<number, PhaseAttempt>,
    attempt: PhaseAttempt,
    workPromise: Promise<PhaseWorkResult>,
  ): Promise<void> {
    return workPromise
      .then((result) => this.logLatePhaseSuccess(network, phase, attempt, result))
      .catch((error) => this.logLatePhaseFailure(network, phase, attempt, error))
      .finally(() => this.clearPhaseAttempt(network.networkId, inFlight, attempt.attemptId));
  }

  private async completePhaseAttempt(
    network: IndexedNetwork,
    phase: PhaseName,
    backlog: BacklogState,
    window: number | null,
    timeoutMs: number,
    started: StartedPhaseAttempt,
  ): Promise<PhaseExecutionResult> {
    const result = await withTimeout(
      started.workPromise,
      timeoutMs,
      `${phase} timed out after ${timeoutMs}ms`,
    );
    const execution = {
      ...result,
      durationMs: Date.now() - started.startedAt,
    };

    if (result.didWork) {
      this.logPhaseSuccess(network, phase, backlog, window, execution);
    }
    return execution;
  }

  private failPhaseAttempt(
    network: IndexedNetwork,
    phase: PhaseName,
    backlog: BacklogState,
    window: number | null,
    inFlight: Map<number, PhaseAttempt>,
    started: StartedPhaseAttempt,
    error: unknown,
  ): PhaseExecutionResult {
    if (isTimeoutError(error)) {
      this.abortTimedOutAttempt(network, phase, inFlight, started, error);
    }
    const execution = failedPhaseExecution(started.startedAt, error);
    this.logPhaseFailure(network, phase, backlog, window, execution);
    return execution;
  }

  private abortTimedOutAttempt(
    network: IndexedNetwork,
    phase: PhaseName,
    inFlight: Map<number, PhaseAttempt>,
    started: StartedPhaseAttempt,
    error: unknown,
  ): void {
    started.attempt.timedOut = true;
    started.attempt.controller.abort(error);
    this.clearPhaseAttempt(network.networkId, inFlight, started.attempt.attemptId);
    console.error(
      `[onlydoge] phase=${phase} network=${network.id} aborting attemptId=${started.attempt.attemptId} durationMs=${Date.now() - started.startedAt} error=${formatError(error)}`,
    );
  }

  private logLatePhaseSuccess(
    network: IndexedNetwork,
    phase: PhaseName,
    attempt: PhaseAttempt,
    result: PhaseWorkResult,
  ): void {
    if (attempt.timedOut) {
      console.log(
        `[onlydoge] phase=${phase} network=${network.id} completed-after-timeout ignored=true attemptId=${attempt.attemptId} durationMs=${Date.now() - attempt.startedAtMs} didWork=${result.didWork} workItems=${result.workItems}`,
      );
    }
  }

  private logLatePhaseFailure(
    network: IndexedNetwork,
    phase: PhaseName,
    attempt: PhaseAttempt,
    error: unknown,
  ): void {
    if (attempt.timedOut) {
      console.error(
        `[onlydoge] phase=${phase} network=${network.id} failed-after-timeout ignored=true attemptId=${attempt.attemptId} error=${formatError(error)}`,
      );
    }
  }

  private clearPhaseAttempt(
    networkId: number,
    inFlight: Map<number, PhaseAttempt>,
    attemptId: string,
  ): void {
    if (inFlight.get(networkId)?.attemptId === attemptId) {
      inFlight.delete(networkId);
    }
  }

  private async syncNetworkWindow(
    network: IndexedNetwork,
    latestBlockHeight: number,
    window: number,
    _signal: AbortSignal,
  ): Promise<PhaseWorkResult> {
    const currentSyncTail = await this.readSyncTail(network.networkId);
    if (currentSyncTail >= latestBlockHeight) {
      await this.recordSyncProgress(network.networkId, currentSyncTail, latestBlockHeight);
      return noPhaseWork();
    }

    const windowEnd = Math.min(latestBlockHeight, currentSyncTail + window);
    const heights = range(currentSyncTail + 1, windowEnd);
    const storedHeights = await this.syncWindowHeights(network, heights);
    const contiguousTail = contiguousStoredTail(currentSyncTail, heights, storedHeights);
    await this.recordSyncProgress(network.networkId, contiguousTail, latestBlockHeight);

    if (contiguousTail <= currentSyncTail) {
      return noPhaseWork();
    }

    await this.configs.setJsonValue(configKeyIndexerSyncTail(network.networkId), contiguousTail);
    return blockRangeWork(currentSyncTail, contiguousTail);
  }

  private async syncWindowHeights(
    network: IndexedNetwork,
    heights: number[],
  ): Promise<Set<number>> {
    const storedHeights = await mapWithConcurrency(
      heights,
      this.settings.syncConcurrency,
      (blockHeight) => this.syncBlockHeight(network, blockHeight),
    );

    return new Set(
      storedHeights.filter((blockHeight): blockHeight is number => blockHeight !== null),
    );
  }

  private async syncBlockHeight(
    network: IndexedNetwork,
    blockHeight: number,
  ): Promise<number | null> {
    try {
      const snapshot = await this.rpc.getBlockSnapshot(network, blockHeight);
      await this.rawBlocks.putPart(network.networkId, blockHeight, 'block', snapshot);
      return blockHeight;
    } catch (error) {
      console.error(
        `[onlydoge] sync failed network=${network.id} block=${blockHeight} error=${formatError(error)}`,
      );
      return null;
    }
  }

  private async recordSyncProgress(
    networkId: PrimaryId,
    syncTail: number,
    latestBlockHeight: number,
  ): Promise<void> {
    await this.configs.setJsonValue(
      configKeyIndexerSyncProgress(networkId),
      this.toProgress(syncTail, latestBlockHeight),
    );
  }

  private async projectNetworkWindow(
    network: IndexedNetwork,
    latestBlockHeight: number,
    window: number,
    _signal: AbortSignal,
  ): Promise<PhaseWorkResult> {
    const currentSyncTail = await this.readSyncTail(network.networkId);
    const currentProcessTail =
      (await this.configs.getJsonValue<number>(configKeyIndexerProcessTail(network.networkId))) ??
      -1;
    if (currentProcessTail >= currentSyncTail) {
      await this.recordProjectProgress(network.networkId, currentProcessTail, latestBlockHeight);
      return noPhaseWork();
    }

    const windowEnd = Math.min(currentSyncTail, currentProcessTail + window);
    const heights = range(currentProcessTail + 1, windowEnd);
    const snapshotWindow = await this.loadAppliedSnapshotWindow(
      network,
      heights,
      windowEnd,
      this.projectStateStore,
    );
    const recovery = await this.prepareProjectRecovery(
      network,
      snapshotWindow,
      currentProcessTail,
      latestBlockHeight,
    );
    if (recovery.result) {
      return recovery.result;
    }

    const { durationMs: projectBlocksMs, value: projections } = await measureAsync(() =>
      this.projectWindow(network, recovery.snapshotsToProject, 'state', recovery.recoveredTail),
    );

    const { durationMs: applyStateMs } = await measureAsync(() =>
      this.projectStateStore.applyProjectionWindow(projections),
    );
    await this.commitProjectTail(network.networkId, windowEnd, latestBlockHeight);

    return blockRangeWork(currentProcessTail, windowEnd, {
      apply_state_ms: applyStateMs,
      load_snapshots_ms: snapshotWindow.loadSnapshotsMs,
      project_blocks_ms: projectBlocksMs,
    });
  }

  private async loadAppliedSnapshotWindow(
    network: IndexedNetwork,
    heights: number[],
    windowEnd: number,
    appliedBlockReader: AppliedBlockSetReader,
  ): Promise<AppliedSnapshotWindow> {
    const { durationMs: loadSnapshotsMs, value: orderedSnapshots } = await measureAsync(() =>
      this.loadSnapshots(network.networkId, heights),
    );
    const appliedBlocks = await appliedBlockReader.listAppliedBlockSet(
      network.networkId,
      this.snapshotBlockIdentities(heights, orderedSnapshots),
    );

    return {
      appliedBlocks,
      heights,
      loadSnapshotsMs,
      orderedSnapshots,
      windowEnd,
    };
  }

  private snapshotBlockIdentities(
    heights: number[],
    orderedSnapshots: Record<string, unknown>[],
  ): Array<{ blockHash: string; blockHeight: number }> {
    return orderedSnapshots.map((snapshot, index) => ({
      blockHeight: heights[index] ?? -1,
      blockHash: this.readSnapshotBlockHash(snapshot),
    }));
  }

  private async prepareProjectRecovery(
    network: IndexedNetwork,
    snapshotWindow: AppliedSnapshotWindow,
    currentProcessTail: number,
    latestBlockHeight: number,
  ): Promise<ProjectRecoveryOutcome> {
    const recovery = this.recoverAppliedSnapshotTail(
      network,
      snapshotWindow.heights,
      snapshotWindow.orderedSnapshots,
      snapshotWindow.appliedBlocks,
      currentProcessTail,
    );
    await this.commitProjectRecoveryIfNeeded(
      network,
      recovery,
      currentProcessTail,
      latestBlockHeight,
    );

    return {
      recoveredTail: recovery.recoveredTail,
      result: recoveredSnapshotResult(
        currentProcessTail,
        recovery,
        snapshotWindow.orderedSnapshots.length,
        snapshotWindow.loadSnapshotsMs,
      ),
      snapshotsToProject: snapshotWindow.orderedSnapshots.slice(recovery.recoveryIndex),
    };
  }

  private recoverAppliedSnapshotTail(
    network: IndexedNetwork,
    heights: number[],
    orderedSnapshots: Record<string, unknown>[],
    appliedBlocks: Set<string>,
    currentTail: number,
  ): AppliedSnapshotRecovery {
    let recoveredTail = currentTail;
    let recoveryIndex = 0;
    for (const [index, snapshot] of orderedSnapshots.entries()) {
      const blockHeight = heights[index] ?? currentTail;
      const blockHash = this.readSnapshotBlockHash(snapshot);
      if (!appliedBlocks.has(projectionBlockIdentity(network.networkId, blockHeight, blockHash))) {
        return { recoveredTail, recoveryIndex: index };
      }

      recoveredTail = blockHeight;
      recoveryIndex = index + 1;
    }

    return { recoveredTail, recoveryIndex };
  }

  private async commitProjectRecovery(
    network: IndexedNetwork,
    recoveredProcessTail: number,
    latestBlockHeight: number,
  ): Promise<void> {
    await this.projectStateStore.finalizeProjectionBootstrap(
      network.networkId,
      recoveredProcessTail,
    );
    await this.configs.setJsonValue(
      configKeyIndexerProcessTail(network.networkId),
      recoveredProcessTail,
    );
    await this.configs.setJsonValue(
      configKeyIndexerProcessProgress(network.networkId),
      this.toProgress(recoveredProcessTail, latestBlockHeight),
    );
  }

  private async commitProjectRecoveryIfNeeded(
    network: IndexedNetwork,
    recovery: AppliedSnapshotRecovery,
    currentProcessTail: number,
    latestBlockHeight: number,
  ): Promise<void> {
    if (recovery.recoveredTail > currentProcessTail) {
      await this.commitProjectRecovery(network, recovery.recoveredTail, latestBlockHeight);
    }
  }

  private async commitProjectTail(
    networkId: PrimaryId,
    processTail: number,
    latestBlockHeight: number,
  ): Promise<void> {
    await this.projectStateStore.finalizeProjectionBootstrap(networkId, processTail);
    await this.configs.setJsonValue(configKeyIndexerProcessTail(networkId), processTail);
    await this.recordProjectProgress(networkId, processTail, latestBlockHeight);
  }

  private async recordProjectProgress(
    networkId: PrimaryId,
    processTail: number,
    latestBlockHeight: number,
  ): Promise<void> {
    await this.configs.setJsonValue(
      configKeyIndexerProcessProgress(networkId),
      this.toProgress(processTail, latestBlockHeight),
    );
  }

  private async commitFactRecovery(
    network: IndexedNetwork,
    recoveredFactTail: number,
    latestBlockHeight: number,
  ): Promise<void> {
    await this.configs.setJsonValue(configKeyIndexerFactTail(network.networkId), recoveredFactTail);
    await this.recordFactProgress(network.networkId, recoveredFactTail, latestBlockHeight);
  }

  private async commitFactRecoveryIfNeeded(
    network: IndexedNetwork,
    recovery: AppliedSnapshotRecovery,
    currentFactTail: number,
    latestBlockHeight: number,
  ): Promise<void> {
    if (recovery.recoveredTail > currentFactTail) {
      await this.commitFactRecovery(network, recovery.recoveredTail, latestBlockHeight);
    }
  }

  private async commitFactTail(
    networkId: PrimaryId,
    factTail: number,
    latestBlockHeight: number,
  ): Promise<void> {
    await this.configs.setJsonValue(configKeyIndexerFactTail(networkId), factTail);
    await this.recordFactProgress(networkId, factTail, latestBlockHeight);
  }

  private async recordFactProgress(
    networkId: PrimaryId,
    factTail: number,
    latestBlockHeight: number,
  ): Promise<void> {
    await this.configs.setJsonValue(
      configKeyIndexerFactProgress(networkId),
      this.toProgress(factTail, latestBlockHeight),
    );
  }

  private async factNetworkWindow(
    network: IndexedNetwork,
    latestBlockHeight: number,
    window: number,
    _signal: AbortSignal,
  ): Promise<PhaseWorkResult> {
    const currentProcessTail = await this.readProcessTail(network.networkId);
    const currentFactTail = await this.getFactTail(network.networkId, currentProcessTail);
    if (currentFactTail >= currentProcessTail) {
      await this.recordFactProgress(network.networkId, currentFactTail, latestBlockHeight);
      return noPhaseWork();
    }

    const windowEnd = Math.min(currentProcessTail, currentFactTail + window);
    const heights = range(currentFactTail + 1, windowEnd);
    const snapshotWindow = await this.loadAppliedSnapshotWindow(
      network,
      heights,
      windowEnd,
      this.factWarehouse,
    );
    const recovery = await this.prepareFactRecovery(
      network,
      snapshotWindow,
      currentFactTail,
      latestBlockHeight,
    );
    if (recovery.result) {
      return recovery.result;
    }

    const { durationMs: projectBlocksMs, value: projections } = await measureAsync(() =>
      this.projectWindow(network, recovery.snapshotsToPersist, 'facts'),
    );
    const persistMetrics = await this.persistFactWindow(network.networkId, projections);
    await this.commitFactTail(network.networkId, windowEnd, latestBlockHeight);

    return blockRangeWork(
      currentFactTail,
      windowEnd,
      factPhaseMetrics(
        snapshotWindow.loadSnapshotsMs,
        recovery.metrics,
        projectBlocksMs,
        persistMetrics,
      ),
    );
  }

  private async prepareFactRecovery(
    network: IndexedNetwork,
    snapshotWindow: AppliedSnapshotWindow,
    currentFactTail: number,
    latestBlockHeight: number,
  ): Promise<FactRecoveryOutcome> {
    const recovery = this.recoverAppliedSnapshotTail(
      network,
      snapshotWindow.heights,
      snapshotWindow.orderedSnapshots,
      snapshotWindow.appliedBlocks,
      currentFactTail,
    );
    await this.commitFactRecoveryIfNeeded(network, recovery, currentFactTail, latestBlockHeight);

    const recoveredSnapshots = snapshotWindow.orderedSnapshots.slice(0, recovery.recoveryIndex);
    const metrics = await this.replayRecoveredFactSnapshots(network, recoveredSnapshots);

    return {
      metrics,
      recoveredTail: recovery.recoveredTail,
      result: recoveredSnapshotResult(
        currentFactTail,
        recovery,
        snapshotWindow.orderedSnapshots.length,
        snapshotWindow.loadSnapshotsMs,
        metrics,
      ),
      snapshotsToPersist: snapshotWindow.orderedSnapshots.slice(recovery.recoveryIndex),
    };
  }

  private async replayRecoveredFactSnapshots(
    network: IndexedNetwork,
    recoveredSnapshots: Record<string, unknown>[],
  ): Promise<Record<string, number>> {
    if (recoveredSnapshots.length === 0) {
      return { apply_links_ms: 0, project_blocks_ms: 0 };
    }

    const { durationMs: projectBlocksMs, value: recoveredProjections } = await measureAsync(() =>
      this.projectWindow(network, recoveredSnapshots, 'facts'),
    );
    const applyLinksMs = await this.applyFactDirectLinksTimed(
      network.networkId,
      recoveredProjections,
    );

    return {
      apply_links_ms: applyLinksMs,
      project_blocks_ms: projectBlocksMs,
    };
  }

  private async persistFactWindow(
    networkId: PrimaryId,
    projections: BlockProjectionBatch[],
  ): Promise<Record<string, number>> {
    const { durationMs: writeFactsMs } = await measureAsync(async () => {
      const factWindow = await this.buildProjectionFactWindow(
        networkId,
        projections,
        this.factWarehouse,
      );
      await this.factWarehouse.applyProjectionFacts(factWindow);
    });
    const applyLinksMs = await this.applyFactDirectLinksTimed(networkId, projections);

    return {
      apply_links_ms: applyLinksMs,
      write_facts_ms: writeFactsMs,
    };
  }

  private async buildProjectionFactWindow(
    networkId: number,
    projections: BlockProjectionBatch[],
    snapshotStore: Pick<
      ProjectionStateStorePort,
      'getBalanceSnapshots' | 'getDirectLinkSnapshots' | 'getUtxoOutputs'
    >,
  ): Promise<ProjectionFactWindow> {
    const orderedProjections = orderProjectionBatches(projections);
    const outputKeys = collectProjectionTouchedOutputKeys(orderedProjections);
    const balanceKeys = collectProjectionBalanceSnapshotKeys(orderedProjections).map(
      parseProjectionBalanceSnapshotKey,
    );
    const directLinkKeys = collectProjectionDirectLinkSnapshotKeys(orderedProjections).map(
      parseProjectionDirectLinkSnapshotKey,
    );

    const [currentOutputs, currentBalances, currentDirectLinks] = await Promise.all([
      snapshotStore.getUtxoOutputs(networkId, outputKeys),
      snapshotStore.getBalanceSnapshots(networkId, balanceKeys),
      snapshotStore.getDirectLinkSnapshots(networkId, directLinkKeys),
    ]);

    const nextOutputs = buildNextProjectionUtxoOutputs(orderedProjections, currentOutputs);

    const nextBalances = new Map<string, ProjectionBalanceSnapshot>();
    for (const projection of orderedProjections) {
      applyAddressMovementsToBalances({
        asOfBlockHeight: projection.blockHeight,
        currentBalances,
        keyForMovement: (movement) =>
          projectionBalanceSnapshotKey(movement.address, movement.assetAddress),
        movements: projection.addressMovements,
        nextBalances,
      });
    }

    const nextDirectLinks = new Map<string, DirectLinkRecord>();
    applyDirectLinkDeltasToSnapshots({
      currentDirectLinks,
      directLinkDeltas: orderedProjections.flatMap((projection) => projection.directLinkDeltas),
      keyForDelta: (delta) =>
        projectionDirectLinkSnapshotKey(delta.fromAddress, delta.toAddress, delta.assetAddress),
      nextDirectLinks,
    });

    return {
      networkId,
      addressMovements: orderedProjections.flatMap((projection) => projection.addressMovements),
      transfers: orderedProjections.flatMap((projection) => projection.transfers),
      appliedBlocks: toProjectionAppliedBlocks(orderedProjections),
      utxoOutputs: outputKeys.flatMap((outputKey) => {
        const output = nextOutputs.get(outputKey) ?? currentOutputs.get(outputKey);
        return output ? [output] : [];
      }),
      balances: balanceKeys.flatMap((key) => {
        const snapshot =
          nextBalances.get(projectionBalanceSnapshotKey(key.address, key.assetAddress)) ??
          currentBalances.get(projectionBalanceSnapshotKey(key.address, key.assetAddress));
        return snapshot ? [snapshot] : [];
      }),
      directLinks: directLinkKeys.flatMap((key) => {
        const snapshot =
          nextDirectLinks.get(
            projectionDirectLinkSnapshotKey(key.fromAddress, key.toAddress, key.assetAddress),
          ) ??
          currentDirectLinks.get(
            projectionDirectLinkSnapshotKey(key.fromAddress, key.toAddress, key.assetAddress),
          );
        return snapshot ? [snapshot] : [];
      }),
    };
  }

  private async projectWindow(
    network: IndexedNetwork,
    snapshots: Record<string, unknown>[],
    mode: ProjectionMode,
    expectedBootstrapTail = -1,
  ): Promise<BlockProjectionBatch[]> {
    return this.projectDogecoinWindow(network, snapshots, mode, expectedBootstrapTail);
  }

  private async projectDogecoinWindow(
    network: IndexedNetwork,
    snapshots: Record<string, unknown>[],
    mode: ProjectionMode,
    expectedBootstrapTail: number,
  ): Promise<BlockProjectionBatch[]> {
    const snapshotStore = mode === 'state' ? this.projectStateStore : this.stateStore;
    const projector = mode === 'state' ? this.dogecoinStateProjector : this.dogecoinFactProjector;
    const externalOutputKeys = this.collectDogecoinExternalOutputKeys(snapshots);
    const persistedOutputs = await snapshotStore.getUtxoOutputs(network.networkId, [
      ...externalOutputKeys,
    ]);
    await this.assertDogecoinPrevoutsAvailable(
      network,
      mode,
      externalOutputKeys.size,
      persistedOutputs.size,
      expectedBootstrapTail,
    );

    return this.projectDogecoinSnapshots(
      network.networkId,
      snapshots,
      mode,
      projector,
      persistedOutputs,
    );
  }

  private async projectDogecoinSnapshots(
    networkId: PrimaryId,
    snapshots: Record<string, unknown>[],
    mode: ProjectionMode,
    projector: DogecoinBlockProjector,
    persistedOutputs: Map<string, ProjectionUtxoOutput>,
  ): Promise<BlockProjectionBatch[]> {
    const localOutputs = new Map<string, ProjectionUtxoOutput>();
    const projections: BlockProjectionBatch[] = [];
    for (const snapshot of snapshots) {
      projections.push(
        await projector.project(
          networkId,
          snapshot,
          {
            localOutputs,
            persistedOutputs,
          },
          {
            includeDirectLinkDeltas: mode === 'facts',
            includeTransfers: mode === 'facts',
            maxTransferEdges: this.settings.dogecoinTransferMaxEdges,
            maxTransferInputAddresses: this.settings.dogecoinTransferMaxInputAddresses,
          },
        ),
      );
    }

    return projections;
  }

  private collectDogecoinExternalOutputKeys(snapshots: Record<string, unknown>[]): Set<string> {
    const knownOutputKeys = new Set<string>();
    const externalOutputKeys = new Set<string>();
    for (const snapshot of snapshots) {
      for (const outputKey of this.dogecoinStateProjector.collectExternalOutputKeys(
        snapshot,
        knownOutputKeys,
      )) {
        externalOutputKeys.add(outputKey);
      }
    }

    return externalOutputKeys;
  }

  private async assertDogecoinPrevoutsAvailable(
    network: IndexedNetwork,
    mode: ProjectionMode,
    requiredPrevouts: number,
    persistedPrevouts: number,
    expectedBootstrapTail: number,
  ): Promise<void> {
    if (!requiresDogecoinPrevoutBootstrap(mode, requiredPrevouts, persistedPrevouts)) {
      return;
    }

    const bootstrapTail = await this.projectStateStore.getProjectionBootstrapTail(
      network.networkId,
    );
    const missingPrevouts = requiredPrevouts - persistedPrevouts;
    if (hasRequiredBootstrapTail(bootstrapTail, expectedBootstrapTail)) {
      throw new Error(`strict metadata prevouts missing: ${missingPrevouts}`);
    }

    console.warn(
      bootstrapRequiredMessage(
        network.id,
        missingPrevouts,
        requiredPrevouts,
        bootstrapTail,
        expectedBootstrapTail,
      ),
    );
    throw new BootstrapRequiredError(
      `bootstrap required for project-state prevouts: ${missingPrevouts} missing`,
    );
  }

  private async applyFactDirectLinks(
    networkId: number,
    projections: BlockProjectionBatch[],
  ): Promise<void> {
    const batches = projections
      .filter((projection) => projection.directLinkDeltas.length > 0)
      .map<ProjectionDirectLinkBatch>((projection) => ({
        networkId: projection.networkId,
        blockHeight: projection.blockHeight,
        blockHash: projection.blockHash,
        directLinkDeltas: projection.directLinkDeltas,
      }));
    if (batches.length === 0) {
      return;
    }

    await this.stateStore.applyDirectLinkDeltasWindow(batches);
    await this.markImpactedSeedsPendingRelink(networkId, projections);
  }

  private async applyFactDirectLinksTimed(
    networkId: PrimaryId,
    projections: BlockProjectionBatch[],
  ): Promise<number> {
    const { durationMs } = await measureAsync(() =>
      this.applyFactDirectLinks(networkId, projections),
    );
    return durationMs;
  }

  private async markImpactedSeedsPendingRelink(
    networkId: number,
    projections: BlockProjectionBatch[],
  ): Promise<void> {
    const fromAddresses = [
      ...new Set(
        projections.flatMap((projection) =>
          projection.directLinkDeltas.map((delta) => delta.fromAddress),
        ),
      ),
    ];
    if (fromAddresses.length === 0) {
      return;
    }

    const [trackedAddresses, reachedSeedIds] = await Promise.all([
      this.seeds.listTrackedAddressesByValues(networkId, fromAddresses),
      this.stateStore.listSourceSeedIdsReachingAddresses(networkId, fromAddresses),
    ]);
    const seedIds = [
      ...new Set([...trackedAddresses.map((address) => address.addressId), ...reachedSeedIds]),
    ];
    if (seedIds.length === 0) {
      return;
    }

    await Promise.all(
      seedIds.map((addressId) => this.seeds.markPendingRelinkSeed(networkId, addressId)),
    );
  }

  private async processPendingRelinkSeeds(
    networkId: number,
    _signal: AbortSignal,
  ): Promise<PhaseWorkResult> {
    const pendingSeeds = await this.seeds.listPendingRelinkSeeds(networkId);
    if (pendingSeeds.length === 0) {
      return { didWork: false, workItems: 0, pendingCount: 0 };
    }

    const batch = pendingSeeds.slice(0, this.settings.relinkBatchSize);
    let relinkedCount = 0;
    await mapWithConcurrency(batch, this.settings.relinkConcurrency, async (seed) => {
      try {
        await this.sourceLinkProjector.rebuild(networkId, seed, {
          frontierBatchSize: this.settings.relinkFrontierBatch,
        });
        await this.seeds.clearPendingRelinkSeed(networkId, seed.addressId);
        relinkedCount += 1;
      } catch (error) {
        console.error(
          `[onlydoge] relink failed network=${networkId} address=${seed.address} error=${formatError(error)}`,
        );
      }
    });

    return {
      didWork: relinkedCount > 0,
      workItems: relinkedCount,
      pendingCount: pendingSeeds.length,
    };
  }

  private async loadSnapshots(
    networkId: number,
    heights: number[],
  ): Promise<Record<string, unknown>[]> {
    const snapshots = await mapWithConcurrency(
      heights,
      Math.min(this.settings.syncConcurrency, heights.length),
      async (blockHeight) => {
        const snapshot = await this.rawBlocks.getPart<Record<string, unknown>>(
          networkId,
          blockHeight,
          'block',
        );
        if (!snapshot) {
          throw new Error(`missing stored snapshot for network=${networkId} block=${blockHeight}`);
        }

        return [blockHeight, snapshot] as const;
      },
    );
    const snapshotsByHeight = new Map(snapshots);

    return heights.map((blockHeight) => {
      const snapshot = snapshotsByHeight.get(blockHeight);
      if (!snapshot) {
        throw new Error(`missing snapshot in memory for network=${networkId} block=${blockHeight}`);
      }

      return snapshot;
    });
  }

  private async getFactTail(networkId: number, processTail: number): Promise<number> {
    const configured = await this.configs.getJsonValue<number>(configKeyIndexerFactTail(networkId));
    if (configured !== null) {
      return configured;
    }

    const warehouseTail = await this.factWarehouse.getAppliedBlockTail(networkId);
    const factTail = warehouseTail === null ? -1 : Math.min(warehouseTail, processTail);
    await this.configs.setJsonValue(configKeyIndexerFactTail(networkId), factTail);
    return factTail;
  }

  private async getBootstrapStatus(
    networkId: number,
    processTail: number,
  ): Promise<BootstrapStatus> {
    const [bootstrapTail, targetTail, phase, startedAtMs, cursorUtxo, cursorBalance] =
      await Promise.all([
        this.projectStateStore.getProjectionBootstrapTail(networkId),
        this.configs.getJsonValue<number>(configKeyProjectionBootstrapTargetTail(networkId)),
        this.configs.getJsonValue<string>(configKeyProjectionBootstrapPhase(networkId)),
        this.configs.getJsonValue<number>(configKeyProjectionBootstrapStartedAt(networkId)),
        this.configs.getJsonValue<string>(configKeyProjectionBootstrapCursorUtxo(networkId)),
        this.configs.getJsonValue<ProjectionBalanceCursor>(
          configKeyProjectionBootstrapCursorBalance(networkId),
        ),
      ]);

    return {
      bootstrapTail,
      cursorBalance,
      cursorUtxo,
      targetTail,
      phase: isBootstrapPhase(phase) ? phase : null,
      startedAtMs,
      required: processTail >= 0 && (bootstrapTail === null || bootstrapTail < processTail),
    };
  }

  private async bootstrapNetworkWindow(
    networkId: number,
    processTail: number,
    signal: AbortSignal,
  ): Promise<PhaseWorkResult> {
    if (processTail < 0) {
      return { didWork: false, workItems: 0 };
    }

    let status = await this.getBootstrapStatus(networkId, processTail);
    if (!status.required) {
      return { didWork: false, workItems: 0 };
    }

    status = await this.ensureBootstrapInitialized(networkId, processTail, signal, status);

    return this.runBootstrapStatusPhase(networkId, status, processTail, signal);
  }

  private async ensureBootstrapInitialized(
    networkId: number,
    processTail: number,
    signal: AbortSignal,
    status: BootstrapStatus,
  ): Promise<BootstrapStatus> {
    if (status.targetTail !== null) {
      return status;
    }

    throwIfAborted(signal);
    await this.initializeBootstrapState(networkId, processTail);
    return this.getBootstrapStatus(networkId, processTail);
  }

  private runBootstrapStatusPhase(
    networkId: number,
    status: BootstrapStatus,
    processTail: number,
    signal: AbortSignal,
  ): Promise<PhaseWorkResult> {
    const targetTail = status.targetTail ?? processTail;
    return this.bootstrapPhaseHandlers(networkId, status, targetTail, signal)[
      bootstrapStatusPhase(status)
    ]();
  }

  private bootstrapPhaseHandlers(
    networkId: number,
    status: BootstrapStatus,
    targetTail: number,
    signal: AbortSignal,
  ): Record<BootstrapPhase, () => Promise<PhaseWorkResult>> {
    return {
      utxos: () => this.bootstrapUtxoState(networkId, targetTail, signal),
      balances: () => this.bootstrapBalanceState(networkId, targetTail, status.startedAtMs, signal),
      done: () => this.completeBootstrapState(networkId, targetTail, status.startedAtMs, 0, signal),
    };
  }

  private async initializeBootstrapState(networkId: number, targetTail: number): Promise<void> {
    await this.projectStateStore.clearProjectionBootstrapState(networkId);
    await this.deleteConfigKeys(
      configKeyProjectionBootstrapTail(networkId),
      configKeyProjectionBootstrapTargetTail(networkId),
      configKeyProjectionBootstrapPhase(networkId),
      configKeyProjectionBootstrapCursorUtxo(networkId),
      configKeyProjectionBootstrapCursorBalance(networkId),
      configKeyProjectionBootstrapStartedAt(networkId),
    );
    await this.configs.setJsonValue(configKeyProjectionBootstrapTargetTail(networkId), targetTail);
    await this.configs.setJsonValue(configKeyProjectionBootstrapPhase(networkId), 'utxos');
    await this.configs.setJsonValue(configKeyProjectionBootstrapStartedAt(networkId), Date.now());
  }

  private async bootstrapUtxoState(
    networkId: number,
    targetTail: number,
    signal: AbortSignal,
  ): Promise<PhaseWorkResult> {
    const cursor =
      (await this.configs.getJsonValue<string>(
        configKeyProjectionBootstrapCursorUtxo(networkId),
      )) ?? null;
    const page = await this.factWarehouse.listCurrentUtxoOutputsPage(
      networkId,
      cursor,
      bootstrapUtxoChunkSize,
      { abortSignal: signal },
    );
    throwIfAborted(signal);
    await this.projectStateStore.upsertProjectionBootstrapUtxoOutputs(page.rows);

    if (page.nextCursor) {
      throwIfAborted(signal);
      await this.configs.setJsonValue(
        configKeyProjectionBootstrapCursorUtxo(networkId),
        page.nextCursor,
      );
    } else {
      throwIfAborted(signal);
      await this.deleteConfigKeys(configKeyProjectionBootstrapCursorUtxo(networkId));
      await this.configs.setJsonValue(configKeyProjectionBootstrapPhase(networkId), 'balances');
    }

    return {
      didWork: true,
      workItems: page.rows.length,
      metrics: {
        cursor: page.nextCursor ?? 'complete',
        rows_imported: page.rows.length,
        table_phase: 'utxos',
        target_tail: targetTail,
      },
    };
  }

  private async bootstrapBalanceState(
    networkId: number,
    targetTail: number,
    startedAtMs: number | null,
    signal: AbortSignal,
  ): Promise<PhaseWorkResult> {
    const cursor =
      (await this.configs.getJsonValue<ProjectionBalanceCursor>(
        configKeyProjectionBootstrapCursorBalance(networkId),
      )) ?? null;
    const page = await this.factWarehouse.listCurrentBalancesPage(
      networkId,
      cursor,
      bootstrapBalanceChunkSize,
      { abortSignal: signal },
    );
    throwIfAborted(signal);
    await this.projectStateStore.upsertProjectionBootstrapBalances(page.rows);

    if (page.nextCursor) {
      throwIfAborted(signal);
      await this.configs.setJsonValue(
        configKeyProjectionBootstrapCursorBalance(networkId),
        page.nextCursor,
      );
      return {
        didWork: true,
        workItems: page.rows.length,
        metrics: {
          cursor: `${page.nextCursor.address}:${page.nextCursor.assetAddress}`,
          rows_imported: page.rows.length,
          table_phase: 'balances',
          target_tail: targetTail,
        },
      };
    }

    throwIfAborted(signal);
    await this.deleteConfigKeys(configKeyProjectionBootstrapCursorBalance(networkId));
    await this.configs.setJsonValue(configKeyProjectionBootstrapPhase(networkId), 'done');
    return this.completeBootstrapState(
      networkId,
      targetTail,
      startedAtMs,
      page.rows.length,
      signal,
    );
  }

  private async completeBootstrapState(
    networkId: number,
    targetTail: number,
    startedAtMs: number | null,
    importedRows: number,
    signal: AbortSignal,
  ): Promise<PhaseWorkResult> {
    throwIfAborted(signal);
    await this.projectStateStore.finalizeProjectionBootstrap(networkId, targetTail);
    throwIfAborted(signal);
    await this.deleteConfigKeys(
      configKeyProjectionBootstrapTargetTail(networkId),
      configKeyProjectionBootstrapPhase(networkId),
      configKeyProjectionBootstrapCursorUtxo(networkId),
      configKeyProjectionBootstrapCursorBalance(networkId),
      configKeyProjectionBootstrapStartedAt(networkId),
    );

    return {
      didWork: true,
      workItems: importedRows,
      metrics: {
        rows_imported: importedRows,
        table_phase: 'done',
        target_tail: targetTail,
        total_duration_ms: startedAtMs === null ? 0 : Date.now() - startedAtMs,
      },
    };
  }

  private async deleteConfigKeys(...keys: string[]): Promise<void> {
    await Promise.all(keys.map((key) => this.configs.deleteByPrefix(key)));
  }

  private async readBacklogState(
    networkId: number,
    latestBlockHeight: number,
  ): Promise<BacklogState> {
    const [syncTail, processTail] = await Promise.all([
      this.configs.getJsonValue<number>(configKeyIndexerSyncTail(networkId)),
      this.configs.getJsonValue<number>(configKeyIndexerProcessTail(networkId)),
    ]);

    const resolvedSyncTail = tailOrInitial(syncTail);
    const resolvedProcessTail = tailOrInitial(processTail);
    return {
      latestBlockHeight,
      syncTail: resolvedSyncTail,
      processTail: resolvedProcessTail,
      backlog: Math.max(0, resolvedSyncTail - resolvedProcessTail),
    };
  }

  private shouldRunSync(network: IndexedNetwork, backlog: number): boolean {
    if (this.syncPausedNetworks.has(network.networkId)) {
      if (backlog > this.settings.syncBacklogLowWatermark) {
        return false;
      }

      this.syncPausedNetworks.delete(network.networkId);
      console.log(
        `[onlydoge] sync resumed network=${network.id} backlog=${backlog} reason=backpressure-cleared`,
      );
      return true;
    }

    if (backlog >= this.settings.syncBacklogHighWatermark) {
      this.syncPausedNetworks.add(network.networkId);
      console.log(
        `[onlydoge] sync paused network=${network.id} backlog=${backlog} reason=backpressure`,
      );
      return false;
    }

    return true;
  }

  private canRunRelink(backlog: BacklogState): boolean {
    const tipDistance = Math.max(0, backlog.latestBlockHeight - backlog.processTail);
    return (
      backlog.backlog <= this.settings.relinkBacklogThreshold &&
      tipDistance <= this.settings.relinkTipDistance
    );
  }

  private getSyncTuning(networkId: number): PhaseTuningState {
    const current = this.syncTuning.get(networkId);
    if (current) {
      return current;
    }

    const initial = {
      window: clamp(
        this.settings.syncWindow,
        this.settings.syncWindowMin,
        this.settings.syncWindowMax,
      ),
      successStreak: 0,
    };
    this.syncTuning.set(networkId, initial);
    return initial;
  }

  private getProjectTuning(networkId: number): PhaseTuningState {
    const current = this.projectTuning.get(networkId);
    if (current) {
      return current;
    }

    const initial = {
      window: clamp(
        this.settings.projectWindow,
        this.settings.projectWindowMin,
        this.settings.projectWindowMax,
      ),
      successStreak: 0,
    };
    this.projectTuning.set(networkId, initial);
    return initial;
  }

  private recordWindowOutcome(
    network: IndexedNetwork,
    phase: 'project-state' | 'sync',
    result: PhaseExecutionResult,
  ): void {
    const context = this.adaptiveWindowContext(network.networkId, phase);
    if (this.recordWindowBackoff(network, phase, result, context)) {
      return;
    }

    if (!result.didWork) {
      return;
    }

    if (result.durationMs > context.targetMs) {
      context.tuning.successStreak = 0;
      return;
    }

    this.recordWindowGrowth(network, phase, context);
  }

  private adaptiveWindowContext(
    networkId: number,
    phase: 'project-state' | 'sync',
  ): AdaptiveWindowContext {
    if (phase === 'sync') {
      return {
        tuning: this.getSyncTuning(networkId),
        minWindow: this.settings.syncWindowMin,
        maxWindow: this.settings.syncWindowMax,
        targetMs: this.settings.syncTargetMs,
      };
    }

    return {
      tuning: this.getProjectTuning(networkId),
      minWindow: this.settings.projectWindowMin,
      maxWindow: this.settings.projectWindowMax,
      targetMs: this.settings.projectTargetMs,
    };
  }

  private recordWindowBackoff(
    network: IndexedNetwork,
    phase: 'project-state' | 'sync',
    result: PhaseExecutionResult,
    context: AdaptiveWindowContext,
  ): boolean {
    if (!result.error || !shouldBackoffWindow(result.error)) {
      return false;
    }

    const nextWindow = Math.max(
      context.minWindow,
      Math.floor(context.tuning.window / 2) || context.minWindow,
    );
    this.updateAdaptiveWindow(network, phase, context, nextWindow, {
      kind: 'backoff',
      reason: formatError(result.error),
    });
    context.tuning.successStreak = 0;
    return true;
  }

  private recordWindowGrowth(
    network: IndexedNetwork,
    phase: 'project-state' | 'sync',
    context: AdaptiveWindowContext,
  ): void {
    context.tuning.successStreak += 1;
    if (context.tuning.successStreak < 5 || context.tuning.window >= context.maxWindow) {
      return;
    }

    const nextWindow = Math.min(context.maxWindow, context.tuning.window * 2);
    this.updateAdaptiveWindow(network, phase, context, nextWindow, { kind: 'growth' });
    context.tuning.successStreak = 0;
  }

  private updateAdaptiveWindow(
    network: IndexedNetwork,
    phase: 'project-state' | 'sync',
    context: AdaptiveWindowContext,
    nextWindow: number,
    event: { kind: 'backoff'; reason: string } | { kind: 'growth' },
  ): void {
    if (nextWindow === context.tuning.window) {
      return;
    }

    console.log(adaptiveWindowLogLine(network.id, phase, context.tuning.window, nextWindow, event));
    context.tuning.window = nextWindow;
  }

  private logPhaseSkipOnce(
    network: IndexedNetwork,
    phase: PhaseName,
    reason: string,
    details: Record<string, number | string> = {},
  ): void {
    const key = `${phase}:${network.networkId}`;
    const message = `${reason}:${JSON.stringify(details)}`;
    if (this.phaseSkipReasons.get(key) === message) {
      return;
    }

    this.phaseSkipReasons.set(key, message);
    console.log(
      `[onlydoge] phase=${phase} network=${network.id} skipped reason=${reason}${formatMetrics(details)}`,
    );
  }

  private clearPhaseSkipReason(networkId: number, phase: PhaseName): void {
    this.phaseSkipReasons.delete(`${phase}:${networkId}`);
  }

  private logPhaseSuccess(
    network: IndexedNetwork,
    phase: PhaseName,
    backlog: BacklogState,
    window: number | null,
    result: PhaseExecutionResult,
  ): void {
    console.log(
      `[onlydoge] phase=${phase} network=${network.id}${phaseRangeText(result)} latest=${backlog.latestBlockHeight} backlog=${backlog.backlog}${phaseWindowText(window)}${phasePendingText(result)} items=${result.workItems} durationMs=${result.durationMs} rate=${phaseRate(result).toFixed(2)}/s${formatMetrics(result.metrics)}`,
    );
  }

  private logPhaseFailure(
    network: IndexedNetwork,
    phase: PhaseName,
    backlog: BacklogState,
    window: number | null,
    result: PhaseExecutionResult,
  ): void {
    const windowText = window === null ? '' : ` window=${window}`;
    const metricsText = formatMetrics(result.metrics);

    console.error(
      `[onlydoge] phase=${phase} network=${network.id} latest=${backlog.latestBlockHeight} backlog=${backlog.backlog}${windowText} durationMs=${result.durationMs}${metricsText} error=${formatError(result.error)}`,
    );
  }

  private toProgress(tail: number, latestBlockHeight: number): number {
    if (latestBlockHeight < 0 || tail < 0) {
      return 0;
    }

    return (tail + 1) / (latestBlockHeight + 1);
  }

  private logIdleOnce(reason: string): void {
    if (this.lastIdleReason === reason) {
      return;
    }

    this.lastIdleReason = reason;
    console.log(`[onlydoge] indexer idle reason=${reason}`);
  }

  private createLease(): PrimaryLease {
    return {
      instanceId: this.instanceId,
      heartbeatAt: nowIsoString(),
    };
  }

  private isLeaseExpired(lease: PrimaryLease | null): boolean {
    if (!lease) {
      return true;
    }

    const heartbeatAt = new Date(lease.heartbeatAt).getTime();
    if (Number.isNaN(heartbeatAt)) {
      return true;
    }

    return Date.now() - heartbeatAt > this.leaseTimeoutMs;
  }

  private toPrimaryLease(value: PrimaryLease | string): PrimaryLease | null {
    if (typeof value === 'string') {
      return null;
    }

    if (isPrimaryLeaseRecord(value)) {
      return value;
    }

    return null;
  }

  private readSnapshotBlockHash(snapshot: Record<string, unknown>): string {
    const hash = requireSnapshotHash(requireSnapshotBlock(snapshot));
    return normalizeSnapshotHash(hash);
  }
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function shouldContinue(signal: AbortSignal | undefined): boolean {
  return !signal?.aborted;
}

function isOwnLease(lease: PrimaryLease | null, instanceId: string): boolean {
  return lease?.instanceId === instanceId;
}

function primaryLeaseClaimMessage(instanceId: string, reason: 'fresh' | 'stale'): string {
  if (reason === 'stale') {
    return `[onlydoge] indexer primary instance=${instanceId} replaced-stale-primary`;
  }

  return `[onlydoge] indexer primary instance=${instanceId}`;
}

function isPrimaryLeaseRecord(value: unknown): value is PrimaryLease {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  return hasStringProperty(value, 'instanceId') && hasStringProperty(value, 'heartbeatAt');
}

function hasStringProperty(value: object, property: string): boolean {
  return property in value && typeof Reflect.get(value, property) === 'string';
}

function requireSnapshotBlock(snapshot: Record<string, unknown>): Record<string, unknown> {
  const block = snapshot.block;
  if (!block || typeof block !== 'object' || Array.isArray(block)) {
    throw new Error('invalid snapshot block payload');
  }

  return block as Record<string, unknown>;
}

function requireSnapshotHash(block: Record<string, unknown>): string {
  const hash = block.hash;
  if (typeof hash !== 'string' || !hash.trim()) {
    throw new Error('missing snapshot block hash');
  }

  return hash;
}

function normalizeSnapshotHash(hash: string): string {
  return hash.trim();
}

class BootstrapRequiredError extends Error {}

function throwIfAborted(signal: AbortSignal): void {
  if (!signal.aborted) {
    return;
  }

  const reason = signal.reason;
  throw reason instanceof Error ? reason : new Error(String(reason ?? 'operation aborted'));
}

async function measureAsync<T>(work: () => Promise<T>): Promise<TimedValue<T>> {
  const startedAt = Date.now();
  const value = await work();
  return {
    durationMs: Date.now() - startedAt,
    value,
  };
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | null = null;

  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timer = setTimeout(() => reject(new Error(message)), timeoutMs);
        timer.unref?.();
      }),
    ]);
  } finally {
    if (timer) {
      clearTimeout(timer);
    }
  }
}
