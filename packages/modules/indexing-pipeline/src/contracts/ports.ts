import type {
  BlockProjectionBatch,
  CoreBlockRecord,
  CoreDogecoinApplyResult,
  CoreDogecoinBlockApplication,
  CoreIndexerStage,
  CoreIndexerState,
  ProjectionBalanceCursor,
  ProjectionBalanceSnapshot,
  ProjectionCurrentBalancePage,
  ProjectionCurrentUtxoPage,
  ProjectionFactWindow,
  ProjectionPageRequestContext,
  ProjectionStateBootstrapSnapshot,
  ProjectionUtxoOutput,
} from '../domain/projection-models';

export type CoordinatorConfigEntry = readonly [key: string, value: unknown];

export interface CoordinatorConfigPort {
  compareAndDeleteJsonValue<T>(key: string, expectedValue: T): Promise<boolean>;
  compareAndSwapJsonValue<T>(key: string, expectedValue: T | null, nextValue: T): Promise<boolean>;
  deleteByPrefix(prefix: string): Promise<void>;
  getJsonValue<T>(key: string): Promise<T | null>;
  setJsonValue<T>(key: string, value: T): Promise<void>;
  /**
   * Writes many keys in one round trip. Meant for progress telemetry that is
   * rewritten every window: adapters may trade the durability of the latest
   * write for throughput (a lost write is simply republished). Callers fall
   * back to per-key `setJsonValue` when an adapter does not provide it.
   */
  setJsonValues?(entries: readonly CoordinatorConfigEntry[]): Promise<void>;
}

export interface DogecoinConfigPort {
  getDogecoinConfig(): Promise<{
    architecture: 'dogecoin';
    blockTime: number;
    id: string;
    rpcEndpoint: string;
    rps: number;
    zmqBlockEndpoint?: string | null;
  }>;
}

export interface RawBlockStoragePort {
  getPart<T extends Record<string, unknown>>(
    blockHeight: number,
    part: string,
    context?: RawBlockStorageRequestContext,
  ): Promise<T | null>;
  putPart(
    blockHeight: number,
    part: string,
    payload: Record<string, unknown>,
    context?: RawBlockStorageRequestContext,
  ): Promise<void>;
}

export interface RawBlockStorageRequestContext {
  abortSignal?: AbortSignal;
  timeoutMs?: number;
}

export interface CoreDogecoinStateStorePort {
  applyCoreDogecoinBlock(
    input: CoreDogecoinBlockApplication,
    context?: CoreDogecoinApplyContext,
  ): Promise<CoreDogecoinApplyResult>;
  applyCoreDogecoinWindow(
    input: CoreDogecoinBlockApplication[],
    context?: CoreDogecoinApplyContext,
  ): Promise<CoreDogecoinApplyResult>;
  getCoreIndexerState(): Promise<CoreIndexerState | null>;
  /**
   * Highest block height the warehouse holds as processed, `null` when it holds
   * none. Stores that cannot tell leave this undefined (or resolve `undefined`)
   * and the indexer trusts its recorded process tail.
   */
  getCoreProcessedTail?(): Promise<number | null | undefined>;
  /**
   * Completes the history of blocks processed while current state was not
   * maintained (see `CoreHistoryFinalizationContext`). Stores whose history
   * is complete by construction leave this undefined.
   */
  finalizeCoreDogecoinHistory?(
    throughBlockHeight: number,
    context?: CoreHistoryFinalizationContext,
  ): Promise<void>;
  getCoreUtxoOutputs(outputKeys: string[]): Promise<Map<string, ProjectionUtxoOutput>>;
  materializeCoreDogecoinCurrentState(
    asOfBlockHeight: number,
    context?: CoreDogecoinApplyContext,
  ): Promise<void>;
  recoverCoreDogecoinWindow(
    fromBlockHeight: number,
    context?: CoreDogecoinApplyContext,
  ): Promise<void>;
  setCoreIndexerError(error: string | null): Promise<void>;
  setCoreIndexerStage(stage: CoreIndexerStage): Promise<void>;
  upsertCoreBlock(record: CoreBlockRecord): Promise<void>;
  /** Batch form of `upsertCoreBlock`; one write per raw sync batch when provided. */
  upsertCoreBlocks?(records: CoreBlockRecord[]): Promise<void>;
  upsertTransactionRefs(
    refs: Array<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      source: 'raw_sync' | 'core_process';
      txIndex: number;
      txid: string;
      version: number;
    }>,
  ): Promise<void>;
  upsertCoreIndexerState(input: {
    lastError?: string | null;
    onlineTip?: number;
    processTail?: number;
    stage?: CoreIndexerStage;
    syncTail?: number;
  }): Promise<CoreIndexerState>;
}

export type CoreWindowInsertStage =
  | 'creates'
  | 'spends'
  | 'movements'
  | 'transactions'
  | 'current_state'
  | 'processed_blocks';

export interface CoreStateMaterializationProgress {
  completedRanges: number;
  rangeCount: number;
}

export type CoreHistoryFinalizationPhase = 'debits' | 'facts';

export interface CoreHistoryFinalizationProgress {
  completedRanges: number;
  phase: CoreHistoryFinalizationPhase;
  rangeCount: number;
}

/**
 * History finalization completes the address movements and transaction facts
 * of blocks that were processed before current state existed. It runs as an
 * ordered list of output-key ranges (debit movements and input totals) and
 * then an ordered list of height bands (fact corrections). A caller that
 * records `onProgress` can hand the last progress back as `resumeFrom` to
 * continue a failed attempt instead of starting over; progress recorded for a
 * different range count is ignored.
 */
export interface CoreHistoryFinalizationContext {
  /** Called after statements that advance no range, such as the block index build. */
  onActivity?: () => Promise<void> | void;
  onProgress?: (progress: CoreHistoryFinalizationProgress) => Promise<void> | void;
  resumeFrom?: CoreHistoryFinalizationProgress;
  statementTimeoutMs?: number;
}

export interface CoreDogecoinApplyContext {
  abortSignal?: AbortSignal;
  /**
   * Current-state materialization runs as an ordered list of output-key
   * ranges. A caller that records `onRangeCompleted` progress can hand it back
   * here to continue a failed attempt instead of clearing and starting over;
   * progress recorded for a different range count is ignored.
   */
  materialization?: {
    /** Called after each statement of the phases that follow the ranges. */
    onActivity?: () => Promise<void> | void;
    onRangeCompleted?: (progress: CoreStateMaterializationProgress) => Promise<void> | void;
    resumeFrom?: CoreStateMaterializationProgress;
  };
  statementTimeoutMs?: number;
  testHooks?: {
    afterStage?: (stage: CoreWindowInsertStage) => void | Promise<void>;
  };
  updateCurrentState?: boolean;
  validatePrevouts?: boolean;
}

export interface TransactionRefWarehousePort {
  getTransactionRef(txid: string): Promise<{
    blockHash: string;
    blockHeight: number;
    blockTime: number;
    txIndex: number;
  } | null>;
  upsertTransactionRefs(
    refs: Array<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      source: 'raw_sync' | 'core_process';
      txIndex: number;
      txid: string;
      version: number;
    }>,
  ): Promise<void>;
}

export interface BlockchainRpcPort {
  getBlockHeight(dogecoin: {
    architecture: 'dogecoin';
    rpcEndpoint: string;
    rps: number;
  }): Promise<number>;
  getBlockSnapshot(
    dogecoin: {
      architecture: 'dogecoin';
      rpcEndpoint: string;
      rps: number;
    },
    blockHeight: number,
  ): Promise<Record<string, unknown>>;
  getBlockSnapshots(
    dogecoin: {
      architecture: 'dogecoin';
      rpcEndpoint: string;
      rps: number;
    },
    blockHeights: number[],
  ): Promise<Record<string, unknown>[]>;
}

export interface ProjectionWarehousePort {
  applyProjectionWindow(batches: BlockProjectionBatch[]): Promise<void>;
  getUtxoOutputs(outputKeys: string[]): Promise<Map<string, ProjectionUtxoOutput>>;
  hasAppliedBlock(blockHeight: number, blockHash: string): Promise<boolean>;
  listAppliedBlockSet(
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>>;
}

export interface ProjectionStateStorePort {
  applyProjectionWindow(batches: BlockProjectionBatch[]): Promise<void>;
  clearProjectionBootstrapState(): Promise<void>;
  finalizeProjectionBootstrap(processTail: number): Promise<void>;
  getCurrentAddressSummary(address: string): Promise<{
    balance: string;
    utxoCount: number;
  } | null>;
  getBalanceSnapshots(
    keys: Array<{
      address: string;
      assetAddress: string;
    }>,
  ): Promise<Map<string, ProjectionBalanceSnapshot>>;
  getProjectionBootstrapTail(): Promise<number | null>;
  getUtxoOutputs(outputKeys: string[]): Promise<Map<string, ProjectionUtxoOutput>>;
  hasAppliedBlock(blockHeight: number, blockHash: string): Promise<boolean>;
  hasProjectionState(): Promise<boolean>;
  importProjectionStateSnapshot(
    snapshot: ProjectionStateBootstrapSnapshot,
    processTail: number,
  ): Promise<void>;
  listAddressUtxos(
    address: string,
    offset?: number,
    limit?: number,
  ): Promise<ProjectionUtxoOutput[]>;
  listAppliedBlockSet(
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>>;
  upsertProjectionBootstrapBalances(rows: ProjectionBalanceSnapshot[]): Promise<void>;
  upsertProjectionBootstrapUtxoOutputs(rows: ProjectionUtxoOutput[]): Promise<void>;
}

export interface ProjectionFactWarehousePort {
  applyProjectionFacts(window: ProjectionFactWindow): Promise<void>;
  exportProjectionStateSnapshot(): Promise<ProjectionStateBootstrapSnapshot>;
  getAppliedBlockTail(): Promise<number | null>;
  hasAppliedBlock(blockHeight: number, blockHash: string): Promise<boolean>;
  listAppliedBlockSet(
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>>;
  listCurrentBalancesPage(
    cursor: ProjectionBalanceCursor | null,
    limit: number,
    context?: ProjectionPageRequestContext,
  ): Promise<ProjectionCurrentBalancePage>;
  listCurrentUtxoOutputsPage(
    cursorOutputKey: string | null,
    limit: number,
    context?: ProjectionPageRequestContext,
  ): Promise<ProjectionCurrentUtxoPage>;
}
