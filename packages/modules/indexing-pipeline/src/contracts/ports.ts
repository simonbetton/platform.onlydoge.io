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

export interface CoordinatorConfigPort {
  compareAndDeleteJsonValue<T>(key: string, expectedValue: T): Promise<boolean>;
  compareAndSwapJsonValue<T>(key: string, expectedValue: T | null, nextValue: T): Promise<boolean>;
  deleteByPrefix(prefix: string): Promise<void>;
  getJsonValue<T>(key: string): Promise<T | null>;
  setJsonValue<T>(key: string, value: T): Promise<void>;
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

export interface CoreDogecoinApplyContext {
  abortSignal?: AbortSignal;
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
