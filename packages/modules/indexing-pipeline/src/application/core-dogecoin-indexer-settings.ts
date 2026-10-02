export type CoreBackfillBlockSource = 'auto' | 'node' | 'storage';

export interface CoreDogecoinIndexerSettings {
  /**
   * Where backfill windows read blocks from. `node` fetches raw blocks from
   * Dogecoin Core in JSON-RPC batches (sequential block-file reads); `storage`
   * reads the per-block snapshots written by raw sync; `auto` prefers the node
   * for finalized heights and falls back to storage when the node fails.
   * Defaults to `storage` when unset.
   */
  coreBackfillBlockSource?: CoreBackfillBlockSource;
  /** Maximum blocks per backfill window. Defaults to `coreProcessWindow` when unset. */
  coreBackfillWindowBlocks?: number;
  /**
   * Target rows (transaction inputs + outputs) per backfill window. A window
   * closes as soon as it holds this many rows, so dense blocks produce short
   * windows and sparse blocks long ones. Unset disables the row target.
   */
  coreBackfillWindowRows?: number;
  coreBlockTimeoutMs: number;
  coreDbStatementTimeoutMs: number;
  coreOnlineTipDistance: number;
  coreProcessLoadConcurrency: number;
  coreProcessWindow: number;
  coreProgressWatchdogMs: number;
  coreRawStorageTimeoutMs: number;
  coreReprocessDepth: number;
  coreSyncCompleteDistance: number;
  leaseHeartbeatIntervalMs: number;
  /** Blocks per JSON-RPC batch during raw sync. */
  syncBatchSize: number;
  /** Maximum parallel RPC batches during raw sync; adapts down under node pressure. */
  syncConcurrency: number;
  /** Attempts per raw sync batch before the window fails. */
  syncRetryAttempts: number;
  /** Base exponential backoff between raw sync batch attempts. */
  syncRetryBaseDelayMs: number;
  /** Blocks per raw sync window (checkpointed per batch round, so large is safe). */
  syncWindow: number;
}
