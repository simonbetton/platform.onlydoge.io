export interface CoreDogecoinIndexerSettings {
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
