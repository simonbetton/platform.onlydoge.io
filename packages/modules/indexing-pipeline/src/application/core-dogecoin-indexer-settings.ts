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
  syncConcurrency: number;
  syncWindow: number;
}
