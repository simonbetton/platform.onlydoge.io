export type MempoolAppearSource = 'catchup' | 'live';

export interface MempoolAppearOutput {
  valueBase: string;
  vout: number;
}

export interface MempoolAppearEvent {
  address: string;
  apiKeyId: string;
  detectedAt: string;
  outputs: MempoolAppearOutput[];
  source: MempoolAppearSource;
  txid: string;
  watchId: string;
}

export interface ActiveMempoolWatch {
  address: string;
  apiKeyId: string;
  createdAt: string;
  expiresAt: string;
  id: string;
  minValueBase: string | null;
}

export interface MempoolWatchDetectorStatus {
  degraded: boolean;
  expiresAt: string;
  observedAt: string;
  observedTxids: number;
  ownerInstanceId: string;
  version: 1;
}

export const MEMPOOL_WATCH_SESSION_MS = 5 * 60 * 1000;
export const MEMPOOL_WATCH_HEARTBEAT_MS = 15_000;
export const MEMPOOL_WATCH_RPC_POLL_MS = 1_000;
export const MEMPOOL_WATCH_RPC_BATCH_SIZE = 100;
export const MEMPOOL_WATCH_RPC_CONCURRENCY = 4;
export const MEMPOOL_WATCH_CACHE_MAX_TXIDS = 100_000;
export const MEMPOOL_WATCH_MAX_CONCURRENT = 5;
export const MEMPOOL_WATCH_DETECTOR_STATUS_KEY = 'mempool_watch_detector_status';
export const MEMPOOL_APPEAR_CHANNEL = 'onlydoge_mempool_appear';
export const MEMPOOL_WATCH_CHANGED_CHANNEL = 'onlydoge_mempool_watch_changed';
