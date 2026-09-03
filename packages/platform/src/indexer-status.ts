import {
  type CoreIndexerState,
  configKeyBlockHeight,
  configKeyDogecoinAnalyticsFactsReady,
  configKeyDogecoinCurrentStateReady,
  configKeyDogecoinHistoryReady,
  configKeyIndexerLastActivityAt,
  configKeyIndexerProcessBlocksPerSecond,
  configKeyIndexerProcessEtaSeconds,
  configKeyIndexerSyncBlocksPerSecond,
  configKeyIndexerSyncEtaSeconds,
} from '@onlydoge/indexing-pipeline';

/** Operator-facing snapshot of indexer progress, served by `/v1/status/` and `bun run status`. */
export interface IndexerStatus {
  chain: 'dogecoin';
  lastActivityAt: string | null;
  lastError: string | null;
  nodeTip: number | null;
  process: IndexerStageProgress;
  readiness: {
    analyticsFacts: boolean;
    currentState: boolean;
    history: boolean;
  };
  stage: CoreIndexerState['stage'] | 'uninitialized';
  sync: IndexerStageProgress;
  updatedAt: string | null;
}

export interface IndexerStageProgress {
  blocksPerSecond: number | null;
  etaSeconds: number | null;
  progress: number;
  remaining: number | null;
  tail: number;
}

export interface IndexerStatusSource {
  getCoreIndexerState(): Promise<CoreIndexerState | null>;
  getJsonValue<T>(key: string): Promise<T | null>;
}

export async function readIndexerStatus(source: IndexerStatusSource): Promise<IndexerStatus> {
  const [state, nodeTip, syncRate, syncEta, processRate, processEta, activity, readiness] =
    await Promise.all([
      source.getCoreIndexerState(),
      source.getJsonValue<number>(configKeyBlockHeight()),
      source.getJsonValue<number>(configKeyIndexerSyncBlocksPerSecond()),
      source.getJsonValue<number>(configKeyIndexerSyncEtaSeconds()),
      source.getJsonValue<number>(configKeyIndexerProcessBlocksPerSecond()),
      source.getJsonValue<number>(configKeyIndexerProcessEtaSeconds()),
      source.getJsonValue<string>(configKeyIndexerLastActivityAt()),
      readReadiness(source),
    ]);
  const tip = resolveNodeTip(nodeTip, state);

  return {
    chain: 'dogecoin',
    lastActivityAt: activity,
    lastError: state?.lastError ? redactStatusError(state.lastError) : null,
    nodeTip: tip,
    process: stageProgress(state?.processTail ?? -1, tip, processRate, processEta),
    readiness,
    stage: state?.stage ?? 'uninitialized',
    sync: stageProgress(state?.syncTail ?? -1, tip, syncRate, syncEta),
    updatedAt: state?.updatedAt ?? null,
  };
}

async function readReadiness(source: IndexerStatusSource): Promise<IndexerStatus['readiness']> {
  const [currentState, history, analyticsFacts] = await Promise.all([
    source.getJsonValue<boolean>(configKeyDogecoinCurrentStateReady()),
    source.getJsonValue<boolean>(configKeyDogecoinHistoryReady()),
    source.getJsonValue<boolean>(configKeyDogecoinAnalyticsFactsReady()),
  ]);
  return {
    analyticsFacts: analyticsFacts === true,
    currentState: currentState === true,
    history: history === true,
  };
}

function resolveNodeTip(blockHeight: number | null, state: CoreIndexerState | null): number | null {
  const candidates = [blockHeight, state?.onlineTip].filter(
    (value): value is number => typeof value === 'number' && value >= 0,
  );
  if (candidates.length === 0) {
    return null;
  }

  return Math.max(...candidates);
}

function stageProgress(
  tail: number,
  nodeTip: number | null,
  blocksPerSecond: number | null,
  etaSeconds: number | null,
): IndexerStageProgress {
  const remaining = nodeTip === null ? null : Math.max(0, nodeTip - tail);
  const progress = nodeTip === null || nodeTip < 0 ? 0 : clamp((tail + 1) / (nodeTip + 1));
  return { blocksPerSecond, etaSeconds, progress, remaining, tail };
}

function clamp(value: number): number {
  return Math.max(0, Math.min(1, value));
}

export function redactStatusError(error: string): string {
  return error
    .replace(/:\/\/[^/@\s]+:[^/@\s]+@/gu, '://***:***@')
    .replace(/(password|token|secret)=\S+/giu, '$1=***')
    .slice(0, 500);
}

export function formatIndexerStatusLine(status: IndexerStatus): string {
  const parts = [
    `stage=${status.stage}`,
    `node_tip=${status.nodeTip ?? 'unknown'}`,
    `sync_tail=${status.sync.tail}`,
    `sync_pct=${percent(status.sync.progress)}`,
    `sync_bps=${status.sync.blocksPerSecond ?? '-'}`,
    `sync_eta=${formatEta(status.sync.etaSeconds)}`,
    `process_tail=${status.process.tail}`,
    `process_pct=${percent(status.process.progress)}`,
    `process_bps=${status.process.blocksPerSecond ?? '-'}`,
    `process_eta=${formatEta(status.process.etaSeconds)}`,
    `last_activity=${status.lastActivityAt ?? '-'}`,
  ];
  if (status.lastError) {
    parts.push(`last_error=${JSON.stringify(status.lastError)}`);
  }
  return parts.join(' ');
}

function percent(value: number): string {
  return `${(value * 100).toFixed(2)}%`;
}

export function formatEta(seconds: number | null): string {
  if (seconds === null) {
    return '-';
  }
  if (seconds < 60) {
    return `${seconds}s`;
  }
  const days = Math.floor(seconds / 86_400);
  const hours = Math.floor((seconds % 86_400) / 3_600);
  const minutes = Math.floor((seconds % 3_600) / 60);
  if (days > 0) {
    return `${days}d${hours}h`;
  }
  if (hours > 0) {
    return `${hours}h${minutes}m`;
  }
  return `${minutes}m`;
}
