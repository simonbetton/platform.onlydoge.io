#!/usr/bin/env bun

import { loadSettings, RelationalMetadataStore } from '@onlydoge/platform';

export interface CoreIndexerHealthState {
  lastError: string | null;
  onlineTip: number;
  processTail: number;
  stage: string;
  syncTail: number;
  updatedAt: string;
}

export interface CoreIndexerHealthInput {
  blockHeight: number | null;
  now: number;
  onlineTipDistance: number;
  state: CoreIndexerHealthState | null;
  watchdogMs: number;
}

async function main(): Promise<void> {
  const settings = loadSettings({ mode: 'indexer' });
  const metadata = await RelationalMetadataStore.connect(settings.database);
  const [state, blockHeight] = await Promise.all([
    metadata.getCoreIndexerState(),
    metadata.getJsonValue<number>('block_height'),
  ]);
  const unhealthy = evaluateCoreIndexerHealth({
    blockHeight,
    now: Date.now(),
    onlineTipDistance: settings.indexer.coreOnlineTipDistance,
    state,
    watchdogMs: settings.indexer.coreProgressWatchdogMs,
  });
  if (unhealthy) {
    throw new Error(`unhealthy core indexer: ${unhealthy}`);
  }
  console.log('ok');
}

const backfillStages = new Set(['sync_backfill', 'process_backfill']);

export function evaluateCoreIndexerHealth(input: CoreIndexerHealthInput): string | null {
  if (!input.state) {
    return 'state=missing';
  }
  const { state } = input;
  const ageMs = coreIndexerStateAgeMs(state.updatedAt, input.now);
  const details = healthDetails(state, input.blockHeight, ageMs);
  if (state.lastError) {
    return `${details} last_error=${redactHealthError(state.lastError)}`;
  }
  if (backfillStages.has(state.stage)) {
    return ageMs > input.watchdogMs ? `${details} reason=stale_progress` : null;
  }
  if (state.stage !== 'online') {
    return `${details} reason=invalid_stage`;
  }
  if (!Number.isFinite(ageMs)) {
    return `${details} reason=invalid_updated_at`;
  }
  const nodeTip = Math.max(input.blockHeight ?? -1, state.onlineTip);
  const lag = Math.max(0, nodeTip - state.processTail);
  return lag > input.onlineTipDistance ? `${details} reason=online_lag` : null;
}

function coreIndexerStateAgeMs(updatedAtValue: string, now: number): number {
  const updatedAt = Date.parse(updatedAtValue);
  return Number.isNaN(updatedAt) ? Number.POSITIVE_INFINITY : Math.max(0, now - updatedAt);
}

function healthDetails(
  state: CoreIndexerHealthState,
  blockHeight: number | null,
  ageMs: number,
): string {
  const nodeTip = Math.max(blockHeight ?? -1, state.onlineTip);
  const lag = Math.max(0, nodeTip - state.processTail);
  return `stage=${state.stage} node_tip=${nodeTip} sync_tail=${state.syncTail} process_tail=${state.processTail} lag=${lag} age_ms=${ageMs}`;
}

function redactHealthError(error: string): string {
  return error
    .replace(/:\/\/[^/@\s]+:[^/@\s]+@/gu, '://***:***@')
    .replace(/(password|token|secret)=\S+/giu, '$1=***')
    .slice(0, 500);
}

if (import.meta.main) {
  void main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
