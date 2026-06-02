#!/usr/bin/env bun

import { loadSettings, RelationalMetadataStore } from '@onlydoge/platform';

async function main() {
  const settings = loadSettings({ mode: 'indexer' });
  const metadata = await RelationalMetadataStore.connect(settings.database);
  const stale = await staleCoreIndexerProgress(metadata, settings.indexer.coreProgressWatchdogMs);
  assertNoStaleCoreIndexerProgress(stale);
  console.log('ok');
}

async function staleCoreIndexerProgress(
  metadata: RelationalMetadataStore,
  watchdogMs: number,
): Promise<string[]> {
  const now = Date.now();
  const stale = await staleCoreIndexerDogecoin(metadata, watchdogMs, now);
  return stale === null ? [] : [stale];
}

async function staleCoreIndexerDogecoin(
  metadata: RelationalMetadataStore,
  watchdogMs: number,
  now: number,
): Promise<string | null> {
  const state = await metadata.getCoreIndexerState();
  if (!isWatchedCoreIndexerState(state)) {
    return null;
  }

  const ageMs = coreIndexerStateAgeMs(state.updatedAt, now);
  return staleCoreIndexerDescription('Dogecoin', state, ageMs, watchdogMs);
}

function isBackfillStage(stage: string): boolean {
  return backfillStages.has(stage);
}

const backfillStages = new Set(['sync_backfill', 'process_backfill']);

function isWatchedCoreIndexerState(
  state: Awaited<ReturnType<RelationalMetadataStore['getCoreIndexerState']>>,
): state is NonNullable<typeof state> {
  return state !== null && isBackfillStage(state.stage);
}

function staleCoreIndexerDescription(
  chainName: string,
  state: NonNullable<Awaited<ReturnType<RelationalMetadataStore['getCoreIndexerState']>>>,
  ageMs: number,
  watchdogMs: number,
): string | null {
  if (ageMs <= watchdogMs) {
    return null;
  }

  return `${chainName}: stage=${state.stage} sync_tail=${state.syncTail} process_tail=${state.processTail} age_ms=${ageMs}`;
}

function coreIndexerStateAgeMs(updatedAtValue: string, now: number): number {
  const updatedAt = Date.parse(updatedAtValue);
  return Number.isNaN(updatedAt) ? Number.POSITIVE_INFINITY : now - updatedAt;
}

function assertNoStaleCoreIndexerProgress(stale: string[]): void {
  if (stale.length > 0) {
    throw new Error(`stale core indexer progress: ${stale.join('; ')}`);
  }
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
