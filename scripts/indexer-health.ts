#!/usr/bin/env bun

import { loadSettings, RelationalMetadataStore } from '@onlydoge/platform';

async function main() {
  const settings = loadSettings({ mode: 'indexer' });
  const metadata = await RelationalMetadataStore.connect(settings.database);
  const networks = await metadata.listActiveNetworks();
  const now = Date.now();
  const stale: string[] = [];

  for (const network of networks) {
    const state = await metadata.getCoreIndexerState(network.networkId);
    if (!state) {
      continue;
    }

    if (state.stage !== 'sync_backfill' && state.stage !== 'process_backfill') {
      continue;
    }

    const updatedAt = Date.parse(state.updatedAt);
    const ageMs = Number.isNaN(updatedAt) ? Number.POSITIVE_INFINITY : now - updatedAt;
    if (ageMs > settings.indexer.coreProgressWatchdogMs) {
      stale.push(
        `${network.name}: stage=${state.stage} sync_tail=${state.syncTail} process_tail=${state.processTail} age_ms=${ageMs}`,
      );
    }
  }

  if (stale.length > 0) {
    throw new Error(`stale core indexer progress: ${stale.join('; ')}`);
  }

  console.log('ok');
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
