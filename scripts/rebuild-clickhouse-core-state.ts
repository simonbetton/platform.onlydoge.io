#!/usr/bin/env bun

import {
  configKeyDogecoinCurrentStateReady,
  configKeyDogecoinHistoryReady,
  configKeyIndexerProcessProgress,
  configKeyIndexerProcessTail,
  configKeyIndexerStage,
} from '@onlydoge/indexing-pipeline';
import {
  ClickHouseWarehouseAdapter,
  loadSettings,
  RelationalMetadataStore,
} from '@onlydoge/platform';
import { Command } from 'commander';
import { resolveDogecoinNetworkId } from './dogecoin-script-utils';

const program = new Command()
  .name('rebuild-clickhouse-core-state')
  .description('Destructively reset ClickHouse Dogecoin current-state tables for a fast rebuild.')
  .option('--execute', 'perform the destructive reset; without this flag the script is a dry run')
  .option('--networkId <id>', 'internal network id to reset')
  .parse();

const options = program.opts<{ execute?: boolean; networkId?: string }>();

async function main() {
  const settings = loadSettings({ mode: 'indexer' });
  if (settings.warehouse.driver !== 'clickhouse') {
    throw new Error('ClickHouse warehouse is required for fast core rebuild');
  }

  const metadata = await RelationalMetadataStore.connect(settings.database);
  const networkId = await resolveDogecoinNetworkId(metadata, options.networkId);
  const current = await metadata.getCoreIndexerState(networkId);

  console.log(
    JSON.stringify(
      {
        mode: options.execute ? 'execute' : 'dry-run',
        networkId,
        current,
        actions: [
          'drop and recreate ClickHouse projection/read/core append tables',
          'set core indexer stage to process_backfill',
          'set core process tail and progress to the beginning',
          'mark Dogecoin current state and history as not ready',
        ],
      },
      null,
      2,
    ),
  );

  if (!options.execute) {
    console.log('dry run only; pass --execute to apply');
    return;
  }

  const warehouse = new ClickHouseWarehouseAdapter(settings.warehouse);
  await warehouse.resetCoreDogecoinStorage();
  await metadata.upsertCoreIndexerState({
    networkId,
    stage: 'process_backfill',
    processTail: -1,
    lastError: null,
  });
  await Promise.all([
    metadata.setJsonValue(configKeyIndexerStage(networkId), 'process_backfill'),
    metadata.setJsonValue(configKeyIndexerProcessTail(networkId), -1),
    metadata.setJsonValue(configKeyIndexerProcessProgress(networkId), 0),
    metadata.setJsonValue(configKeyDogecoinCurrentStateReady(networkId), false),
    metadata.setJsonValue(configKeyDogecoinHistoryReady(networkId), false),
  ]);
  console.log('reset complete');
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
