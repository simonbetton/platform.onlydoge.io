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
  const context = await createRebuildContext();

  console.log(
    JSON.stringify(
      {
        mode: rebuildMode(),
        networkId: context.networkId,
        current: context.current,
        actions: rebuildActions(),
      },
      null,
      2,
    ),
  );

  if (!options.execute) {
    console.log('dry run only; pass --execute to apply');
    return;
  }

  await executeRebuild(context);
  console.log('reset complete');
}

async function createRebuildContext() {
  const settings = loadSettings({ mode: 'indexer' });
  if (settings.warehouse.driver !== 'clickhouse') {
    throw new Error('ClickHouse warehouse is required for fast core rebuild');
  }

  const metadata = await RelationalMetadataStore.connect(settings.database);
  const networkId = await resolveDogecoinNetworkId(metadata, options.networkId);
  const current = await metadata.getCoreIndexerState(networkId);
  return { current, metadata, networkId, settings };
}

function rebuildMode(): 'dry-run' | 'execute' {
  return options.execute ? 'execute' : 'dry-run';
}

function rebuildActions(): string[] {
  return [
    'drop and recreate ClickHouse projection/read/core append tables',
    'set core indexer stage to process_backfill',
    'set core process tail and progress to the beginning',
    'mark Dogecoin current state and history as not ready',
  ];
}

async function executeRebuild(context: Awaited<ReturnType<typeof createRebuildContext>>) {
  const warehouse = new ClickHouseWarehouseAdapter(context.settings.warehouse);
  await warehouse.resetCoreDogecoinStorage();
  await context.metadata.upsertCoreIndexerState({
    networkId: context.networkId,
    stage: 'process_backfill',
    processTail: -1,
    lastError: null,
  });
  await Promise.all([
    context.metadata.setJsonValue(configKeyIndexerStage(context.networkId), 'process_backfill'),
    context.metadata.setJsonValue(configKeyIndexerProcessTail(context.networkId), -1),
    context.metadata.setJsonValue(configKeyIndexerProcessProgress(context.networkId), 0),
    context.metadata.setJsonValue(configKeyDogecoinCurrentStateReady(context.networkId), false),
    context.metadata.setJsonValue(configKeyDogecoinHistoryReady(context.networkId), false),
  ]);
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
