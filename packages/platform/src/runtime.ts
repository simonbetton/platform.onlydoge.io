import { AccessControlService } from '@onlydoge/access-control';
import { AnalyticsQueryService } from '@onlydoge/analytics-query';
import { EntityLabelingService } from '@onlydoge/entity-labeling';
import { ExplorerQueryService } from '@onlydoge/explorer-query';
import { CoreDogecoinIndexerService } from '@onlydoge/indexing-pipeline';
import { InvestigationQueryService } from '@onlydoge/investigation-query';
import { NetworkCatalogService } from '@onlydoge/network-catalog';

import {
  ClickHouseCoreDogecoinStateStore,
  isClickHouseCoreDogecoinStore,
} from './core-dogecoin-state-store';
import { RelationalMetadataStore } from './metadata-store';
import { createRawBlockStorage } from './raw-block-storage';
import { HttpBlockchainRpcGateway } from './rpc';
import { type AppSettings, loadSettings } from './settings';
import {
  CompositeWarehouseAdapter,
  createFactWarehouse,
  MirroredProjectionStateStore,
} from './warehouse';

export interface Runtime {
  accessControl: AccessControlService;
  analyticsQuery: AnalyticsQueryService;
  entityLabeling: EntityLabelingService;
  explorerQuery: ExplorerQueryService;
  indexingPipeline: Pick<CoreDogecoinIndexerService, 'runOnce' | 'start'>;
  investigationQuery: InvestigationQueryService;
  metadata: RelationalMetadataStore;
  networkCatalog: NetworkCatalogService;
  settings: AppSettings;
}

export async function createRuntime(input?: {
  ip?: string;
  mode?: string;
  port?: number;
}): Promise<Runtime> {
  const settings = loadSettings(input);
  const metadata = await RelationalMetadataStore.connect(settings.database);
  const rawBlockStorage = createRawBlockStorage(settings.storage);
  const rpc = new HttpBlockchainRpcGateway();
  const factWarehouse = await createFactWarehouse(settings.warehouse);
  const stateStore = new MirroredProjectionStateStore(metadata, factWarehouse, factWarehouse);
  const explorerStateStore =
    settings.warehouse.driver === 'clickhouse' ? factWarehouse : stateStore;
  const explorerWarehouse = new CompositeWarehouseAdapter(explorerStateStore, factWarehouse);
  const coreStateStore = createCoreStateStore(settings, metadata, factWarehouse);

  const accessControl = new AccessControlService(metadata);
  await accessControl.deleteExpiredAuditEvents(settings.auditRetentionDays);
  const entityLabeling = new EntityLabelingService(
    metadata,
    metadata,
    metadata,
    metadata,
    metadata,
    metadata,
  );
  const networkCatalog = new NetworkCatalogService(metadata, metadata, rpc, {
    markNetworksUpdated: () => metadata.setJsonValue('networks_updated', 1),
    softDeleteAddressesByNetworkIds: (networkIds) =>
      entityLabeling.softDeleteAddressesByNetworkIds(networkIds),
  });
  const analyticsQuery = new AnalyticsQueryService(metadata, metadata, factWarehouse);
  const investigationQuery = new InvestigationQueryService(metadata, explorerWarehouse, metadata);
  const explorerQuery = new ExplorerQueryService(
    metadata,
    metadata,
    explorerWarehouse,
    rawBlockStorage,
    metadata,
    rpc,
  );
  const indexingPipeline = new CoreDogecoinIndexerService(
    metadata,
    metadata,
    rawBlockStorage,
    rpc,
    coreStateStore,
    settings.indexer,
  );

  return {
    settings,
    metadata,
    accessControl,
    analyticsQuery,
    networkCatalog,
    entityLabeling,
    explorerQuery,
    investigationQuery,
    indexingPipeline,
  };
}

function createCoreStateStore(
  settings: AppSettings,
  metadata: RelationalMetadataStore,
  factWarehouse: Awaited<ReturnType<typeof createFactWarehouse>>,
) {
  if (settings.warehouse.driver !== 'clickhouse') {
    return metadata;
  }

  return new ClickHouseCoreDogecoinStateStore(metadata, requireClickHouseCoreStore(factWarehouse));
}

function requireClickHouseCoreStore(
  factWarehouse: Awaited<ReturnType<typeof createFactWarehouse>>,
) {
  if (!isClickHouseCoreDogecoinStore(factWarehouse)) {
    throw new Error('ClickHouse warehouse does not support Dogecoin core state operations');
  }

  return factWarehouse;
}
