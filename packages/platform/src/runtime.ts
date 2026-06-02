import { AccessControlService } from '@onlydoge/access-control';
import { AnalyticsQueryService } from '@onlydoge/analytics-query';
import { ExplorerQueryService } from '@onlydoge/explorer-query';
import { CoreDogecoinIndexerService } from '@onlydoge/indexing-pipeline';

import {
  ClickHouseCoreDogecoinStateStore,
  isClickHouseCoreDogecoinStore,
} from './core-dogecoin-state-store';
import { DogecoinMempoolSamplerService } from './mempool-sampler';
import { RelationalMetadataStore } from './metadata-store';
import { createRawBlockStorage } from './raw-block-storage';
import { HttpBlockchainRpcGateway } from './rpc';
import { type AppSettings, type DogecoinSettings, loadSettings } from './settings';
import {
  CompositeWarehouseAdapter,
  createFactWarehouse,
  MirroredProjectionStateStore,
} from './warehouse';

export interface Runtime {
  accessControl: AccessControlService;
  analyticsQuery: AnalyticsQueryService;
  explorerQuery: ExplorerQueryService;
  indexingPipeline: Pick<CoreDogecoinIndexerService, 'runOnce' | 'start'>;
  metadata: RelationalMetadataStore;
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
  const stateStore = new MirroredProjectionStateStore(metadata, factWarehouse);
  const explorerStateStore =
    settings.warehouse.driver === 'clickhouse' ? factWarehouse : stateStore;
  const explorerWarehouse = new CompositeWarehouseAdapter(explorerStateStore, factWarehouse);
  const coreStateStore = createCoreStateStore(settings, metadata, factWarehouse);

  const accessControl = new AccessControlService(metadata);
  await accessControl.deleteExpiredAuditEvents(settings.auditRetentionDays);
  const dogecoin = new SingletonDogecoinConfig(settings.dogecoin);
  const analyticsQuery = new AnalyticsQueryService(metadata, factWarehouse);
  const explorerQuery = new ExplorerQueryService(
    dogecoin,
    explorerWarehouse,
    rawBlockStorage,
    metadata,
    rpc,
  );
  const coreIndexer = new CoreDogecoinIndexerService(
    metadata,
    dogecoin,
    rawBlockStorage,
    rpc,
    coreStateStore,
    settings.indexer,
  );
  const mempoolSampler = new DogecoinMempoolSamplerService(
    dogecoin,
    rpc,
    factWarehouse,
    settings.dogecoin,
  );
  const indexingPipeline = new DogecoinIndexerRuntime(coreIndexer, mempoolSampler);

  return {
    settings,
    metadata,
    accessControl,
    analyticsQuery,
    explorerQuery,
    indexingPipeline,
  };
}

class DogecoinIndexerRuntime {
  public constructor(
    private readonly coreIndexer: Pick<CoreDogecoinIndexerService, 'runOnce' | 'start'>,
    private readonly mempoolSampler: Pick<DogecoinMempoolSamplerService, 'start'>,
  ) {}

  public runOnce(): Promise<boolean> {
    return this.coreIndexer.runOnce();
  }

  public async start(signal?: AbortSignal): Promise<void> {
    await Promise.all([this.coreIndexer.start(signal), this.mempoolSampler.start(signal)]);
  }
}

class SingletonDogecoinConfig {
  public constructor(private readonly settings: DogecoinSettings) {}

  public async getDogecoinConfig() {
    return this.dogecoinRef();
  }

  private dogecoinRef() {
    return {
      architecture: 'dogecoin' as const,
      blockTime: this.settings.blockTime,
      chainId: this.settings.chainId,
      id: 'dogecoin',
      name: 'Dogecoin',
      rpcEndpoint: this.settings.rpcEndpoint,
      rps: this.settings.rps,
      zmqBlockEndpoint: this.settings.zmqBlockEndpoint ?? null,
    };
  }
}

function createCoreStateStore(
  settings: AppSettings,
  metadata: RelationalMetadataStore,
  factWarehouse: Awaited<ReturnType<typeof createFactWarehouse>>,
) {
  void settings;
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
