import { AccessControlService } from '@onlydoge/access-control';
import { AnalyticsQueryService } from '@onlydoge/analytics-query';
import { ExplorerQueryService, type ExplorerWarehousePort } from '@onlydoge/explorer-query';
import {
  CoreDogecoinIndexerService,
  type ProjectionStateStorePort,
} from '@onlydoge/indexing-pipeline';

import {
  ClickHouseCoreDogecoinStateStore,
  isClickHouseCoreDogecoinStore,
} from './core-dogecoin-state-store';
import { asServiceLogger, createLogger } from './logger';
import { MempoolAppearDetectorService } from './mempool-appear-detector';
import { DogecoinMempoolSamplerService } from './mempool-sampler';
import { createMempoolWatchBus } from './mempool-watch-bus';
import { MempoolWatchSessionService } from './mempool-watch-session';
import { RelationalMetadataStore } from './metadata-store';
import { createRawBlockStorage } from './raw-block-storage';
import { HttpBlockchainRpcGateway } from './rpc';
import { type AppSettings, type DogecoinSettings, loadSettings } from './settings';
import {
  CompositeWarehouseAdapter,
  createFactWarehouse,
  MirroredProjectionStateStore,
} from './warehouse';
import { BridgedZmqRawTxSource, NoopMempoolRawTxSource } from './zmq-rawtx-source';

export interface Runtime {
  accessControl: AccessControlService;
  analyticsQuery: AnalyticsQueryService;
  explorerQuery: ExplorerQueryService;
  indexingPipeline: Pick<CoreDogecoinIndexerService, 'runOnce' | 'start'>;
  mempoolWatch: MempoolWatchSessionService;
  metadata: RelationalMetadataStore;
  settings: AppSettings;
}

export async function createRuntime(input?: {
  ip?: string;
  mode?: string;
  port?: number;
}): Promise<Runtime> {
  const settings = loadSettings(input);
  const serviceLoggers = createServiceLoggers();
  const metadata = await RelationalMetadataStore.connect(settings.database);
  const rawBlockStorage = createRawBlockStorage(settings.storage);
  const rpc = new HttpBlockchainRpcGateway(settings.dogecoin.rpcTimeoutMs);
  const factWarehouse = await createFactWarehouse(
    settings.warehouse,
    metadata,
    serviceLoggers.warehouse,
  );
  const stateStore = new MirroredProjectionStateStore(metadata, factWarehouse);
  const explorerWarehouse = createExplorerWarehouse(
    settings.warehouse.driver,
    stateStore,
    factWarehouse,
  );
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
    { logger: serviceLoggers.coreIndexer },
  );
  const mempoolSampler = new DogecoinMempoolSamplerService(
    dogecoin,
    rpc,
    factWarehouse,
    settings.dogecoin,
    serviceLoggers.mempoolSampler,
  );

  const shareInProcess = settings.isHttp && settings.isIndexer;
  assertMempoolWatchTopology(settings);
  const mempoolWatchBus = createMempoolWatchBus({
    database: settings.database,
    shareInProcess,
  });
  await mempoolWatchBus.start();

  const mempoolWatch = new MempoolWatchSessionService(metadata, mempoolWatchBus);

  const appearDetector = new MempoolAppearDetectorService(
    metadata,
    rpc,
    dogecoin,
    mempoolWatchBus,
    createZmqRawTxSource(settings.dogecoin, serviceLoggers.zmqRawTx),
    {
      cacheMaxTxids: settings.dogecoin.mempoolWatchCacheMaxTxids,
      logger: serviceLoggers.mempoolAppear,
      rpcBatchSize: settings.dogecoin.mempoolWatchRpcBatchSize,
      rpcConcurrency: settings.dogecoin.mempoolWatchRpcConcurrency,
      rpcPollMs: settings.dogecoin.mempoolWatchRpcPollMs,
    },
  );

  const indexingPipeline = new DogecoinIndexerRuntime(coreIndexer, mempoolSampler, appearDetector);

  return {
    settings,
    metadata,
    accessControl,
    analyticsQuery,
    explorerQuery,
    indexingPipeline,
    mempoolWatch,
  };
}

export function assertMempoolWatchTopology(
  settings: Pick<AppSettings, 'database' | 'isHttp' | 'isIndexer'>,
): void {
  const isSplitHttpRole = settings.isHttp && !settings.isIndexer;
  if (isSplitHttpRole && settings.database.driver !== 'postgres') {
    throw new Error('split HTTP/indexer mode requires Postgres for mempool watch delivery');
  }
}

export function createExplorerWarehouse(
  driver: AppSettings['warehouse']['driver'],
  stateStore: Pick<
    ProjectionStateStorePort,
    'getCurrentAddressSummary' | 'getUtxoOutputs' | 'listAddressUtxos'
  >,
  factWarehouse: ExplorerWarehousePort,
): ExplorerWarehousePort {
  if (driver === 'clickhouse') {
    return factWarehouse;
  }

  return new CompositeWarehouseAdapter(stateStore, factWarehouse);
}

class DogecoinIndexerRuntime {
  public constructor(
    private readonly coreIndexer: Pick<CoreDogecoinIndexerService, 'runOnce' | 'start'>,
    private readonly mempoolSampler: Pick<DogecoinMempoolSamplerService, 'start'>,
    private readonly appearDetector: Pick<MempoolAppearDetectorService, 'start'>,
  ) {}

  public runOnce(): Promise<boolean> {
    return this.coreIndexer.runOnce();
  }

  public async start(signal?: AbortSignal): Promise<void> {
    await Promise.all([
      this.coreIndexer.start(signal),
      this.mempoolSampler.start(signal),
      this.appearDetector.start(signal),
    ]);
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
      zmqTxEndpoint: this.settings.zmqTxEndpoint ?? null,
    };
  }
}

function createZmqRawTxSource(
  settings: DogecoinSettings,
  logger: ReturnType<typeof asServiceLogger>,
) {
  const endpoint = settings.zmqTxEndpoint ?? settings.zmqBlockEndpoint ?? null;
  if (!endpoint) {
    return new NoopMempoolRawTxSource();
  }

  return new BridgedZmqRawTxSource(endpoint, logger);
}

function createServiceLoggers() {
  return {
    coreIndexer: asServiceLogger(createLogger({ component: 'core-indexer', service: 'onlydoge' })),
    mempoolAppear: asServiceLogger(
      createLogger({ component: 'mempool-appear', service: 'onlydoge' }),
    ),
    mempoolSampler: asServiceLogger(
      createLogger({ component: 'mempool-sampler', service: 'onlydoge' }),
    ),
    warehouse: asServiceLogger(createLogger({ component: 'warehouse', service: 'onlydoge' })),
    zmqRawTx: asServiceLogger(createLogger({ component: 'zmq-rawtx', service: 'onlydoge' })),
  };
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
