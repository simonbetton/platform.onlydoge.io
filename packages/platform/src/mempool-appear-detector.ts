import { randomUUID } from 'node:crypto';

import { type DogecoinTransaction, isDogecoinTransaction } from '@onlydoge/indexing-pipeline';
import { noopServiceLogger, nowIsoString, type ServiceLogger } from '@onlydoge/shared-kernel';
import type { MempoolWatchBus } from './mempool-watch-bus';
import { matchingOutputsForAddress } from './mempool-watch-match';
import {
  type ActiveMempoolWatch,
  MEMPOOL_WATCH_CACHE_MAX_TXIDS,
  MEMPOOL_WATCH_DETECTOR_STATUS_KEY,
  MEMPOOL_WATCH_RPC_BATCH_SIZE,
  MEMPOOL_WATCH_RPC_CONCURRENCY,
  MEMPOOL_WATCH_RPC_POLL_MS,
  type MempoolAppearEvent,
  type MempoolWatchDetectorStatus,
} from './mempool-watch-types';
import type { BridgedZmqRawTxSource, MempoolRawTxSource } from './zmq-rawtx-source';

export interface MempoolWatchRegistryPort {
  compareAndSwapJsonValue<T>(key: string, expectedValue: T | null, nextValue: T): Promise<boolean>;
  getJsonValue<T>(key: string): Promise<T | null>;
  listActiveMempoolWatches(): Promise<ActiveMempoolWatch[]>;
}

export interface MempoolAppearRpcPort {
  decodeRawTransaction(
    dogecoin: { architecture: 'dogecoin'; rpcEndpoint: string; rps: number },
    rawTxHex: string,
  ): Promise<Record<string, unknown>>;
  getMempoolSnapshot(dogecoin: {
    architecture: 'dogecoin';
    rpcEndpoint: string;
    rps: number;
  }): Promise<{
    entries: Record<string, Record<string, unknown>>;
  }>;
  getRawTransaction(
    dogecoin: { architecture: 'dogecoin'; rpcEndpoint: string; rps: number },
    txid: string,
  ): Promise<Record<string, unknown>>;
  getRawTransactions(
    dogecoin: { architecture: 'dogecoin'; rpcEndpoint: string; rps: number },
    txids: string[],
  ): Promise<Record<string, unknown>[]>;
}

export interface MempoolAppearDogecoinConfigPort {
  getDogecoinConfig(): Promise<{
    architecture: 'dogecoin';
    rpcEndpoint: string;
    rps: number;
  }>;
}

export interface MempoolAppearDetectorOptions {
  cacheMaxTxids?: number;
  logger?: ServiceLogger;
  ownerInstanceId?: string;
  rpcBatchSize?: number;
  rpcConcurrency?: number;
  rpcPollMs?: number;
}

type CachedMempoolTransaction = Pick<DogecoinTransaction, 'txid' | 'vout'>;

export class MempoolAppearDetectorService {
  private watchesByAddress = new Map<string, ActiveMempoolWatch[]>();
  private cachedTransactions = new Map<string, CachedMempoolTransaction>();
  private workTail = Promise.resolve();
  private readonly cacheMaxTxids: number;
  private readonly logger: ServiceLogger;
  private readonly ownerInstanceId: string;
  private readonly rpcBatchSize: number;
  private readonly rpcConcurrency: number;
  private readonly rpcPollMs: number;

  public constructor(
    private readonly registry: MempoolWatchRegistryPort,
    private readonly rpc: MempoolAppearRpcPort,
    private readonly dogecoin: MempoolAppearDogecoinConfigPort,
    private readonly bus: MempoolWatchBus,
    private readonly zmqSource: MempoolRawTxSource,
    options: MempoolAppearDetectorOptions = {},
  ) {
    this.cacheMaxTxids = options.cacheMaxTxids ?? MEMPOOL_WATCH_CACHE_MAX_TXIDS;
    this.logger = options.logger ?? noopServiceLogger();
    this.ownerInstanceId = options.ownerInstanceId ?? randomUUID();
    this.rpcBatchSize = options.rpcBatchSize ?? MEMPOOL_WATCH_RPC_BATCH_SIZE;
    this.rpcConcurrency = options.rpcConcurrency ?? MEMPOOL_WATCH_RPC_CONCURRENCY;
    this.rpcPollMs = options.rpcPollMs ?? MEMPOOL_WATCH_RPC_POLL_MS;
  }

  public async start(signal?: AbortSignal): Promise<void> {
    const unsubscribe = this.bus.subscribeWatchChanged(() => {
      void this.refreshWatches().catch((error) => {
        this.logger.error(this.errorBindings(error), 'mempool watch refresh failed');
      });
    });
    await this.refreshWatches();

    const zmqTask = this.zmqSource.start((hex) => this.handleRawTxHex(hex), signal);
    const pollTask = this.pollLoop(signal);

    try {
      await Promise.all([zmqTask, pollTask]);
    } finally {
      unsubscribe();
    }
  }

  public async refreshWatches(): Promise<void> {
    await this.serializeWork(async () => {
      const previousIds = new Set(
        [...this.watchesByAddress.values()].flat().map((watch) => watch.id),
      );
      const watches = await this.registry.listActiveMempoolWatches();
      this.watchesByAddress = watchesByAddress(watches);
      if (watches.length === 0) {
        return;
      }

      await this.synchronizeMempool('catchup');
      const newWatches = watches.filter((watch) => !previousIds.has(watch.id));
      await this.matchCachedTransactions(newWatches, 'catchup');
    });
  }

  public get cachedTransactionCount(): number {
    return this.cachedTransactions.size;
  }

  public async handleDecodedTransaction(
    transaction: DogecoinTransaction,
    source: MempoolAppearEvent['source'],
  ): Promise<void> {
    const txid = transaction.txid?.trim();
    if (!txid) {
      return;
    }

    if (!this.cachedTransactions.has(txid) && this.cachedTransactions.size < this.cacheMaxTxids) {
      this.cachedTransactions.set(txid, compactTransaction(transaction));
    }
    if (this.watchesByAddress.size === 0) {
      return;
    }

    await this.matchTransaction(transaction, [...this.watchesByAddress.values()].flat(), source);
  }

  private async handleRawTxHex(rawTxHex: string): Promise<void> {
    // Skip RPC decode when nobody is watching — ZMQ rawtx is a firehose and
    // decoderawtransaction was saturating dogecoind's rpcworkqueue.
    if (this.watchesByAddress.size === 0) {
      return;
    }

    try {
      const dogecoin = await this.dogecoin.getDogecoinConfig();
      const decoded = await this.rpc.decodeRawTransaction(dogecoin, rawTxHex);
      if (!isDogecoinTransaction(decoded)) {
        return;
      }
      await this.serializeWork(() => this.handleDecodedTransaction(decoded, 'live'));
    } catch (error) {
      this.logger.error(this.errorBindings(error), 'mempool appear decode failed');
    }
  }

  private async pollLoop(signal?: AbortSignal): Promise<void> {
    while (signal?.aborted !== true) {
      try {
        if (this.watchesByAddress.size > 0) {
          await this.serializeWork(() => this.synchronizeMempool('live'));
        }
      } catch (error) {
        this.logger.error(this.errorBindings(error), 'mempool appear poll failed');
      }
      await sleep(this.rpcPollMs, signal);
    }
  }

  private async synchronizeMempool(source: MempoolAppearEvent['source']): Promise<void> {
    const dogecoin = await this.dogecoin.getDogecoinConfig();
    const snapshot = await this.rpc.getMempoolSnapshot(dogecoin);
    const snapshotTxids = Object.keys(snapshot.entries);
    if (snapshotTxids.length > this.cacheMaxTxids) {
      this.cachedTransactions.clear();
      await this.persistDetectorStatus(true, snapshotTxids.length);
      return;
    }

    const nextCache = new Map<string, CachedMempoolTransaction>();
    const missingTxids: string[] = [];
    for (const txid of snapshotTxids) {
      const cached = this.cachedTransactions.get(txid);
      if (cached) {
        nextCache.set(txid, cached);
      } else {
        missingTxids.push(txid);
      }
    }

    const hydrated = await mapWithConcurrency(
      chunkArray(missingTxids, this.rpcBatchSize),
      this.rpcConcurrency,
      (batch) => this.rpc.getRawTransactions(dogecoin, batch),
    );
    const newTransactions: CachedMempoolTransaction[] = [];
    for (const record of hydrated.flat()) {
      if (!isDogecoinTransaction(record)) {
        continue;
      }
      const compact = compactTransaction(record);
      const txid = compact.txid?.trim();
      if (!txid || !snapshot.entries[txid]) {
        continue;
      }
      nextCache.set(txid, compact);
      newTransactions.push(compact);
    }

    this.cachedTransactions = nextCache;
    await this.persistDetectorStatus(false, snapshotTxids.length);
    if (source === 'live') {
      await this.matchCachedTransactions(
        newTransactions.length === 0 ? [] : [...this.watchesByAddress.values()].flat(),
        source,
        newTransactions,
      );
    }
  }

  private async matchCachedTransactions(
    watches: ActiveMempoolWatch[],
    source: MempoolAppearEvent['source'],
    transactions: CachedMempoolTransaction[] = [...this.cachedTransactions.values()],
  ): Promise<void> {
    for (const transaction of transactions) {
      await this.matchTransaction(transaction, watches, source);
    }
  }

  private async matchTransaction(
    transaction: DogecoinTransaction,
    watches: ActiveMempoolWatch[],
    source: MempoolAppearEvent['source'],
  ): Promise<void> {
    const txid = transaction.txid?.trim();
    if (!txid) {
      return;
    }
    for (const watch of watches) {
      const outputs = matchingOutputsForAddress(transaction, watch.address, watch.minValueBase);
      if (!outputs) {
        continue;
      }
      await this.bus.publishAppear({
        address: watch.address,
        apiKeyId: watch.apiKeyId,
        detectedAt: nowIsoString(),
        outputs,
        source,
        txid,
        watchId: watch.id,
      });
    }
  }

  private async persistDetectorStatus(degraded: boolean, observedTxids: number): Promise<void> {
    for (let attempt = 0; attempt < 4; attempt += 1) {
      const current = await this.registry.getJsonValue<MempoolWatchDetectorStatus>(
        MEMPOOL_WATCH_DETECTOR_STATUS_KEY,
      );
      const now = Date.now();
      const currentExpiresAt = current ? Date.parse(current.expiresAt) : Number.NaN;
      const isExpired =
        current === null || !Number.isFinite(currentExpiresAt) || currentExpiresAt <= now;
      const isOwner = current?.ownerInstanceId === this.ownerInstanceId;
      if (!degraded && current?.degraded === true && !isExpired && !isOwner) {
        return;
      }
      if (degraded && current?.degraded === true && !isExpired && !isOwner) {
        return;
      }

      const observedAt = new Date(now).toISOString();
      const next: MempoolWatchDetectorStatus = {
        version: 1,
        ownerInstanceId: this.ownerInstanceId,
        degraded,
        observedTxids,
        observedAt,
        expiresAt: new Date(now + this.rpcPollMs * 3).toISOString(),
      };
      if (
        await this.registry.compareAndSwapJsonValue(
          MEMPOOL_WATCH_DETECTOR_STATUS_KEY,
          current,
          next,
        )
      ) {
        return;
      }
    }
    throw new Error('mempool watch detector status CAS did not converge');
  }

  private serializeWork<T>(work: () => Promise<T>): Promise<T> {
    const result = this.workTail.then(work, work);
    this.workTail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
  private errorBindings(error: unknown): Record<string, unknown> {
    return {
      component: 'mempool-appear',
      err: error instanceof Error ? error : new Error(formatError(error)),
    };
  }
}

function watchesByAddress(watches: ActiveMempoolWatch[]): Map<string, ActiveMempoolWatch[]> {
  const result = new Map<string, ActiveMempoolWatch[]>();
  for (const watch of watches) {
    const list = result.get(watch.address) ?? [];
    list.push(watch);
    result.set(watch.address, list);
  }
  return result;
}

function compactTransaction(transaction: DogecoinTransaction): CachedMempoolTransaction {
  return {
    txid: transaction.txid?.trim() ?? '',
    ...(transaction.vout ? { vout: transaction.vout } : {}),
  };
}

function chunkArray<T>(values: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

async function mapWithConcurrency<T, R>(
  values: T[],
  concurrency: number,
  work: (value: T) => Promise<R>,
): Promise<R[]> {
  const results = new Array<R>(values.length);
  let nextIndex = 0;
  await Promise.all(
    Array.from({ length: Math.min(concurrency, values.length) }, async () => {
      while (nextIndex < values.length) {
        const index = nextIndex;
        nextIndex += 1;
        results[index] = await work(values[index] as T);
      }
    }),
  );
  return results;
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener(
      'abort',
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

function formatError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export type { BridgedZmqRawTxSource };
