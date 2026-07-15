import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  MempoolAppearDetectorService,
  type MempoolWatchRegistryPort,
} from '../../packages/platform/src/mempool-appear-detector';
import { InProcessMempoolWatchBus } from '../../packages/platform/src/mempool-watch-bus';
import {
  type ActiveMempoolWatch,
  MEMPOOL_WATCH_DETECTOR_STATUS_KEY,
  type MempoolWatchDetectorStatus,
} from '../../packages/platform/src/mempool-watch-types';
import type { MempoolRawTxSource } from '../../packages/platform/src/zmq-rawtx-source';

describe('MempoolAppearDetectorService', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('publishes appears for watched addresses from decoded transactions', async () => {
    const bus = new InProcessMempoolWatchBus();
    const appears: string[] = [];
    bus.subscribeAppears((event) => {
      appears.push(event.txid);
    });

    const watch = createWatch('mw_1', 'DWatch');

    const detector = new MempoolAppearDetectorService(
      createRegistry([watch]),
      createRpc(),
      dogecoinConfig(),
      bus,
      noopSource(),
    );

    await detector.refreshWatches();
    await detector.handleDecodedTransaction(
      {
        txid: 'tx-detected',
        vout: [{ n: 0, value: 1, scriptPubKey: { address: 'DWatch' } }],
      },
      'live',
    );

    expect(appears).toEqual(['tx-detected']);
  });

  it('does not rpc-decode zmq rawtx when no watches are active', async () => {
    vi.useFakeTimers();
    const decodeRawTransaction = vi.fn(async () => {
      throw new Error('should not decode');
    });
    let onRawTx: ((hex: string) => Promise<void>) | undefined;

    const detector = new MempoolAppearDetectorService(
      createRegistry([]),
      createRpc({ decodeRawTransaction }),
      dogecoinConfig(),
      new InProcessMempoolWatchBus(),
      {
        async start(handler) {
          onRawTx = async (hex) => {
            await handler(hex);
          };
        },
      },
    );

    const abort = new AbortController();
    const started = detector.start(abort.signal);
    await vi.advanceTimersByTimeAsync(0);
    expect(onRawTx).toBeTypeOf('function');

    await onRawTx?.('010203');
    expect(decodeRawTransaction).not.toHaveBeenCalled();

    abort.abort();
    await started;
  });

  it('shares hydration and rematches cached transactions for a second watch', async () => {
    const bus = new InProcessMempoolWatchBus();
    const first = createWatch('mw_1', 'DPay');
    const second = createWatch('mw_2', 'DPay');
    const watches = [first];
    const registry = createRegistry(watches);
    const getMempoolSnapshot = vi.fn(async () => ({ entries: { cached: {} } }));
    const getRawTransactions = vi.fn(async () => [transaction('cached', 'DPay')]);
    const events: Array<{ source: string; watchId: string }> = [];
    bus.subscribeAppears((event) => events.push(event));
    const detector = new MempoolAppearDetectorService(
      registry,
      createRpc({ getMempoolSnapshot, getRawTransactions }),
      dogecoinConfig(),
      bus,
      noopSource(),
    );

    await detector.refreshWatches();
    watches.push(second);
    await detector.refreshWatches();

    expect(getMempoolSnapshot).toHaveBeenCalledTimes(2);
    expect(getRawTransactions).toHaveBeenCalledTimes(1);
    expect(events).toEqual(
      expect.arrayContaining([expect.objectContaining({ source: 'catchup', watchId: 'mw_2' })]),
    );
  });

  it('hydrates large snapshots in bounded batches and concurrency', async () => {
    const txids = Array.from({ length: 10_000 }, (_, index) => `tx-${index}`);
    let inFlight = 0;
    let peakInFlight = 0;
    const batchSizes: number[] = [];
    const getRawTransactions = vi.fn(async (_dogecoin: unknown, batch: string[]) => {
      batchSizes.push(batch.length);
      inFlight += 1;
      peakInFlight = Math.max(peakInFlight, inFlight);
      await Promise.resolve();
      inFlight -= 1;
      return batch.map((txid) => transaction(txid, 'DOther'));
    });
    const detector = new MempoolAppearDetectorService(
      createRegistry([createWatch('mw_1', 'DPay')]),
      createRpc({
        getMempoolSnapshot: async () => ({
          entries: Object.fromEntries(txids.map((txid) => [txid, {}])),
        }),
        getRawTransactions,
      }),
      dogecoinConfig(),
      new InProcessMempoolWatchBus(),
      noopSource(),
      { rpcBatchSize: 100, rpcConcurrency: 4 },
    );

    await detector.refreshWatches();

    expect(Math.max(...batchSizes)).toBe(100);
    expect(peakInFlight).toBeLessThanOrEqual(4);
    expect(getRawTransactions).toHaveBeenCalledTimes(100);
    expect(detector.cachedTransactionCount).toBe(10_000);
  });

  it('evicts departed txids so cache size follows the current snapshot', async () => {
    let txids = ['a', 'b', 'c'];
    const detector = new MempoolAppearDetectorService(
      createRegistry([createWatch('mw_1', 'DPay')]),
      createRpc({
        getMempoolSnapshot: async () => ({
          entries: Object.fromEntries(txids.map((txid) => [txid, {}])),
        }),
        getRawTransactions: async (_dogecoin: unknown, batch: string[]) =>
          batch.map((txid) => transaction(txid, 'DOther')),
      }),
      dogecoinConfig(),
      new InProcessMempoolWatchBus(),
      noopSource(),
    );

    await detector.refreshWatches();
    expect(detector.cachedTransactionCount).toBe(3);
    txids = ['c'];
    await detector.refreshWatches();
    expect(detector.cachedTransactionCount).toBe(1);
  });

  it('marks overflow degraded and recovers only under the configured cap', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-15T00:00:00.000Z'));
    let txids = ['a', 'b', 'c'];
    const registry = createRegistry([createWatch('mw_1', 'DPay')]);
    const detector = new MempoolAppearDetectorService(
      registry,
      createRpc({
        getMempoolSnapshot: async () => ({
          entries: Object.fromEntries(txids.map((txid) => [txid, {}])),
        }),
        getRawTransactions: async (_dogecoin: unknown, batch: string[]) =>
          batch.map((txid) => transaction(txid, 'DOther')),
      }),
      dogecoinConfig(),
      new InProcessMempoolWatchBus(),
      noopSource(),
      { cacheMaxTxids: 2, ownerInstanceId: 'owner-a', rpcPollMs: 1_000 },
    );

    await detector.refreshWatches();
    expect(registry.status()?.degraded).toBe(true);
    expect(registry.status()?.observedTxids).toBe(3);
    expect(detector.cachedTransactionCount).toBe(0);

    txids = ['a', 'b'];
    await detector.refreshWatches();
    expect(registry.status()?.degraded).toBe(false);
    expect(detector.cachedTransactionCount).toBe(2);
  });

  it('does not clear another live owner degradation but reclaims it after expiry', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-15T00:00:00.000Z'));
    const registry = createRegistry([createWatch('mw_1', 'DPay')]);
    const rpc = createRpc({
      getMempoolSnapshot: async () => ({ entries: { a: {}, b: {} } }),
      getRawTransactions: async (_dogecoin: unknown, batch: string[]) =>
        batch.map((txid) => transaction(txid, 'DOther')),
    });
    const first = new MempoolAppearDetectorService(
      registry,
      rpc,
      dogecoinConfig(),
      new InProcessMempoolWatchBus(),
      noopSource(),
      { cacheMaxTxids: 1, ownerInstanceId: 'owner-a', rpcPollMs: 1_000 },
    );
    const second = new MempoolAppearDetectorService(
      registry,
      rpc,
      dogecoinConfig(),
      new InProcessMempoolWatchBus(),
      noopSource(),
      { cacheMaxTxids: 2, ownerInstanceId: 'owner-b', rpcPollMs: 1_000 },
    );

    await first.refreshWatches();
    await second.refreshWatches();
    expect(registry.status()).toMatchObject({ degraded: true, ownerInstanceId: 'owner-a' });

    await vi.advanceTimersByTimeAsync(3_001);
    await second.refreshWatches();
    expect(registry.status()).toMatchObject({ degraded: false, ownerInstanceId: 'owner-b' });
  });
});

function noopSource(): MempoolRawTxSource {
  return {
    async start() {},
  };
}

function createWatch(id: string, address: string): ActiveMempoolWatch {
  return {
    id,
    apiKeyId: `key-${id}`,
    address,
    minValueBase: null,
    expiresAt: new Date(Date.now() + 60_000).toISOString(),
    createdAt: new Date().toISOString(),
  };
}

function transaction(txid: string, address: string) {
  return {
    txid,
    vout: [{ n: 0, value: 1, scriptPubKey: { address } }],
  };
}

function dogecoinConfig() {
  return {
    getDogecoinConfig: async () => ({
      architecture: 'dogecoin' as const,
      rpcEndpoint: 'http://127.0.0.1:22555',
      rps: 10,
    }),
  };
}

function createRpc(overrides: Record<string, unknown> = {}) {
  return {
    decodeRawTransaction: async () => {
      throw new Error('unused');
    },
    getMempoolSnapshot: async () => ({ entries: {} }),
    getRawTransaction: async () => {
      throw new Error('unused');
    },
    getRawTransactions: async () => [],
    ...overrides,
  };
}

function createRegistry(watches: ActiveMempoolWatch[]) {
  const values = new Map<string, unknown>();
  const registry: MempoolWatchRegistryPort & {
    status(): MempoolWatchDetectorStatus | null;
  } = {
    async compareAndSwapJsonValue<T>(key: string, expected: T | null, next: T) {
      const current = (values.get(key) as T | undefined) ?? null;
      if (JSON.stringify(current) !== JSON.stringify(expected)) {
        return false;
      }
      values.set(key, next);
      return true;
    },
    async getJsonValue<T>(key: string) {
      return (values.get(key) as T | undefined) ?? null;
    },
    async listActiveMempoolWatches() {
      return [...watches];
    },
    status() {
      return (
        (values.get(MEMPOOL_WATCH_DETECTOR_STATUS_KEY) as MempoolWatchDetectorStatus | undefined) ??
        null
      );
    },
  };
  return registry;
}
