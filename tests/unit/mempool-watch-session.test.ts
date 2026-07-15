import { describe, expect, it, vi } from 'vitest';

import { InProcessMempoolWatchBus } from '../../packages/platform/src/mempool-watch-bus';
import {
  MempoolWatchSessionService,
  type MempoolWatchSseEvent,
} from '../../packages/platform/src/mempool-watch-session';
import type { ActiveMempoolWatch } from '../../packages/platform/src/mempool-watch-types';
import {
  MEMPOOL_WATCH_MAX_CONCURRENT,
  type MempoolWatchDetectorStatus,
} from '../../packages/platform/src/mempool-watch-types';
import { ConflictError, TooEarlyError } from '../../packages/shared-kernel/src/domain/errors';

describe('MempoolWatchSessionService', () => {
  it('emits catch-up appear and closes', async () => {
    const registry = createRegistry();
    const bus = new InProcessMempoolWatchBus();
    bus.subscribeWatchChanged(() => {
      const watch = [...registry.watches.values()][0];
      if (watch) {
        void bus.publishAppear({
          address: watch.address,
          apiKeyId: watch.apiKeyId,
          detectedAt: new Date().toISOString(),
          outputs: [{ vout: 0, valueBase: '200000000' }],
          source: 'catchup',
          txid: 'tx-catchup',
          watchId: watch.id,
        });
      }
    });
    const service = new MempoolWatchSessionService(registry, bus);

    const events = [];
    for await (const event of service.openSession({
      apiKeyId: 'key_1',
      address: 'DPay',
    })) {
      events.push(event);
    }

    expect(events.some((event) => event.event === 'mempool.watch.appeared')).toBe(true);
    const appeared = events.find((event) => event.event === 'mempool.watch.appeared');
    expect(appeared?.event === 'mempool.watch.appeared' && appeared.data.source).toBe('catchup');
    expect(registry.watches.size).toBe(0);
  });

  it('rejects a sixth concurrent session for the same API key', async () => {
    const registry = createRegistry();
    const bus = new InProcessMempoolWatchBus();
    const service = new MempoolWatchSessionService(registry, bus);

    const aborts: AbortController[] = [];
    const iterators: Array<AsyncIterator<MempoolWatchSseEvent>> = [];
    for (let index = 0; index < 5; index += 1) {
      const abort = new AbortController();
      aborts.push(abort);
      const iterator = service
        .openSession({
          apiKeyId: 'key_1',
          address: `DPay${index}`,
          signal: abort.signal,
        })
        [Symbol.asyncIterator]();
      iterators.push(iterator);
      await iterator.next();
    }
    await vi.waitFor(() => expect(registry.watches.size).toBe(5));

    await expect(
      service.openSession({ apiKeyId: 'key_1', address: 'DPayOverflow' }).next(),
    ).rejects.toBeInstanceOf(ConflictError);

    for (const abort of aborts) {
      abort.abort();
    }
    await Promise.all(iterators.map((iterator) => iterator.next()));
  });

  it('emits live appear from the bus', async () => {
    const registry = createRegistry();
    const bus = new InProcessMempoolWatchBus();
    const service = new MempoolWatchSessionService(registry, bus);

    const events: MempoolWatchSseEvent[] = [];
    const session = service.openSession({
      apiKeyId: 'key_live',
      address: 'DLive',
    });

    const consume = (async () => {
      for await (const event of session) {
        events.push(event);
      }
    })();

    await vi.waitFor(() =>
      expect(events.some((event) => event.event === 'comment' && event.data === 'connected')).toBe(
        true,
      ),
    );
    const watch = [...registry.watches.values()][0];
    if (!watch) {
      throw new Error('expected active watch');
    }
    await bus.publishAppear({
      address: 'DLive',
      apiKeyId: 'key_live',
      detectedAt: new Date().toISOString(),
      outputs: [{ vout: 0, valueBase: '100000000' }],
      source: 'live',
      txid: 'tx-live',
      watchId: watch.id,
    });
    await consume;

    expect(events.some((event) => event.event === 'mempool.watch.appeared')).toBe(true);
  });

  it('rejects while an unexpired detector degradation marker is active', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-15T00:00:00.000Z'));
    const registry = createRegistry({
      version: 1,
      ownerInstanceId: 'indexer-a',
      degraded: true,
      observedTxids: 100_001,
      observedAt: new Date().toISOString(),
      expiresAt: new Date(Date.now() + 3_000).toISOString(),
    });
    const service = new MempoolWatchSessionService(registry, new InProcessMempoolWatchBus());

    await expect(
      service.openSession({ apiKeyId: 'key_1', address: 'DPay' }).next(),
    ).rejects.toBeInstanceOf(TooEarlyError);
    expect(registry.watches.size).toBe(0);
    vi.useRealTimers();
  });

  it('ignores an expired detector degradation marker', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-07-15T00:00:04.000Z'));
    const registry = createRegistry({
      version: 1,
      ownerInstanceId: 'indexer-a',
      degraded: true,
      observedTxids: 100_001,
      observedAt: '2026-07-15T00:00:00.000Z',
      expiresAt: '2026-07-15T00:00:03.000Z',
    });
    const abort = new AbortController();
    const service = new MempoolWatchSessionService(registry, new InProcessMempoolWatchBus());
    const stream = service.openSession({
      apiKeyId: 'key_1',
      address: 'DPay',
      signal: abort.signal,
    });

    await expect(stream.next()).resolves.toMatchObject({
      value: { event: 'comment', data: 'connected' },
    });
    abort.abort();
    await stream.next();
    vi.useRealTimers();
  });
});

function createRegistry(status: MempoolWatchDetectorStatus | null = null) {
  const watches = new Map<string, ActiveMempoolWatch>();
  return {
    watches,
    async createActiveMempoolWatch(input: {
      address: string;
      apiKeyId: string;
      expiresAt: string;
      id: string;
      minValueBase: string | null;
    }): Promise<ActiveMempoolWatch> {
      let activeForKey = 0;
      for (const watch of watches.values()) {
        if (watch.apiKeyId === input.apiKeyId) {
          activeForKey += 1;
        }
      }
      if (activeForKey >= MEMPOOL_WATCH_MAX_CONCURRENT) {
        throw new ConflictError(
          `mempool watch session limit reached for this API key (${MEMPOOL_WATCH_MAX_CONCURRENT})`,
        );
      }
      const record: ActiveMempoolWatch = {
        ...input,
        createdAt: new Date().toISOString(),
      };
      watches.set(record.id, record);
      return record;
    },
    async deleteActiveMempoolWatch(id: string): Promise<void> {
      watches.delete(id);
    },
    async getJsonValue<T>(): Promise<T | null> {
      return status as T | null;
    },
  };
}
