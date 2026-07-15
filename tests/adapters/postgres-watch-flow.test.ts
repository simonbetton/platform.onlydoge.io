import {
  MempoolWatchSessionService,
  PostgresMempoolWatchBus,
  RelationalMetadataStore,
} from '@onlydoge/platform';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { DockerService } from './docker-service';
import { postgresUrl, startPostgres } from './services';

const adapterTimeoutMs = 180_000;
let postgres: DockerService | null = null;

describe.skipIf(process.env.ONLYDOGE_RUN_ADAPTER_TESTS !== '1')(
  'split Postgres watch event flow',
  () => {
    beforeAll(async () => {
      postgres = await startPostgres();
    }, adapterTimeoutMs);

    afterAll(async () => {
      await postgres?.stop();
    }, 30_000);

    it(
      'registers on the HTTP side and receives one indexer-published SSE event',
      async () => {
        const database = {
          driver: 'postgres' as const,
          location: postgresUrl(requireValue(postgres)),
        };
        const metadata = await RelationalMetadataStore.connect(database);
        const httpBus = new PostgresMempoolWatchBus(database);
        const indexerBus = new PostgresMempoolWatchBus(database);
        await Promise.all([httpBus.start(), indexerBus.start()]);

        const watchChanged = eventPromise(indexerBus.subscribeWatchChanged.bind(indexerBus));
        const session = new MempoolWatchSessionService(metadata, httpBus);
        const stream = session.openSession({
          address: 'DAdapterWatchAddress',
          apiKeyId: 'key_adapter_http',
        });

        try {
          await expect(stream.next()).resolves.toMatchObject({
            value: { event: 'comment', data: 'connected' },
          });
          await expect(watchChanged).resolves.toBeUndefined();

          const [watch] = await metadata.listActiveMempoolWatches();
          if (!watch) {
            throw new Error('HTTP watch registration was not persisted');
          }

          await indexerBus.publishAppear({
            address: watch.address,
            apiKeyId: watch.apiKeyId,
            detectedAt: '2026-07-15T00:00:00.000Z',
            outputs: [{ valueBase: '100000000', vout: 0 }],
            source: 'live',
            txid: 'adapter-mempool-tx',
            watchId: watch.id,
          });

          await expect(stream.next()).resolves.toMatchObject({
            done: false,
            value: {
              data: {
                address: watch.address,
                outputs: [{ valueBase: '100000000', vout: 0 }],
                txid: 'adapter-mempool-tx',
              },
              event: 'mempool.watch.appeared',
            },
          });
        } finally {
          await stream.return(undefined);
          await Promise.allSettled([httpBus.stop(), indexerBus.stop()]);
          await metadata.close();
        }
      },
      adapterTimeoutMs,
    );
  },
);

function eventPromise(subscribe: (handler: () => void) => () => void): Promise<void> {
  return new Promise((resolve) => {
    const unsubscribe = subscribe(() => {
      unsubscribe();
      resolve();
    });
  });
}

function requireValue<T>(value: T | null): T {
  if (value === null) {
    throw new Error('Postgres adapter was not initialized');
  }
  return value;
}
