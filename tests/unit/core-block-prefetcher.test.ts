import { CoreBlockPrefetcher, type CoreBlockPrefetcherOptions } from '@onlydoge/indexing-pipeline';
import { describe, expect, it } from 'vitest';

function snapshot(height: number, weight = 1): Record<string, unknown> {
  return { height, weight };
}

function options(overrides: Partial<CoreBlockPrefetcherOptions> = {}): CoreBlockPrefetcherOptions {
  return {
    batchSize: 2,
    concurrency: 2,
    maxBufferedBlocks: 100,
    maxBufferedWeight: 100,
    weigh: (value) => Number(value.weight ?? 1),
    ...overrides,
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

async function settle(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

describe('core block prefetcher', () => {
  it('returns snapshots in height order and fetches each height once', async () => {
    const batches: number[][] = [];
    const prefetcher = new CoreBlockPrefetcher(async (heights) => {
      batches.push(heights);
      return heights.map((height) => snapshot(height));
    }, options());

    const loaded: unknown[] = [];
    for (let height = 10; height <= 16; height += 1) {
      loaded.push((await prefetcher.get(height, 16)).height);
    }

    expect(loaded).toEqual([10, 11, 12, 13, 14, 15, 16]);
    expect(batches).toEqual([[10, 11], [12, 13], [14, 15], [16]]);
  });

  it('keeps fetching ahead of the consumer while it is busy', async () => {
    const batches: number[][] = [];
    const prefetcher = new CoreBlockPrefetcher(
      async (heights) => {
        batches.push(heights);
        return heights.map((height) => snapshot(height));
      },
      options({ concurrency: 1 }),
    );

    await prefetcher.get(0, 9);
    await settle();

    // Only height 0 was asked for; the rest of the range was read ahead.
    expect(batches).toEqual([
      [0, 1],
      [2, 3],
      [4, 5],
      [6, 7],
      [8, 9],
    ]);
    expect(prefetcher.bufferedBlocks).toBe(10);
  });

  it('never reads past the limit height', async () => {
    const requested: number[] = [];
    const prefetcher = new CoreBlockPrefetcher(async (heights) => {
      requested.push(...heights);
      return heights.map((height) => snapshot(height));
    }, options());

    await prefetcher.get(5, 7);
    await settle();

    expect(requested).toEqual([5, 6, 7]);
  });

  it('bounds read-ahead by buffered weight', async () => {
    const requested: number[] = [];
    const prefetcher = new CoreBlockPrefetcher(
      async (heights) => {
        requested.push(...heights);
        return heights.map((height) => snapshot(height, 10));
      },
      options({ batchSize: 1, concurrency: 1, maxBufferedWeight: 30 }),
    );

    await prefetcher.get(0, 1_000);
    await settle();
    // Three loaded blocks already fill the 30-weight budget.
    expect(requested).toEqual([0, 1, 2]);

    await prefetcher.get(1, 1_000);
    await settle();
    // Consuming block 0 frees budget for exactly one more block.
    expect(requested).toEqual([0, 1, 2, 3]);
  });

  it('bounds read-ahead by buffered blocks', async () => {
    const requested: number[] = [];
    const prefetcher = new CoreBlockPrefetcher(
      async (heights) => {
        requested.push(...heights);
        return heights.map((height) => snapshot(height, 0));
      },
      options({ batchSize: 2, concurrency: 4, maxBufferedBlocks: 6 }),
    );

    await prefetcher.get(0, 1_000);
    await settle();

    expect(requested).toEqual([0, 1, 2, 3, 4, 5]);
  });

  it('charges in-flight batches at the observed average weight', async () => {
    const pending: Array<ReturnType<typeof deferred<Record<string, unknown>[]>>> = [];
    const requested: number[][] = [];
    const prefetcher = new CoreBlockPrefetcher(
      (heights) => {
        requested.push(heights);
        const next = deferred<Record<string, unknown>[]>();
        pending.push(next);
        return next.promise;
      },
      options({ batchSize: 1, concurrency: 8, maxBufferedWeight: 25 }),
    );

    const first = prefetcher.get(0, 1_000);
    await settle();
    // Nothing is known about block weight yet, so the first round uses the
    // whole concurrency budget.
    expect(requested).toHaveLength(8);

    for (const [index, batch] of pending.entries()) {
      batch.resolve([snapshot(index, 10)]);
    }
    await first;
    await settle();

    // 8 loaded blocks x 10 already exceed the 25-weight budget: no new fetches.
    expect(requested).toHaveLength(8);
  });

  it('drops read-ahead and restarts when the cursor jumps', async () => {
    const batches: number[][] = [];
    const prefetcher = new CoreBlockPrefetcher(async (heights) => {
      batches.push(heights);
      return heights.map((height) => snapshot(height));
    }, options());

    await prefetcher.get(0, 3);
    await settle();
    batches.length = 0;

    await expect(prefetcher.get(100, 101)).resolves.toMatchObject({ height: 100 });
    expect(batches[0]).toEqual([100, 101]);
    // A rewind restarts too: processing replays the same heights after recovery.
    await expect(prefetcher.get(0, 1)).resolves.toMatchObject({ height: 0 });
  });

  it('surfaces a failed batch to the consumer and recovers on the next read', async () => {
    let failures = 1;
    const prefetcher = new CoreBlockPrefetcher(async (heights) => {
      if (heights.includes(2) && failures > 0) {
        failures -= 1;
        throw new Error('node unreachable');
      }
      return heights.map((height) => snapshot(height));
    }, options());

    await expect(prefetcher.get(0, 5)).resolves.toMatchObject({ height: 0 });
    await expect(prefetcher.get(1, 5)).resolves.toMatchObject({ height: 1 });
    await expect(prefetcher.get(2, 5)).rejects.toThrow('node unreachable');
    await expect(prefetcher.get(2, 5)).resolves.toMatchObject({ height: 2 });
  });

  it('does not leak unhandled rejections from read-ahead nobody consumes', async () => {
    const unhandled: unknown[] = [];
    const onUnhandled = (error: unknown) => unhandled.push(error);
    process.on('unhandledRejection', onUnhandled);
    try {
      const prefetcher = new CoreBlockPrefetcher(async (heights) => {
        if (heights[0] !== 0) {
          throw new Error('read-ahead failed');
        }
        return heights.map((height) => snapshot(height));
      }, options());

      await prefetcher.get(0, 9);
      await settle();
      prefetcher.reset();
      await settle();
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }

    expect(unhandled).toEqual([]);
  });

  it('rejects batches that return the wrong number of snapshots', async () => {
    const prefetcher = new CoreBlockPrefetcher(async () => [snapshot(0)], options());

    await expect(prefetcher.get(0, 1)).rejects.toThrow(
      'core block prefetch size mismatch requested=2 received=1',
    );
  });

  it('ignores results of fetches that were in flight when it was reset', async () => {
    const stale = deferred<Record<string, unknown>[]>();
    let calls = 0;
    const prefetcher = new CoreBlockPrefetcher(
      (heights) => {
        calls += 1;
        return calls === 1
          ? stale.promise
          : Promise.resolve(heights.map((height) => snapshot(height)));
      },
      options({ batchSize: 1, concurrency: 1 }),
    );

    const abandoned = prefetcher.get(0, 0);
    prefetcher.reset();
    stale.resolve([{ height: 'stale' }]);
    await expect(abandoned).rejects.toThrow();

    await expect(prefetcher.get(0, 0)).resolves.toMatchObject({ height: 0 });
  });
});
