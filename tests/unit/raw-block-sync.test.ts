import {
  AdaptiveConcurrencyController,
  ContiguousFrontier,
  RawBlockSyncer,
  type RawBlockSyncSettings,
} from '@onlydoge/indexing-pipeline';
import { describe, expect, it } from 'vitest';

const dogecoin = {
  architecture: 'dogecoin' as const,
  id: 'dogecoin',
  rpcEndpoint: 'http://doge.example/',
  rps: 10,
};

function settings(overrides: Partial<RawBlockSyncSettings> = {}): RawBlockSyncSettings {
  return {
    coreRawStorageTimeoutMs: 1_000,
    syncBatchSize: 2,
    syncConcurrency: 4,
    syncRetryAttempts: 3,
    syncRetryBaseDelayMs: 1,
    ...overrides,
  };
}

function snapshot(height: number): Record<string, unknown> {
  return {
    block: {
      hash: `hash-${height}`,
      height,
      previousblockhash: height === 0 ? undefined : `hash-${height - 1}`,
      time: 1_700_000_000 + height,
      tx: [{ txid: `tx-${height}` }],
    },
  };
}

function parse(value: Record<string, unknown>) {
  const block = value.block as {
    hash: string;
    height: number;
    previousblockhash?: string;
    time: number;
    tx: Array<{ txid: string }>;
  };
  return {
    hash: block.hash,
    height: block.height,
    previousHash: block.previousblockhash ?? null,
    time: block.time,
    txids: block.tx.map((tx) => tx.txid),
  };
}

function createSyncer(
  rpc: (heights: number[]) => Promise<Record<string, unknown>[]>,
  overrides: Partial<RawBlockSyncSettings> = {},
) {
  const stored: number[] = [];
  const refs: number[] = [];
  const syncer = new RawBlockSyncer(
    { getBlockSnapshots: (_dogecoin, heights) => rpc(heights) },
    {
      async putPart(height) {
        stored.push(height);
      },
    },
    {
      async upsertCoreBlock() {},
      async upsertTransactionRefs(input) {
        refs.push(...input.map((ref) => ref.blockHeight));
      },
    },
    parse,
    settings(overrides),
    { sleep: async () => {} },
  );
  return { refs, stored, syncer };
}

describe('raw block syncer', () => {
  it('fetches in batches, stores every block, and checkpoints the contiguous frontier', async () => {
    const batches: number[][] = [];
    const checkpoints: number[] = [];
    const { syncer, stored, refs } = createSyncer(async (heights) => {
      batches.push(heights);
      return heights.map(snapshot);
    });

    const result = await syncer.sync(dogecoin, [10, 11, 12, 13, 14], {
      onCheckpoint: async (frontier) => {
        checkpoints.push(frontier);
      },
    });

    expect(batches).toEqual([[10, 11], [12, 13], [14]]);
    expect(stored.sort((a, b) => a - b)).toEqual([10, 11, 12, 13, 14]);
    expect(refs.sort((a, b) => a - b)).toEqual([10, 11, 12, 13, 14]);
    expect(checkpoints).toEqual([14]);
    expect(result).toMatchObject({ blocks: 5, failedAttempts: 0, frontier: 14 });
  });

  it('retries failed batches with backoff and reduces concurrency under pressure', async () => {
    let failures = 2;
    const { syncer } = createSyncer(async (heights) => {
      if (failures > 0) {
        failures -= 1;
        throw new Error('rpc timeout');
      }
      return heights.map(snapshot);
    });

    const result = await syncer.sync(dogecoin, [0, 1, 2, 3]);

    expect(result.failedAttempts).toBe(2);
    expect(result.frontier).toBe(3);
    expect(syncer.currentConcurrency).toBeLessThan(4);
  });

  it('gives up after the configured attempts and surfaces the last error', async () => {
    const { syncer, stored } = createSyncer(async () => {
      throw new Error('node unreachable');
    });

    await expect(syncer.sync(dogecoin, [0, 1])).rejects.toThrow('node unreachable');
    expect(stored).toEqual([]);
  });

  it('checkpoints only contiguous progress across rounds', async () => {
    const checkpoints: number[] = [];
    const { syncer } = createSyncer(async (heights) => heights.map(snapshot), {
      syncBatchSize: 1,
      syncConcurrency: 2,
    });

    await syncer.sync(dogecoin, [5, 6, 7], {
      onCheckpoint: async (frontier) => {
        checkpoints.push(frontier);
      },
    });

    expect(checkpoints).toEqual([6, 7]);
  });

  it('rejects snapshots that do not match the requested height', async () => {
    const { syncer } = createSyncer(async (heights) => heights.map(() => snapshot(99)), {
      syncRetryAttempts: 1,
    });

    await expect(syncer.sync(dogecoin, [1])).rejects.toThrow('raw block height mismatch');
  });

  it('aborts between rounds when the caller is no longer active', async () => {
    const { syncer } = createSyncer(async (heights) => heights.map(snapshot), {
      syncBatchSize: 1,
      syncConcurrency: 1,
    });
    let rounds = 0;

    await expect(
      syncer.sync(dogecoin, [0, 1, 2], {
        assertActive: () => {
          rounds += 1;
          if (rounds > 1) {
            throw new Error('lease lost');
          }
        },
      }),
    ).rejects.toThrow('lease lost');
  });
});

describe('adaptive concurrency controller', () => {
  it('halves on failure and ramps up after sustained success', () => {
    const controller = new AdaptiveConcurrencyController(8, 2);
    expect(controller.value).toBe(8);
    controller.recordFailure();
    expect(controller.value).toBe(4);
    controller.recordFailure();
    controller.recordFailure();
    controller.recordFailure();
    expect(controller.value).toBe(1);
    controller.recordSuccess();
    expect(controller.value).toBe(1);
    controller.recordSuccess();
    expect(controller.value).toBe(2);
  });
});

describe('contiguous frontier', () => {
  it('advances only through unbroken height ranges', () => {
    const frontier = new ContiguousFrontier(9);
    frontier.markCompleted([12, 13]);
    expect(frontier.frontier).toBe(9);
    expect(frontier.consumeAdvance()).toBe(false);
    frontier.markCompleted([10]);
    expect(frontier.frontier).toBe(10);
    frontier.markCompleted([11]);
    expect(frontier.frontier).toBe(13);
    expect(frontier.consumeAdvance()).toBe(true);
    expect(frontier.consumeAdvance()).toBe(false);
  });
});
