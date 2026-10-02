import {
  type CoreDogecoinApplyContext,
  configKeyDogecoinCurrentStateMaterialization,
  configKeyDogecoinCurrentStateReady,
  configKeyDogecoinHistoryReady,
} from '@onlydoge/indexing-pipeline';
import {
  ClickHouseCoreDogecoinStateStore,
  type ClickHouseCoreDogecoinStore,
} from '@onlydoge/platform';
import { describe, expect, it } from 'vitest';

import { createTestApp } from '../helpers';

type Materialize = ClickHouseCoreDogecoinStore['materializeCoreDogecoinCurrentState'];

describe('clickhouse core dogecoin state store', () => {
  it('checkpoints every finished range and clears the checkpoint when done', async () => {
    const ctx = await createTestApp('indexer');

    try {
      const { metadata } = ctx.runtime;
      const checkpoints: unknown[] = [];
      const forwarded: unknown[] = [];
      const store = new ClickHouseCoreDogecoinStateStore(
        metadata,
        clickHouseStore(async (_asOfBlockHeight, context) => {
          for (const completedRanges of [1, 2]) {
            await context?.materialization?.onRangeCompleted?.({
              completedRanges,
              rangeCount: 258,
            });
            checkpoints.push(
              await metadata.getJsonValue(configKeyDogecoinCurrentStateMaterialization()),
            );
          }
        }),
      );

      await store.materializeCoreDogecoinCurrentState(120, {
        materialization: {
          onRangeCompleted: (progress) => {
            forwarded.push(progress);
          },
        },
      });

      expect(checkpoints).toEqual([
        { asOfBlockHeight: 120, completedRanges: 1, rangeCount: 258 },
        { asOfBlockHeight: 120, completedRanges: 2, rangeCount: 258 },
      ]);
      expect(forwarded).toEqual([
        { completedRanges: 1, rangeCount: 258 },
        { completedRanges: 2, rangeCount: 258 },
      ]);
      await expect(
        metadata.getJsonValue(configKeyDogecoinCurrentStateMaterialization()),
      ).resolves.toBeNull();
      await expect(metadata.getJsonValue(configKeyDogecoinCurrentStateReady())).resolves.toBe(true);
      await expect(metadata.getJsonValue(configKeyDogecoinHistoryReady())).resolves.toBe(false);
    } finally {
      await ctx.cleanup();
    }
  });

  it('resumes from the checkpoint of an attempt at the same height', async () => {
    const ctx = await createTestApp('indexer');

    try {
      const { metadata } = ctx.runtime;
      const contexts: Array<CoreDogecoinApplyContext | undefined> = [];
      const store = new ClickHouseCoreDogecoinStateStore(
        metadata,
        clickHouseStore(async (_asOfBlockHeight, context) => {
          contexts.push(context);
        }),
      );
      await metadata.setJsonValue(configKeyDogecoinCurrentStateMaterialization(), {
        asOfBlockHeight: 120,
        completedRanges: 100,
        rangeCount: 258,
      });

      await store.materializeCoreDogecoinCurrentState(120, { statementTimeoutMs: 45_000 });

      expect(contexts[0]?.statementTimeoutMs).toBe(45_000);
      expect(contexts[0]?.materialization?.resumeFrom).toEqual({
        completedRanges: 100,
        rangeCount: 258,
      });
    } finally {
      await ctx.cleanup();
    }
  });

  it('starts over when the checkpoint belongs to another height or is malformed', async () => {
    const ctx = await createTestApp('indexer');

    try {
      const { metadata } = ctx.runtime;
      const contexts: Array<CoreDogecoinApplyContext | undefined> = [];
      const store = new ClickHouseCoreDogecoinStateStore(
        metadata,
        clickHouseStore(async (_asOfBlockHeight, context) => {
          contexts.push(context);
        }),
      );

      for (const checkpoint of [
        { asOfBlockHeight: 119, completedRanges: 100, rangeCount: 258 },
        { asOfBlockHeight: 120, completedRanges: 'many', rangeCount: 258 },
        { asOfBlockHeight: 120, completedRanges: 100 },
      ]) {
        await metadata.setJsonValue(configKeyDogecoinCurrentStateMaterialization(), checkpoint);
        await store.materializeCoreDogecoinCurrentState(120);
      }

      expect(contexts).toHaveLength(3);
      for (const context of contexts) {
        expect(context?.materialization?.resumeFrom).toBeUndefined();
      }
    } finally {
      await ctx.cleanup();
    }
  });

  it('keeps the checkpoint and the readiness flag untouched when materialization fails', async () => {
    const ctx = await createTestApp('indexer');

    try {
      const { metadata } = ctx.runtime;
      const store = new ClickHouseCoreDogecoinStateStore(
        metadata,
        clickHouseStore(async (_asOfBlockHeight, context) => {
          await context?.materialization?.onRangeCompleted?.({
            completedRanges: 7,
            rangeCount: 258,
          });
          throw new Error('warehouse request timed out');
        }),
      );

      await expect(store.materializeCoreDogecoinCurrentState(120)).rejects.toThrow(
        'warehouse request timed out',
      );

      await expect(
        metadata.getJsonValue(configKeyDogecoinCurrentStateMaterialization()),
      ).resolves.toEqual({ asOfBlockHeight: 120, completedRanges: 7, rangeCount: 258 });
      await expect(metadata.getJsonValue(configKeyDogecoinCurrentStateReady())).resolves.toBeNull();
    } finally {
      await ctx.cleanup();
    }
  });

  it('reports the warehouse tail only when the warehouse can tell', async () => {
    const ctx = await createTestApp('indexer');

    try {
      const { metadata } = ctx.runtime;
      const withTail = new ClickHouseCoreDogecoinStateStore(metadata, {
        ...clickHouseStore(async () => {}),
        getCoreProcessedTail: async () => 41,
      });
      const withoutTail = new ClickHouseCoreDogecoinStateStore(
        metadata,
        clickHouseStore(async () => {}),
      );

      await expect(withTail.getCoreProcessedTail()).resolves.toBe(41);
      await expect(withoutTail.getCoreProcessedTail()).resolves.toBeUndefined();
    } finally {
      await ctx.cleanup();
    }
  });
});

function clickHouseStore(materialize: Materialize): ClickHouseCoreDogecoinStore {
  return {
    async applyCoreDogecoinWindow() {
      return { applied: false, processTail: -1 };
    },
    async getUtxoOutputs() {
      return new Map();
    },
    materializeCoreDogecoinCurrentState: materialize,
    async recoverCoreDogecoinWindow() {},
    async upsertTransactionRefs() {},
  };
}
