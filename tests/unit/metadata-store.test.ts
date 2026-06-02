import { configKeyDogecoinHistoryReady } from '@onlydoge/indexing-pipeline';
import { describe, expect, it } from 'vitest';

import { createTestApp } from '../helpers';

describe('relational metadata store', () => {
  it('stores singleton JSON config values with compare-and-swap semantics', async () => {
    const ctx = await createTestApp('indexer');

    try {
      await expect(ctx.runtime.metadata.canReadDogecoinHistory()).resolves.toBe(false);

      await expect(
        ctx.runtime.metadata.compareAndSwapJsonValue(configKeyDogecoinHistoryReady(), null, true),
      ).resolves.toBe(true);
      await expect(
        ctx.runtime.metadata.compareAndSwapJsonValue(configKeyDogecoinHistoryReady(), null, false),
      ).resolves.toBe(false);
      await expect(ctx.runtime.metadata.canReadDogecoinHistory()).resolves.toBe(true);

      await ctx.runtime.metadata.setJsonValue('dogecoin:test:tail', 12);
      await ctx.runtime.metadata.deleteByPrefix('dogecoin:test:');
      await expect(ctx.runtime.metadata.getJsonValue('dogecoin:test:tail')).resolves.toBeNull();
    } finally {
      await ctx.cleanup();
    }
  });

  it('persists API keys and audit events without legacy secret-key storage', async () => {
    const ctx = await createTestApp();

    try {
      const created = await ctx.runtime.accessControl.createKey({ role: 'admin' });
      const apiToken = created.key;
      if (!apiToken) {
        throw new TypeError('expected bootstrap API token');
      }
      const actor = await ctx.runtime.accessControl.authenticate(apiToken);
      if (!actor) {
        throw new TypeError('expected authenticated actor');
      }

      await expect(ctx.runtime.metadata.countApiKeys()).resolves.toBe(1);
      await expect(ctx.runtime.metadata.countActiveAdminApiKeys()).resolves.toBe(1);
      await expect(ctx.runtime.metadata.getApiKeyById(actor.id)).resolves.toMatchObject({
        id: actor.id,
        role: 'admin',
        isActive: true,
      });

      const stored = await ctx.runtime.metadata.getApiKeyById(actor.id);
      expect(stored?.secretKeyHash).not.toContain('sk_');

      await ctx.runtime.metadata.createAuditEvent({
        actorApiKeyId: actor.apiKeyId,
        actorApiKey: actor.id,
        actorRole: actor.role,
        createdAt: '2026-01-01T00:00:00.000Z',
        error: null,
        ip: null,
        method: 'GET',
        operation: 'read',
        outcome: 'success',
        ownerApiKey: null,
        ownerApiKeyId: null,
        path: '/v1/explorer/blocks',
        requestId: 'req_metadata_test',
        resourceIds: [],
        resourceType: 'block',
        route: '/v1/explorer/blocks',
        statusCode: 200,
        userAgent: null,
      });

      await expect(
        ctx.runtime.metadata.listAuditEvents({ actorApiKeyId: actor.apiKeyId }),
      ).resolves.toEqual([
        expect.objectContaining({
          actorApiKey: actor.id,
          path: '/v1/explorer/blocks',
          resourceType: 'block',
        }),
      ]);
    } finally {
      await ctx.cleanup();
    }
  });

  it('stores singleton core indexer state and raw block identities', async () => {
    const ctx = await createTestApp('indexer');

    try {
      await expect(ctx.runtime.metadata.getCoreIndexerState()).resolves.toBeNull();

      await expect(
        ctx.runtime.metadata.upsertCoreIndexerState({
          onlineTip: 10,
          processTail: 7,
          stage: 'process_backfill',
          syncTail: 9,
        }),
      ).resolves.toMatchObject({
        lastError: null,
        onlineTip: 10,
        processTail: 7,
        stage: 'process_backfill',
        syncTail: 9,
      });

      await ctx.runtime.metadata.setCoreIndexerError('boom');
      await expect(ctx.runtime.metadata.getCoreIndexerState()).resolves.toMatchObject({
        lastError: 'boom',
        processTail: 7,
      });

      await ctx.runtime.metadata.upsertCoreBlock({
        blockHeight: 1,
        blockHash: 'block-1',
        previousBlockHash: 'block-0',
        blockTime: 1_700_000_060,
        txCount: 2,
        rawStorageKey: '1/block.json.gz',
        fetchedAt: '2026-01-01T00:00:00.000Z',
        processedAt: null,
      });

      await ctx.runtime.metadata.upsertCoreBlock({
        blockHeight: 1,
        blockHash: 'block-1b',
        previousBlockHash: 'block-0',
        blockTime: 1_700_000_061,
        txCount: 3,
        rawStorageKey: '1/block.json.gz',
        fetchedAt: '2026-01-01T00:00:01.000Z',
        processedAt: '2026-01-01T00:00:02.000Z',
      });
    } finally {
      await ctx.cleanup();
    }
  });
});
