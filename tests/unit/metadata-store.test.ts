import { createClient as createLibsqlClient } from '@libsql/client';
import { ApiKey } from '@onlydoge/access-control';
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

  it('allows exactly one absent-key claim under contention', async () => {
    const ctx = await createTestApp('indexer');

    try {
      const candidates = Array.from({ length: 20 }, (_, index) => ({ owner: `worker-${index}` }));
      const results = await Promise.all(
        candidates.map((candidate) =>
          ctx.runtime.metadata.compareAndSwapJsonValue('test:absent-claim', null, candidate),
        ),
      );
      const winnerIndex = results.findIndex(Boolean);

      expect(results.filter(Boolean)).toHaveLength(1);
      expect(winnerIndex).toBeGreaterThanOrEqual(0);
      await expect(ctx.runtime.metadata.getJsonValue('test:absent-claim')).resolves.toEqual(
        candidates[winnerIndex],
      );
    } finally {
      await ctx.cleanup();
    }
  });

  it('allows exactly one matching-value swap under contention', async () => {
    const ctx = await createTestApp('indexer');

    try {
      const key = 'test:existing-claim';
      const expected = { epoch: 1, owner: 'original' };
      const candidates = [
        { epoch: 2, owner: 'worker-a' },
        { epoch: 2, owner: 'worker-b' },
      ];
      await ctx.runtime.metadata.setJsonValue(key, expected);

      const results = await Promise.all(
        candidates.map((candidate) =>
          ctx.runtime.metadata.compareAndSwapJsonValue(key, expected, candidate),
        ),
      );
      const winnerIndex = results.findIndex(Boolean);

      expect(results.filter(Boolean)).toHaveLength(1);
      expect(winnerIndex).toBeGreaterThanOrEqual(0);
      await expect(ctx.runtime.metadata.getJsonValue(key)).resolves.toEqual(
        candidates[winnerIndex],
      );
      await expect(
        ctx.runtime.metadata.compareAndSwapJsonValue(key, expected, { epoch: 3, owner: 'stale' }),
      ).resolves.toBe(false);
      await expect(ctx.runtime.metadata.getJsonValue(key)).resolves.toEqual(
        candidates[winnerIndex],
      );
    } finally {
      await ctx.cleanup();
    }
  });

  it('deletes only when the stored JSON matches exactly', async () => {
    const ctx = await createTestApp('indexer');

    try {
      const key = 'test:delete-cas';
      const marker = { version: 1, owner: 'worker-a' };
      await ctx.runtime.metadata.setJsonValue(key, marker);

      await expect(
        ctx.runtime.metadata.compareAndDeleteJsonValue(key, { version: 1, owner: 'worker-b' }),
      ).resolves.toBe(false);
      await expect(ctx.runtime.metadata.getJsonValue(key)).resolves.toEqual(marker);
      await expect(ctx.runtime.metadata.compareAndDeleteJsonValue(key, marker)).resolves.toBe(true);
      await expect(ctx.runtime.metadata.getJsonValue(key)).resolves.toBeNull();
    } finally {
      await ctx.cleanup();
    }
  });

  it('updates the timestamp after a successful swap', async () => {
    const ctx = await createTestApp('indexer');
    const client = createLibsqlClient({ url: `file:${ctx.tempRoot}/onlydoge.sqlite.db` });

    try {
      const key = 'test:cas-timestamp';
      await ctx.runtime.metadata.setJsonValue(key, { epoch: 1 });
      const before = await client.execute({
        sql: 'SELECT updated_at FROM app_config WHERE key = ?',
        args: [key],
      });
      await new Promise((resolve) => setTimeout(resolve, 5));

      await expect(
        ctx.runtime.metadata.compareAndSwapJsonValue(key, { epoch: 1 }, { epoch: 2 }),
      ).resolves.toBe(true);
      const after = await client.execute({
        sql: 'SELECT updated_at FROM app_config WHERE key = ?',
        args: [key],
      });

      expect(after.rows[0]?.updated_at).not.toBe(before.rows[0]?.updated_at);
    } finally {
      client.close();
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

  it('atomically creates exactly one bootstrap API key', async () => {
    const ctx = await createTestApp();

    try {
      const candidates = Array.from(
        { length: 8 },
        (_, index) => ApiKey.create({ id: `key_bootstrap_${index}`, role: 'admin' }).record,
      );
      const results = await Promise.all(
        candidates.map((record) => ctx.runtime.metadata.createBootstrapApiKey(record)),
      );

      expect(results.filter((result) => result.created)).toHaveLength(1);
      await expect(ctx.runtime.metadata.countApiKeys()).resolves.toBe(1);
      await expect(ctx.runtime.metadata.countActiveAdminApiKeys()).resolves.toBe(1);
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

      await expect(
        ctx.runtime.metadata.upsertCoreIndexerState({
          lastError: null,
          syncTail: 10,
        }),
      ).resolves.toMatchObject({
        lastError: null,
        syncTail: 10,
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
