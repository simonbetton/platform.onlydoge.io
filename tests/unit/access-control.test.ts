import { InMemoryApiKeyRateLimiter } from '@onlydoge/access-control';
import { describe, expect, it } from 'vitest';

import { createTestApp } from '../helpers';

describe('access control', () => {
  it('tracks rate limits independently per API key subject', () => {
    let now = 0;
    const limiter = new InMemoryApiKeyRateLimiter(
      {
        maxRequests: 2,
        windowMs: 1000,
      },
      () => now,
    );

    expect(limiter.consume('key-1')).toMatchObject({
      allowed: true,
      limit: 2,
      remaining: 1,
      resetSeconds: 1,
    });
    expect(limiter.consume('key-1')).toMatchObject({
      allowed: true,
      remaining: 0,
    });
    expect(limiter.consume('key-1')).toMatchObject({
      allowed: false,
      remaining: 0,
      retryAfterSeconds: 1,
    });

    expect(limiter.consume('key-2')).toMatchObject({
      allowed: true,
      remaining: 1,
    });

    now = 1000;
    expect(limiter.consume('key-1')).toMatchObject({
      allowed: true,
      remaining: 1,
    });
  });

  it('prunes audit events outside the retention window', async () => {
    const ctx = await createTestApp();

    try {
      const created = await ctx.runtime.accessControl.createKey({});
      const apiToken = created.key;
      if (!apiToken) {
        throw new TypeError('expected API token');
      }
      const actor = await ctx.runtime.accessControl.authenticate(apiToken);
      if (!actor) {
        throw new TypeError('expected authenticated API key');
      }

      const baseEvent = {
        actorApiKeyId: actor.apiKeyId,
        actorApiKey: actor.id,
        actorRole: actor.role,
        error: null,
        ip: null,
        method: 'GET',
        operation: 'read',
        outcome: 'success' as const,
        ownerApiKey: null,
        ownerApiKeyId: null,
        path: '/v1/networks',
        requestId: 'req_test',
        resourceIds: [],
        resourceType: 'network',
        route: '/v1/networks',
        statusCode: 200,
        userAgent: null,
      };

      await ctx.runtime.metadata.createAuditEvent({
        ...baseEvent,
        id: 'aud_old',
        createdAt: '2000-01-01T00:00:00.000Z',
      });
      await ctx.runtime.metadata.createAuditEvent({
        ...baseEvent,
        id: 'aud_new',
        createdAt: new Date(Date.now() + 60_000).toISOString(),
      });

      await ctx.runtime.accessControl.deleteExpiredAuditEvents(365);

      await expect(ctx.runtime.accessControl.listAuditEvents(actor, {})).resolves.toEqual({
        events: [expect.objectContaining({ id: 'aud_new' })],
      });
    } finally {
      await ctx.cleanup();
    }
  });
});
