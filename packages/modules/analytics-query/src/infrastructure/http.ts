import { type AuthenticatedApiKeyResolver, protectedRouteDetail } from '@onlydoge/access-control';
import { Elysia, t } from 'elysia';

import type { AnalyticsQueryService } from '../application/analytics-query-service';

const analyticsTag = 'Analytics';

const analyticsQueryBodySchema = t.Object({
  sql: t.String({
    description:
      'Generated SQL. It must be a single SELECT against the curated analytics schema and use OnlyDoge placeholders.',
  }),
  network: t.Optional(
    t.String({
      description:
        'Network id. Omit when a single Dogecoin network is configured and should be used as the default.',
      examples: ['net_dogecoin'],
    }),
  ),
  from: t.String({
    description: 'Inclusive window start as ISO-8601 or Unix seconds.',
    examples: ['2026-05-30T00:00:00.000Z'],
  }),
  to: t.String({
    description: 'Exclusive window end as ISO-8601 or Unix seconds.',
    examples: ['2026-05-31T00:00:00.000Z'],
  }),
  limit: t.Optional(
    t.Number({
      description: 'Server-bound result limit placeholder value. Maximum 1000.',
      examples: [100],
    }),
  ),
});

export function buildAnalyticsQueryHttp(
  service: AnalyticsQueryService,
  resolveAuthenticatedApiKey: AuthenticatedApiKeyResolver,
) {
  return new Elysia({ prefix: '/v1/analytics' })
    .get('/schema', () => service.schema(), {
      detail: protectedRouteDetail({
        tags: [analyticsTag],
        summary: 'Get AI analytics schema',
        description:
          'Returns the curated ClickHouse analytics schema, required placeholders, constraints, and examples for AI SQL generation.',
      }),
    })
    .post(
      '/query',
      ({ body, request }) => service.query(resolveAuthenticatedApiKey(request), body),
      {
        detail: protectedRouteDetail({
          tags: [analyticsTag],
          summary: 'Run guarded AI analytics SQL',
          description:
            'Executes generated SQL against the curated finalized Dogecoin analytics surface after validation, preflight, rate limiting, and ClickHouse resource limits.',
        }),
        body: analyticsQueryBodySchema,
      },
    );
}
