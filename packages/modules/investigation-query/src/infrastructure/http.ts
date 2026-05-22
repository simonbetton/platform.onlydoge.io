import { protectedRouteDetail } from '@onlydoge/access-control';
import { Elysia, t } from 'elysia';

import type { InvestigationQueryService } from '../application/investigation-query-service';

const healthTag = 'Health';
const investigationTag = 'Investigation';

const infoOperationDetail = {
  ...protectedRouteDetail({
    tags: [investigationTag],
    summary: 'Investigate address or entity',
  }),
  description:
    'Looks up a Dogecoin address or an entity id and returns known balances, linked sources, tags, risk summary, and related metadata. Example searches: `?q=DTestAddress123`, `?q=ent_exampleentity`.',
  responses: {
    200: {
      description: 'Investigation result for the supplied address or entity.',
      content: {
        'application/json': {
          examples: {
            addressLookup: {
              summary: 'Address lookup with entity and risk context',
              value: {
                addresses: ['DTestAddress123'],
                risk: {
                  level: 'high',
                  reasons: ['entity', 'source'],
                },
                assets: [
                  {
                    network: 'net_dogecoin',
                    balance: '2500000000',
                  },
                ],
                tokens: [],
                sources: [
                  {
                    network: 'net_dogecoin',
                    entity: 'ent_sourceentity',
                    from: 'DSourceWallet456',
                    to: 'DTestAddress123',
                    hops: 2,
                  },
                ],
                networks: [
                  {
                    id: 'net_dogecoin',
                    name: 'Dogecoin Mainnet',
                    chainId: 0,
                  },
                ],
                entities: [
                  {
                    id: 'ent_exampleentity',
                    name: 'Example Entity',
                    description: 'Tracked counterparty',
                    data: {},
                    tags: ['tag_sanctions'],
                  },
                ],
                tags: [
                  {
                    id: 'tag_sanctions',
                    name: 'Sanctions',
                    riskLevel: 'high',
                  },
                ],
              },
            },
          },
        },
      },
    },
    400: {
      description: 'The query string is missing.',
      content: {
        'application/json': {
          examples: {
            missingQuery: {
              summary: 'Missing q parameter',
              value: {
                error: 'missing input params',
              },
            },
          },
        },
      },
    },
  },
};

export function buildInvestigationQueryHttp(service: InvestigationQueryService) {
  return new Elysia()
    .use(
      new Elysia({ prefix: '/v1/heartbeat' }).get(
        '/',
        async () => {
          await service.heartbeat();
          return new Response(null, {
            status: 204,
            headers: {
              'cache-control': 'no-store',
            },
          });
        },
        {
          detail: {
            tags: [healthTag],
            summary: 'Heartbeat',
            description:
              'Public lightweight health check for API dependencies used by uptime probes.',
            responses: {
              204: {
                description: 'API dependencies are reachable.',
              },
            },
          },
        },
      ),
    )
    .use(
      new Elysia({ prefix: '/v1/stats' }).get('/', () => service.stats(), {
        detail: protectedRouteDetail({
          tags: [investigationTag],
          summary: 'Get indexing stats',
          description:
            'Returns per-network indexing progress, online-tip visibility, stage, and latest error information.',
        }),
      }),
    )
    .use(
      new Elysia({ prefix: '/v1/info' }).get('/', ({ query }) => service.info(query.q), {
        detail: infoOperationDetail,
        query: t.Object({
          q: t.Optional(
            t.String({
              description:
                'Address or entity id to investigate, for example `DTestAddress123` or `ent_exampleentity`.',
              examples: ['DTestAddress123', 'ent_exampleentity'],
            }),
          ),
        }),
      }),
    );
}
