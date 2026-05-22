import { type AuthenticatedApiKeyResolver, protectedRouteDetail } from '@onlydoge/access-control';
import { parseNonNegativeInteger } from '@onlydoge/shared-kernel';
import { Elysia, t } from 'elysia';

import type { NetworkCatalogService } from '../application/network-catalog-service';

const networkCatalogTag = 'Network Catalog';

const paginationQuerySchema = t.Object({
  offset: t.Optional(
    t.String({
      description: 'Zero-based number of records to skip.',
      examples: ['0', '25'],
    }),
  ),
  limit: t.Optional(
    t.String({
      description: 'Maximum number of records to return.',
      examples: ['25', '100'],
    }),
  ),
});

const networkIdParamSchema = t.Object({
  id: t.String({
    description: 'Network id.',
    examples: ['net_dogecoin'],
  }),
});

const tokenIdParamSchema = t.Object({
  id: t.String({
    description: 'Token id for a currency such as DOGE.',
    examples: ['tok_doge'],
  }),
});

const networkCreateBodySchema = t.Object({
  id: t.Optional(
    t.String({
      description: 'Optional network id. If omitted, OnlyDoge generates a `net_` id.',
      examples: ['net_dogecoin'],
    }),
  ),
  name: t.String({
    description: 'Human-readable network name.',
    examples: ['Dogecoin Mainnet'],
  }),
  architecture: t.Literal('dogecoin', {
    description: 'Network family. Only Dogecoin is currently supported.',
  }),
  blockTime: t.Number({
    description: 'Expected block interval in seconds.',
    examples: [60],
  }),
  rpcEndpoint: t.String({
    description:
      'Dogecoin Core JSON-RPC endpoint. Credentials are accepted in the URL but are masked in responses and logs.',
    examples: ['https://user:pass@doge.example/rpc'],
  }),
  chainId: t.Optional(
    t.Number({
      description: 'Chain id namespace for this network.',
      examples: [0],
    }),
  ),
  rps: t.Optional(
    t.Number({
      description: 'RPC request-per-second budget for indexing work.',
      examples: [100],
    }),
  ),
  zmqBlockEndpoint: t.Optional(
    t.Nullable(
      t.String({
        description: 'Optional Dogecoin Core ZMQ block endpoint.',
        examples: ['tcp://127.0.0.1:28332'],
      }),
    ),
  ),
});

const networkUpdateBodySchema = t.Object({
  name: t.Optional(
    t.String({
      description: 'Human-readable network name.',
      examples: ['Dogecoin Mainnet'],
    }),
  ),
  architecture: t.Optional(
    t.Literal('dogecoin', {
      description: 'Network family. Only Dogecoin is currently supported.',
    }),
  ),
  blockTime: t.Optional(
    t.Number({
      description: 'Expected block interval in seconds.',
      examples: [60],
    }),
  ),
  rpcEndpoint: t.Optional(
    t.String({
      description:
        'Dogecoin Core JSON-RPC endpoint. Credentials are accepted in the URL but are masked in responses and logs.',
      examples: ['https://user:pass@doge.example/rpc'],
    }),
  ),
  chainId: t.Optional(
    t.Number({
      description: 'Chain id namespace for this network.',
      examples: [0],
    }),
  ),
  rps: t.Optional(
    t.Number({
      description: 'RPC request-per-second budget for indexing work.',
      examples: [100],
    }),
  ),
  zmqBlockEndpoint: t.Optional(
    t.Nullable(
      t.String({
        description: 'Optional Dogecoin Core ZMQ block endpoint.',
        examples: ['tcp://127.0.0.1:28332'],
      }),
    ),
  ),
});

const tokenCreateBodySchema = t.Object({
  id: t.Optional(
    t.String({
      description: 'Optional token id for a currency. If omitted, OnlyDoge generates a `tok_` id.',
      examples: ['tok_doge'],
    }),
  ),
  network: t.String({
    description: 'Network id this token belongs to.',
    examples: ['net_dogecoin'],
  }),
  name: t.String({
    description: 'Token display name.',
    examples: ['Dogecoin'],
  }),
  symbol: t.String({
    description: 'Token ticker or short symbol.',
    examples: ['DOGE'],
  }),
  address: t.Optional(
    t.String({
      description:
        'Token contract or asset address when applicable. Native currencies such as DOGE can omit it.',
      examples: ['DTokenAddress123'],
    }),
  ),
  decimals: t.Number({
    description: 'Token decimal precision.',
    examples: [8],
  }),
});

export function buildNetworkCatalogHttp(
  service: NetworkCatalogService,
  resolveAuthenticatedApiKey: AuthenticatedApiKeyResolver,
) {
  return new Elysia()
    .use(
      new Elysia({ prefix: '/v1/networks' })
        .post(
          '/',
          async ({ body, request }) =>
            service.createNetwork(resolveAuthenticatedApiKey(request), body),
          {
            detail: protectedRouteDetail({
              tags: [networkCatalogTag],
              summary: 'Create network',
              description:
                'Registers a Dogecoin network for indexing. The RPC endpoint is health-checked before the network is saved.',
            }),
            body: networkCreateBodySchema,
          },
        )
        .get(
          '/',
          async ({ query }) =>
            service.listNetworks(
              parseNonNegativeInteger(query.offset),
              parseNonNegativeInteger(query.limit),
            ),
          {
            detail: protectedRouteDetail({
              tags: [networkCatalogTag],
              summary: 'List networks',
              description: 'Lists active configured networks.',
            }),
            query: paginationQuerySchema,
          },
        )
        .get('/:id', async ({ params }) => service.getNetwork(params.id), {
          detail: protectedRouteDetail({
            tags: [networkCatalogTag],
            summary: 'Get network',
            description: 'Returns one active network by id.',
          }),
          params: networkIdParamSchema,
        })
        .put(
          '/:id',
          async ({ params, body, request }) => {
            await service.updateNetwork(resolveAuthenticatedApiKey(request), params.id, body);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedRouteDetail({
              tags: [networkCatalogTag],
              summary: 'Update network',
              description:
                'Updates mutable network settings. Updated RPC endpoints are health-checked before saving.',
              responses: {
                204: {
                  description: 'Network updated.',
                },
              },
            }),
            params: networkIdParamSchema,
            body: networkUpdateBodySchema,
          },
        )
        .delete(
          '/',
          async ({ body, request }) => {
            await service.deleteNetworks(resolveAuthenticatedApiKey(request), body.networks);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedRouteDetail({
              tags: [networkCatalogTag],
              summary: 'Delete networks',
              description: 'Soft-deletes one or more networks and their associated address labels.',
              responses: {
                204: {
                  description: 'Networks deleted.',
                },
              },
            }),
            body: t.Object({
              networks: t.Array(
                t.String({
                  description: 'Network id.',
                  examples: ['net_dogecoin'],
                }),
                {
                  description: 'Network ids to delete.',
                },
              ),
            }),
          },
        ),
    )
    .use(
      new Elysia({ prefix: '/v1/tokens' })
        .post(
          '/',
          async ({ body, request }) =>
            service.createToken(resolveAuthenticatedApiKey(request), body),
          {
            detail: protectedRouteDetail({
              tags: [networkCatalogTag],
              summary: 'Create token',
              description: 'Registers currency token metadata for an existing network.',
            }),
            body: tokenCreateBodySchema,
          },
        )
        .get(
          '/',
          async ({ query }) =>
            service.listTokens(
              parseNonNegativeInteger(query.offset),
              parseNonNegativeInteger(query.limit),
            ),
          {
            detail: protectedRouteDetail({
              tags: [networkCatalogTag],
              summary: 'List tokens',
              description: 'Lists currency token metadata with related network summaries.',
            }),
            query: paginationQuerySchema,
          },
        )
        .get('/:id', async ({ params }) => service.getToken(params.id), {
          detail: protectedRouteDetail({
            tags: [networkCatalogTag],
            summary: 'Get token',
            description: 'Returns currency token metadata and its related network summary.',
          }),
          params: tokenIdParamSchema,
        })
        .delete(
          '/',
          async ({ body, request }) => {
            await service.deleteTokens(resolveAuthenticatedApiKey(request), body.tokens);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedRouteDetail({
              tags: [networkCatalogTag],
              summary: 'Delete tokens',
              description: 'Deletes one or more currency token metadata records.',
              responses: {
                204: {
                  description: 'Tokens deleted.',
                },
              },
            }),
            body: t.Object({
              tokens: t.Array(
                t.String({
                  description: 'Token id for a currency such as DOGE.',
                  examples: ['tok_doge'],
                }),
                {
                  description: 'Token ids to delete.',
                },
              ),
            }),
          },
        ),
    );
}
