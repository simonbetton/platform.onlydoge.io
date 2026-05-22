import { type AuthenticatedApiKeyResolver, protectedRouteDetail } from '@onlydoge/access-control';
import { parseNonNegativeInteger } from '@onlydoge/shared-kernel';
import { Elysia, t } from 'elysia';

import type { ExplorerQueryService } from '../application/explorer-query-service';

const explorerTag = 'Explorer';

const networkQuerySchema = t.Object({
  network: t.Optional(
    t.String({
      description:
        'Network id. Omit when a single Dogecoin network is configured and should be used as the default.',
      examples: ['net_dogecoin'],
    }),
  ),
});

const paginatedNetworkQuerySchema = t.Object({
  network: t.Optional(
    t.String({
      description:
        'Network id. Omit when a single Dogecoin network is configured and should be used as the default.',
      examples: ['net_dogecoin'],
    }),
  ),
  offset: t.Optional(
    t.String({
      description: 'Zero-based number of records to skip.',
      examples: ['0', '25'],
    }),
  ),
  limit: t.Optional(
    t.String({
      description: 'Maximum number of records to return.',
      examples: ['20', '50'],
    }),
  ),
});

const addressParamsSchema = t.Object({
  address: t.String({
    description: 'Dogecoin address.',
    examples: ['DTestAddress123'],
  }),
});

function readPagination(query: { limit?: string; offset?: string }) {
  return {
    offset: parseNonNegativeInteger(query.offset),
    limit: parseNonNegativeInteger(query.limit),
  };
}

export function buildExplorerQueryHttp(
  service: ExplorerQueryService,
  resolveAuthenticatedApiKey: AuthenticatedApiKeyResolver,
) {
  const describeProtected = (summary: string, description: string) =>
    protectedRouteDetail({
      tags: [explorerTag],
      summary,
      description,
    });

  return new Elysia({ prefix: '/v1/explorer' })
    .get('/networks', () => service.listNetworks(), {
      detail: describeProtected(
        'List explorer networks',
        'Lists Dogecoin explorer networks and their indexing status. Use this first to discover the default network id.',
      ),
    })
    .get(
      '/search',
      ({ query, request }) =>
        service.search(resolveAuthenticatedApiKey(request), query.q, query.network),
      {
        detail: describeProtected(
          'Search explorer',
          'Searches the indexed Dogecoin explorer by block height, block hash, txid, or address.',
        ),
        query: t.Object({
          q: t.Optional(
            t.String({
              description: 'Block height, block hash, transaction id, or Dogecoin address.',
              examples: ['123456', 'DTestAddress123'],
            }),
          ),
          network: t.Optional(
            t.String({
              description:
                'Network id. Omit when a single Dogecoin network is configured and should be used as the default.',
              examples: ['net_dogecoin'],
            }),
          ),
        }),
      },
    )
    .get(
      '/blocks',
      ({ query }) =>
        service.listBlocks(
          query.network,
          parseNonNegativeInteger(query.offset),
          parseNonNegativeInteger(query.limit),
        ),
      {
        detail: describeProtected(
          'List blocks',
          'Lists recent indexed Dogecoin blocks in descending chain order.',
        ),
        query: paginatedNetworkQuerySchema,
      },
    )
    .get(
      '/mempool',
      ({ query }) => {
        const { limit, offset } = readPagination(query);
        return service.listMempool(query.network, offset, limit);
      },
      {
        detail: describeProtected(
          'List mempool transactions',
          'Returns a bounded realtime snapshot of the Dogecoin node mempool, ordered by newest node-reported entry time first.',
        ),
        query: paginatedNetworkQuerySchema,
      },
    )
    .get('/blocks/:ref', ({ params, query }) => service.getBlock(params.ref, query.network), {
      detail: describeProtected(
        'Get block',
        'Returns a block summary and transaction summaries by indexed height or block hash.',
      ),
      params: t.Object({
        ref: t.String({
          description: 'Block height or block hash.',
          examples: ['123456', '0000000000000000000000000000000000000000000000000000000000000000'],
        }),
      }),
      query: networkQuerySchema,
    })
    .get(
      '/transactions/:txid',
      ({ params, query, request }) =>
        service.getTransaction(resolveAuthenticatedApiKey(request), params.txid, query.network),
      {
        detail: describeProtected(
          'Get transaction',
          'Returns a Dogecoin transaction with inputs, outputs, and label overlays.',
        ),
        params: t.Object({
          txid: t.String({
            description: 'Dogecoin transaction id.',
            examples: ['doge-tx-2'],
          }),
        }),
        query: networkQuerySchema,
      },
    )
    .get(
      '/addresses/:address',
      ({ params, query, request }) =>
        service.getAddress(resolveAuthenticatedApiKey(request), params.address, query.network),
      {
        detail: describeProtected(
          'Get address',
          'Returns an address balance summary with investigation overlay data.',
        ),
        params: addressParamsSchema,
        query: networkQuerySchema,
      },
    )
    .get(
      '/addresses/:address/transactions',
      ({ params, query }) => {
        const { limit, offset } = readPagination(query);
        return service.listAddressTransactions(params.address, query.network, offset, limit);
      },
      {
        detail: describeProtected(
          'List address transactions',
          'Returns reverse-chronological transaction history for a Dogecoin address.',
        ),
        params: addressParamsSchema,
        query: paginatedNetworkQuerySchema,
      },
    )
    .get(
      '/addresses/:address/utxos',
      ({ params, query }) => {
        const { limit, offset } = readPagination(query);
        return service.listAddressUtxos(params.address, query.network, offset, limit);
      },
      {
        detail: describeProtected(
          'List address UTXOs',
          'Returns current spendable UTXOs for a Dogecoin address.',
        ),
        params: addressParamsSchema,
        query: paginatedNetworkQuerySchema,
      },
    );
}
