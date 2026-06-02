import { type AuthenticatedApiKeyResolver, protectedRouteDetail } from '@onlydoge/access-control';
import { parseNonNegativeInteger } from '@onlydoge/shared-kernel';
import { Elysia, t } from 'elysia';

import type { ExplorerQueryService } from '../application/explorer-query-service';

const explorerTag = 'Explorer';

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
    .get(
      '/search',
      ({ query, request }) => {
        resolveAuthenticatedApiKey(request);
        return service.search(query.q);
      },
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
        }),
      },
    )
    .get(
      '/blocks',
      ({ query }) =>
        service.listBlocks(
          parseNonNegativeInteger(query.offset),
          parseNonNegativeInteger(query.limit),
        ),
      {
        detail: describeProtected(
          'List blocks',
          'Lists recent raw-synced Dogecoin blocks in descending chain order.',
        ),
        query: paginationQuerySchema,
      },
    )
    .get(
      '/mempool',
      ({ query }) => {
        const { limit, offset } = readPagination(query);
        return service.listMempool(offset, limit);
      },
      {
        detail: describeProtected(
          'List mempool transactions',
          'Returns a bounded realtime snapshot of the Dogecoin node mempool, ordered by newest node-reported entry time first.',
        ),
        query: paginationQuerySchema,
      },
    )
    .get('/blocks/:ref', ({ params }) => service.getBlock(params.ref), {
      detail: describeProtected(
        'Get block',
        'Returns a block summary and transaction summaries by raw-synced height or indexed block hash.',
      ),
      params: t.Object({
        ref: t.String({
          description: 'Block height or block hash.',
          examples: ['123456', '0000000000000000000000000000000000000000000000000000000000000000'],
        }),
      }),
    })
    .get(
      '/transactions/:txid',
      ({ params, request }) => {
        resolveAuthenticatedApiKey(request);
        return service.getTransaction(params.txid);
      },
      {
        detail: describeProtected(
          'Get transaction',
          'Returns a Dogecoin transaction with inputs, outputs, fees, and spend status.',
        ),
        params: t.Object({
          txid: t.String({
            description: 'Dogecoin transaction id.',
            examples: ['doge-tx-2'],
          }),
        }),
      },
    )
    .get(
      '/addresses/:address',
      ({ params, request }) => {
        resolveAuthenticatedApiKey(request);
        return service.getAddress(params.address);
      },
      {
        detail: describeProtected('Get address', 'Returns a canonical address balance summary.'),
        params: addressParamsSchema,
      },
    )
    .get(
      '/addresses/:address/transactions',
      ({ params, query }) => {
        const { limit, offset } = readPagination(query);
        return service.listAddressTransactions(params.address, offset, limit);
      },
      {
        detail: describeProtected(
          'List address transactions',
          'Returns reverse-chronological transaction history for a Dogecoin address.',
        ),
        params: addressParamsSchema,
        query: paginationQuerySchema,
      },
    )
    .get(
      '/addresses/:address/utxos',
      ({ params, query }) => {
        const { limit, offset } = readPagination(query);
        return service.listAddressUtxos(params.address, offset, limit);
      },
      {
        detail: describeProtected(
          'List address UTXOs',
          'Returns current spendable UTXOs for a Dogecoin address.',
        ),
        params: addressParamsSchema,
        query: paginationQuerySchema,
      },
    );
}
