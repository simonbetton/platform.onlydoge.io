import { protectedOperationDetail } from '@onlydoge/access-control';
import { parseNonNegativeInteger } from '@onlydoge/shared-kernel';
import { Elysia, t } from 'elysia';

import type { ExplorerQueryService } from '../application/explorer-query-service';

const networkQuerySchema = t.Object({
  network: t.Optional(t.String()),
});

const paginatedNetworkQuerySchema = t.Object({
  network: t.Optional(t.String()),
  offset: t.Optional(t.String()),
  limit: t.Optional(t.String()),
});

const addressParamsSchema = t.Object({
  address: t.String(),
});

function readPagination(query: { limit?: string; offset?: string }) {
  return {
    offset: parseNonNegativeInteger(query.offset),
    limit: parseNonNegativeInteger(query.limit),
  };
}

export function buildExplorerQueryHttp(service: ExplorerQueryService) {
  const describeProtected = (description: string) => ({
    ...protectedOperationDetail,
    description,
  });

  return new Elysia({ prefix: '/v1/explorer' })
    .get('/networks', () => service.listNetworks(), {
      detail: describeProtected('Lists Dogecoin explorer networks and their indexing status.'),
    })
    .get('/search', ({ query }) => service.search(query.q, query.network), {
      detail: describeProtected(
        'Searches the indexed Dogecoin explorer by block height, block hash, txid, or address.',
      ),
      query: t.Object({
        q: t.Optional(t.String()),
        network: t.Optional(t.String()),
      }),
    })
    .get(
      '/blocks',
      ({ query }) =>
        service.listBlocks(
          query.network,
          parseNonNegativeInteger(query.offset),
          parseNonNegativeInteger(query.limit),
        ),
      {
        detail: describeProtected('Lists recent indexed Dogecoin blocks.'),
        query: paginatedNetworkQuerySchema,
      },
    )
    .get('/blocks/:ref', ({ params, query }) => service.getBlock(params.ref, query.network), {
      detail: describeProtected('Returns a block by indexed height or block hash.'),
      params: t.Object({
        ref: t.String(),
      }),
      query: networkQuerySchema,
    })
    .get(
      '/transactions/:txid',
      ({ params, query }) => service.getTransaction(params.txid, query.network),
      {
        detail: describeProtected(
          'Returns a Dogecoin transaction with inputs, outputs, and label overlays.',
        ),
        params: t.Object({
          txid: t.String(),
        }),
        query: networkQuerySchema,
      },
    )
    .get(
      '/addresses/:address',
      ({ params, query }) => service.getAddress(params.address, query.network),
      {
        detail: describeProtected('Returns an address summary with investigation overlay data.'),
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
        detail: describeProtected('Returns current spendable UTXOs for a Dogecoin address.'),
        params: addressParamsSchema,
        query: paginatedNetworkQuerySchema,
      },
    )
    .post('/hd-wallet/balance', ({ body }) => service.getHdWalletBalance(body), {
      detail: describeProtected(
        'Scans a Dogecoin account xpub receive/change chains and returns aggregate HD wallet balance.',
      ),
      body: t.Object({
        xpub: t.String(),
        network: t.Optional(t.String()),
        chains: t.Optional(t.Array(t.Number())),
        gapLimit: t.Optional(t.Number()),
      }),
    });
}
