import { type AuthenticatedApiKeyResolver, protectedRouteDetail } from '@onlydoge/access-control';
import {
  defaultPageLimit,
  maxPageLimit,
  maxPageOffset,
  parseBoundedNonNegativeInteger,
} from '@onlydoge/shared-kernel';
import { Elysia, sse, t } from 'elysia';

import type { ExplorerQueryService } from '../application/explorer-query-service';
import type { ExplorerMempoolWatchPort } from '../contracts/ports';

const explorerTag = 'Explorer';

const paginationQuerySchema = t.Object({
  offset: t.Optional(
    t.String({
      description: `Zero-based number of records to skip. Maximum ${maxPageOffset}.`,
      examples: ['0', '25'],
    }),
  ),
  limit: t.Optional(
    t.String({
      description: `Maximum number of records to return. Defaults to ${defaultPageLimit}; maximum ${maxPageLimit}. Values above the maximum return a validation error.`,
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
    offset: parseBoundedNonNegativeInteger(query.offset, {
      defaultValue: 0,
      field: 'offset',
      maximum: maxPageOffset,
    }),
    limit: parseBoundedNonNegativeInteger(query.limit, {
      defaultValue: defaultPageLimit,
      field: 'limit',
      maximum: maxPageLimit,
    }),
  };
}

export function buildExplorerQueryHttp(
  service: ExplorerQueryService,
  resolveAuthenticatedApiKey: AuthenticatedApiKeyResolver,
  mempoolWatch?: ExplorerMempoolWatchPort,
) {
  const describeProtected = (summary: string, description: string) =>
    protectedRouteDetail({
      tags: [explorerTag],
      summary,
      description,
    });

  const app = new Elysia({ prefix: '/v1/explorer' })
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
      ({ query }) => {
        const { limit, offset } = readPagination(query);
        return service.listBlocks(offset, limit);
      },
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
    .get(
      '/blocks/:ref',
      ({ params, query }) => {
        const { limit, offset } = readPagination(query);
        return service.getBlock(params.ref, offset, limit);
      },
      {
        detail: describeProtected(
          'Get block',
          'Returns a block summary and a bounded page of transaction summaries by raw-synced height or indexed block hash. Transaction indexes remain relative to the full block.',
        ),
        params: t.Object({
          ref: t.String({
            description: 'Block height or block hash.',
            examples: [
              '123456',
              '0000000000000000000000000000000000000000000000000000000000000000',
            ],
          }),
        }),
        query: paginationQuerySchema,
      },
    )
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

  if (!mempoolWatch) {
    return app;
  }

  return app.get(
    '/mempool/watch',
    async function* ({ query, request, set }) {
      const actor = resolveAuthenticatedApiKey(request);
      set.headers['cache-control'] = 'no-store';
      set.headers['x-accel-buffering'] = 'no';

      const abort = new AbortController();
      request.signal.addEventListener(
        'abort',
        () => {
          abort.abort();
        },
        { once: true },
      );

      for await (const event of mempoolWatch.openSession({
        apiKeyId: actor.id,
        address: query.address,
        ...(query.minValueBase === undefined ? {} : { minValueBase: query.minValueBase }),
        signal: abort.signal,
      })) {
        if (event.event === 'comment') {
          yield sse({ data: event.data });
          continue;
        }

        yield sse({
          event: event.event,
          data: event.data,
        });
      }
    },
    {
      detail: describeProtected(
        'Watch mempool address',
        'Opens a one-shot SSE session for a single Dogecoin receive address. Emits mempool.watch.appeared on the first matching mempool output (or catch-up hit), otherwise mempool.watch.timeout after five minutes. Up to five concurrent sessions per API key.',
      ),
      query: t.Object({
        address: t.String({
          description: 'Dogecoin address to watch for receiving outputs in the mempool.',
          examples: ['DTestAddress123'],
        }),
        minValueBase: t.Optional(
          t.String({
            description:
              'Optional minimum sum of matching output values in base units (1 DOGE = 100000000).',
            examples: ['100000000'],
          }),
        ),
      }),
    },
  );
}
