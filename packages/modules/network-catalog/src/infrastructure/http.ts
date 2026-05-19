import { protectedOperationDetail } from '@onlydoge/access-control';
import { parseNonNegativeInteger } from '@onlydoge/shared-kernel';
import { Elysia, t } from 'elysia';

import type { NetworkCatalogService } from '../application/network-catalog-service';

export function buildNetworkCatalogHttp(service: NetworkCatalogService) {
  return new Elysia()
    .use(
      new Elysia({ prefix: '/v1/networks' })
        .post('/', async ({ body }) => service.createNetwork(body), {
          detail: protectedOperationDetail,
          body: t.Object({
            id: t.Optional(t.String()),
            name: t.String(),
            architecture: t.Literal('dogecoin'),
            blockTime: t.Number(),
            rpcEndpoint: t.String(),
            chainId: t.Optional(t.Number()),
            rps: t.Optional(t.Number()),
            zmqBlockEndpoint: t.Optional(t.Nullable(t.String())),
          }),
        })
        .get(
          '/',
          async ({ query }) =>
            service.listNetworks(
              parseNonNegativeInteger(query.offset),
              parseNonNegativeInteger(query.limit),
            ),
          {
            detail: protectedOperationDetail,
          },
        )
        .get('/:id', async ({ params }) => service.getNetwork(params.id), {
          detail: protectedOperationDetail,
        })
        .put(
          '/:id',
          async ({ params, body }) => {
            await service.updateNetwork(params.id, body);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedOperationDetail,
            body: t.Object({
              name: t.Optional(t.String()),
              architecture: t.Optional(t.Literal('dogecoin')),
              blockTime: t.Optional(t.Number()),
              rpcEndpoint: t.Optional(t.String()),
              chainId: t.Optional(t.Number()),
              rps: t.Optional(t.Number()),
              zmqBlockEndpoint: t.Optional(t.Nullable(t.String())),
            }),
          },
        )
        .delete(
          '/',
          async ({ body }) => {
            await service.deleteNetworks(body.networks);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedOperationDetail,
            body: t.Object({
              networks: t.Array(t.String()),
            }),
          },
        ),
    )
    .use(
      new Elysia({ prefix: '/v1/tokens' })
        .post('/', async ({ body }) => service.createToken(body), {
          detail: protectedOperationDetail,
          body: t.Object({
            id: t.Optional(t.String()),
            network: t.String(),
            name: t.String(),
            symbol: t.String(),
            address: t.Optional(t.String()),
            decimals: t.Number(),
          }),
        })
        .get(
          '/',
          async ({ query }) =>
            service.listTokens(
              parseNonNegativeInteger(query.offset),
              parseNonNegativeInteger(query.limit),
            ),
          {
            detail: protectedOperationDetail,
          },
        )
        .get('/:id', async ({ params }) => service.getToken(params.id), {
          detail: protectedOperationDetail,
        })
        .delete(
          '/',
          async ({ body }) => {
            await service.deleteTokens(body.tokens);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedOperationDetail,
            body: t.Object({
              tokens: t.Array(t.String()),
            }),
          },
        ),
    );
}
