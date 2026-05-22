import { type AuthenticatedApiKeyResolver, protectedRouteDetail } from '@onlydoge/access-control';
import { Elysia, t } from 'elysia';

import type { EntityLabelingService } from '../application/entity-labeling-service';

function parsePagination(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }

  const parsed = Number(value);
  return paginationValue(parsed);
}

function isPaginationValue(value: number): boolean {
  return [Number.isInteger(value), value >= 0].every(Boolean);
}

function paginationValue(value: number): number | undefined {
  if (!isPaginationValue(value)) {
    return undefined;
  }

  return value;
}

const entityLabelingTag = 'Entity Labeling';

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

const entityIdParamSchema = t.Object({
  id: t.String({
    description: 'Entity id.',
    examples: ['ent_exchange'],
  }),
});

const addressIdParamSchema = t.Object({
  id: t.String({
    description: 'Address label id.',
    examples: ['adr_hot_wallet'],
  }),
});

const tagIdParamSchema = t.Object({
  id: t.String({
    description: 'Tag id.',
    examples: ['tag_sanctions'],
  }),
});

const dataSchema = t.Optional(
  t.Record(t.String(), t.Any(), {
    description: 'Free-form metadata for the record.',
    examples: [{ source: 'case-note-123' }],
  }),
);

const riskLevelSchema = t.Union([t.Literal('low'), t.Literal('high')], {
  description: 'Risk severity applied by this tag.',
  examples: ['high'],
});

export function buildEntityLabelingHttp(
  service: EntityLabelingService,
  resolveAuthenticatedApiKey: AuthenticatedApiKeyResolver,
) {
  return new Elysia()
    .use(
      new Elysia({ prefix: '/v1/entities' })
        .post(
          '/',
          async ({ body, request }) =>
            service.createEntity(resolveAuthenticatedApiKey(request), body),
          {
            detail: protectedRouteDetail({
              tags: [entityLabelingTag],
              summary: 'Create entity',
              description:
                'Creates an investigation entity and optionally assigns existing risk tags.',
            }),
            body: t.Object({
              id: t.Optional(
                t.String({
                  description: 'Optional entity id. If omitted, OnlyDoge generates an `ent_` id.',
                  examples: ['ent_exchange'],
                }),
              ),
              name: t.Optional(
                t.Nullable(
                  t.String({
                    description: 'Optional display name.',
                    examples: ['Example Exchange'],
                  }),
                ),
              ),
              description: t.String({
                description: 'Investigator-facing entity description.',
                examples: ['Exchange cluster from internal case notes.'],
              }),
              data: dataSchema,
              tags: t.Optional(
                t.Array(
                  t.String({
                    description: 'Tag id.',
                    examples: ['tag_sanctions'],
                  }),
                  {
                    description: 'Existing tags to assign to the entity.',
                  },
                ),
              ),
            }),
          },
        )
        .get(
          '/',
          async ({ query, request }) =>
            service.listEntities(
              resolveAuthenticatedApiKey(request),
              parsePagination(query.offset),
              parsePagination(query.limit),
            ),
          {
            detail: protectedRouteDetail({
              tags: [entityLabelingTag],
              summary: 'List entities',
              description:
                'Lists active investigation entities with related tag, address, and network context.',
            }),
            query: paginationQuerySchema,
          },
        )
        .get(
          '/:id',
          async ({ params, request }) =>
            service.getEntity(resolveAuthenticatedApiKey(request), params.id),
          {
            detail: protectedRouteDetail({
              tags: [entityLabelingTag],
              summary: 'Get entity',
              description:
                'Returns one entity with related tags, addresses, and network summaries.',
            }),
            params: entityIdParamSchema,
          },
        )
        .put(
          '/:id',
          async ({ params, body, request }) => {
            await service.updateEntity(resolveAuthenticatedApiKey(request), params.id, body);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedRouteDetail({
              tags: [entityLabelingTag],
              summary: 'Update entity',
              description: 'Updates entity metadata and optionally replaces its assigned tag list.',
              responses: {
                204: {
                  description: 'Entity updated.',
                },
              },
            }),
            params: entityIdParamSchema,
            body: t.Object({
              name: t.Optional(
                t.Nullable(
                  t.String({
                    description: 'Optional display name. Send null to clear it.',
                    examples: ['Example Exchange'],
                  }),
                ),
              ),
              description: t.Optional(
                t.String({
                  description: 'Investigator-facing entity description.',
                  examples: ['Updated exchange cluster from internal case notes.'],
                }),
              ),
              data: dataSchema,
              tags: t.Optional(
                t.Array(
                  t.String({
                    description: 'Tag id.',
                    examples: ['tag_sanctions'],
                  }),
                  {
                    description: 'Replacement list of tag ids assigned to the entity.',
                  },
                ),
              ),
            }),
          },
        )
        .delete(
          '/',
          async ({ body, request }) => {
            await service.deleteEntities(resolveAuthenticatedApiKey(request), body.entities);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedRouteDetail({
              tags: [entityLabelingTag],
              summary: 'Delete entities',
              description: 'Soft-deletes one or more entities and their associated address labels.',
              responses: {
                204: {
                  description: 'Entities deleted.',
                },
              },
            }),
            body: t.Object({
              entities: t.Array(
                t.String({
                  description: 'Entity id.',
                  examples: ['ent_exchange'],
                }),
                {
                  description: 'Entity ids to delete.',
                },
              ),
            }),
          },
        ),
    )
    .use(
      new Elysia({ prefix: '/v1/addresses' })
        .post(
          '/',
          async ({ body, request }) =>
            service.createAddresses(resolveAuthenticatedApiKey(request), body),
          {
            detail: protectedRouteDetail({
              tags: [entityLabelingTag],
              summary: 'Create address labels',
              description:
                'Labels one or more Dogecoin addresses for an existing entity on an existing network.',
            }),
            body: t.Object({
              entity: t.String({
                description: 'Entity id that owns these address labels.',
                examples: ['ent_exchange'],
              }),
              network: t.String({
                description: 'Network id for the address labels.',
                examples: ['net_dogecoin'],
              }),
              addresses: t.Array(
                t.Object({
                  address: t.String({
                    description: 'Dogecoin address to label.',
                    examples: ['DTestAddress123'],
                  }),
                  description: t.String({
                    description: 'Investigator-facing address description.',
                    examples: ['Hot wallet observed in case note 123.'],
                  }),
                  data: dataSchema,
                }),
                {
                  description: 'Address labels to create.',
                },
              ),
            }),
          },
        )
        .get(
          '/',
          async ({ query, request }) =>
            service.listAddresses(
              resolveAuthenticatedApiKey(request),
              parsePagination(query.offset),
              parsePagination(query.limit),
            ),
          {
            detail: protectedRouteDetail({
              tags: [entityLabelingTag],
              summary: 'List address labels',
              description: 'Lists active address labels with related network summaries.',
            }),
            query: paginationQuerySchema,
          },
        )
        .get(
          '/:id',
          async ({ params, request }) =>
            service.getAddress(resolveAuthenticatedApiKey(request), params.id),
          {
            detail: protectedRouteDetail({
              tags: [entityLabelingTag],
              summary: 'Get address label',
              description: 'Returns one address label and its related network summary.',
            }),
            params: addressIdParamSchema,
          },
        )
        .delete(
          '/',
          async ({ body, request }) => {
            await service.deleteAddresses(resolveAuthenticatedApiKey(request), body.addresses);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedRouteDetail({
              tags: [entityLabelingTag],
              summary: 'Delete address labels',
              description: 'Soft-deletes one or more address labels by id.',
              responses: {
                204: {
                  description: 'Address labels deleted.',
                },
              },
            }),
            body: t.Object({
              addresses: t.Array(
                t.String({
                  description: 'Address label id.',
                  examples: ['adr_hot_wallet'],
                }),
                {
                  description: 'Address label ids to delete.',
                },
              ),
            }),
          },
        ),
    )
    .use(
      new Elysia({ prefix: '/v1/tags' })
        .post(
          '/',
          async ({ body, request }) => service.createTag(resolveAuthenticatedApiKey(request), body),
          {
            detail: protectedRouteDetail({
              tags: [entityLabelingTag],
              summary: 'Create tag',
              description:
                'Creates a reusable risk tag that can be assigned to investigation entities.',
            }),
            body: t.Object({
              id: t.Optional(
                t.String({
                  description: 'Optional tag id. If omitted, OnlyDoge generates a `tag_` id.',
                  examples: ['tag_sanctions'],
                }),
              ),
              name: t.String({
                description: 'Tag display name.',
                examples: ['Sanctions'],
              }),
              riskLevel: riskLevelSchema,
            }),
          },
        )
        .get(
          '/',
          async ({ query, request }) =>
            service.listTags(
              resolveAuthenticatedApiKey(request),
              parsePagination(query.offset),
              parsePagination(query.limit),
            ),
          {
            detail: protectedRouteDetail({
              tags: [entityLabelingTag],
              summary: 'List tags',
              description:
                'Lists tags with related entities, address labels, and network summaries.',
            }),
            query: paginationQuerySchema,
          },
        )
        .get(
          '/:id',
          async ({ params, request }) =>
            service.getTag(resolveAuthenticatedApiKey(request), params.id),
          {
            detail: protectedRouteDetail({
              tags: [entityLabelingTag],
              summary: 'Get tag',
              description:
                'Returns one risk tag with related entities, address labels, and network summaries.',
            }),
            params: tagIdParamSchema,
          },
        )
        .put(
          '/:id',
          async ({ params, body, request }) => {
            await service.updateTag(resolveAuthenticatedApiKey(request), params.id, body);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedRouteDetail({
              tags: [entityLabelingTag],
              summary: 'Update tag',
              description: 'Updates the display name or risk level for a tag.',
              responses: {
                204: {
                  description: 'Tag updated.',
                },
              },
            }),
            params: tagIdParamSchema,
            body: t.Object({
              name: t.Optional(
                t.String({
                  description: 'Tag display name.',
                  examples: ['Sanctions'],
                }),
              ),
              riskLevel: t.Optional(riskLevelSchema),
            }),
          },
        )
        .delete(
          '/',
          async ({ body, request }) => {
            await service.deleteTags(resolveAuthenticatedApiKey(request), body.tags);
            return new Response(null, { status: 204 });
          },
          {
            detail: protectedRouteDetail({
              tags: [entityLabelingTag],
              summary: 'Delete tags',
              description: 'Deletes one or more tags by id.',
              responses: {
                204: {
                  description: 'Tags deleted.',
                },
              },
            }),
            body: t.Object({
              tags: t.Array(
                t.String({
                  description: 'Tag id.',
                  examples: ['tag_sanctions'],
                }),
                {
                  description: 'Tag ids to delete.',
                },
              ),
            }),
          },
        ),
    );
}
