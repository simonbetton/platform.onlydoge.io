import { OnlyDogeError, UnauthorizedError } from '@onlydoge/shared-kernel';
import { Elysia, t } from 'elysia';

import type { AccessControlService } from '../application/access-control-service';

function parsePagination(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }

  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new OnlyDogeError(`invalid parameter for \`${value}\`: ${value}`);
  }

  return parsed;
}

export const apiTokenSecuritySchemeName = 'ApiTokenAuth';
const apiTokenSecurityRequirement: Record<typeof apiTokenSecuritySchemeName, []> = {
  [apiTokenSecuritySchemeName]: [],
};
export const protectedOperationDetail = {
  security: [apiTokenSecurityRequirement],
};

export function protectedRouteDetail<T extends object>(detail?: T) {
  return {
    ...protectedOperationDetail,
    ...(detail ?? {}),
  };
}

const accessControlTag = 'Access Control';

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

export function buildAccessControlHttp(service: AccessControlService) {
  return new Elysia({ prefix: '/v1/keys' })
    .post('/', async ({ body }) => service.createKey(body), {
      detail: {
        tags: [accessControlTag],
        summary: 'Create API key',
        description:
          'Creates an API key and returns the plaintext secret once. The first key can be created without authentication; after one key exists this route also requires `x-api-token`.',
      },
      body: t.Object({
        id: t.Optional(
          t.String({
            description: 'Optional key id. If omitted, OnlyDoge generates a `key_` id.',
            examples: ['key_operator'],
          }),
        ),
      }),
    })
    .get(
      '/',
      async ({ query }) => {
        const offset = parsePagination(query.offset);
        const limit = parsePagination(query.limit);

        return service.listKeys(offset, limit);
      },
      {
        detail: protectedRouteDetail({
          tags: [accessControlTag],
          summary: 'List API keys',
          description:
            'Lists API key metadata. Plaintext key secrets are only returned by `POST /v1/keys` and are omitted here.',
        }),
        query: paginationQuerySchema,
      },
    )
    .get('/:id', async ({ params }) => service.getKey(params.id), {
      detail: protectedRouteDetail({
        tags: [accessControlTag],
        summary: 'Get API key',
        description: 'Returns metadata for one API key without exposing the plaintext secret.',
      }),
      params: t.Object({
        id: t.String({
          description: 'API key id.',
          examples: ['key_operator'],
        }),
      }),
    })
    .put(
      '/:id',
      async ({ params, body }) => {
        await service.updateKey(params.id, body);
        return new Response(null, { status: 204 });
      },
      {
        detail: protectedRouteDetail({
          tags: [accessControlTag],
          summary: 'Update API key',
          description:
            'Activates or deactivates an API key. Deactivated keys can no longer authenticate protected routes.',
          responses: {
            204: {
              description: 'API key updated.',
            },
          },
        }),
        params: t.Object({
          id: t.String({
            description: 'API key id.',
            examples: ['key_operator'],
          }),
        }),
        body: t.Object({
          isActive: t.Optional(
            t.Boolean({
              description: 'Whether this key can authenticate requests.',
              examples: [false],
            }),
          ),
        }),
      },
    )
    .delete(
      '/',
      async ({ body }) => {
        await service.deleteKeys([...body.keys]);
        return new Response(null, { status: 204 });
      },
      {
        detail: protectedRouteDetail({
          tags: [accessControlTag],
          summary: 'Delete API keys',
          description: 'Deletes one or more API keys by id.',
          responses: {
            204: {
              description: 'API keys deleted.',
            },
          },
        }),
        body: t.Object({
          keys: t.Array(
            t.String({
              description: 'API key id.',
              examples: ['key_operator'],
            }),
            {
              description: 'API key ids to delete.',
            },
          ),
        }),
      },
    );
}

export async function enforceApiTokenAuth(
  service: AccessControlService,
  method: string,
  path: string,
  apiTokenHeader: string | null,
): Promise<void> {
  const normalizedPath = normalizeAuthPath(path);

  if (isPublicRoute(normalizedPath)) {
    return;
  }

  const hasConfiguredKeys = await service.hasConfiguredKeys();
  if (isBootstrapKeyRoute(method, normalizedPath, hasConfiguredKeys)) {
    return;
  }

  if (!hasConfiguredKeys) {
    throw new UnauthorizedError();
  }

  await authenticateOrThrow(service, apiTokenHeader);
}

function normalizeAuthPath(path: string): string {
  return path !== '/' && path.endsWith('/') ? path.slice(0, -1) : path;
}

function isPublicRoute(path: string): boolean {
  return path === '/up' || path.startsWith('/v1/heartbeat') || path.startsWith('/openapi');
}

function isBootstrapKeyRoute(method: string, path: string, hasConfiguredKeys: boolean): boolean {
  return method.toUpperCase() === 'POST' && path === '/v1/keys' && !hasConfiguredKeys;
}

async function authenticateOrThrow(
  service: AccessControlService,
  apiTokenHeader: string | null,
): Promise<void> {
  try {
    await service.authenticate(apiTokenHeader);
  } catch (_error) {
    throw new UnauthorizedError();
  }
}
