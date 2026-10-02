import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { buildApiApp } from '@onlydoge/api';
import { createRuntime } from '@onlydoge/platform';
import { describe, expect, it } from 'vitest';

const repoRoot = join(import.meta.dirname, '..', '..');

describe('docs consistency', () => {
  it('documents explorer routes that are registered in the API app', async () => {
    const runtime = await createRuntime({ mode: 'both', ip: '127.0.0.1', port: 0 });
    const app = buildApiApp(runtime);
    const registeredRoutes = new Set(
      app.routes.map((route) => route.path).filter((path) => path.startsWith('/v1/explorer/')),
    );

    const explorerDocs = readFileSync(join(repoRoot, 'docs/dogecoin-explorer-api.md'), 'utf8');
    const documentedRoutes = [...explorerDocs.matchAll(/`(GET \/v1\/explorer\/[^`]+)`/gu)].map(
      (match) => match[1]?.replace(/^GET /u, '').split('?')[0] ?? '',
    );

    for (const route of documentedRoutes) {
      expect(registeredRoutes.has(route), `missing documented route ${route}`).toBe(true);
    }
  });

  it('does not claim the local app runs with bun --watch', () => {
    const readme = readFileSync(join(repoRoot, 'README.md'), 'utf8');
    expect(readme).not.toMatch(/bun run --watch/u);
    expect(readfileContains(join(repoRoot, 'README.md'), 'GET /v1/explorer/mempool/watch')).toBe(
      true,
    );
  });

  it('documents external node RPC and ZMQ settings in the env example', () => {
    const example = readFileSync(join(repoRoot, '.env.example'), 'utf8');
    expect(example).toContain('ONLYDOGE_DOGECOIN_RPC_ENDPOINT=');
    expect(example).toContain('ONLYDOGE_DOGECOIN_ZMQ_BLOCK_ENDPOINT=');
    expect(example).toContain('ONLYDOGE_DOGECOIN_ZMQ_TX_ENDPOINT=');
  });
});

function readfileContains(path: string, needle: string): boolean {
  return readFileSync(path, 'utf8').includes(needle);
}
