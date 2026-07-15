import { readFile } from 'node:fs/promises';
import { describe, expect, it } from 'vitest';

const composePath = new URL('../../docker-compose.prod.yml', import.meta.url);

async function serviceBlock(name: string): Promise<string> {
  const compose = await readFile(composePath, 'utf8');
  const marker = `  ${name}:\n`;
  const start = compose.indexOf(marker);
  expect(start).toBeGreaterThanOrEqual(0);
  const remainder = compose.slice(start + marker.length);
  const nextService = remainder.search(/\n {2}[a-z][\w-]*:\n/u);
  return compose.slice(start, nextService < 0 ? undefined : start + marker.length + nextService);
}

describe('production Compose Dogecoin runtime', () => {
  it('passes required RPC settings to API and indexer roles', async () => {
    for (const service of ['onlydoge-api', 'onlydoge-indexer']) {
      const block = await serviceBlock(service);
      expect(block).toContain('ONLYDOGE_DOGECOIN_RPC_ENDPOINT:');
      expect(block).toContain('ONLYDOGE_DOGECOIN_RPC_RPS:');
    }
  });

  it('passes optional ZMQ settings only to the indexer role', async () => {
    const api = await serviceBlock('onlydoge-api');
    const indexer = await serviceBlock('onlydoge-indexer');

    expect(api).not.toContain('ONLYDOGE_DOGECOIN_ZMQ_BLOCK_ENDPOINT:');
    expect(api).not.toContain('ONLYDOGE_DOGECOIN_ZMQ_TX_ENDPOINT:');
    expect(indexer).toContain('ONLYDOGE_DOGECOIN_ZMQ_BLOCK_ENDPOINT:');
    expect(indexer).toContain('ONLYDOGE_DOGECOIN_ZMQ_TX_ENDPOINT:');
  });
});
