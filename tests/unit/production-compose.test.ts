import { readFile } from 'node:fs/promises';
import { describe, expect, it } from 'vitest';

const composePath = new URL('../../docker-compose.yml', import.meta.url);

async function topLevelBlock(name: string): Promise<string> {
  const compose = await readFile(composePath, 'utf8');
  const match = compose.match(new RegExp(`^${name}:[^\\n]*\\n((?: {2}.*\\n?)*)`, 'mu'));
  expect(match, `missing ${name}`).not.toBeNull();
  return match?.[1] ?? '';
}

async function serviceBlock(name: string): Promise<string> {
  const compose = await readFile(composePath, 'utf8');
  const marker = `  ${name}:\n`;
  const start = compose.indexOf(marker);
  expect(start, `missing service ${name}`).toBeGreaterThanOrEqual(0);
  const remainder = compose.slice(start + marker.length);
  const nextService = remainder.search(/\n {2}[a-z][\w-]*:\n/u);
  return remainder.slice(0, nextService < 0 ? undefined : nextService);
}

describe('default Compose Dogecoin runtime', () => {
  it('passes RPC settings to both API and indexer roles through the shared env anchor', async () => {
    const shared = await topLevelBlock('x-onlydoge-env');
    expect(shared).toContain('ONLYDOGE_DOGECOIN_RPC_ENDPOINT:');
    expect(shared).toContain('ONLYDOGE_DOGECOIN_RPC_RPS:');
    expect(shared).toContain('ONLYDOGE_DOGECOIN_RPC_TIMEOUT_MS:');
    expect(await serviceBlock('onlydoge-api')).toContain('<<: *onlydoge-env');
    expect(await serviceBlock('onlydoge-indexer')).toContain('<<: *onlydoge-indexer-env');
  });

  it('passes ZMQ and sync tuning only to the indexer role', async () => {
    const shared = await topLevelBlock('x-onlydoge-env');
    const indexer = await topLevelBlock('x-onlydoge-indexer-env');

    expect(shared).not.toContain('ONLYDOGE_DOGECOIN_ZMQ_BLOCK_ENDPOINT:');
    expect(shared).not.toContain('ONLYDOGE_INDEXER_SYNC_WINDOW:');
    expect(indexer).toContain('<<: *onlydoge-env');
    expect(indexer).toContain('ONLYDOGE_DOGECOIN_ZMQ_BLOCK_ENDPOINT:');
    expect(indexer).toContain('ONLYDOGE_DOGECOIN_ZMQ_TX_ENDPOINT:');
    expect(indexer).toContain('ONLYDOGE_INDEXER_SYNC_WINDOW:');
  });

  it('does not make the indexer wait on the bundled node so external nodes work', async () => {
    const indexer = await serviceBlock('onlydoge-indexer');
    expect(indexer).not.toMatch(/depends_on:[\s\S]*?dogecoin:/u);
  });

  it('does not make the API wait on postgres or clickhouse health so it can serve while they recover', async () => {
    const api = await serviceBlock('onlydoge-api');
    expect(api).not.toMatch(/postgres:[\s\S]*?condition: service_healthy/u);
    expect(api).not.toMatch(/clickhouse:[\s\S]*?condition: service_healthy/u);
  });
});
