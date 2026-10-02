import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { HttpBlockchainRpcGateway } from '@onlydoge/platform';
import { afterEach, describe, expect, it, vi } from 'vitest';

const fixturesDir = join(import.meta.dirname, '../fixtures/dogecoin-blocks');

describe('http blockchain rpc gateway', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('sends basic auth headers for rpc endpoints with credentials', async () => {
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      Response.json({
        result: 123,
        error: null,
      }),
    );

    const gateway = new HttpBlockchainRpcGateway();

    await expect(
      gateway.assertHealthy(
        'dogecoin',
        'http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/',
      ),
    ).resolves.toBeUndefined();

    expect(fetchMock).toHaveBeenCalledWith(
      'http://dogecoin-rpc.example.com:22555/',
      expect.objectContaining({
        method: 'POST',
        headers: expect.objectContaining({
          authorization: 'Basic cnBjLXVzZXI6cnBjLXBhc3N3b3Jk',
          'content-type': 'application/json',
        }),
      }),
    );
  });

  it('times out hung dogecoin rpc health checks', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(
      (_input: RequestInfo | URL, init?: RequestInit) => {
        return new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener('abort', () => {
            reject(init.signal?.reason ?? new Error('aborted'));
          });
        });
      },
    );

    const gateway = new HttpBlockchainRpcGateway(5);
    const healthCheck = gateway.assertHealthy(
      'dogecoin',
      'http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/',
    );

    await expect(healthCheck).rejects.toMatchObject({
      message: 'could not connect to `http://***:***@dogecoin-rpc.example.com:22555/`',
    });
    await expect(healthCheck).rejects.not.toThrow(/rpc-user|rpc-password/u);
  });

  it('masks rpc endpoint credentials in invalid rpc response errors', async () => {
    vi.spyOn(globalThis, 'fetch').mockResolvedValue(
      Response.json({
        result: null,
        error: { message: 'rpc failed' },
      }),
    );

    const gateway = new HttpBlockchainRpcGateway();
    const blockHeight = gateway.getBlockHeight({
      architecture: 'dogecoin',
      rpcEndpoint: 'https://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/rpc',
      rps: Number.MAX_SAFE_INTEGER,
    });

    await expect(blockHeight).rejects.toMatchObject({
      message: 'could not connect to `https://***:***@dogecoin-rpc.example.com:22555/rpc`',
    });
    await expect(blockHeight).rejects.not.toThrow(/rpc-user|rpc-password/u);
  });

  it('throttles rpc calls using the configured rps', async () => {
    vi.useFakeTimers();

    const fetchMock = vi.spyOn(globalThis, 'fetch').mockImplementation(async () =>
      Response.json({
        result: 123,
        error: null,
      }),
    );
    const gateway = new HttpBlockchainRpcGateway();

    const first = gateway.getBlockHeight({
      architecture: 'dogecoin',
      rpcEndpoint: 'http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/',
      rps: 1,
    });
    const second = gateway.getBlockHeight({
      architecture: 'dogecoin',
      rpcEndpoint: 'http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/',
      rps: 1,
    });
    const pending = Promise.all([first, second]);

    await vi.advanceTimersByTimeAsync(0);
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(999);
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(1);
    await pending;
    expect(fetchMock).toHaveBeenCalledTimes(2);
    vi.useRealTimers();
  });

  it('retries dogecoin work-queue overload responses', async () => {
    vi.useFakeTimers();

    let calls = 0;
    const fetchMock = vi.spyOn(globalThis, 'fetch').mockImplementation(async () => {
      calls += 1;
      if (calls < 3) {
        return new Response('Work queue depth exceeded', { status: 500 });
      }

      return Response.json({
        result: 99,
        error: null,
      });
    });

    const gateway = new HttpBlockchainRpcGateway();
    const heightPromise = gateway.getBlockHeight({
      architecture: 'dogecoin',
      rpcEndpoint: 'http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/',
      rps: Number.MAX_SAFE_INTEGER,
    });

    await vi.advanceTimersByTimeAsync(0);
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(100);
    expect(fetchMock).toHaveBeenCalledTimes(2);

    await vi.advanceTimersByTimeAsync(200);
    await expect(heightPromise).resolves.toBe(99);
    expect(fetchMock).toHaveBeenCalledTimes(3);
    vi.useRealTimers();
  });

  it('surfaces work-queue overload after retries are exhausted', async () => {
    vi.useFakeTimers();

    const fetchMock = vi
      .spyOn(globalThis, 'fetch')
      .mockImplementation(async () => new Response('Work queue depth exceeded', { status: 500 }));

    const gateway = new HttpBlockchainRpcGateway();
    const heightPromise = gateway.getBlockHeight({
      architecture: 'dogecoin',
      rpcEndpoint: 'http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/',
      rps: Number.MAX_SAFE_INTEGER,
    });

    const assertion = expect(heightPromise).rejects.toMatchObject({
      message:
        'dogecoin rpc work queue exceeded at `http://***:***@dogecoin-rpc.example.com:22555/`',
    });

    await vi.advanceTimersByTimeAsync(0);
    await vi.advanceTimersByTimeAsync(100);
    await vi.advanceTimersByTimeAsync(200);
    await vi.advanceTimersByTimeAsync(400);
    await assertion;
    expect(fetchMock).toHaveBeenCalledTimes(4);
    vi.useRealTimers();
  });

  it('loads dogecoin blocks as raw hex in two JSON-RPC batches and decodes them locally', async () => {
    const bodies: unknown[] = [];
    const rawBlocks: Record<number, string> = {
      0: readFixture('0.hex'),
      1: readFixture('1.hex'),
    };
    const hashes: Record<number, string> = {
      0: readExpected(0).hash,
      1: readExpected(1).hash,
    };

    vi.spyOn(globalThis, 'fetch').mockImplementation(
      async (_input: RequestInfo | URL, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body ?? 'null')) as Array<{
          id: number;
          method: string;
          params: unknown[];
        }>;
        bodies.push(body);
        return Response.json(
          body.map((call) => ({
            id: call.id,
            error: null,
            result:
              call.method === 'getblockhash'
                ? hashes[Number(call.params[0])]
                : rawBlocks[heightForHash(hashes, String(call.params[0]))],
          })),
        );
      },
    );

    const gateway = new HttpBlockchainRpcGateway();
    const snapshots = await gateway.getBlockSnapshots(
      {
        architecture: 'dogecoin',
        rpcEndpoint: 'http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/',
        rps: Number.MAX_SAFE_INTEGER,
      },
      [0, 1],
    );

    expect(snapshots.map((snapshot) => snapshot.block)).toMatchObject([
      { hash: hashes[0], height: 0, tx: [{ txid: readExpected(0).tx[0]?.txid }] },
      { hash: hashes[1], height: 1, previousblockhash: hashes[0] },
    ]);
    expect(bodies).toEqual([
      [
        { jsonrpc: '1.0', id: 0, method: 'getblockhash', params: [0] },
        { jsonrpc: '1.0', id: 1, method: 'getblockhash', params: [1] },
      ],
      [
        { jsonrpc: '1.0', id: 0, method: 'getblock', params: [hashes[0], false] },
        { jsonrpc: '1.0', id: 1, method: 'getblock', params: [hashes[1], false] },
      ],
    ]);
  });

  it('rejects raw blocks whose decoded hash does not match getblockhash', async () => {
    vi.spyOn(globalThis, 'fetch').mockImplementation(
      async (_input: RequestInfo | URL, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body ?? 'null')) as Array<{
          id: number;
          method: string;
        }>;
        return Response.json(
          body.map((call) => ({
            id: call.id,
            error: null,
            result: call.method === 'getblockhash' ? 'f'.repeat(64) : readFixture('1.hex'),
          })),
        );
      },
    );

    const gateway = new HttpBlockchainRpcGateway();
    await expect(
      gateway.getBlockSnapshot(
        {
          architecture: 'dogecoin',
          rpcEndpoint: 'http://dogecoin-rpc.example.com:22555/',
          rps: Number.MAX_SAFE_INTEGER,
        },
        1,
      ),
    ).rejects.toThrow('dogecoin block hash mismatch');
  });

  it('reads dogecoin mempool info and verbose entries', async () => {
    const bodies: Array<{ method?: string; params?: unknown[] }> = [];
    vi.spyOn(globalThis, 'fetch').mockImplementation(
      async (_input: RequestInfo | URL, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body ?? '{}')) as {
          method?: string;
          params?: unknown[];
        };
        bodies.push(body);

        if (body.method === 'getmempoolinfo') {
          return Response.json({
            result: {
              size: 1,
              bytes: 250,
            },
            error: null,
          });
        }

        if (body.method === 'getrawmempool') {
          return Response.json({
            result: {
              'doge-mempool-tx': {
                size: 250,
                fee: '0.00100000',
              },
            },
            error: null,
          });
        }

        return Response.json({ result: null, error: { message: 'unexpected method' } });
      },
    );

    const gateway = new HttpBlockchainRpcGateway();

    await expect(
      gateway.getMempoolSnapshot({
        architecture: 'dogecoin',
        rpcEndpoint: 'http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/',
        rps: Number.MAX_SAFE_INTEGER,
      }),
    ).resolves.toMatchObject({
      info: {
        size: 1,
        bytes: 250,
      },
      entries: {
        'doge-mempool-tx': {
          size: 250,
          fee: '0.00100000',
        },
      },
    });

    expect(bodies.map((body) => [body.method, body.params])).toEqual([
      ['getmempoolinfo', []],
      ['getrawmempool', [true]],
    ]);
  });
});

function readFixture(name: string): string {
  return readFileSync(join(fixturesDir, name), 'utf8').trim();
}

function readExpected(height: number): { hash: string; tx: Array<{ txid: string }> } {
  return JSON.parse(readFixture(`${height}.expected.json`));
}

function heightForHash(hashes: Record<number, string>, hash: string): number {
  const entry = Object.entries(hashes).find(([, value]) => value === hash);
  return entry ? Number(entry[0]) : -1;
}
