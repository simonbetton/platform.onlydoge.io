import { HttpBlockchainRpcGateway } from '@onlydoge/platform';
import { afterEach, describe, expect, it, vi } from 'vitest';

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

    await expect(
      gateway.assertHealthy(
        'dogecoin',
        'http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/',
      ),
    ).rejects.toMatchObject({
      message:
        'could not connect to `http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/`',
    });
  });

  it('throttles rpc calls using the configured rps', async () => {
    vi.useFakeTimers();

    const fetchMock = vi.spyOn(globalThis, 'fetch').mockResolvedValue({
      ok: true,
      json: async () => ({
        result: 123,
        error: null,
      }),
    } as Response);
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
