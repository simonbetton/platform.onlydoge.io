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

  it('loads dogecoin blocks with boolean verbosity and batched tx hydration', async () => {
    const bodies: Array<
      | { id?: unknown; method?: string; params?: unknown[] }
      | Array<{
          id?: unknown;
          method?: string;
          params?: unknown[];
        }>
    > = [];

    vi.spyOn(globalThis, 'fetch').mockImplementation(
      async (_input: RequestInfo | URL, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body ?? 'null')) as
          | { id?: unknown; method?: string; params?: unknown[] }
          | Array<{ id?: unknown; method?: string; params?: unknown[] }>;
        bodies.push(body);

        if (Array.isArray(body)) {
          return Response.json(
            body.map((call) => ({
              id: call.id,
              result: {
                txid: String(call.params?.[0] ?? ''),
                vin: [],
                vout: [],
              },
              error: null,
            })),
          );
        }

        if (body.method === 'getblockhash') {
          return Response.json({ result: 'block-hash', error: null });
        }

        if (body.method === 'getblock') {
          return Response.json({
            result: {
              hash: 'block-hash',
              height: 42,
              time: 1_700_000_000,
              previousblockhash: 'prev-hash',
              tx: ['tx-a', 'tx-b'],
            },
            error: null,
          });
        }

        return Response.json({ result: null, error: { message: 'unexpected method' } });
      },
    );

    const gateway = new HttpBlockchainRpcGateway();
    await expect(
      gateway.getBlockSnapshot(
        {
          architecture: 'dogecoin',
          rpcEndpoint: 'http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/',
          rps: Number.MAX_SAFE_INTEGER,
        },
        42,
      ),
    ).resolves.toEqual({
      block: {
        hash: 'block-hash',
        height: 42,
        time: 1_700_000_000,
        previousblockhash: 'prev-hash',
        tx: [
          { txid: 'tx-a', vin: [], vout: [] },
          { txid: 'tx-b', vin: [], vout: [] },
        ],
      },
    });

    expect(bodies).toEqual([
      {
        jsonrpc: '1.0',
        id: 'onlydoge',
        method: 'getblockhash',
        params: [42],
      },
      {
        jsonrpc: '1.0',
        id: 'onlydoge',
        method: 'getblock',
        params: ['block-hash', true],
      },
      [
        {
          jsonrpc: '1.0',
          id: 0,
          method: 'getrawtransaction',
          params: ['tx-a', true],
        },
        {
          jsonrpc: '1.0',
          id: 1,
          method: 'getrawtransaction',
          params: ['tx-b', true],
        },
      ],
    ]);
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

  it('hydrates genesis coinbase from the raw block when getrawtransaction cannot find it', async () => {
    const genesisHash = '1a91e3dace36e2be3bf030a65679fe821aa1d6ef92e7c9902eb318182c355691';
    const genesisTxid = '5b2a3f53f605d62c53e62932dac6925e3d74afa5a4b459745c36d42d0ed26a69';
    const genesisBlockHex =
      '010000000000000000000000000000000000000000000000000000000000000000000000696ad20e2dd4365c7459b4a4a5af743d5e92c6da3229e6532cd605f6533f2a5b24a6a152f0ff0f1e678601000101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1004ffff001d0104084e696e746f6e646fffffffff010058850c020000004341040184710fa689ad5023690c80f3a49c8f13f8d45b8c857fbcbc8bc4a8e4d3eb4b10f4d4604fa08dce601aaf0f470216fe1b51850b4acf21b179c45070ac7b03a9ac00000000';
    const genesisTxHex =
      '01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1004ffff001d0104084e696e746f6e646fffffffff010058850c020000004341040184710fa689ad5023690c80f3a49c8f13f8d45b8c857fbcbc8bc4a8e4d3eb4b10f4d4604fa08dce601aaf0f470216fe1b51850b4acf21b179c45070ac7b03a9ac00000000';
    const genesisTransaction = {
      txid: genesisTxid,
      vin: [{ coinbase: '04ffff001d0104084e696e746f6e646f', sequence: 4294967295 }],
      vout: [
        {
          value: 88,
          n: 0,
          scriptPubKey: {
            type: 'pubkey',
            addresses: ['DQmCZQo3thCvTxkyAhPHfY7DVLqFtJ2ji6'],
          },
        },
      ],
    };
    const bodies: unknown[] = [];

    vi.spyOn(globalThis, 'fetch').mockImplementation(
      async (_input: RequestInfo | URL, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body ?? 'null')) as
          | { id?: unknown; method?: string; params?: unknown[] }
          | Array<{ id?: unknown; method?: string; params?: unknown[] }>;
        bodies.push(body);

        if (Array.isArray(body)) {
          if (body[0]?.method === 'decoderawtransaction') {
            return Response.json(
              body.map((call) => ({
                id: call.id,
                result: call.params?.[0] === genesisTxHex ? genesisTransaction : null,
                error: call.params?.[0] === genesisTxHex ? null : { message: 'bad hex' },
              })),
            );
          }

          return Response.json(
            body.map((call) => ({
              id: call.id,
              result: null,
              error: {
                code: -5,
                message:
                  'No such mempool or blockchain transaction. Use gettransaction for wallet transactions.',
              },
            })),
          );
        }

        if (body.method === 'getblockhash') {
          return Response.json({ result: genesisHash, error: null });
        }

        if (body.method === 'getblock' && body.params?.[1] === true) {
          return Response.json({
            result: {
              hash: genesisHash,
              height: 0,
              time: 1_386_325_540,
              tx: [genesisTxid],
            },
            error: null,
          });
        }

        if (body.method === 'getblock' && body.params?.[1] === false) {
          return Response.json({ result: genesisBlockHex, error: null });
        }

        return Response.json({ result: null, error: { message: 'unexpected method' } });
      },
    );

    const gateway = new HttpBlockchainRpcGateway();
    await expect(
      gateway.getBlockSnapshot(
        {
          architecture: 'dogecoin',
          rpcEndpoint: 'http://rpc-user:rpc-password@dogecoin-rpc.example.com:22555/',
          rps: Number.MAX_SAFE_INTEGER,
        },
        0,
      ),
    ).resolves.toEqual({
      block: {
        hash: genesisHash,
        height: 0,
        time: 1_386_325_540,
        tx: [genesisTransaction],
      },
    });

    expect(bodies).toEqual([
      {
        jsonrpc: '1.0',
        id: 'onlydoge',
        method: 'getblockhash',
        params: [0],
      },
      {
        jsonrpc: '1.0',
        id: 'onlydoge',
        method: 'getblock',
        params: [genesisHash, true],
      },
      [
        {
          jsonrpc: '1.0',
          id: 0,
          method: 'getrawtransaction',
          params: [genesisTxid, true],
        },
      ],
      {
        jsonrpc: '1.0',
        id: 'onlydoge',
        method: 'getblock',
        params: [genesisHash, false],
      },
      [
        {
          jsonrpc: '1.0',
          id: 0,
          method: 'decoderawtransaction',
          params: [genesisTxHex],
        },
      ],
    ]);
  });
});
