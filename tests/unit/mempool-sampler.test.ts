import {
  DogecoinMempoolSamplerService,
  type MempoolSampleRow,
  mempoolSampleRows,
} from '@onlydoge/platform';
import { describe, expect, it, vi } from 'vitest';

describe('dogecoin mempool sampler', () => {
  it('converts verbose mempool entries into analytics sample rows', () => {
    const rows = mempoolSampleRows({
      fetchedAt: '2026-06-01T12:34:56.789Z',
      entries: {
        'tx-a': {
          time: 1_780_000_000,
          height: 6_000_000,
          vsize: 250,
          fee: 0.01,
        },
      },
    });

    expect(rows).toEqual([
      {
        sampledAt: '2026-06-01 12:34:56',
        txid: 'tx-a',
        entryTime: 1_780_000_000,
        height: 6_000_000,
        sizeBytes: 250,
        feeBase: '1000000',
        feeRateBasePerKilobyte: '4000000',
        rawJson: JSON.stringify({
          time: 1_780_000_000,
          height: 6_000_000,
          vsize: 250,
          fee: 0.01,
        }),
      },
    ]);
  });

  it('persists one sampler batch from Dogecoin RPC', async () => {
    const insertMempoolSamples = vi.fn(async (_rows: MempoolSampleRow[]) => {});
    const service = new DogecoinMempoolSamplerService(
      {
        async getDogecoinConfig() {
          return {
            architecture: 'dogecoin' as const,
            rpcEndpoint: 'http://dogecoin-rpc.example',
            rps: 10,
          };
        },
      },
      {
        async getMempoolSnapshot() {
          return {
            fetchedAt: '2026-06-01T12:34:56.789Z',
            info: {},
            entries: {
              'tx-a': {
                time: 1_780_000_000,
                size: 500,
                fees: { base: '0.02000000' },
              },
            },
          };
        },
      },
      { insertMempoolSamples },
      { mempoolSampleIntervalMs: 15_000 },
    );

    await expect(service.runOnce()).resolves.toBe(true);
    expect(insertMempoolSamples).toHaveBeenCalledWith([
      expect.objectContaining({
        txid: 'tx-a',
        feeBase: '2000000',
        feeRateBasePerKilobyte: '4000000',
      }),
    ]);
  });
});
