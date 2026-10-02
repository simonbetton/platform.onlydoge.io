import { loadSettings } from '@onlydoge/platform';
import { describe, expect, it } from 'vitest';

describe('mempool watch settings', () => {
  it('uses bounded detector defaults', () => {
    const settings = loadSettings({ env: { ONLYDOGE_MODE: 'both' } });

    expect(settings.dogecoin).toMatchObject({
      mempoolWatchCacheMaxTxids: 100_000,
      mempoolWatchRpcBatchSize: 100,
      mempoolWatchRpcConcurrency: 4,
      mempoolWatchRpcPollMs: 1_000,
    });
  });

  it('parses detector limits from the environment', () => {
    const settings = loadSettings({
      env: {
        ONLYDOGE_MODE: 'both',
        ONLYDOGE_MEMPOOL_WATCH_CACHE_MAX_TXIDS: '1234',
        ONLYDOGE_MEMPOOL_WATCH_RPC_BATCH_SIZE: '25',
        ONLYDOGE_MEMPOOL_WATCH_RPC_CONCURRENCY: '2',
        ONLYDOGE_MEMPOOL_WATCH_RPC_POLL_MS: '750',
      },
    });

    expect(settings.dogecoin).toMatchObject({
      mempoolWatchCacheMaxTxids: 1234,
      mempoolWatchRpcBatchSize: 25,
      mempoolWatchRpcConcurrency: 2,
      mempoolWatchRpcPollMs: 750,
    });
  });
});
