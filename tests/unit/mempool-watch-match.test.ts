import { describe, expect, it } from 'vitest';

import { matchingOutputsForAddress } from '../../packages/platform/src/mempool-watch-match';

describe('matchingOutputsForAddress', () => {
  it('returns matching receive outputs', () => {
    const outputs = matchingOutputsForAddress(
      {
        txid: 'tx-1',
        vout: [
          {
            n: 0,
            value: 1.5,
            scriptPubKey: { address: 'DWatchMe' },
          },
          {
            n: 1,
            value: 0.25,
            scriptPubKey: { address: 'DOther' },
          },
        ],
      },
      'DWatchMe',
      null,
    );

    expect(outputs).toEqual([{ vout: 0, valueBase: '150000000' }]);
  });

  it('requires minValueBase across matching outputs', () => {
    const below = matchingOutputsForAddress(
      {
        txid: 'tx-2',
        vout: [
          {
            n: 0,
            value: 0.4,
            scriptPubKey: { address: 'DWatchMe' },
          },
          {
            n: 1,
            value: 0.4,
            scriptPubKey: { address: 'DWatchMe' },
          },
        ],
      },
      'DWatchMe',
      '100000000',
    );
    expect(below).toBeNull();

    const enough = matchingOutputsForAddress(
      {
        txid: 'tx-3',
        vout: [
          {
            n: 0,
            value: 0.6,
            scriptPubKey: { address: 'DWatchMe' },
          },
          {
            n: 1,
            value: 0.5,
            scriptPubKey: { address: 'DWatchMe' },
          },
        ],
      },
      'DWatchMe',
      '100000000',
    );
    expect(enough).toEqual([
      { vout: 0, valueBase: '60000000' },
      { vout: 1, valueBase: '50000000' },
    ]);
  });
});
