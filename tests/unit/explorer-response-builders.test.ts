import { describe, expect, it } from 'vitest';

import {
  addressDetail,
  addressSearchResult,
  outputIndex,
  outputScriptType,
  spentByTxid,
  spentInBlock,
} from '../../packages/modules/explorer-query/src/application/explorer-response-builders';

describe('explorer response builders', () => {
  it('builds address search and detail responses from optional summaries', () => {
    const summary = {
      balance: '100',
      receivedBase: '150',
      sentBase: '50',
      txCount: 2,
      utxoCount: 1,
    };

    expect(addressSearchResult('DAddress', null)).toBeNull();
    expect(addressSearchResult('DAddress', summary)).toEqual({
      type: 'address',
      address: 'DAddress',
      balance: '100',
      txCount: 2,
    });
    expect(addressDetail('DAddress', null)).toMatchObject({
      balance: '0',
      txCount: 0,
      utxoCount: 0,
    });
  });

  it('normalizes output fields from raw and projected UTXO outputs', () => {
    expect(outputIndex({ value: 1, n: 2 }, 0)).toBe(2);
    expect(outputIndex({ value: 1 }, 5)).toBe(5);
    expect(outputScriptType({ value: 1, scriptPubKey: { type: ' pubkeyhash ' } })).toBe(
      'pubkeyhash',
    );
    expect(spentByTxid(undefined)).toBeNull();
    expect(
      spentByTxid({
        blockHeight: 1,
        blockHash: 'block-1',
        blockTime: 1,
        txid: 'tx-1',
        txIndex: 0,
        vout: 0,
        outputKey: 'tx-1:0',
        address: 'DAddress',
        scriptType: 'pubkeyhash',
        valueBase: '100',
        isCoinbase: false,
        isSpendable: true,
        spentByTxid: 'tx-2',
        spentInBlock: 2,
        spentInputIndex: 0,
      }),
    ).toBe('tx-2');
    expect(spentInBlock(undefined)).toBeNull();
  });
});
