import type { AddressMovement } from '@onlydoge/indexing-pipeline';
import { describe, expect, it } from 'vitest';

import {
  aggregateAddressTransactions,
  assertNonNegativeBalance,
  currentBalanceNextCursor,
  currentBalancePageRows,
  emptyWarehouseState,
  inMemoryAddressSummary,
  mergeWarehouseState,
  nextBalanceAmount,
  paginateAddressTransactions,
  summarizeNativeMovements,
  type WarehouseState,
} from '../../packages/platform/src/warehouse-state';

describe('warehouse state helpers', () => {
  it('normalizes partial snapshots into complete warehouse state', () => {
    const state = mergeWarehouseState({
      balances: [
        {
          networkId: 7,
          address: 'DAddress',
          assetAddress: '',
          balance: '10',
          asOfBlockHeight: 3,
        },
      ],
    });

    expect(state).toMatchObject({
      ...emptyWarehouseState(),
      balances: [
        {
          networkId: 7,
          address: 'DAddress',
          assetAddress: '',
          balance: '10',
          asOfBlockHeight: 3,
        },
      ],
    } satisfies WarehouseState);
  });

  it('pages current balances by network, address, asset, and cursor', () => {
    const rows = currentBalancePageRows(
      [
        balanceRow('DCharlie', '', 7),
        balanceRow('DAlice', 'TOKEN', 7),
        balanceRow('DAlice', '', 7),
        balanceRow('DBob', '', 9),
      ],
      7,
      { address: 'DAlice', assetAddress: '' },
      2,
    );

    expect(rows.map((row) => `${row.address}:${row.assetAddress}`)).toEqual([
      'DAlice:TOKEN',
      'DCharlie:',
    ]);
    expect(currentBalanceNextCursor(rows, 2)).toEqual({
      address: 'DCharlie',
      assetAddress: '',
    });
    expect(currentBalanceNextCursor(rows.slice(0, 1), 2)).toBeNull();
  });

  it('summarizes native movement totals and address transaction pages', () => {
    const movements = [
      movement({ txid: 'tx-1', direction: 'credit', amountBase: '100', blockHeight: 2 }),
      movement({ txid: 'tx-1', direction: 'debit', amountBase: '25', blockHeight: 2 }),
      movement({
        txid: 'tx-2',
        direction: 'credit',
        amountBase: '7',
        blockHeight: 3,
        txIndex: 4,
      }),
    ];

    const totals = summarizeNativeMovements(movements);
    expect(totals).toEqual({ receivedBase: 107n, sentBase: 25n, txCount: 2 });
    expect(inMemoryAddressSummary('82', totals, 1)).toEqual({
      balance: '82',
      receivedBase: '107',
      sentBase: '25',
      txCount: 2,
      utxoCount: 1,
    });
    expect(
      inMemoryAddressSummary('0', { receivedBase: 0n, sentBase: 0n, txCount: 0 }, 0),
    ).toBeNull();

    const page = paginateAddressTransactions(aggregateAddressTransactions(movements), 0, 1);
    expect(page).toEqual([
      {
        blockHash: 'block-3',
        blockHeight: 3,
        blockTime: 1_700_000_003,
        txid: 'tx-2',
        txIndex: 4,
        receivedBase: '7',
        sentBase: '0',
      },
    ]);
  });

  it('applies balance deltas and rejects negative balances', () => {
    const credit = movement({ direction: 'credit', amountBase: '15' });
    const debit = movement({ direction: 'debit', amountBase: '6' });
    const overdraft = movement({ direction: 'debit', amountBase: '20' });

    expect(nextBalanceAmount(undefined, credit)).toBe(15n);
    expect(nextBalanceAmount('15', debit)).toBe(9n);
    expect(() => assertNonNegativeBalance(overdraft, -1n)).toThrow(
      'negative balance for 7:DAddress:',
    );
  });
});

function balanceRow(address: string, assetAddress: string, networkId: number) {
  return {
    networkId,
    address,
    assetAddress,
    balance: '1',
    asOfBlockHeight: 1,
  };
}

function movement(overrides: Partial<AddressMovement>): AddressMovement {
  const blockHeight = overrides.blockHeight ?? 1;
  return {
    movementId: `${overrides.txid ?? 'tx'}:${overrides.direction ?? 'credit'}`,
    networkId: 7,
    blockHeight,
    blockHash: `block-${blockHeight}`,
    blockTime: 1_700_000_000 + blockHeight,
    txid: 'tx',
    txIndex: 0,
    entryIndex: 0,
    address: 'DAddress',
    assetAddress: '',
    direction: 'credit',
    amountBase: '1',
    outputKey: null,
    derivationMethod: 'test',
    ...overrides,
  };
}
