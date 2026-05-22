import { describe, expect, it } from 'vitest';

import {
  chunkQueryValues,
  clickHouseBalanceCursorClause,
  clickHouseBalancePageParams,
  clickHouseClientOptions,
  clickHouseOutputKeyCursorClause,
  clickHouseOutputPageParams,
  clickHousePagination,
  createAbortableRequestContext,
  formatBalanceTupleList,
  formatClickHouseStringLiteral,
  formatDirectLinkTupleList,
  queryTimeoutMs,
  toAddressMovementInsertRow,
  toAppliedBlockInsertRow,
  toBalanceInsertRow,
  toClickHouseMaxExecutionTimeSeconds,
  toCurrentBalancePage,
  toCurrentUtxoPage,
  toDirectLinkInsertRow,
  toTransferInsertRow,
  toUtxoInsertRow,
  warehouseInfrastructureMessage,
} from '../../packages/platform/src/warehouse-query';

describe('warehouse query helpers', () => {
  it('builds ClickHouse options, cursor clauses, and page params', () => {
    expect(
      clickHouseClientOptions(
        {
          driver: 'clickhouse',
          location: 'http://clickhouse:8123',
          database: 'onlydoge',
          user: 'doge',
          password: 'secret',
        },
        1234,
      ),
    ).toEqual({
      url: 'http://clickhouse:8123',
      request_timeout: 1234,
      database: 'onlydoge',
      username: 'doge',
      password: 'secret',
    });
    expect(clickHouseOutputKeyCursorClause(null)).toBe('');
    expect(clickHouseOutputKeyCursorClause('tx:1')).toContain('output_key >');
    expect(clickHouseOutputPageParams(7, 'tx:1', 50)).toEqual({
      networkId: 7,
      cursorOutputKey: 'tx:1',
      limit: 50,
    });
    expect(clickHouseBalanceCursorClause(null)).toBe('');
    expect(clickHouseBalanceCursorClause({ address: 'DA', assetAddress: '' })).toContain(
      'cursorAddress',
    );
    expect(clickHouseBalancePageParams(7, { address: 'DA', assetAddress: '' }, 50)).toEqual({
      networkId: 7,
      cursorAddress: 'DA',
      cursorAssetAddress: '',
      limit: 50,
    });
  });

  it('computes ClickHouse pagination and next cursors', () => {
    const utxoRows = [
      {
        networkId: 7,
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
        spentByTxid: null,
        spentInBlock: null,
        spentInputIndex: null,
      },
    ];
    expect(toCurrentUtxoPage(utxoRows, 1)).toEqual({
      rows: utxoRows,
      nextCursor: 'tx-1:0',
    });
    expect(
      toCurrentBalancePage(
        [
          {
            networkId: 7,
            address: 'DAddress',
            assetAddress: '',
            balance: '100',
            asOfBlockHeight: 1,
          },
        ],
        1,
      ),
    ).toMatchObject({ nextCursor: { address: 'DAddress', assetAddress: '' } });
    expect(clickHousePagination(10, 25)).toEqual({
      limitClause: 'LIMIT {limit:UInt64}',
      offsetClause: 'OFFSET {offset:UInt64}',
      queryParams: { limit: 25, offset: 10 },
    });
  });

  it('chunks query values and formats tuple filters safely', () => {
    expect(chunkQueryValues(['aa', 'bb', 'cc'], { maxValues: 2 })).toEqual([['aa', 'bb'], ['cc']]);
    expect(chunkQueryValues(['abcdef', 'ghijkl'], { maxBytes: 8 })).toEqual([
      ['abcdef'],
      ['ghijkl'],
    ]);
    expect(formatClickHouseStringLiteral("D'A\\B")).toBe("'D\\'A\\\\B'");
    expect(formatBalanceTupleList(['7:DAddress:'])).toBe("(('DAddress', ''))");
    expect(formatDirectLinkTupleList(['7:DFrom:DTo:'])).toBe("(('DFrom', 'DTo', ''))");
  });

  it('normalizes warehouse failures and request timeouts', () => {
    expect(warehouseInfrastructureMessage(new Error('connect ECONNREFUSED clickhouse'))).toBe(
      'warehouse unavailable',
    );
    expect(warehouseInfrastructureMessage(new Error('MEMORY_LIMIT_EXCEEDED'))).toBe(
      'warehouse query exceeded memory limit',
    );
    expect(warehouseInfrastructureMessage(new Error('The operation was aborted'))).toBe(
      'warehouse request timed out',
    );
    expect(toClickHouseMaxExecutionTimeSeconds(1001)).toBe(2);
    expect(queryTimeoutMs(undefined, 30)).toBe(30);
    expect(queryTimeoutMs({ timeoutMs: 5 }, 30)).toBe(5);

    const controller = new AbortController();
    const request = createAbortableRequestContext(controller.signal, 10_000);
    controller.abort(new Error('stop'));
    expect(request.signal.aborted).toBe(true);
    expect(request.didTimeout()).toBe(false);
    request.cleanup();
  });

  it('maps projection rows into ClickHouse insert records', () => {
    expect(
      toUtxoInsertRow(
        {
          networkId: 7,
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
          isCoinbase: true,
          isSpendable: false,
          spentByTxid: 'tx-2',
          spentInBlock: 2,
          spentInputIndex: 0,
        },
        9,
      ),
    ).toMatchObject({
      network_id: 7,
      output_key: 'tx-1:0',
      is_coinbase: 1,
      is_spendable: 0,
      version: 9,
    });
    expect(toAddressMovementInsertRow(addressMovement())).toMatchObject({ movement_id: 'm-1' });
    expect(toTransferInsertRow(transferRow())).toMatchObject({ transfer_id: 't-1' });
    expect(
      toBalanceInsertRow(
        {
          networkId: 7,
          address: 'DAddress',
          assetAddress: '',
          balance: '100',
          asOfBlockHeight: 1,
        },
        2,
      ),
    ).toMatchObject({ address: 'DAddress', version: 2 });
    expect(toDirectLinkInsertRow(directLinkRow(), 3)).toMatchObject({
      from_address: 'DFrom',
      version: 3,
    });
    expect(toAppliedBlockInsertRow({ networkId: 7, blockHeight: 1, blockHash: 'block-1' })).toEqual(
      {
        network_id: 7,
        block_height: 1,
        block_hash: 'block-1',
      },
    );
  });
});

function addressMovement() {
  return {
    movementId: 'm-1',
    networkId: 7,
    blockHeight: 1,
    blockHash: 'block-1',
    blockTime: 1,
    txid: 'tx-1',
    txIndex: 0,
    entryIndex: 0,
    address: 'DAddress',
    assetAddress: '',
    direction: 'credit' as const,
    amountBase: '100',
    outputKey: 'tx-1:0',
    derivationMethod: 'test',
  };
}

function transferRow() {
  return {
    transferId: 't-1',
    networkId: 7,
    blockHeight: 1,
    blockHash: 'block-1',
    blockTime: 1,
    txid: 'tx-1',
    txIndex: 0,
    transferIndex: 0,
    assetAddress: '',
    fromAddress: 'DFrom',
    toAddress: 'DTo',
    amountBase: '100',
    derivationMethod: 'test',
    confidence: 1,
    isChange: false,
    inputAddressCount: 1,
    outputAddressCount: 1,
  };
}

function directLinkRow() {
  return {
    networkId: 7,
    fromAddress: 'DFrom',
    toAddress: 'DTo',
    assetAddress: '',
    transferCount: 1,
    totalAmountBase: '100',
    firstSeenBlockHeight: 1,
    lastSeenBlockHeight: 1,
  };
}
