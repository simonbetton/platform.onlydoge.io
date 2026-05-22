import { describe, expect, it } from 'vitest';

import {
  addressDetail,
  addressSearchResult,
  buildExplorerTransferBasis,
  type ExplorerTransferBasis,
  outputIndex,
  outputScriptType,
  projectExplorerTransfers,
  spentByTxid,
  spentInBlock,
  withTransactionLabels,
} from '../../packages/modules/explorer-query/src/application/explorer-response-builders';
import type {
  ExplorerLabelRef,
  ExplorerTransactionInput,
  ExplorerTransactionOutput,
  ExplorerTransactionSummary,
} from '../../packages/modules/explorer-query/src/domain/query-models';

describe('explorer response builders', () => {
  it('labels transaction parts without mutating the source arrays', () => {
    const label: ExplorerLabelRef = {
      entity: 'entity-1',
      name: 'Known Entity',
      riskLevel: 'high',
      tags: ['source'],
    };
    const inputs: ExplorerTransactionInput[] = [
      { address: 'DInput', outputKey: 'prev:0', valueBase: '100' },
    ];
    const outputs: ExplorerTransactionOutput[] = [
      {
        address: 'DOutput',
        outputKey: 'tx:0',
        valueBase: '50',
        vout: 0,
        scriptType: 'pubkeyhash',
        isSpendable: true,
        spentByTxid: null,
        spentInBlock: null,
      },
    ];

    const labeled = withTransactionLabels(inputs, outputs, new Map([['DInput', label]]));

    expect(labeled.inputs[0]).toEqual({ ...inputs[0], label });
    expect(labeled.outputs[0]).toEqual(outputs[0]);
    expect(inputs[0]?.label).toBeUndefined();
    expect(labeled.inputs[0]).not.toBe(inputs[0]);
  });

  it('builds address search and detail responses from optional summaries and labels', () => {
    const summary = {
      balance: '100',
      receivedBase: '150',
      sentBase: '50',
      txCount: 2,
      utxoCount: 1,
    };
    const label: ExplorerLabelRef = {
      entity: 'entity-1',
      name: null,
      riskLevel: 'low',
      tags: [],
    };

    expect(addressSearchResult('doge', 'DAddress', null, undefined)).toBeNull();
    expect(addressSearchResult('doge', 'DAddress', summary, label)).toEqual({
      type: 'address',
      network: 'doge',
      address: 'DAddress',
      hasLabel: true,
      balance: '100',
      txCount: 2,
      riskLevel: 'low',
    });
    expect(addressDetail('doge', 'DAddress', null)).toMatchObject({
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
        spentByTxid: 'tx-2',
        spentInBlock: 2,
        spentInputIndex: 0,
      }),
    ).toBe('tx-2');
    expect(spentInBlock(undefined)).toBeNull();
  });

  it('projects transaction transfers from pro-rata input ownership', () => {
    const summary: ExplorerTransactionSummary = {
      network: 'doge',
      txid: 'tx-1',
      txIndex: 0,
      blockHeight: 1,
      blockHash: 'block-1',
      blockTime: 1,
      isCoinbase: false,
      inputCount: 2,
      outputCount: 1,
      totalInputBase: '200',
      totalOutputBase: '100',
      feeBase: '100',
    };
    const inputs: ExplorerTransactionInput[] = [
      { address: 'DA', outputKey: 'prev-a:0', valueBase: '100' },
      { address: 'DB', outputKey: 'prev-b:0', valueBase: '100' },
    ];
    const outputs: ExplorerTransactionOutput[] = [
      {
        address: 'DC',
        outputKey: 'tx-1:0',
        valueBase: '100',
        vout: 0,
        scriptType: 'pubkeyhash',
        isSpendable: true,
        spentByTxid: null,
        spentInBlock: null,
      },
    ];

    const basis = buildExplorerTransferBasis(summary, inputs, outputs);
    expect(basis?.addresses).toEqual(['DA', 'DB']);
    expect(projectExplorerTransfers(outputs, basis as ExplorerTransferBasis)).toEqual([
      { from: 'DA', to: 'DC', amountBase: '50' },
      { from: 'DB', to: 'DC', amountBase: '50' },
    ]);
    expect(
      buildExplorerTransferBasis({ ...summary, isCoinbase: true }, inputs, outputs),
    ).toBeNull();
  });
});
