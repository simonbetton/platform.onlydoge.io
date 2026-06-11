import {
  type ExplorerConfigPort,
  type ExplorerCoreBlockPort,
  type ExplorerDogecoinConfigPort,
  type ExplorerMempoolRpcPort,
  ExplorerQueryService,
  type ExplorerRawBlockPort,
  type ExplorerWarehousePort,
} from '@onlydoge/explorer-query';
import {
  configKeyIndexerProcessTail,
  configKeyIndexerSyncTail,
  type ProjectionUtxoOutput,
} from '@onlydoge/indexing-pipeline';
import { describe, expect, it } from 'vitest';

describe('ExplorerQueryService', () => {
  it('resolves transaction details from raw synced blocks when derived refs lag', async () => {
    const rawTxid = 'raw-synced-tx';
    const createdLookups: string[][] = [];
    const fullLookups: string[][] = [];
    const previousOutput = projectionOutput({
      address: 'DPreviousOutput111111111111111111111',
      outputKey: 'previous-tx:0',
      txid: 'previous-tx',
      valueBase: '5000000000',
    });
    const service = createService({
      rawBlocks: new Map([
        [
          3,
          {
            block: {
              hash: 'raw-block-3',
              height: 3,
              time: 1_700_000_180,
              tx: [
                {
                  txid: 'raw-coinbase',
                  vin: [{ coinbase: 'coinbase' }],
                  vout: [
                    {
                      n: 0,
                      value: '10.00000000',
                      scriptPubKey: {
                        addresses: ['DMiner111111111111111111111111111'],
                        type: 'pubkeyhash',
                      },
                    },
                  ],
                },
                {
                  txid: rawTxid,
                  vin: [{ txid: 'previous-tx', vout: 0 }],
                  vout: [
                    {
                      n: 0,
                      value: '49.00000000',
                      scriptPubKey: {
                        addresses: ['DRecipient111111111111111111111111'],
                        type: 'pubkeyhash',
                      },
                    },
                  ],
                },
              ],
            },
          },
        ],
      ]),
      utxoOutputs: new Map([[previousOutput.outputKey, previousOutput]]),
      createdLookups,
      fullLookups,
    });

    await expect(service.search(rawTxid)).resolves.toMatchObject({
      matches: [
        {
          blockHash: 'raw-block-3',
          blockHeight: 3,
          blockTime: 1_700_000_180,
          txid: rawTxid,
          type: 'transaction',
        },
      ],
    });

    const details = await service.getTransaction(rawTxid);

    expect(details.transaction).toMatchObject({
      blockHash: 'raw-block-3',
      blockHeight: 3,
      blockTime: 1_700_000_180,
      feeBase: '100000000',
      totalInputBase: '5000000000',
      totalOutputBase: '4900000000',
      txIndex: 1,
      txid: rawTxid,
    });
    expect(details.inputs).toEqual([
      expect.objectContaining({
        address: previousOutput.address,
        outputKey: previousOutput.outputKey,
        valueBase: previousOutput.valueBase,
      }),
    ]);
    expect(details.outputs).toEqual([
      expect.objectContaining({
        address: 'DRecipient111111111111111111111111',
        outputKey: `${rawTxid}:0`,
        valueBase: '4900000000',
      }),
    ]);
    expect(createdLookups).toContainEqual(['previous-tx:0']);
    expect(fullLookups).toContainEqual([`${rawTxid}:0`]);
  });
});

function createService(input: {
  createdLookups?: string[][];
  fullLookups?: string[][];
  rawBlocks: Map<number, Record<string, unknown>>;
  utxoOutputs: Map<string, ProjectionUtxoOutput>;
}): ExplorerQueryService {
  const configs: ExplorerConfigPort = {
    async canReadDogecoinHistory() {
      return true;
    },
    async getJsonValue<T>(key: string) {
      const values = new Map<string, unknown>([
        [configKeyIndexerProcessTail(), 2],
        [configKeyIndexerSyncTail(), 3],
      ]);
      return (values.get(key) ?? null) as T | null;
    },
  };
  const warehouse: ExplorerWarehousePort = {
    async getAddressSummary() {
      return null;
    },
    async getAppliedBlockByHash() {
      return null;
    },
    async getTransactionRef() {
      return null;
    },
    async getCreatedUtxoOutputs(outputKeys) {
      input.createdLookups?.push(outputKeys);
      return new Map(
        outputKeys
          .map((outputKey) => input.utxoOutputs.get(outputKey))
          .filter((output): output is ProjectionUtxoOutput => Boolean(output))
          .map((output) => [output.outputKey, output]),
      );
    },
    async getUtxoOutputs(outputKeys) {
      input.fullLookups?.push(outputKeys);
      return new Map(
        outputKeys
          .map((outputKey) => input.utxoOutputs.get(outputKey))
          .filter((output): output is ProjectionUtxoOutput => Boolean(output))
          .map((output) => [output.outputKey, output]),
      );
    },
    async listAddressTransactions() {
      return [];
    },
    async listAddressUtxos() {
      return [];
    },
    async listAppliedBlocks() {
      return [];
    },
  };
  const rawBlocks: ExplorerRawBlockPort = {
    async getPart<T extends Record<string, unknown>>(blockHeight: number) {
      return (input.rawBlocks.get(blockHeight) as T | undefined) ?? null;
    },
  };
  const dogecoin: ExplorerDogecoinConfigPort = {
    async getDogecoinConfig() {
      return {
        architecture: 'dogecoin',
        blockTime: 60,
        chainId: 1,
        id: 'dogecoin',
        name: 'Dogecoin',
        rpcEndpoint: 'http://dogecoin.example',
        rps: 10,
      };
    },
  };
  const coreBlocks: ExplorerCoreBlockPort = {
    async getCoreBlockByHash() {
      return null;
    },
  };
  const mempoolRpc: ExplorerMempoolRpcPort = {
    async getMempoolSnapshot() {
      return { entries: {}, fetchedAt: new Date(0).toISOString(), info: {} };
    },
  };

  return new ExplorerQueryService(dogecoin, warehouse, rawBlocks, configs, coreBlocks, mempoolRpc);
}

function projectionOutput(overrides: Partial<ProjectionUtxoOutput>): ProjectionUtxoOutput {
  return {
    address: '',
    blockHash: 'previous-block',
    blockHeight: 2,
    blockTime: 1_700_000_120,
    isCoinbase: false,
    isSpendable: true,
    outputKey: 'tx:0',
    scriptType: 'pubkeyhash',
    spentByTxid: null,
    spentInBlock: null,
    spentInputIndex: null,
    txIndex: 0,
    txid: 'tx',
    valueBase: '0',
    vout: 0,
    ...overrides,
  };
}
