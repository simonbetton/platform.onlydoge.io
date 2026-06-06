import type { ProjectionUtxoOutput } from '@onlydoge/indexing-pipeline';

export interface ExplorerDogecoinConfigPort {
  getDogecoinConfig(): Promise<{
    architecture: 'dogecoin';
    blockTime: number;
    chainId: number;
    id: string;
    name: string;
    rpcEndpoint: string;
    rps: number;
    zmqBlockEndpoint?: string | null;
  }>;
}

export interface ExplorerConfigPort {
  canReadDogecoinHistory(): Promise<boolean>;
  getJsonValue<T>(key: string): Promise<T | null>;
}

export interface ExplorerCoreBlockPort {
  getCoreBlockByHash(blockHash: string): Promise<{
    blockHash: string;
    blockHeight: number;
  } | null>;
}

export interface ExplorerRawBlockPort {
  getPart<T extends Record<string, unknown>>(blockHeight: number, part: string): Promise<T | null>;
}

export interface ExplorerMempoolRpcPort {
  getMempoolSnapshot(dogecoin: {
    architecture: 'dogecoin';
    rpcEndpoint: string;
    rps: number;
  }): Promise<{
    entries: Record<string, Record<string, unknown>>;
    fetchedAt: string;
    info: Record<string, unknown>;
  }>;
}

export interface ExplorerWarehousePort {
  getUtxoOutputs(outputKeys: string[]): Promise<Map<string, ProjectionUtxoOutput>>;
  getAddressSummary(address: string): Promise<{
    balance: string;
    receivedBase: string;
    sentBase: string;
    txCount: number;
    utxoCount: number;
  } | null>;
  getAppliedBlockByHash(blockHash: string): Promise<{
    blockHash: string;
    blockHeight: number;
  } | null>;
  getTransactionRef(txid: string): Promise<{
    blockHash: string;
    blockHeight: number;
    blockTime: number;
    txIndex: number;
  } | null>;
  listAddressTransactions(
    address: string,
    offset?: number,
    limit?: number,
  ): Promise<
    Array<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      receivedBase: string;
      sentBase: string;
      txIndex: number;
      txid: string;
    }>
  >;
  listAddressUtxos(
    address: string,
    offset?: number,
    limit?: number,
  ): Promise<ProjectionUtxoOutput[]>;
  listAppliedBlocks(
    offset?: number,
    limit?: number,
  ): Promise<
    Array<{
      blockHash: string;
      blockHeight: number;
    }>
  >;
}
