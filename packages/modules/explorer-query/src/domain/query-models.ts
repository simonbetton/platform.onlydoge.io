import type { ProjectionUtxoOutput } from '@onlydoge/indexing-pipeline';

export interface ExplorerBlockSummary {
  hash: string;
  height: number;
  time: number;
  txCount: number;
}

export interface ExplorerMempoolTransaction {
  ancestorCount: number | null;
  ancestorFeesBase: string | null;
  ancestorSizeBytes: number | null;
  depends: string[];
  descendantCount: number | null;
  descendantFeesBase: string | null;
  descendantSizeBytes: number | null;
  feeBase: string | null;
  feeRateBasePerKilobyte: string | null;
  height: number | null;
  modifiedFeeBase: string | null;
  sizeBytes: number | null;
  time: number | null;
  txid: string;
}

export interface ExplorerMempoolResponse {
  bytes: number | null;
  fetchedAt: string;
  limit: number;
  maxMempoolBytes: number | null;
  mempoolMinFeeBasePerKilobyte: string | null;
  minRelayFeeBasePerKilobyte: string | null;
  offset: number;
  returnedCount: number;
  totalCount: number;
  transactions: ExplorerMempoolTransaction[];
  usageBytes: number | null;
}

export interface ExplorerTransactionSummary {
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  feeBase: string | null;
  inputCount: number;
  isCoinbase: boolean;
  outputCount: number;
  totalInputBase: string;
  totalOutputBase: string;
  txIndex: number;
  txid: string;
}

export interface ExplorerTransactionInput {
  address: string;
  outputKey: string;
  valueBase: string;
}

export interface ExplorerTransactionOutput {
  address: string;
  isSpendable: boolean;
  outputKey: string;
  scriptType: string;
  spentByTxid: string | null;
  spentInBlock: number | null;
  valueBase: string;
  vout: number;
}

export interface ExplorerTransactionDetail {
  inputs: ExplorerTransactionInput[];
  outputs: ExplorerTransactionOutput[];
  transaction: ExplorerTransactionSummary;
}

export interface ExplorerSearchResult {
  address?: string;
  balance?: string;
  blockHash?: string;
  blockHeight?: number;
  blockTime?: number;
  txCount?: number;
  txid?: string;
  type: 'address' | 'block' | 'transaction';
}

export interface ExplorerAddressDetail {
  address: {
    address: string;
    balance: string;
    receivedBase: string;
    sentBase: string;
    txCount: number;
    utxoCount: number;
  };
}

export interface ExplorerAddressTransactionSummary {
  receivedBase: string;
  sentBase: string;
  transaction: ExplorerTransactionSummary;
}

export interface ExplorerAddressUtxo
  extends Pick<
    ProjectionUtxoOutput,
    | 'blockHash'
    | 'blockHeight'
    | 'blockTime'
    | 'outputKey'
    | 'spentByTxid'
    | 'spentInBlock'
    | 'txid'
    | 'txIndex'
    | 'valueBase'
    | 'vout'
  > {
  address: string;
  scriptType: string;
}
