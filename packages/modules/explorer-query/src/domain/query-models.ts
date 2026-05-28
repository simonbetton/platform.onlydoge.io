import type { ProjectionUtxoOutput } from '@onlydoge/indexing-pipeline';
import type { InfoResponse } from '@onlydoge/investigation-query';
import type { RiskLevel } from '@onlydoge/shared-kernel';

export interface ExplorerLabelRef {
  entity: string;
  name: string | null;
  riskLevel: RiskLevel;
  tags: string[];
}

export interface ExplorerNetworkSummary {
  blockHeight: number;
  blockTime: number;
  chainId: number;
  id: string;
  isDefault: boolean;
  name: string;
  processTail: number;
  processed: number;
  syncTail: number;
  synced: number;
  tipLagBlocks: number;
}

export interface ExplorerBlockSummary {
  hash: string;
  height: number;
  network: string;
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
  network: string;
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
  network: string;
  outputCount: number;
  totalInputBase: string;
  totalOutputBase: string;
  txIndex: number;
  txid: string;
}

export interface ExplorerTransactionInput {
  address: string;
  label?: ExplorerLabelRef;
  outputKey: string;
  valueBase: string;
}

export interface ExplorerTransactionOutput {
  address: string;
  isSpendable: boolean;
  label?: ExplorerLabelRef;
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
  overlay: {
    labels: ExplorerLabelRef[];
  };
  transaction: ExplorerTransactionSummary;
  transfers: Array<{
    amountBase: string;
    from: string;
    to: string;
  }>;
}

export interface ExplorerSearchResult {
  address?: string;
  balance?: string;
  blockHash?: string;
  blockHeight?: number;
  blockTime?: number;
  hasLabel?: boolean;
  network: string;
  riskLevel?: 'high' | 'low';
  txCount?: number;
  txid?: string;
  type: 'address' | 'block' | 'transaction';
}

export interface ExplorerAddressDetail {
  address: {
    address: string;
    balance: string;
    network: string;
    receivedBase: string;
    sentBase: string;
    txCount: number;
    utxoCount: number;
  };
  overlay: InfoResponse;
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
  network: string;
  scriptType: string;
}
