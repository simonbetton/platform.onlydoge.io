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
  processed: number;
  synced: number;
}

export interface ExplorerBlockSummary {
  hash: string;
  height: number;
  network: string;
  time: number;
  txCount: number;
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

export interface ExplorerHdWalletBalance {
  balanceBase: string;
  cache: {
    expiresAt: string;
    hit: boolean;
    ttlSeconds: number;
  };
  chains: ExplorerHdWalletChainBalance[];
  complete: boolean;
  gapLimit: number;
  network: string;
  scannedAddressCount: number;
  usedAddressCount: number;
  utxoCount: number;
  xpubDepth: number;
  xpubFingerprint: number;
}

export interface ExplorerHdWalletChainBalance {
  balanceBase: string;
  chain: number;
  complete: boolean;
  gapLimit: number;
  lastScannedIndex: number | null;
  lastUsedIndex: number | null;
  nextUnusedIndex: number;
  role: 'receive' | 'change';
  scannedAddressCount: number;
  usedAddressCount: number;
  usedAddresses: ExplorerHdWalletUsedAddress[];
  utxoCount: number;
}

export interface ExplorerHdWalletUsedAddress {
  address: string;
  balance: string;
  chain: number;
  index: number;
  path: string;
  receivedBase: string;
  role: 'receive' | 'change';
  sentBase: string;
  txCount: number;
  utxoCount: number;
}

export interface ExplorerAddressUtxo
  extends Pick<
    ProjectionUtxoOutput,
    | 'blockHash'
    | 'blockHeight'
    | 'blockTime'
    | 'outputKey'
    | 'scriptPubKey'
    | 'spentByTxid'
    | 'spentInBlock'
    | 'txid'
    | 'txIndex'
    | 'valueBase'
    | 'vout'
  > {
  address: string;
  confirmations: number;
  network: string;
  scriptType: string;
}
