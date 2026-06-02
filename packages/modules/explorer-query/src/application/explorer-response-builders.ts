import type { DogecoinVout, ProjectionUtxoOutput } from '@onlydoge/indexing-pipeline';

import type { ExplorerAddressDetail, ExplorerSearchResult } from '../domain/query-models';

export type ExplorerAddressSummary = ExplorerAddressDetail['address'];

export interface WarehouseAddressSummary {
  balance: string;
  receivedBase: string;
  sentBase: string;
  txCount: number;
  utxoCount: number;
}

export function outputIndex(output: DogecoinVout, fallback: number): number {
  return output.n ?? fallback;
}

export function outputScriptType(output: DogecoinVout): string {
  return output.scriptPubKey?.type?.trim() ?? '';
}

export function spentByTxid(output: ProjectionUtxoOutput | undefined): string | null {
  return output ? output.spentByTxid : null;
}

export function spentInBlock(output: ProjectionUtxoOutput | undefined): number | null {
  return output ? output.spentInBlock : null;
}

export function addressSearchResult(
  address: string,
  summary: WarehouseAddressSummary | null,
): ExplorerSearchResult | null {
  if (!summary) {
    return null;
  }

  return {
    type: 'address',
    address,
    ...addressSearchSummaryFields(summary),
  };
}

export function addressDetail(
  address: string,
  summary: WarehouseAddressSummary | null,
): ExplorerAddressSummary {
  if (!summary) {
    return {
      address,
      balance: '0',
      receivedBase: '0',
      sentBase: '0',
      txCount: 0,
      utxoCount: 0,
    };
  }

  return {
    address,
    balance: summary.balance,
    receivedBase: summary.receivedBase,
    sentBase: summary.sentBase,
    txCount: summary.txCount,
    utxoCount: summary.utxoCount,
  };
}

function addressSearchSummaryFields(
  summary: WarehouseAddressSummary | null,
): Pick<ExplorerSearchResult, 'balance' | 'txCount'> {
  return summary ? { balance: summary.balance, txCount: summary.txCount } : {};
}
