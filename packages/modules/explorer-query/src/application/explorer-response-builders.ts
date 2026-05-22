import {
  allocateProRataAmount,
  type DogecoinVout,
  formatAmountBase,
  type ProjectionUtxoOutput,
  parseAmountBase,
} from '@onlydoge/indexing-pipeline';

import type {
  ExplorerAddressDetail,
  ExplorerLabelRef,
  ExplorerSearchResult,
  ExplorerTransactionInput,
  ExplorerTransactionOutput,
  ExplorerTransactionSummary,
} from '../domain/query-models';

export type ExplorerAddressSummary = ExplorerAddressDetail['address'];

export interface WarehouseAddressSummary {
  balance: string;
  receivedBase: string;
  sentBase: string;
  txCount: number;
  utxoCount: number;
}

export interface LabeledTransactionParts {
  inputs: ExplorerTransactionInput[];
  outputs: ExplorerTransactionOutput[];
}

export interface ExplorerTransferBasis {
  addresses: string[];
  inputTotals: Map<string, bigint>;
  totalInput: bigint;
}

export interface ExplorerProjectedTransfer {
  amountBase: string;
  from: string;
  to: string;
}

export function withTransactionLabels(
  inputs: ExplorerTransactionInput[],
  outputs: ExplorerTransactionOutput[],
  labelMap: Map<string, ExplorerLabelRef>,
): LabeledTransactionParts {
  return {
    inputs: inputs.map((input) => withInputLabel(input, labelMap.get(input.address))),
    outputs: outputs.map((output) => withOutputLabel(output, labelMap.get(output.address))),
  };
}

export function outputIndex(output: DogecoinVout, fallback: number): number {
  return output.n ?? fallback;
}

export function outputScriptType(output: DogecoinVout): string {
  return output.scriptPubKey?.type?.trim() ?? '';
}

export function spentByTxid(output: ProjectionUtxoOutput | undefined): string | null {
  return output?.spentByTxid ?? null;
}

export function spentInBlock(output: ProjectionUtxoOutput | undefined): number | null {
  return output?.spentInBlock ?? null;
}

export function addressSearchResult(
  networkId: string,
  address: string,
  summary: WarehouseAddressSummary | null,
  label: ExplorerLabelRef | undefined,
): ExplorerSearchResult | null {
  if (isEmptyAddressSearchResult(summary, label)) {
    return null;
  }

  return {
    type: 'address',
    network: networkId,
    address,
    hasLabel: Boolean(label),
    ...addressSearchSummaryFields(summary),
    ...addressSearchLabelFields(label),
  };
}

export function addressDetail(
  network: string,
  address: string,
  summary: WarehouseAddressSummary | null,
): ExplorerAddressSummary {
  if (!summary) {
    return {
      network,
      address,
      balance: '0',
      receivedBase: '0',
      sentBase: '0',
      txCount: 0,
      utxoCount: 0,
    };
  }

  return {
    network,
    address,
    balance: summary.balance,
    receivedBase: summary.receivedBase,
    sentBase: summary.sentBase,
    txCount: summary.txCount,
    utxoCount: summary.utxoCount,
  };
}

export function buildExplorerTransferBasis(
  summary: ExplorerTransactionSummary,
  inputs: ExplorerTransactionInput[],
  outputs: ExplorerTransactionOutput[],
): ExplorerTransferBasis | null {
  if (!hasExplorerTransferEndpoints(summary, inputs, outputs)) {
    return null;
  }

  const inputTotals = explorerInputTotals(inputs);

  return {
    inputTotals,
    totalInput: [...inputTotals.values()].reduce((sum, value) => sum + value, 0n),
    addresses: [...inputTotals.keys()].sort((left, right) => left.localeCompare(right)),
  };
}

export function projectExplorerTransfers(
  outputs: ExplorerTransactionOutput[],
  basis: ExplorerTransferBasis,
): ExplorerProjectedTransfer[] {
  const transfers: ExplorerProjectedTransfer[] = [];

  for (const output of outputs) {
    appendExplorerTransfersForOutput(output, basis, transfers);
  }

  return transfers;
}

function withInputLabel(
  input: ExplorerTransactionInput,
  label: ExplorerLabelRef | undefined,
): ExplorerTransactionInput {
  return label ? { ...input, label } : { ...input };
}

function withOutputLabel(
  output: ExplorerTransactionOutput,
  label: ExplorerLabelRef | undefined,
): ExplorerTransactionOutput {
  return label ? { ...output, label } : { ...output };
}

function isEmptyAddressSearchResult(
  summary: WarehouseAddressSummary | null,
  label: ExplorerLabelRef | undefined,
): boolean {
  return !summary && !label;
}

function addressSearchSummaryFields(
  summary: WarehouseAddressSummary | null,
): Pick<ExplorerSearchResult, 'balance' | 'txCount'> {
  return summary ? { balance: summary.balance, txCount: summary.txCount } : {};
}

function addressSearchLabelFields(
  label: ExplorerLabelRef | undefined,
): Pick<ExplorerSearchResult, 'riskLevel'> {
  return label ? { riskLevel: label.riskLevel } : {};
}

function hasExplorerTransferEndpoints(
  summary: ExplorerTransactionSummary,
  inputs: ExplorerTransactionInput[],
  outputs: ExplorerTransactionOutput[],
): boolean {
  return !summary.isCoinbase && inputs.length > 0 && outputs.length > 0;
}

function explorerInputTotals(inputs: ExplorerTransactionInput[]): Map<string, bigint> {
  const inputTotals = new Map<string, bigint>();
  for (const input of inputs) {
    const current = inputTotals.get(input.address) ?? 0n;
    inputTotals.set(input.address, current + parseAmountBase(input.valueBase));
  }

  return inputTotals;
}

function appendExplorerTransfersForOutput(
  output: ExplorerTransactionOutput,
  basis: ExplorerTransferBasis,
  transfers: ExplorerProjectedTransfer[],
): void {
  if (!output.address) {
    return;
  }

  const allocations = allocateProRataAmount(
    parseAmountBase(output.valueBase),
    basis.addresses,
    basis.inputTotals,
    basis.totalInput,
  );
  for (const allocation of allocations) {
    if (allocation.amount <= 0n) {
      continue;
    }

    transfers.push({
      from: allocation.address,
      to: output.address,
      amountBase: formatAmountBase(allocation.amount),
    });
  }
}
