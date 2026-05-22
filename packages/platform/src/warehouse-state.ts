import {
  type AddressMovement,
  type BlockProjectionBatch,
  type DirectLinkRecord,
  formatAmountBase,
  type ProjectionBalanceCursor,
  type ProjectionBalanceSnapshot,
  type ProjectionUtxoOutput,
  parseAmountBase,
  type SourceLinkRecord,
} from '@onlydoge/indexing-pipeline';
import type { PrimaryId } from '@onlydoge/shared-kernel';

export interface BalanceRow {
  address: string;
  assetAddress: string;
  asOfBlockHeight: number;
  balance: string;
  networkId: PrimaryId;
}

export interface WarehouseState {
  appliedBlocks: Array<{
    blockHash: string;
    blockHeight: number;
    networkId: PrimaryId;
  }>;
  directLinkAppliedBlocks: Array<{
    blockHash: string;
    blockHeight: number;
    networkId: PrimaryId;
  }>;
  addressMovements: AddressMovement[];
  balances: BalanceRow[];
  directLinks: DirectLinkRecord[];
  sourceLinks: SourceLinkRecord[];
  tokens: Array<{
    address: string;
    decimals: number;
    id: string;
    name: string;
    networkId: PrimaryId;
    symbol: string;
  }>;
  transfers: BlockProjectionBatch['transfers'];
  utxoOutputs: ProjectionUtxoOutput[];
}

export interface AddressMovementTotals {
  receivedBase: bigint;
  sentBase: bigint;
  txCount: number;
}

export interface AddressSummary {
  balance: string;
  receivedBase: string;
  sentBase: string;
  txCount: number;
  utxoCount: number;
}

export interface AddressTransactionAggregate {
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  receivedBase: bigint;
  sentBase: bigint;
  txIndex: number;
  txid: string;
}

export interface AddressTransactionSummary {
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  receivedBase: string;
  sentBase: string;
  txIndex: number;
  txid: string;
}

export const emptyWarehouseState = (): WarehouseState => ({
  appliedBlocks: [],
  directLinkAppliedBlocks: [],
  utxoOutputs: [],
  addressMovements: [],
  transfers: [],
  balances: [],
  directLinks: [],
  sourceLinks: [],
  tokens: [],
});

export function mergeWarehouseState(
  input: Partial<WarehouseState> | null | undefined,
): WarehouseState {
  const source = input ?? {};
  return {
    ...emptyWarehouseState(),
    ...source,
    appliedBlocks: rowsOrEmpty(source.appliedBlocks),
    directLinkAppliedBlocks: rowsOrEmpty(source.directLinkAppliedBlocks),
    utxoOutputs: rowsOrEmpty(source.utxoOutputs),
    addressMovements: rowsOrEmpty(source.addressMovements),
    transfers: rowsOrEmpty(source.transfers),
    balances: rowsOrEmpty(source.balances),
    directLinks: rowsOrEmpty(source.directLinks),
    sourceLinks: rowsOrEmpty(source.sourceLinks),
    tokens: rowsOrEmpty(source.tokens),
  };
}

export function currentBalancePageRows(
  balances: BalanceRow[],
  networkId: PrimaryId,
  cursor: ProjectionBalanceCursor | null,
  limit: number,
): BalanceRow[] {
  return balances
    .filter((row) => isCurrentBalancePageRow(row, networkId, cursor))
    .sort(compareBalanceRows)
    .slice(0, limit)
    .map((row) => ({ ...row }));
}

export function currentBalanceNextCursor(
  rows: ProjectionBalanceSnapshot[],
  limit: number,
): ProjectionBalanceCursor | null {
  const last = rows.at(-1);
  return rows.length === limit && last
    ? { address: last.address, assetAddress: last.assetAddress }
    : null;
}

export function summarizeNativeMovements(movements: AddressMovement[]): AddressMovementTotals {
  const txids = new Set<string>();
  let receivedBase = 0n;
  let sentBase = 0n;
  for (const movement of movements) {
    txids.add(movement.txid);
    if (movement.direction === 'credit') {
      receivedBase += parseAmountBase(movement.amountBase);
    } else {
      sentBase += parseAmountBase(movement.amountBase);
    }
  }

  return { receivedBase, sentBase, txCount: txids.size };
}

export function inMemoryAddressSummary(
  balance: string,
  totals: AddressMovementTotals,
  utxoCount: number,
): AddressSummary | null {
  if (totals.txCount === 0 && balance === '0' && utxoCount === 0) {
    return null;
  }

  return {
    balance,
    receivedBase: formatAmountBase(totals.receivedBase),
    sentBase: formatAmountBase(totals.sentBase),
    txCount: totals.txCount,
    utxoCount,
  };
}

export function aggregateAddressTransactions(
  movements: AddressMovement[],
): Map<string, AddressTransactionAggregate> {
  const aggregates = new Map<string, AddressTransactionAggregate>();
  for (const movement of movements) {
    applyMovementToAggregate(aggregates, movement);
  }

  return aggregates;
}

export function paginateAddressTransactions(
  aggregates: Map<string, AddressTransactionAggregate>,
  offset: number,
  limit: number | undefined,
): AddressTransactionSummary[] {
  return [...aggregates.values()]
    .sort(compareAddressTransactionAggregates)
    .slice(offset, paginationEnd(offset, limit))
    .map(formatAddressTransactionAggregate);
}

export function nextBalanceAmount(
  currentBalance: string | undefined,
  movement: AddressMovement,
): bigint {
  const currentAmount = parseAmountBase(currentBalance ?? '0');
  const movementAmount = parseAmountBase(movement.amountBase);
  return movement.direction === 'credit'
    ? currentAmount + movementAmount
    : currentAmount - movementAmount;
}

export function assertNonNegativeBalance(movement: AddressMovement, nextAmount: bigint): void {
  if (nextAmount < 0n) {
    throw new Error(
      `negative balance for ${movement.networkId}:${movement.address}:${movement.assetAddress}`,
    );
  }
}

function rowsOrEmpty<T>(rows: T[] | undefined): T[] {
  return rows ?? [];
}

function isCurrentBalancePageRow(
  row: BalanceRow,
  networkId: PrimaryId,
  cursor: ProjectionBalanceCursor | null,
): boolean {
  return row.networkId === networkId && (cursor === null || isAfterBalanceCursor(row, cursor));
}

function isAfterBalanceCursor(row: BalanceRow, cursor: ProjectionBalanceCursor): boolean {
  return (
    row.address > cursor.address ||
    (row.address === cursor.address && row.assetAddress > cursor.assetAddress)
  );
}

function compareBalanceRows(left: BalanceRow, right: BalanceRow): number {
  return (
    left.address.localeCompare(right.address) || left.assetAddress.localeCompare(right.assetAddress)
  );
}

function applyMovementToAggregate(
  aggregates: Map<string, AddressTransactionAggregate>,
  movement: AddressMovement,
): void {
  const current = aggregates.get(movement.txid) ?? createAddressTransactionAggregate(movement);
  if (movement.direction === 'credit') {
    current.receivedBase += parseAmountBase(movement.amountBase);
  } else {
    current.sentBase += parseAmountBase(movement.amountBase);
  }
  aggregates.set(movement.txid, current);
}

function createAddressTransactionAggregate(movement: AddressMovement): AddressTransactionAggregate {
  return {
    blockHeight: movement.blockHeight,
    blockHash: movement.blockHash,
    blockTime: movement.blockTime,
    txid: movement.txid,
    txIndex: movement.txIndex,
    receivedBase: 0n,
    sentBase: 0n,
  };
}

function compareAddressTransactionAggregates(
  left: AddressTransactionAggregate,
  right: AddressTransactionAggregate,
): number {
  return (
    right.blockHeight - left.blockHeight ||
    right.txIndex - left.txIndex ||
    right.txid.localeCompare(left.txid)
  );
}

function paginationEnd(offset: number, limit: number | undefined): number | undefined {
  return limit === undefined ? undefined : offset + limit;
}

function formatAddressTransactionAggregate(
  row: AddressTransactionAggregate,
): AddressTransactionSummary {
  return {
    blockHash: row.blockHash,
    blockHeight: row.blockHeight,
    blockTime: row.blockTime,
    txid: row.txid,
    txIndex: row.txIndex,
    receivedBase: formatAmountBase(row.receivedBase),
    sentBase: formatAmountBase(row.sentBase),
  };
}
