import type { AnalyticsTransactionFact } from '@onlydoge/analytics-query';
import {
  type AddressMovement,
  formatAmountBase,
  type ProjectionBalanceCursor,
  type ProjectionBalanceSnapshot,
  type ProjectionUtxoOutput,
  parseAmountBase,
} from '@onlydoge/indexing-pipeline';

export interface BalanceRow {
  address: string;
  assetAddress: string;
  asOfBlockHeight: number;
  balance: string;
}

export interface WarehouseState {
  appliedBlocks: Array<{
    blockHash: string;
    blockHeight: number;
  }>;
  addressMovements: AddressMovement[];
  balances: BalanceRow[];
  transactionFacts: AnalyticsTransactionFact[];
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
  utxoOutputs: [],
  addressMovements: [],
  transactionFacts: [],
  balances: [],
});

export function mergeWarehouseState(
  input: Partial<WarehouseState> | null | undefined,
): WarehouseState {
  const source = input ?? {};
  return {
    ...emptyWarehouseState(),
    ...source,
    appliedBlocks: rowsOrEmpty(source.appliedBlocks),
    utxoOutputs: rowsOrEmpty(source.utxoOutputs),
    addressMovements: rowsOrEmpty(source.addressMovements),
    transactionFacts: rowsOrEmpty(source.transactionFacts),
    balances: rowsOrEmpty(source.balances),
  };
}

export function currentBalancePageRows(
  balances: BalanceRow[],
  cursor: ProjectionBalanceCursor | null,
  limit: number,
): BalanceRow[] {
  return balances
    .filter((row) => isCurrentBalancePageRow(row, cursor))
    .sort(compareBalanceRows)
    .slice(0, limit)
    .map((row) => ({ ...row }));
}

export function currentBalanceNextCursor(
  rows: ProjectionBalanceSnapshot[],
  limit: number,
): ProjectionBalanceCursor | null {
  const last = rows.at(-1);
  if (!hasNextBalanceCursor(rows, limit, last)) {
    return null;
  }

  return { address: last.address, assetAddress: last.assetAddress };
}

function hasNextBalanceCursor(
  rows: ProjectionBalanceSnapshot[],
  limit: number,
  last: ProjectionBalanceSnapshot | undefined,
): last is ProjectionBalanceSnapshot {
  return [rows.length === limit, last !== undefined].every(Boolean);
}

export function summarizeNativeMovements(movements: AddressMovement[]): AddressMovementTotals {
  const txids = new Set<string>();
  let receivedBase = 0n;
  let sentBase = 0n;
  for (const movement of movements) {
    txids.add(movement.txid);
    const nextTotals = nativeMovementTotals(movement, receivedBase, sentBase);
    receivedBase = nextTotals.receivedBase;
    sentBase = nextTotals.sentBase;
  }

  return { receivedBase, sentBase, txCount: txids.size };
}

function nativeMovementTotals(
  movement: AddressMovement,
  receivedBase: bigint,
  sentBase: bigint,
): Pick<AddressMovementTotals, 'receivedBase' | 'sentBase'> {
  if (movement.direction === 'credit') {
    return {
      receivedBase: receivedBase + parseAmountBase(movement.amountBase),
      sentBase,
    };
  }

  return {
    receivedBase,
    sentBase: sentBase + parseAmountBase(movement.amountBase),
  };
}

export function inMemoryAddressSummary(
  balance: string,
  totals: AddressMovementTotals,
  utxoCount: number,
): AddressSummary | null {
  if (isEmptyAddressSummary(balance, totals, utxoCount)) {
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

function isEmptyAddressSummary(
  balance: string,
  totals: AddressMovementTotals,
  utxoCount: number,
): boolean {
  return [totals.txCount === 0, balance === '0', utxoCount === 0].every(Boolean);
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
  return applyMovementAmount(currentAmount, movementAmount, movement.direction);
}

function applyMovementAmount(
  currentAmount: bigint,
  movementAmount: bigint,
  direction: AddressMovement['direction'],
): bigint {
  return direction === 'credit' ? currentAmount + movementAmount : currentAmount - movementAmount;
}

export function assertNonNegativeBalance(movement: AddressMovement, nextAmount: bigint): void {
  if (nextAmount < 0n) {
    throw new Error(`negative balance for ${movement.address}:${movement.assetAddress}`);
  }
}

function rowsOrEmpty<T>(rows: T[] | undefined): T[] {
  return rows ?? [];
}

function isCurrentBalancePageRow(row: BalanceRow, cursor: ProjectionBalanceCursor | null): boolean {
  return isCurrentBalanceCursorRow(row, cursor);
}

function isAfterBalanceCursor(row: BalanceRow, cursor: ProjectionBalanceCursor): boolean {
  return [row.address > cursor.address, isAfterSameAddressBalanceCursor(row, cursor)].some(Boolean);
}

function isCurrentBalanceCursorRow(
  row: BalanceRow,
  cursor: ProjectionBalanceCursor | null,
): boolean {
  return cursor === null ? true : isAfterBalanceCursor(row, cursor);
}

function isAfterSameAddressBalanceCursor(
  row: BalanceRow,
  cursor: ProjectionBalanceCursor,
): boolean {
  return [row.address === cursor.address, row.assetAddress > cursor.assetAddress].every(Boolean);
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
  const current = applyMovementAmountToAggregate(
    aggregates.get(movement.txid) ?? createAddressTransactionAggregate(movement),
    movement,
  );
  aggregates.set(movement.txid, current);
}

function applyMovementAmountToAggregate(
  current: AddressTransactionAggregate,
  movement: AddressMovement,
): AddressTransactionAggregate {
  if (movement.direction === 'credit') {
    current.receivedBase += parseAmountBase(movement.amountBase);
    return current;
  }

  current.sentBase += parseAmountBase(movement.amountBase);
  return current;
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
  return firstNonZeroComparison([
    right.blockHeight - left.blockHeight,
    right.txIndex - left.txIndex,
    right.txid.localeCompare(left.txid),
  ]);
}

function firstNonZeroComparison(values: number[]): number {
  return values.find((value) => value !== 0) ?? 0;
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
