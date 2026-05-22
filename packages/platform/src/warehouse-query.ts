import type { createClient } from '@clickhouse/client';
import type {
  AddressMovement,
  BlockProjectionBatch,
  DirectLinkRecord,
  ProjectionAppliedBlock,
  ProjectionBalanceCursor,
  ProjectionBalanceSnapshot,
  ProjectionCurrentBalancePage,
  ProjectionCurrentUtxoPage,
  ProjectionPageRequestContext,
  ProjectionUtxoOutput,
} from '@onlydoge/indexing-pipeline';
import type { PrimaryId } from '@onlydoge/shared-kernel';

import type { WarehouseSettings } from './settings';

export type ClickHouseClientOptions = NonNullable<Parameters<typeof createClient>[0]>;
type ClickHouseQueryLimits = {
  maxBytes: number;
  maxValues: number;
};

const maxClickHouseQueryValuesPerChunk = 256;
const maxClickHouseQueryValueBytesPerChunk = 12_000;

export function clickHouseClientOptions(
  settings: WarehouseSettings,
  requestTimeoutMs: number,
): ClickHouseClientOptions {
  const options: ClickHouseClientOptions = {
    url: settings.location,
    request_timeout: requestTimeoutMs,
  };
  assignClickHouseStringOption(options, 'database', settings.database);
  assignClickHouseStringOption(options, 'username', settings.user);
  assignClickHouseStringOption(options, 'password', settings.password);
  return options;
}

export function clickHouseOutputKeyCursorClause(cursorOutputKey: string | null): string {
  return cursorOutputKey === null ? '' : 'AND output_key > {cursorOutputKey:String}';
}

export function clickHouseOutputPageParams(
  networkId: PrimaryId,
  cursorOutputKey: string | null,
  limit: number,
): Record<string, number | string> {
  if (cursorOutputKey === null) {
    return { networkId, limit };
  }

  return {
    networkId,
    limit,
    cursorOutputKey,
  };
}

export function toCurrentUtxoPage(
  rows: ProjectionUtxoOutput[],
  limit: number,
): ProjectionCurrentUtxoPage {
  return {
    rows,
    nextCursor: rows.length === limit ? (rows.at(-1)?.outputKey ?? null) : null,
  };
}

export function clickHouseBalanceCursorClause(cursor: ProjectionBalanceCursor | null): string {
  if (cursor === null) {
    return '';
  }

  return `AND (
    address > {cursorAddress:String}
    OR (address = {cursorAddress:String} AND asset_address > {cursorAssetAddress:String})
  )`;
}

export function clickHouseBalancePageParams(
  networkId: PrimaryId,
  cursor: ProjectionBalanceCursor | null,
  limit: number,
): Record<string, number | string> {
  if (cursor === null) {
    return { networkId, limit };
  }

  return {
    networkId,
    limit,
    cursorAddress: cursor.address,
    cursorAssetAddress: cursor.assetAddress,
  };
}

export function toCurrentBalancePage(
  rows: ProjectionBalanceSnapshot[],
  limit: number,
): ProjectionCurrentBalancePage {
  if (rows.length !== limit) {
    return { rows, nextCursor: null };
  }

  const last = rows[rows.length - 1];
  if (!last) {
    return { rows, nextCursor: null };
  }

  return {
    rows,
    nextCursor: {
      address: last.address,
      assetAddress: last.assetAddress,
    },
  };
}

export function chunkQueryValues<T>(
  values: T[],
  options?: {
    maxBytes?: number;
    maxValues?: number;
  },
): T[][] {
  const limits = clickHouseQueryLimits(options);
  const chunks: T[][] = [];
  let currentChunk: T[] = [];
  let currentBytes = 0;

  for (const value of values) {
    const valueBytes = String(value).length + 3;
    if (shouldStartNextClickHouseChunk(currentChunk, currentBytes, valueBytes, limits)) {
      chunks.push(currentChunk);
      currentChunk = [];
      currentBytes = 0;
    }

    currentChunk.push(value);
    currentBytes += valueBytes;
  }

  if (currentChunk.length > 0) {
    chunks.push(currentChunk);
  }

  return chunks;
}

export function warehouseInfrastructureMessage(error: unknown): string {
  const message = describeWarehouseError(error);
  if (isWarehouseUnavailableMessage(message)) {
    return 'warehouse unavailable';
  }

  if (isWarehouseTimeoutMessage(message)) {
    return 'warehouse request timed out';
  }

  if (isWarehouseMemoryLimitMessage(message)) {
    return 'warehouse query exceeded memory limit';
  }

  return 'warehouse query failed';
}

export function createAbortableRequestContext(signal: AbortSignal | undefined, timeoutMs: number) {
  const controller = new AbortController();
  let timedOut = false;
  let listener: (() => void) | null = null;

  if (signal) {
    if (signal.aborted) {
      controller.abort(signal.reason);
    } else {
      listener = () => controller.abort(signal.reason);
      signal.addEventListener('abort', listener, { once: true });
    }
  }

  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort(new Error(`warehouse request timed out after ${timeoutMs}ms`));
  }, timeoutMs);
  timer.unref?.();

  return {
    signal: controller.signal,
    didTimeout: () => timedOut,
    cleanup: () => {
      clearTimeout(timer);
      if (signal && listener) {
        signal.removeEventListener('abort', listener);
      }
    },
  };
}

export function queryTimeoutMs(
  context: ProjectionPageRequestContext | undefined,
  defaultTimeoutMs: number,
): number {
  return context?.timeoutMs ?? defaultTimeoutMs;
}

export function toClickHouseMaxExecutionTimeSeconds(timeoutMs: number): number {
  return Math.max(1, Math.ceil(timeoutMs / 1000));
}

export function formatBalanceTupleList(keys: string[]): string {
  return formatTupleList(
    keys.map((key) => {
      const [, address = '', assetAddress = ''] = key.split(':');
      return [address, assetAddress];
    }),
  );
}

export function formatDirectLinkTupleList(keys: string[]): string {
  return formatTupleList(
    keys.map((key) => {
      const [, fromAddress = '', toAddress = '', assetAddress = ''] = key.split(':');
      return [fromAddress, toAddress, assetAddress];
    }),
  );
}

export function clickHousePagination(
  offset: number,
  limit: number | undefined,
): {
  limitClause: string;
  offsetClause: string;
  queryParams: Record<string, number>;
} {
  return {
    limitClause: clickHouseLimitClause(limit),
    offsetClause: clickHouseOffsetClause(offset),
    queryParams: clickHousePaginationParams(offset, limit),
  };
}

export function toUtxoInsertRow(
  row: ProjectionUtxoOutput,
  version: number,
): Record<string, unknown> {
  return {
    network_id: row.networkId,
    block_height: row.blockHeight,
    block_hash: row.blockHash,
    block_time: row.blockTime,
    txid: row.txid,
    tx_index: row.txIndex,
    vout: row.vout,
    output_key: row.outputKey,
    address: row.address,
    script_type: row.scriptType,
    value_base: row.valueBase,
    is_coinbase: row.isCoinbase ? 1 : 0,
    is_spendable: row.isSpendable ? 1 : 0,
    spent_by_txid: row.spentByTxid,
    spent_in_block: row.spentInBlock,
    spent_input_index: row.spentInputIndex,
    version,
  };
}

export function toAddressMovementInsertRow(row: AddressMovement): Record<string, unknown> {
  return {
    movement_id: row.movementId,
    network_id: row.networkId,
    block_height: row.blockHeight,
    block_hash: row.blockHash,
    block_time: row.blockTime,
    txid: row.txid,
    tx_index: row.txIndex,
    entry_index: row.entryIndex,
    address: row.address,
    asset_address: row.assetAddress,
    direction: row.direction,
    amount_base: row.amountBase,
    output_key: row.outputKey,
    derivation_method: row.derivationMethod,
  };
}

export function toTransferInsertRow(
  row: BlockProjectionBatch['transfers'][number],
): Record<string, unknown> {
  return {
    transfer_id: row.transferId,
    network_id: row.networkId,
    block_height: row.blockHeight,
    block_hash: row.blockHash,
    block_time: row.blockTime,
    txid: row.txid,
    tx_index: row.txIndex,
    transfer_index: row.transferIndex,
    asset_address: row.assetAddress,
    from_address: row.fromAddress,
    to_address: row.toAddress,
    amount_base: row.amountBase,
    derivation_method: row.derivationMethod,
    confidence: row.confidence,
    is_change: row.isChange ? 1 : 0,
    input_address_count: row.inputAddressCount,
    output_address_count: row.outputAddressCount,
  };
}

export function toBalanceInsertRow(
  row: ProjectionBalanceSnapshot,
  version: number,
): Record<string, unknown> {
  return {
    network_id: row.networkId,
    address: row.address,
    asset_address: row.assetAddress,
    balance: row.balance,
    as_of_block_height: row.asOfBlockHeight,
    version,
  };
}

export function toDirectLinkInsertRow(
  row: DirectLinkRecord,
  version: number,
): Record<string, unknown> {
  return {
    network_id: row.networkId,
    from_address: row.fromAddress,
    to_address: row.toAddress,
    asset_address: row.assetAddress,
    transfer_count: row.transferCount,
    total_amount_base: row.totalAmountBase,
    first_seen_block_height: row.firstSeenBlockHeight,
    last_seen_block_height: row.lastSeenBlockHeight,
    version,
  };
}

export function toAppliedBlockInsertRow(row: ProjectionAppliedBlock): Record<string, unknown> {
  return {
    network_id: row.networkId,
    block_height: row.blockHeight,
    block_hash: row.blockHash,
  };
}

export function formatClickHouseStringLiteral(value: string): string {
  return `'${value.replaceAll('\\', '\\\\').replaceAll("'", "\\'")}'`;
}

function assignClickHouseStringOption(
  options: ClickHouseClientOptions,
  key: 'database' | 'password' | 'username',
  value: string | undefined,
): void {
  if (value !== undefined) {
    options[key] = value;
  }
}

function clickHouseQueryLimits(options?: {
  maxBytes?: number;
  maxValues?: number;
}): ClickHouseQueryLimits {
  return {
    maxBytes: numberOrDefault(options?.maxBytes, maxClickHouseQueryValueBytesPerChunk),
    maxValues: numberOrDefault(options?.maxValues, maxClickHouseQueryValuesPerChunk),
  };
}

function shouldStartNextClickHouseChunk<T>(
  chunk: T[],
  chunkBytes: number,
  valueBytes: number,
  limits: ClickHouseQueryLimits,
): boolean {
  if (chunk.length === 0) {
    return false;
  }

  return chunk.length >= limits.maxValues || chunkBytes + valueBytes > limits.maxBytes;
}

function describeWarehouseError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  return errorMessageProperty(error) ?? String(error);
}

function errorMessageProperty(error: unknown): string | null {
  if (!hasMessageProperty(error)) {
    return null;
  }

  return stringOrNull(Reflect.get(error, 'message'));
}

function hasMessageProperty(error: unknown): error is { message: unknown } {
  return typeof error === 'object' && error !== null && 'message' in error;
}

function stringOrNull(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}

function isWarehouseUnavailableMessage(message: string): boolean {
  return ['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'socket hang up', 'EAI_AGAIN'].some((needle) =>
    message.includes(needle),
  );
}

function isWarehouseTimeoutMessage(message: string): boolean {
  return ['request timed out', 'The operation was aborted', 'AbortError'].some((needle) =>
    message.includes(needle),
  );
}

function isWarehouseMemoryLimitMessage(message: string): boolean {
  return (
    message.includes('MEMORY_LIMIT_EXCEEDED') || message.includes('User memory limit exceeded')
  );
}

function formatTupleList(rows: string[][]): string {
  return `(${rows.map((row) => `(${row.map(formatClickHouseStringLiteral).join(', ')})`).join(', ')})`;
}

function numberOrDefault(value: number | undefined, fallback: number): number {
  return value ?? fallback;
}

function clickHouseLimitClause(limit: number | undefined): string {
  return limit === undefined ? '' : 'LIMIT {limit:UInt64}';
}

function clickHouseOffsetClause(offset: number): string {
  return offset > 0 ? 'OFFSET {offset:UInt64}' : '';
}

function clickHousePaginationParams(
  offset: number,
  limit: number | undefined,
): Record<string, number> {
  const queryParams: Record<string, number> = {};
  addDefinedQueryParam(queryParams, 'limit', limit);
  addPositiveQueryParam(queryParams, 'offset', offset);
  return queryParams;
}

function addDefinedQueryParam(
  queryParams: Record<string, number>,
  key: string,
  value: number | undefined,
): void {
  if (value !== undefined) {
    queryParams[key] = value;
  }
}

function addPositiveQueryParam(
  queryParams: Record<string, number>,
  key: string,
  value: number,
): void {
  if (value > 0) {
    queryParams[key] = value;
  }
}
