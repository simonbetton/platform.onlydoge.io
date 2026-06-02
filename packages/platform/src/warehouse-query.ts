import type { createClient } from '@clickhouse/client';
import type {
  AddressMovement,
  ProjectionAppliedBlock,
  ProjectionBalanceCursor,
  ProjectionBalanceSnapshot,
  ProjectionCurrentBalancePage,
  ProjectionCurrentUtxoPage,
  ProjectionPageRequestContext,
  ProjectionUtxoOutput,
} from '@onlydoge/indexing-pipeline';

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
  credentials?: { password?: string; user?: string },
): ClickHouseClientOptions {
  const options: ClickHouseClientOptions = {
    url: settings.location,
    request_timeout: requestTimeoutMs,
  };
  assignClickHouseStringOption(options, 'database', settings.database);
  assignClickHouseStringOption(options, 'username', credentials?.user ?? settings.user);
  assignClickHouseStringOption(options, 'password', credentials?.password ?? settings.password);
  return options;
}

export function clickHouseOutputKeyCursorClause(cursorOutputKey: string | null): string {
  return cursorOutputKey === null ? '' : 'AND output_key > {cursorOutputKey:String}';
}

export function clickHouseOutputPageParams(
  cursorOutputKey: string | null,
  limit: number,
): Record<string, number | string> {
  if (cursorOutputKey === null) {
    return { limit };
  }

  return {
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
    nextCursor: currentUtxoNextCursor(rows, limit),
  };
}

function currentUtxoNextCursor(rows: ProjectionUtxoOutput[], limit: number): string | null {
  if (rows.length !== limit) {
    return null;
  }

  return currentUtxoLastOutputKey(rows);
}

function currentUtxoLastOutputKey(rows: ProjectionUtxoOutput[]): string | null {
  const last = rows.at(-1);
  return last ? last.outputKey : null;
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
  cursor: ProjectionBalanceCursor | null,
  limit: number,
): Record<string, number | string> {
  if (cursor === null) {
    return { limit };
  }

  return {
    limit,
    cursorAddress: cursor.address,
    cursorAssetAddress: cursor.assetAddress,
  };
}

export function toCurrentBalancePage(
  rows: ProjectionBalanceSnapshot[],
  limit: number,
): ProjectionCurrentBalancePage {
  return {
    rows,
    nextCursor: currentBalanceNextCursor(rows, limit),
  };
}

function currentBalanceNextCursor(
  rows: ProjectionBalanceSnapshot[],
  limit: number,
): ProjectionBalanceCursor | null {
  const last = rows.at(-1);
  if (!hasCurrentBalanceNextCursor(rows, limit, last)) {
    return null;
  }

  return {
    address: last.address,
    assetAddress: last.assetAddress,
  };
}

function hasCurrentBalanceNextCursor(
  rows: ProjectionBalanceSnapshot[],
  limit: number,
  last: ProjectionBalanceSnapshot | undefined,
): last is ProjectionBalanceSnapshot {
  return [rows.length === limit, last !== undefined].every(Boolean);
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
    const nextChunk = nextClickHouseChunk(currentChunk, currentBytes, valueBytes, limits);
    currentChunk = nextChunk.currentChunk;
    currentBytes = nextChunk.currentBytes;
    appendChunkIfStarted(chunks, nextChunk.startedChunk);

    currentChunk.push(value);
    currentBytes += valueBytes;
  }

  appendNonEmptyChunk(chunks, currentChunk);

  return chunks;
}

function appendNonEmptyChunk<T>(chunks: T[][], chunk: T[]): void {
  if (chunk.length > 0) {
    chunks.push(chunk);
  }
}

function nextClickHouseChunk<T>(
  currentChunk: T[],
  currentBytes: number,
  valueBytes: number,
  limits: ClickHouseQueryLimits,
): { currentBytes: number; currentChunk: T[]; startedChunk: T[] | null } {
  if (!shouldStartNextClickHouseChunk(currentChunk, currentBytes, valueBytes, limits)) {
    return { currentChunk, currentBytes, startedChunk: null };
  }

  return { currentChunk: [], currentBytes: 0, startedChunk: currentChunk };
}

function appendChunkIfStarted<T>(chunks: T[][], chunk: T[] | null): void {
  if (chunk) {
    chunks.push(chunk);
  }
}

export function warehouseInfrastructureMessage(error: unknown): string {
  const message = describeWarehouseError(error);
  return warehouseInfrastructureLabel(
    warehouseMessageClassifiers.find((classifier) => classifier.matches(message)),
  );
}

function warehouseInfrastructureLabel(
  classifier: (typeof warehouseMessageClassifiers)[number] | undefined,
): string {
  if (!classifier) {
    return 'warehouse query failed';
  }

  return classifier.label;
}

const warehouseMessageClassifiers = [
  { label: 'warehouse unavailable', matches: isWarehouseUnavailableMessage },
  { label: 'warehouse request timed out', matches: isWarehouseTimeoutMessage },
  { label: 'warehouse query exceeded memory limit', matches: isWarehouseMemoryLimitMessage },
];

export function createAbortableRequestContext(signal: AbortSignal | undefined, timeoutMs: number) {
  const controller = new AbortController();
  let timedOut = false;
  let listener: (() => void) | null = null;

  listener = attachParentAbortSignal(signal, controller);

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
      removeParentAbortSignal(signal, listener);
    },
  };
}

function attachParentAbortSignal(
  signal: AbortSignal | undefined,
  controller: AbortController,
): (() => void) | null {
  if (!signal) {
    return null;
  }

  return attachKnownParentAbortSignal(signal, controller);
}

function attachKnownParentAbortSignal(
  signal: AbortSignal,
  controller: AbortController,
): (() => void) | null {
  if (signal.aborted) {
    controller.abort(signal.reason);
    return null;
  }

  const listener = () => controller.abort(signal.reason);
  signal.addEventListener('abort', listener, { once: true });
  return listener;
}

function removeParentAbortSignal(
  signal: AbortSignal | undefined,
  listener: (() => void) | null,
): void {
  if (!signal) {
    return;
  }

  removeKnownParentAbortSignal(signal, listener);
}

function removeKnownParentAbortSignal(signal: AbortSignal, listener: (() => void) | null): void {
  if (listener) {
    signal.removeEventListener('abort', listener);
  }
}

export function queryTimeoutMs(
  context: ProjectionPageRequestContext | undefined,
  defaultTimeoutMs: number,
): number {
  return contextTimeoutMs(context) ?? defaultTimeoutMs;
}

function contextTimeoutMs(context: ProjectionPageRequestContext | undefined): number | undefined {
  return context?.timeoutMs;
}

export function toClickHouseMaxExecutionTimeSeconds(timeoutMs: number): number {
  return Math.max(1, Math.ceil(timeoutMs / 1000));
}

export function formatBalanceTupleList(keys: string[]): string {
  return formatTupleList(
    keys.map((key) => {
      const [address = '', assetAddress = ''] = key.split(':');
      return [address, assetAddress];
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
    is_coinbase: clickHouseBoolean(row.isCoinbase),
    is_spendable: clickHouseBoolean(row.isSpendable),
    spent_by_txid: row.spentByTxid,
    spent_in_block: row.spentInBlock,
    spent_input_index: row.spentInputIndex,
    version,
  };
}

function clickHouseBoolean(value: boolean): 0 | 1 {
  return value ? 1 : 0;
}

export function toAddressMovementInsertRow(row: AddressMovement): Record<string, unknown> {
  return {
    movement_id: row.movementId,
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

export function toBalanceInsertRow(
  row: ProjectionBalanceSnapshot,
  version: number,
): Record<string, unknown> {
  return {
    address: row.address,
    asset_address: row.assetAddress,
    balance: row.balance,
    as_of_block_height: row.asOfBlockHeight,
    version,
  };
}

export function toAnalyticsBalanceCurrentInsertRow(
  row: ProjectionBalanceSnapshot,
  version: number,
): Record<string, unknown> {
  return {
    address: row.address,
    asset_address: row.assetAddress,
    balance: row.balance,
    as_of_block_height: row.asOfBlockHeight,
    version,
  };
}

export function toAppliedBlockInsertRow(row: ProjectionAppliedBlock): Record<string, unknown> {
  return {
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
    maxBytes: numberOrDefault(optionalMaxBytes(options), maxClickHouseQueryValueBytesPerChunk),
    maxValues: numberOrDefault(optionalMaxValues(options), maxClickHouseQueryValuesPerChunk),
  };
}

function optionalMaxBytes(options?: { maxBytes?: number }): number | undefined {
  return options?.maxBytes;
}

function optionalMaxValues(options?: { maxValues?: number }): number | undefined {
  return options?.maxValues;
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

  return [chunk.length >= limits.maxValues, chunkBytes + valueBytes > limits.maxBytes].some(
    Boolean,
  );
}

function describeWarehouseError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  return warehouseErrorFallbackMessage(error);
}

function warehouseErrorFallbackMessage(error: unknown): string {
  return errorMessageProperty(error) ?? String(error);
}

function errorMessageProperty(error: unknown): string | null {
  if (!hasMessageProperty(error)) {
    return null;
  }

  return stringOrNull(Reflect.get(error, 'message'));
}

function hasMessageProperty(error: unknown): error is { message: unknown } {
  return [typeof error === 'object', error !== null, hasProperty(error, 'message')].every(Boolean);
}

function hasProperty(value: unknown, key: string): boolean {
  return Object(value) === value && key in Object(value);
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
  return ['MEMORY_LIMIT_EXCEEDED', 'User memory limit exceeded'].some((needle) =>
    message.includes(needle),
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
