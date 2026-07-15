import { AsyncLocalStorage } from 'node:async_hooks';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';

import { createClient } from '@clickhouse/client';
import {
  type AnalyticsQueryColumn,
  type AnalyticsQueryEstimate,
  type AnalyticsQueryExecutionResult,
  type AnalyticsQueryLimits,
  type AnalyticsQueryParams,
  type AnalyticsTransactionFact,
  type AnalyticsWarehousePort,
  analyticsBalancesCurrentTable,
  analyticsQueryMaxResultRows,
  analyticsTransactionsTable,
  mempoolSamplesTable,
} from '@onlydoge/analytics-query';
import type { ExplorerCreatedUtxoOutput, ExplorerWarehousePort } from '@onlydoge/explorer-query';
import {
  type AddressMovement,
  type BlockProjectionBatch,
  buildProjectionStateChanges,
  type CoreDogecoinApplyContext,
  type CoreDogecoinApplyResult,
  type CoreDogecoinBlockApplication,
  type CoreWindowInsertStage,
  formatAmountBase,
  mapWithConcurrency,
  type ProjectionAppliedBlock,
  type ProjectionBalanceCursor,
  type ProjectionBalanceSnapshot,
  type ProjectionCurrentBalancePage,
  type ProjectionCurrentUtxoPage,
  type ProjectionFactWarehousePort,
  type ProjectionFactWindow,
  type ProjectionPageRequestContext,
  type ProjectionStateBootstrapSnapshot,
  type ProjectionStateStorePort,
  type ProjectionUtxoOutput,
  type ProjectionWarehousePort,
  projectionBalanceSnapshotKey,
  projectionBlockIdentity,
  resolvePendingProjectionWindow,
  toProjectionAppliedBlocks,
} from '@onlydoge/indexing-pipeline';
import {
  InfrastructureError,
  noopServiceLogger,
  type ServiceLogger,
} from '@onlydoge/shared-kernel';

import {
  buildCoreCurrentStateOutputKeyRanges,
  clickHouseCoreDogecoinTables,
  clickHouseStringRangeClause,
  clickHouseStringRangeParams,
} from './clickhouse-core-dogecoin';
import { runClickHouseMigrations } from './clickhouse-migrations';
import type { ClickHouseCoreDogecoinStore } from './core-dogecoin-state-store';
import type { SchemaLockPort } from './schema-lock';
import type { WarehouseSettings } from './settings';
import {
  chunkQueryValues,
  clickHouseBalanceCursorClause,
  clickHouseBalancePageParams,
  clickHouseClientOptions,
  clickHouseOutputKeyCursorClause,
  clickHouseOutputPageParams,
  clickHousePagination,
  createAbortableRequestContext,
  formatBalanceTupleList,
  queryTimeoutMs,
  toAddressMovementInsertRow,
  toAnalyticsBalanceCurrentInsertRow,
  toAppliedBlockInsertRow,
  toBalanceInsertRow,
  toClickHouseMaxExecutionTimeSeconds,
  toCurrentBalancePage,
  toCurrentUtxoPage,
  toUtxoInsertRow,
  warehouseInfrastructureMessage,
} from './warehouse-query';
import {
  aggregateAddressTransactions,
  assertNonNegativeBalance,
  type BalanceRow,
  currentBalanceNextCursor,
  currentBalancePageRows,
  emptyWarehouseState,
  inMemoryAddressSummary,
  mergeWarehouseState,
  nextBalanceAmount,
  paginateAddressTransactions,
  summarizeNativeMovements,
  type WarehouseState,
} from './warehouse-state';

interface VersionedBalanceRow extends BalanceRow {
  version: number;
}

export interface MempoolSampleRow {
  entryTime: number | null;
  feeBase: string | null;
  feeRateBasePerKilobyte: string | null;
  height: number | null;
  rawJson: string;
  sampledAt: string;
  sizeBytes: number | null;
  txid: string;
}

export interface MempoolSampleWarehousePort {
  insertMempoolSamples(rows: MempoolSampleRow[]): Promise<void>;
}

interface CoreUtxoCreateRow {
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  outputKey: string;
  txIndex: number;
  txid: string;
  vout: number;
}

interface CoreUtxoSpendRow {
  outputKey: string;
  spentByTxid: string;
  spentInBlock: number;
  spentInputIndex: number;
}

interface CoreProcessedBlockRow {
  blockHash: string;
  blockHeight: number;
}

type CoreDogecoinSpend = CoreDogecoinBlockApplication['utxoSpends'][number];

interface CoreCurrentStateWindow {
  nextBalances: VersionedBalanceRow[];
  nextOutputs: Map<string, ProjectionUtxoOutput>;
  windowEnd: number;
}

interface CoreCurrentStateMutation {
  balanceDeltas: Map<string, { address: string; amount: bigint; assetAddress: string }>;
  nextOutputs: Map<string, ProjectionUtxoOutput>;
}

interface CoreWindowShapeState {
  created: Set<string>;
  previousHash: string | null;
  previousHeight: number | null;
  spent: Set<string>;
}

class MissingCoreCurrentPrevoutError extends Error {
  public constructor(public readonly outputKey: string) {
    super(`missing current dogecoin prevout: ${outputKey}`);
    this.name = 'MissingCoreCurrentPrevoutError';
  }
}

const maxClickHouseHotOutputKeyValuesPerChunk = 128;
const maxClickHouseHotOutputKeyBytesPerChunk = 6_000;
const maxClickHouseCoreOutputKeyValuesPerChunk = 512;
const maxClickHouseCoreOutputKeyBytesPerChunk = 48_000;
const maxClickHouseCoreOutputKeyQueryConcurrency = 4;
const explorerClickHouseSettings: ClickHouseCommandSettings = {
  max_execution_time: 30,
  max_rows_to_read: '10000000',
  max_bytes_to_read: '1073741824',
  max_result_rows: '100000',
  result_overflow_mode: 'throw',
  timeout_before_checking_execution_speed: 0,
};
const addressMovementsTable = 'dogecoin_address_movements_v1';
const addressMovementsByAddressTable = 'dogecoin_address_movements_by_address_v1';
const appliedBlocksTable = clickHouseCoreDogecoinTables.appliedBlocks;
const balancesTable = clickHouseCoreDogecoinTables.balances;
const coreProcessedBlocksTable = clickHouseCoreDogecoinTables.coreProcessedBlocks;
const coreUtxoCreatesTable = clickHouseCoreDogecoinTables.coreUtxoCreates;
const coreUtxoSpendsTable = clickHouseCoreDogecoinTables.coreUtxoSpends;
const utxoCurrentStateTable = clickHouseCoreDogecoinTables.currentUtxos;
const utxoCurrentStateByAddressTable = clickHouseCoreDogecoinTables.currentUtxosByAddress;
const transactionRefsTable = 'dogecoin_transaction_refs_v1';
const coreCurrentStateOutputKeyRanges = buildCoreCurrentStateOutputKeyRanges();
type ClickHouseClient = ReturnType<typeof createClient>;
type ClickHouseCommandParameters = Parameters<ClickHouseClient['command']>[0];
type ClickHouseCommandSettings = NonNullable<ClickHouseCommandParameters['clickhouse_settings']>;
type ClickHouseInsertParameters = Parameters<ClickHouseClient['insert']>[0];
type ClickHouseJsonQueryParameters = Parameters<ClickHouseClient['query']>[0];
type ClickHouseRequestContext = ReturnType<typeof createAbortableRequestContext>;
type ClickHouseJsonResult = {
  data?: Array<Record<string, unknown>>;
  meta?: AnalyticsQueryColumn[];
  statistics?: {
    bytes_read?: number;
    elapsed?: number;
    rows_read?: number;
  };
};
type AddressMovementSummaryRow = {
  receivedBase: string;
  sentBase: string;
  txCount: number;
};
type CoreBenchmarkTables = {
  appliedBlocks: string;
  balances: string;
  creates: string;
  currentUtxos: string;
  processedBlocks: string;
  spends: string;
};
export class InMemoryWarehouseAdapter
  implements
    ProjectionWarehousePort,
    ProjectionFactWarehousePort,
    ExplorerWarehousePort,
    ClickHouseCoreDogecoinStore,
    AnalyticsWarehousePort,
    MempoolSampleWarehousePort
{
  protected state: WarehouseState = emptyWarehouseState();
  private bootstrapTail: number | null = null;

  public async insertMempoolSamples(_rows: MempoolSampleRow[]): Promise<void> {}

  public applyCoreDogecoinBlock(
    input: CoreDogecoinBlockApplication,
    context?: CoreDogecoinApplyContext,
  ) {
    return this.applyCoreDogecoinWindow([input], context);
  }

  public async applyCoreDogecoinWindow(
    input: CoreDogecoinBlockApplication[],
    _context?: CoreDogecoinApplyContext,
  ): Promise<CoreDogecoinApplyResult> {
    if (input.length === 0) {
      return { applied: false, processTail: -1 };
    }

    const pending = this.pendingInMemoryCoreApplications(input);
    if (pending.length === 0) {
      return unappliedCoreWindowResult(input);
    }

    const createdOutputs = coreCreatedOutputsByKey(pending);
    const currentOutputs = await this.getUtxoOutputs(externalCoreSpendKeys(pending));
    for (const application of pending) {
      this.applyInMemoryCoreApplication(application, createdOutputs, currentOutputs);
    }
    await this.afterMutation();

    return appliedCoreWindowResult(input, pending);
  }

  public async materializeCoreDogecoinCurrentState(
    _asOfBlockHeight: number,
    _context?: CoreDogecoinApplyContext,
  ): Promise<void> {
    this.rebuildBalancesFromCurrentUtxos();
    await this.afterMutation();
  }

  public async recoverCoreDogecoinWindow(
    fromBlockHeight: number,
    _context?: CoreDogecoinApplyContext,
  ): Promise<void> {
    this.rewindInMemoryCoreTail(fromBlockHeight);
    await this.afterMutation();
  }

  private pendingInMemoryCoreApplications(
    input: CoreDogecoinBlockApplication[],
  ): CoreDogecoinBlockApplication[] {
    for (const application of input) {
      const existing = this.state.appliedBlocks.find(
        (block) => block.blockHeight === application.blockHeight,
      );
      if (existing && existing.blockHash !== application.blockHash) {
        this.rewindInMemoryCoreTail(application.blockHeight);
        return input.filter((candidate) => candidate.blockHeight >= application.blockHeight);
      }
    }

    return input.filter(
      (application) => !this.hasAppliedBlockRecord(application, this.state.appliedBlocks),
    );
  }

  private applyInMemoryCoreApplication(
    application: CoreDogecoinBlockApplication,
    createdOutputs: Map<string, ProjectionUtxoOutput>,
    currentOutputs: Map<string, ProjectionUtxoOutput>,
  ): void {
    const movements = coreApplicationAddressMovements(application, createdOutputs, currentOutputs);
    const facts = coreApplicationTransactionFacts(application, createdOutputs, currentOutputs);
    this.applyBatchUtxoCreates(application.utxoCreates);
    this.applyBatchUtxoSpends(application.utxoSpends);
    this.applyBatchAddressMovements(movements, application.blockHeight);
    for (const fact of facts) {
      this.upsertAnalyticsTransactionFact(fact);
    }
    this.appendAppliedBlock({
      blockHeight: application.blockHeight,
      blockHash: application.blockHash,
    });
  }

  private rewindInMemoryCoreTail(fromBlockHeight: number): void {
    this.state.appliedBlocks = this.state.appliedBlocks.filter(
      (block) => block.blockHeight < fromBlockHeight,
    );
    this.state.addressMovements = this.state.addressMovements.filter(
      (movement) => movement.blockHeight < fromBlockHeight,
    );
    this.state.transactionFacts = this.state.transactionFacts.filter(
      (fact) => fact.blockHeight < fromBlockHeight,
    );
    this.state.transactionRefs = this.state.transactionRefs.filter(
      (ref) => ref.blockHeight < fromBlockHeight,
    );
    this.state.utxoOutputs = this.state.utxoOutputs.flatMap((output) =>
      rewindInMemoryUtxoOutput(output, fromBlockHeight),
    );
    this.rebuildBalancesFromCurrentUtxos();
  }

  private rebuildBalancesFromCurrentUtxos(): void {
    const balances = new Map<string, BalanceRow>();
    const asOfBlockHeight = this.state.appliedBlocks.reduce(
      (height, block) => Math.max(height, block.blockHeight),
      -1,
    );
    for (const output of this.state.utxoOutputs) {
      if (!isUnspentSpendableOutput(output) || output.address.length === 0) {
        continue;
      }

      const key = balanceKey(output.address, '');
      const current = balances.get(key);
      const balance = (BigInt(current?.balance ?? '0') + BigInt(output.valueBase)).toString();
      balances.set(key, {
        address: output.address,
        assetAddress: '',
        balance,
        asOfBlockHeight,
      });
    }

    this.state.balances = [...balances.values()];
  }

  public async getBalanceSnapshots(
    keys: Array<{
      address: string;
      assetAddress: string;
    }>,
  ): Promise<Map<string, ProjectionBalanceSnapshot>> {
    const keySet = new Set(
      keys.map((key) => projectionBalanceSnapshotKey(key.address, key.assetAddress)),
    );
    const rows = this.state.balances.filter((balance) =>
      keySet.has(projectionBalanceSnapshotKey(balance.address, balance.assetAddress)),
    );

    return new Map(
      rows.map((row) => [projectionBalanceSnapshotKey(row.address, row.assetAddress), { ...row }]),
    );
  }

  public async clearProjectionBootstrapState(): Promise<void> {
    this.state.utxoOutputs = [];
    this.state.balances = [];
    this.state.appliedBlocks = [];
    this.bootstrapTail = null;
  }

  public async finalizeProjectionBootstrap(processTail: number): Promise<void> {
    this.bootstrapTail = processTail;
  }

  public async getProjectionBootstrapTail(): Promise<number | null> {
    return this.bootstrapTail;
  }

  public async listCurrentUtxoOutputsPage(
    cursorOutputKey: string | null,
    limit: number,
    _context?: ProjectionPageRequestContext,
  ): Promise<ProjectionCurrentUtxoPage> {
    const rows = this.state.utxoOutputs
      .filter((row) => isCurrentUtxoOutputPageRow(row, cursorOutputKey))
      .sort((left, right) => left.outputKey.localeCompare(right.outputKey))
      .slice(0, limit)
      .map((row) => ({ ...row }));

    return {
      rows,
      nextCursor: currentUtxoOutputNextCursor(rows, limit),
    };
  }

  public async listCurrentBalancesPage(
    cursor: ProjectionBalanceCursor | null,
    limit: number,
    _context?: ProjectionPageRequestContext,
  ): Promise<ProjectionCurrentBalancePage> {
    const rows = currentBalancePageRows(this.state.balances, cursor, limit);

    return {
      rows,
      nextCursor: currentBalanceNextCursor(rows, limit),
    };
  }

  public async upsertProjectionBootstrapBalances(rows: ProjectionBalanceSnapshot[]): Promise<void> {
    for (const row of rows) {
      this.upsertProjectionBootstrapBalance(row);
    }
  }

  private upsertProjectionBootstrapBalance(row: ProjectionBalanceSnapshot): void {
    const index = this.state.balances.findIndex((candidate) => isBalanceSnapshot(candidate, row));
    if (index >= 0) {
      this.state.balances[index] = { ...row };
      return;
    }

    this.state.balances.push({ ...row });
  }

  public async upsertProjectionBootstrapUtxoOutputs(rows: ProjectionUtxoOutput[]): Promise<void> {
    for (const row of rows) {
      this.upsertUtxoOutput(row);
    }
  }

  public async getCurrentAddressSummary(address: string) {
    const balance = this.getNativeBalance(address);
    const utxoCount = this.countSpendableUtxos(address);

    return currentAddressSummaryOrNull(balance, utxoCount);
  }

  public async listAppliedBlocks(offset = 0, limit?: number) {
    const rows = this.state.appliedBlocks.sort(
      (left, right) => right.blockHeight - left.blockHeight,
    );

    return rows.slice(offset, limit === undefined ? undefined : offset + limit);
  }

  public async getAppliedBlockByHash(blockHash: string) {
    return this.state.appliedBlocks.find((block) => block.blockHash === blockHash) ?? null;
  }

  public async getTransactionRef(txid: string) {
    const indexedRef = this.latestInMemoryTransactionRef(txid);
    if (indexedRef) {
      return indexedRef;
    }

    const output = this.state.utxoOutputs.find((candidate) => candidate.txid === txid);
    if (!output) {
      return null;
    }

    return {
      blockHeight: output.blockHeight,
      blockHash: output.blockHash,
      blockTime: output.blockTime,
      txIndex: output.txIndex,
    };
  }

  public async upsertTransactionRefs(
    refs: Array<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      source: 'raw_sync' | 'core_process';
      txIndex: number;
      txid: string;
      version: number;
    }>,
  ): Promise<void> {
    for (const ref of refs) {
      const existingIndex = this.state.transactionRefs.findIndex(
        (candidate) => candidate.txid === ref.txid,
      );
      if (existingIndex < 0) {
        this.state.transactionRefs.push({ ...ref });
        continue;
      }

      const existing = this.state.transactionRefs[existingIndex];
      if (!existing || ref.version >= existing.version) {
        this.state.transactionRefs[existingIndex] = { ...ref };
      }
    }

    await this.afterMutation();
  }

  private latestInMemoryTransactionRef(txid: string) {
    const ref = this.state.transactionRefs
      .filter((candidate) => candidate.txid === txid)
      .sort((left, right) => right.version - left.version)[0];
    if (!ref) {
      return null;
    }

    return {
      blockHeight: ref.blockHeight,
      blockHash: ref.blockHash,
      blockTime: ref.blockTime,
      txIndex: ref.txIndex,
    };
  }

  public async getAddressSummary(address: string) {
    const balance = this.getNativeBalance(address);
    const movements = this.getNativeMovements(address);
    const utxoCount = this.countSpendableUtxos(address);
    const totals = summarizeNativeMovements(movements);

    return inMemoryAddressSummary(balance, totals, utxoCount);
  }

  private getNativeBalance(address: string): string {
    return balanceOrZero(
      this.state.balances.find((candidate) => isNativeBalance(candidate, address)),
    );
  }

  private getNativeMovements(address: string): AddressMovement[] {
    return this.state.addressMovements.filter((candidate) => isNativeMovement(candidate, address));
  }

  private countSpendableUtxos(address: string): number {
    return this.state.utxoOutputs.filter((candidate) => isSpendableAddressUtxo(candidate, address))
      .length;
  }

  public async listAddressTransactions(address: string, offset = 0, limit?: number) {
    const aggregates = aggregateAddressTransactions(this.getNativeMovements(address));
    const rows = paginateAddressTransactions(aggregates, offset, limit);
    return rows.map((row) => {
      const fact = this.state.transactionFacts.find(
        (candidate) =>
          candidate.txid === row.txid &&
          candidate.blockHeight === row.blockHeight &&
          candidate.blockHash === row.blockHash,
      );

      return {
        ...row,
        feeBase: fact?.feeBase ?? null,
        inputCount: fact?.inputCount ?? 0,
        isCoinbase: fact?.isCoinbase ?? false,
        outputCount: fact?.outputCount ?? 0,
        totalInputBase: fact?.totalInputBase ?? '0',
        totalOutputBase: fact?.grossOutputBase ?? '0',
      };
    });
  }

  public async listAddressUtxos(address: string, offset = 0, limit?: number) {
    return this.state.utxoOutputs
      .filter((candidate) => isSpendableAddressUtxo(candidate, address))
      .sort(compareAddressUtxos)
      .slice(offset, limit === undefined ? undefined : offset + limit);
  }

  public async getUtxoOutput(outputKey: string): Promise<ProjectionUtxoOutput | null> {
    return this.state.utxoOutputs.find((output) => output.outputKey === outputKey) ?? null;
  }

  public async getUtxoOutputs(outputKeys: string[]): Promise<Map<string, ProjectionUtxoOutput>> {
    const outputs = this.state.utxoOutputs.filter((output) =>
      isRequestedUtxoOutput(output, outputKeys),
    );

    return new Map(outputs.map((output) => [output.outputKey, output]));
  }

  public async getCreatedUtxoOutputs(
    outputKeys: string[],
  ): Promise<Map<string, ExplorerCreatedUtxoOutput>> {
    return this.getUtxoOutputs(outputKeys);
  }

  public async hasAppliedBlock(blockHeight: number, blockHash: string): Promise<boolean> {
    return this.state.appliedBlocks.some((candidate) =>
      isAppliedBlockRecord(candidate, blockHeight, blockHash),
    );
  }

  public async listAppliedBlockSet(
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>> {
    return new Set(
      blocks
        .filter((block) =>
          this.state.appliedBlocks.some((candidate) =>
            isAppliedBlockRecord(candidate, block.blockHeight, block.blockHash),
          ),
        )
        .map((block) => projectionBlockIdentity(block.blockHeight, block.blockHash)),
    );
  }

  public async hasProjectionState(): Promise<boolean> {
    return this.state.appliedBlocks.length > 0;
  }

  public async getAppliedBlockTail(): Promise<number | null> {
    const tail = this.state.appliedBlocks.reduce<number | null>(
      (current, candidate) =>
        current === null ? candidate.blockHeight : Math.max(current, candidate.blockHeight),
      null,
    );
    return tail;
  }

  public async importProjectionStateSnapshot(
    snapshot: ProjectionStateBootstrapSnapshot,
  ): Promise<void> {
    this.state.appliedBlocks = [...snapshot.appliedBlocks];
    this.state.utxoOutputs = [...snapshot.utxoOutputs];
    this.state.balances = [...snapshot.balances];
    await this.afterMutation();
  }

  public async applyProjectionWindow(batches: BlockProjectionBatch[]): Promise<void> {
    for (const batch of batches) {
      await this.applyBlockProjection(batch);
    }
  }

  public async applyProjectionFacts(window: ProjectionFactWindow): Promise<void> {
    this.applyProjectionFactOutputs(window.utxoOutputs);
    this.applyProjectionFactMovements(window.addressMovements);
    this.applyProjectionFactBalances(window.balances);
    this.applyProjectionFactBlocks(window.appliedBlocks);
    await this.afterMutation();
  }

  private applyProjectionFactOutputs(outputs: ProjectionUtxoOutput[]): void {
    for (const output of outputs) {
      this.upsertUtxoOutput(output);
    }
  }

  private applyProjectionFactMovements(movements: AddressMovement[]): void {
    for (const movement of movements) {
      this.appendProjectionFactMovement(movement);
    }
  }

  private appendProjectionFactMovement(movement: AddressMovement): void {
    if (this.appendUniqueAddressMovement(movement)) {
      this.state.addressMovements.push(movement);
    }
  }

  private applyProjectionFactBalances(balances: ProjectionBalanceSnapshot[]): void {
    for (const balance of balances) {
      this.upsertBalance(balance);
    }
  }

  private applyProjectionFactBlocks(blocks: ProjectionAppliedBlock[]): void {
    for (const block of blocks) {
      this.appendAppliedBlock(block);
    }
  }

  public async exportProjectionStateSnapshot(): Promise<ProjectionStateBootstrapSnapshot> {
    return {
      appliedBlocks: [...this.state.appliedBlocks],
      utxoOutputs: [...this.state.utxoOutputs],
      balances: [...this.state.balances],
    };
  }

  public async applyBlockProjection(batch: BlockProjectionBatch): Promise<void> {
    const alreadyApplied = await this.hasAppliedBlock(batch.blockHeight, batch.blockHash);
    if (alreadyApplied) {
      return;
    }

    this.applyBatchUtxoCreates(batch.utxoCreates);
    this.applyBatchUtxoSpends(batch.utxoSpends);
    this.applyBatchAddressMovements(batch.addressMovements, batch.blockHeight);
    this.appendAppliedBlock({
      blockHeight: batch.blockHeight,
      blockHash: batch.blockHash,
    });
    await this.afterMutation();
  }

  public async insertAnalyticsTransactionFacts(rows: AnalyticsTransactionFact[]): Promise<void> {
    for (const row of rows) {
      this.upsertAnalyticsTransactionFact(row);
    }
    await this.afterMutation();
  }

  public async backfillAnalyticsTransactionFacts(input: {
    throughBlockHeight: number;
  }): Promise<{ rowsInserted: number | null; throughBlockHeight: number }> {
    const rows = this.buildInMemoryAnalyticsTransactionFacts(input.throughBlockHeight);
    this.state.transactionFacts = this.state.transactionFacts.filter(
      (row) => row.blockHeight > input.throughBlockHeight,
    );
    for (const row of rows) {
      this.upsertAnalyticsTransactionFact(row);
    }
    await this.afterMutation();
    return {
      rowsInserted: rows.length,
      throughBlockHeight: input.throughBlockHeight,
    };
  }

  public async preflightAnalyticsQuery(input: {
    params: AnalyticsQueryParams;
  }): Promise<AnalyticsQueryEstimate> {
    return {
      estimatedRows: this.filteredAnalyticsFacts(input.params).length,
      estimatedBytes: null,
    };
  }

  public async executeAnalyticsQuery(input: {
    params: AnalyticsQueryParams;
    sql: string;
  }): Promise<AnalyticsQueryExecutionResult> {
    const rows = executeInMemoryAnalyticsSql(input.sql, this.filteredAnalyticsFacts(input.params));
    return {
      rows,
      columns: inMemoryAnalyticsColumns(rows),
      statistics: {
        elapsed: null,
        rowsRead: this.filteredAnalyticsFacts(input.params).length,
        bytesRead: null,
      },
      warnings: ['in-memory analytics execution is test-only'],
    };
  }

  protected async afterMutation(): Promise<void> {}

  private upsertAnalyticsTransactionFact(row: AnalyticsTransactionFact): void {
    const index = this.state.transactionFacts.findIndex((candidate) => candidate.txid === row.txid);
    if (index >= 0) {
      this.state.transactionFacts[index] = { ...row };
      return;
    }

    this.state.transactionFacts.push({ ...row });
  }

  private buildInMemoryAnalyticsTransactionFacts(
    throughBlockHeight: number,
  ): AnalyticsTransactionFact[] {
    const outputs = this.state.utxoOutputs.filter((row) => row.blockHeight <= throughBlockHeight);
    const outputsByKey = new Map(outputs.map((row) => [row.outputKey, row]));
    const grouped = new Map<string, ProjectionUtxoOutput[]>();
    for (const output of outputs) {
      grouped.set(output.txid, [...(grouped.get(output.txid) ?? []), output]);
    }

    return [...grouped.values()].map((txOutputs) =>
      analyticsFactFromOutputs(txOutputs, outputsByKey, throughBlockHeight),
    );
  }

  private filteredAnalyticsFacts(params: AnalyticsQueryParams): AnalyticsTransactionFact[] {
    return this.state.transactionFacts.filter(
      (row) =>
        row.blockTime >= params.fromTime &&
        row.blockTime < params.toTime &&
        row.blockHeight <= params.maxFinalizedHeight,
    );
  }

  private applyBatchUtxoCreates(outputs: ProjectionUtxoOutput[]): void {
    for (const output of outputs) {
      this.upsertUtxoOutput(output, false);
    }
  }

  private applyBatchUtxoSpends(spends: BlockProjectionBatch['utxoSpends']): void {
    for (const spend of spends) {
      const output = this.requireUtxoOutput(spend.outputKey);
      output.spentByTxid = spend.spentByTxid;
      output.spentInBlock = spend.spentInBlock;
      output.spentInputIndex = spend.spentInputIndex;
    }
  }

  private applyBatchAddressMovements(movements: AddressMovement[], blockHeight: number): void {
    for (const movement of movements) {
      this.applyBatchAddressMovementIfUnique(movement, blockHeight);
    }
  }

  private applyBatchAddressMovementIfUnique(movement: AddressMovement, blockHeight: number): void {
    if (!this.appendUniqueAddressMovement(movement)) {
      return;
    }

    this.state.addressMovements.push(movement);
    this.applyBalanceDelta(movement, blockHeight);
  }

  private requireUtxoOutput(outputKey: string): ProjectionUtxoOutput {
    const output = this.state.utxoOutputs.find((candidate) => candidate.outputKey === outputKey);
    if (!output) {
      throw new Error(`missing utxo output: ${outputKey}`);
    }

    return output;
  }

  private upsertUtxoOutput(output: ProjectionUtxoOutput, clone = true): void {
    const existingIndex = this.state.utxoOutputs.findIndex((candidate) =>
      isSameUtxoOutput(candidate, output),
    );
    const next = utxoOutputForWrite(output, clone);
    if (existingIndex >= 0) {
      this.state.utxoOutputs[existingIndex] = next;
      return;
    }

    this.state.utxoOutputs.push(next);
  }

  private appendUniqueAddressMovement(movement: AddressMovement): boolean {
    return !this.state.addressMovements.some(
      (candidate) => candidate.movementId === movement.movementId,
    );
  }

  private upsertBalance(balance: ProjectionBalanceSnapshot): void {
    const existing = this.state.balances.find((candidate) => isBalanceSnapshot(candidate, balance));
    if (existing) {
      existing.balance = balance.balance;
      existing.asOfBlockHeight = balance.asOfBlockHeight;
    } else {
      this.state.balances.push({ ...balance });
    }
  }

  private appendAppliedBlock(block: ProjectionAppliedBlock): void {
    if (!this.hasAppliedBlockRecord(block, this.state.appliedBlocks)) {
      this.state.appliedBlocks.push({ ...block });
    }
  }

  private hasAppliedBlockRecord(
    block: ProjectionAppliedBlock,
    blocks: ProjectionAppliedBlock[],
  ): boolean {
    return blocks.some((candidate) =>
      isAppliedBlockRecord(candidate, block.blockHeight, block.blockHash),
    );
  }

  private applyBalanceDelta(movement: AddressMovement, blockHeight: number): void {
    const current = this.findBalanceSnapshot(movement);
    const nextAmount = nextBalanceAmount(current?.balance, movement);
    assertNonNegativeBalance(movement, nextAmount);
    this.writeBalanceSnapshot(movement, blockHeight, current, nextAmount);
  }

  private findBalanceSnapshot(movement: AddressMovement): BalanceRow | undefined {
    return this.state.balances.find((candidate) => isMovementBalanceSnapshot(candidate, movement));
  }

  private writeBalanceSnapshot(
    movement: AddressMovement,
    blockHeight: number,
    current: BalanceRow | undefined,
    nextAmount: bigint,
  ): void {
    if (current) {
      current.balance = formatAmountBase(nextAmount);
      current.asOfBlockHeight = blockHeight;
      return;
    }

    this.state.balances.push({
      address: movement.address,
      assetAddress: movement.assetAddress,
      balance: formatAmountBase(nextAmount),
      asOfBlockHeight: blockHeight,
    });
  }
}

export class DuckDbWarehouseAdapter extends InMemoryWarehouseAdapter {
  public constructor(private readonly path: string) {
    super();
  }

  public async boot(): Promise<void> {
    try {
      const contents = await readFile(this.path, 'utf8');
      const loadedState: Partial<WarehouseState> = JSON.parse(contents);
      this.state = mergeWarehouseState(loadedState);
    } catch {
      await this.afterMutation();
    }
  }

  protected override async afterMutation(): Promise<void> {
    await mkdir(dirname(this.path), { recursive: true });
    await writeFile(this.path, JSON.stringify(this.state, null, 2));
  }
}

function isCurrentUtxoOutputPageRow(
  row: ProjectionUtxoOutput,
  cursorOutputKey: string | null,
): boolean {
  return isAfterOutputCursor(row.outputKey, cursorOutputKey);
}

function isAfterOutputCursor(outputKey: string, cursorOutputKey: string | null): boolean {
  if (cursorOutputKey === null) {
    return true;
  }

  return outputKey > cursorOutputKey;
}

function currentUtxoOutputNextCursor(rows: ProjectionUtxoOutput[], limit: number): string | null {
  if (rows.length !== limit) {
    return null;
  }

  return lastCurrentUtxoOutputKey(rows);
}

function lastCurrentUtxoOutputKey(rows: ProjectionUtxoOutput[]): string | null {
  const last = rows.at(-1);
  if (!last) {
    return null;
  }

  return last.outputKey;
}

function isBalanceSnapshot(
  candidate: ProjectionBalanceSnapshot,
  balance: ProjectionBalanceSnapshot,
): boolean {
  return [
    candidate.address === balance.address,
    candidate.assetAddress === balance.assetAddress,
  ].every(Boolean);
}

function currentAddressSummaryOrNull(
  balance: string,
  utxoCount: number,
): {
  balance: string;
  utxoCount: number;
} | null {
  if (isEmptyCurrentAddressSummary(balance, utxoCount)) {
    return null;
  }

  return { balance, utxoCount };
}

function isEmptyCurrentAddressSummary(balance: string, utxoCount: number): boolean {
  return [balance === '0', utxoCount === 0].every(Boolean);
}

function balanceOrZero(balance: ProjectionBalanceSnapshot | undefined): string {
  if (!balance) {
    return '0';
  }

  return balance.balance;
}

function isNativeBalance(candidate: ProjectionBalanceSnapshot, address: string): boolean {
  return [candidate.address === address, candidate.assetAddress === ''].every(Boolean);
}

function isNativeMovement(candidate: AddressMovement, address: string): boolean {
  return [candidate.address === address, candidate.assetAddress === ''].every(Boolean);
}

function isSpendableAddressUtxo(candidate: ProjectionUtxoOutput, address: string): boolean {
  return [
    candidate.address === address,
    candidate.isSpendable,
    candidate.spentByTxid === null,
  ].every(Boolean);
}

function compareAddressUtxos(left: ProjectionUtxoOutput, right: ProjectionUtxoOutput): number {
  return firstNonZero([
    right.blockHeight - left.blockHeight,
    right.txIndex - left.txIndex,
    left.vout - right.vout,
  ]);
}

function firstNonZero(values: number[]): number {
  return values.find((value) => value !== 0) ?? 0;
}

function isRequestedUtxoOutput(output: ProjectionUtxoOutput, outputKeys: string[]): boolean {
  return outputKeys.includes(output.outputKey);
}

function isAppliedBlockRecord(
  candidate: ProjectionAppliedBlock,
  blockHeight: number,
  blockHash: string,
): boolean {
  return [candidate.blockHeight === blockHeight, candidate.blockHash === blockHash].every(Boolean);
}

function isSameUtxoOutput(candidate: ProjectionUtxoOutput, output: ProjectionUtxoOutput): boolean {
  return candidate.outputKey === output.outputKey;
}

function isMovementBalanceSnapshot(candidate: BalanceRow, movement: AddressMovement): boolean {
  return [
    candidate.address === movement.address,
    candidate.assetAddress === movement.assetAddress,
  ].every(Boolean);
}

function isUnspentSpendableOutput(
  output: ProjectionUtxoOutput | undefined,
): output is ProjectionUtxoOutput {
  if (!output) {
    return false;
  }

  return [output.isSpendable, output.spentByTxid === null].every(Boolean);
}

function rewindInMemoryUtxoOutput(
  output: ProjectionUtxoOutput,
  fromBlockHeight: number,
): ProjectionUtxoOutput[] {
  if (output.blockHeight >= fromBlockHeight) {
    return [];
  }
  if (output.spentInBlock !== null && output.spentInBlock >= fromBlockHeight) {
    return [
      {
        ...output,
        spentByTxid: null,
        spentInBlock: null,
        spentInputIndex: null,
      },
    ];
  }

  return [output];
}

function analyticsFactFromOutputs(
  outputs: ProjectionUtxoOutput[],
  outputsByKey: Map<string, ProjectionUtxoOutput>,
  version: number,
): AnalyticsTransactionFact {
  const sorted = [...outputs].sort((left, right) => left.vout - right.vout);
  const first = requireAnalyticsOutput(sorted);
  const isCoinbase = sorted.some((output) => output.isCoinbase);
  const spentInputs = [...outputsByKey.values()].filter((output) =>
    isSpentByTransaction(output, first.txid),
  );
  const totalInput = spentInputs.reduce((sum, output) => sum + BigInt(output.valueBase), 0n);
  const grossOutput = sorted.reduce((sum, output) => sum + BigInt(output.valueBase), 0n);

  return {
    blockHeight: first.blockHeight,
    blockHash: first.blockHash,
    blockTime: first.blockTime,
    txid: first.txid,
    txIndex: first.txIndex,
    isCoinbase,
    inputCount: spentInputs.length,
    outputCount: sorted.length,
    totalInputBase: totalInput.toString(),
    grossOutputBase: grossOutput.toString(),
    feeBase: analyticsFeeBase(isCoinbase, totalInput, grossOutput),
    version,
  };
}

function requireAnalyticsOutput(outputs: ProjectionUtxoOutput[]): ProjectionUtxoOutput {
  const first = outputs[0];
  if (!first) {
    throw new Error('analytics transaction fact requires at least one output');
  }

  return first;
}

function isSpentByTransaction(output: ProjectionUtxoOutput, txid: string): boolean {
  return output.spentByTxid === txid;
}

function analyticsFeeBase(
  isCoinbase: boolean,
  totalInput: bigint,
  grossOutput: bigint,
): string | null {
  if (isCoinbase || totalInput === 0n) {
    return null;
  }

  return (totalInput - grossOutput).toString();
}

function executeInMemoryAnalyticsSql(
  sql: string,
  facts: AnalyticsTransactionFact[],
): Array<Record<string, unknown>> {
  const normalized = sql.toLowerCase();
  if (normalized.includes('avgornull') || normalized.includes('avg(')) {
    return [inMemoryAverageFeeRow(facts)];
  }
  if (normalized.includes('fee_base')) {
    return inMemoryTopFeeRows(facts);
  }
  if (normalized.includes('gross_output')) {
    return inMemoryTopGrossOutputRows(facts);
  }

  return facts.slice(0, analyticsQueryMaxResultRows).map(inMemoryAnalyticsFactRow);
}

function inMemoryAverageFeeRow(facts: AnalyticsTransactionFact[]): Record<string, unknown> {
  const feeValues = facts
    .filter((fact) => !fact.isCoinbase && fact.feeBase !== null)
    .map((fact) => BigInt(fact.feeBase as string));
  if (feeValues.length === 0) {
    return { average_fee_base: null };
  }

  const total = feeValues.reduce((sum, fee) => sum + fee, 0n);
  return { average_fee_base: (total / BigInt(feeValues.length)).toString() };
}

function inMemoryTopFeeRows(facts: AnalyticsTransactionFact[]): Array<Record<string, unknown>> {
  return facts
    .filter((fact) => !fact.isCoinbase && fact.feeBase !== null)
    .sort(compareAnalyticsFeeDesc)
    .slice(0, analyticsQueryMaxResultRows)
    .map((fact) => ({
      txid: fact.txid,
      block_height: fact.blockHeight,
      block_time: fact.blockTime,
      fee_base: fact.feeBase,
    }));
}

function inMemoryTopGrossOutputRows(
  facts: AnalyticsTransactionFact[],
): Array<Record<string, unknown>> {
  return facts
    .filter((fact) => !fact.isCoinbase)
    .sort(compareAnalyticsGrossOutputDesc)
    .slice(0, analyticsQueryMaxResultRows)
    .map((fact) => ({
      txid: fact.txid,
      block_height: fact.blockHeight,
      block_time: fact.blockTime,
      gross_output_base: fact.grossOutputBase,
    }));
}

function inMemoryAnalyticsFactRow(fact: AnalyticsTransactionFact): Record<string, unknown> {
  return {
    block_height: fact.blockHeight,
    block_hash: fact.blockHash,
    block_time: fact.blockTime,
    txid: fact.txid,
    tx_index: fact.txIndex,
    is_coinbase: fact.isCoinbase ? 1 : 0,
    input_count: fact.inputCount,
    output_count: fact.outputCount,
    total_input_base_i256: fact.totalInputBase,
    gross_output_base_i256: fact.grossOutputBase,
    fee_base_i256: fact.feeBase,
  };
}

function compareAnalyticsFeeDesc(
  left: AnalyticsTransactionFact,
  right: AnalyticsTransactionFact,
): number {
  return compareBigIntDesc(left.feeBase ?? '0', right.feeBase ?? '0');
}

function compareAnalyticsGrossOutputDesc(
  left: AnalyticsTransactionFact,
  right: AnalyticsTransactionFact,
): number {
  return compareBigIntDesc(left.grossOutputBase, right.grossOutputBase);
}

function compareBigIntDesc(left: string, right: string): number {
  const diff = BigInt(right) - BigInt(left);
  if (diff > 0n) {
    return 1;
  }
  if (diff < 0n) {
    return -1;
  }

  return 0;
}

function inMemoryAnalyticsColumns(rows: Array<Record<string, unknown>>): AnalyticsQueryColumn[] {
  const first = rows[0];
  if (!first) {
    return [];
  }

  return Object.entries(first).map(([name, value]) => ({
    name,
    type: inMemoryAnalyticsColumnType(value),
  }));
}

function inMemoryAnalyticsColumnType(value: unknown): string {
  return typeof value === 'number' ? 'UInt64' : 'String';
}

function utxoOutputForWrite(output: ProjectionUtxoOutput, clone: boolean): ProjectionUtxoOutput {
  if (clone) {
    return { ...output };
  }

  return output;
}

export class ClickHouseWarehouseAdapter
  implements
    ProjectionWarehousePort,
    ProjectionFactWarehousePort,
    ExplorerWarehousePort,
    ClickHouseCoreDogecoinStore,
    AnalyticsWarehousePort,
    MempoolSampleWarehousePort
{
  private readonly client: ReturnType<typeof createClient>;
  private readonly analyticsClient: ReturnType<typeof createClient> | null;
  private readonly explorerReadContext = new AsyncLocalStorage<boolean>();
  private readonly logger: ServiceLogger;
  private readonly requestTimeoutMs: number;

  public constructor(
    private readonly settings: WarehouseSettings,
    private readonly schemaLock?: SchemaLockPort,
    logger: ServiceLogger = noopServiceLogger(),
  ) {
    this.logger = logger;
    this.requestTimeoutMs = settings.requestTimeoutMs ?? 30_000;
    this.client = createClient(clickHouseClientOptions(settings, this.requestTimeoutMs));
    const analyticsCredentials = analyticsClickHouseCredentials(settings);
    this.analyticsClient = analyticsCredentials
      ? createClient(clickHouseClientOptions(settings, this.requestTimeoutMs, analyticsCredentials))
      : null;
  }

  public async boot(): Promise<void> {
    if (!this.schemaLock) {
      throw new Error('ClickHouse warehouse boot requires a metadata schema lock');
    }
    await runClickHouseMigrations(this.settings, this.schemaLock);
  }

  public runExplorerRead<T>(work: () => Promise<T>): Promise<T> {
    return this.explorerReadContext.run(true, work);
  }

  public async applyCoreDogecoinWindow(
    input: CoreDogecoinBlockApplication[],
    context?: CoreDogecoinApplyContext,
  ): Promise<CoreDogecoinApplyResult> {
    if (input.length === 0) {
      return { applied: false, processTail: -1 };
    }

    return this.applyCoreDogecoinWindowWithDeadline(input, context);
  }

  public async recoverCoreDogecoinWindow(
    fromBlockHeight: number,
    context?: CoreDogecoinApplyContext,
  ): Promise<void> {
    await this.rewindCoreDogecoinWindow(fromBlockHeight, context);
  }

  private async applyCoreDogecoinWindowWithDeadline(
    input: CoreDogecoinBlockApplication[],
    context?: CoreDogecoinApplyContext,
  ): Promise<CoreDogecoinApplyResult> {
    const timeoutMs = coreWindowTimeoutMs(context, this.requestTimeoutMs);
    const requestContext = createAbortableRequestContext(context?.abortSignal, timeoutMs);
    return this.applyCoreDogecoinWindowWithRequestContext(input, requestContext, context)
      .catch((error) => {
        throw this.coreWindowRequestError(error, requestContext, timeoutMs);
      })
      .finally(() => requestContext.cleanup());
  }

  private coreWindowRequestError(
    error: unknown,
    requestContext: ClickHouseRequestContext,
    timeoutMs: number,
  ): unknown {
    if (requestContext.didTimeout()) {
      return this.toDeadlineInfrastructureError(error, requestContext, timeoutMs);
    }
    return error;
  }

  private async applyCoreDogecoinWindowWithRequestContext(
    input: CoreDogecoinBlockApplication[],
    requestContext: ClickHouseRequestContext,
    context?: CoreDogecoinApplyContext,
  ): Promise<CoreDogecoinApplyResult> {
    const pending = await this.pendingCoreWindowApplications(input, context, requestContext);

    if (pending.length === 0) {
      return unappliedCoreWindowResult(input);
    }

    await this.validatePendingCoreWindow(pending, context, requestContext);
    await this.insertPendingCoreWindowWithRecovery(pending, context, requestContext);
    return appliedCoreWindowResult(input, pending);
  }

  private async pendingCoreWindowApplications(
    input: CoreDogecoinBlockApplication[],
    context: CoreDogecoinApplyContext | undefined,
    requestContext: ClickHouseRequestContext,
  ): Promise<CoreDogecoinBlockApplication[]> {
    const processedBlocks = await this.getCoreProcessedBlocks(
      input.map((application) => application.blockHeight),
      requestContext,
    );
    const reorgHeight = firstCoreReorgHeight(input, processedBlocks);
    if (reorgHeight !== null) {
      await this.rewindCoreDogecoinWindow(reorgHeight, context);
      return input.filter((application) => application.blockHeight >= reorgHeight);
    }

    return input.filter((application) =>
      isPendingCoreApplication(application, processedBlocks.get(application.blockHeight)),
    );
  }

  private async rewindCoreDogecoinWindow(
    fromBlockHeight: number,
    context: CoreDogecoinApplyContext | undefined,
  ): Promise<void> {
    await this.deleteCoreDogecoinTail(fromBlockHeight, context);
    if (!shouldUpdateCoreCurrentState(context)) {
      return;
    }

    await this.rematerializeCoreCurrentStateAt(fromBlockHeight - 1, context);
  }

  private async deleteCoreDogecoinTail(
    fromBlockHeight: number,
    context: CoreDogecoinApplyContext | undefined,
  ): Promise<void> {
    const settings = {
      ...clickHouseCoreMaterializationSettings(context),
      mutations_sync: '2',
    };
    const deletes = [
      {
        table: coreUtxoCreatesTable,
        heightColumn: 'block_height',
      },
      {
        table: coreUtxoSpendsTable,
        heightColumn: 'spent_in_block',
      },
      {
        table: coreProcessedBlocksTable,
        heightColumn: 'block_height',
      },
      {
        table: addressMovementsTable,
        heightColumn: 'block_height',
      },
      {
        table: addressMovementsByAddressTable,
        heightColumn: 'block_height',
      },
      {
        table: appliedBlocksTable,
        heightColumn: 'block_height',
      },
      {
        table: analyticsTransactionsTable,
        heightColumn: 'block_height',
      },
      {
        table: transactionRefsTable,
        heightColumn: 'block_height',
      },
    ];

    for (const deletion of deletes) {
      await this.executeCommand({
        query: `ALTER TABLE ${deletion.table} DELETE WHERE ${deletion.heightColumn} >= {fromBlockHeight:UInt64}`,
        query_params: { fromBlockHeight },
        clickhouse_settings: settings,
      });
    }
  }

  private async rematerializeCoreCurrentStateAt(
    asOfBlockHeight: number,
    context: CoreDogecoinApplyContext | undefined,
  ): Promise<void> {
    await this.clearCoreDogecoinCurrentState({
      currentUtxosTable: utxoCurrentStateTable,
      currentUtxosByAddressTable: utxoCurrentStateByAddressTable,
      balancesTable,
      appliedBlocksTable,
      ...coreApplyContextOption(context),
    });
    if (asOfBlockHeight < 0) {
      return;
    }

    await this.insertCoreCurrentStateMaterialization({
      asOfBlockHeight,
      createsTable: coreUtxoCreatesTable,
      spendsTable: coreUtxoSpendsTable,
      currentUtxosTable: utxoCurrentStateTable,
      currentUtxosByAddressTable: utxoCurrentStateByAddressTable,
      balancesTable,
      appliedBlocksTable,
      processedBlocksTable: coreProcessedBlocksTable,
      ...coreApplyContextOption(context),
    });
  }

  private async validatePendingCoreWindow(
    pending: CoreDogecoinBlockApplication[],
    context: CoreDogecoinApplyContext | undefined,
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    this.assertCoreWindowShape(pending);
    await this.assertCoreWindowPreviousBlock(pending[0], requestContext);
    await this.assertCoreWindowPrevoutsIfEnabled(pending, context, requestContext);
  }

  private async assertCoreWindowPrevoutsIfEnabled(
    pending: CoreDogecoinBlockApplication[],
    context: CoreDogecoinApplyContext | undefined,
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    if (!shouldValidateCorePrevouts(context)) {
      return;
    }

    await this.assertCoreWindowPrevouts(pending, requestContext);
  }

  private async insertPendingCoreWindow(
    pending: CoreDogecoinBlockApplication[],
    context: CoreDogecoinApplyContext | undefined,
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    const createRows = pending.flatMap((application) => application.utxoCreates);
    const spendRows = pending.flatMap((application) => application.utxoSpends);
    await this.insertRows(
      coreUtxoCreatesTable,
      createRows.map(toCoreUtxoCreateInsertRow),
      requestContext,
    );
    await runCoreWindowInsertStageHook(context, 'creates');
    await this.insertRows(
      coreUtxoSpendsTable,
      spendRows.map(toCoreUtxoSpendInsertRow),
      requestContext,
    );
    await runCoreWindowInsertStageHook(context, 'spends');
    await this.insertCoreAddressMovements(pending, requestContext);
    await runCoreWindowInsertStageHook(context, 'movements');
    await this.insertCoreTransactionFacts(pending, requestContext);
    await runCoreWindowInsertStageHook(context, 'transactions');
    await this.applyCoreCurrentStateWindowIfEnabled(pending, context, requestContext);
    await runCoreWindowInsertStageHook(context, 'current_state');
    await this.insertRows(
      coreProcessedBlocksTable,
      pending.map(toCoreProcessedBlockInsertRow),
      requestContext,
    );
    await runCoreWindowInsertStageHook(context, 'processed_blocks');
  }

  private async insertPendingCoreWindowWithRecovery(
    pending: CoreDogecoinBlockApplication[],
    context: CoreDogecoinApplyContext | undefined,
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    try {
      await this.insertPendingCoreWindow(pending, context, requestContext);
      return;
    } catch (error) {
      if (!shouldRecoverMissingCoreCurrentPrevout(error, context)) {
        throw error;
      }

      await this.recoverCoreCurrentStateForPendingWindow(pending, context, requestContext, error);
    }

    try {
      await this.insertPendingCoreWindow(pending, context, requestContext);
    } catch (retryError) {
      await this.cleanFailedCoreWindowFacts(pending, context);
      throw retryError;
    }
  }

  private async recoverCoreCurrentStateForPendingWindow(
    pending: CoreDogecoinBlockApplication[],
    context: CoreDogecoinApplyContext | undefined,
    requestContext: ClickHouseRequestContext,
    error: MissingCoreCurrentPrevoutError,
  ): Promise<void> {
    const windowStart = coreWindowStart(pending);
    const windowEnd = coreWindowEnd(pending);
    this.logger.warn(
      {
        action: 'repair-prevouts',
        component: 'warehouse',
        endHeight: windowEnd,
        outputKey: error.outputKey,
        phase: 'core-current-state-recovery',
        reason: 'missing-current-prevout',
        startHeight: windowStart,
      },
      'repairing missing core current prevouts',
    );
    await this.cleanFailedCoreWindowFacts(pending, context);
    await this.repairMissingCoreCurrentPrevouts(pending, requestContext);
  }

  private async cleanFailedCoreWindowFacts(
    pending: CoreDogecoinBlockApplication[],
    context: CoreDogecoinApplyContext | undefined,
  ): Promise<void> {
    const windowStart = coreWindowStart(pending);
    if (windowStart < 0) {
      return;
    }

    await this.deleteCoreDogecoinTail(windowStart, context);
  }

  private async repairMissingCoreCurrentPrevouts(
    pending: CoreDogecoinBlockApplication[],
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    const outputKeys = externalCoreSpendKeys(pending);
    const currentOutputs = await this.getCurrentUtxoOutputMap(outputKeys, requestContext);
    const missingOutputKeys = missingUtxoOutputKeys(outputKeys, currentOutputs);
    if (missingOutputKeys.length === 0) {
      return;
    }

    const repairedRows = await this.queryCoreHistoricalUtxoOutputRows(
      missingOutputKeys,
      requestContext,
      coreWindowStart(pending) - 1,
    );
    assertCoreCurrentPrevoutRepairs(missingOutputKeys, repairedRows);

    await this.insertRows(
      utxoCurrentStateTable,
      repairedRows.map((row) =>
        toUtxoInsertRow(unspentCoreCurrentRepair(row), coreCurrentCreateVersion(row.blockHeight)),
      ),
      requestContext,
    );
  }

  private async insertCoreAddressMovements(
    pending: CoreDogecoinBlockApplication[],
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    const movements = await this.buildCoreAddressMovements(pending, requestContext);
    await this.insertRows(
      addressMovementsTable,
      movements.map(toAddressMovementInsertRow),
      requestContext,
    );
  }

  private async buildCoreAddressMovements(
    pending: CoreDogecoinBlockApplication[],
    requestContext: ClickHouseRequestContext,
  ): Promise<AddressMovement[]> {
    const createdOutputs = coreCreatedOutputsByKey(pending);
    const currentOutputs = await this.getCurrentUtxoOutputMap(
      externalCoreSpendKeys(pending),
      requestContext,
    );

    return pending.flatMap((application) =>
      coreApplicationAddressMovements(application, createdOutputs, currentOutputs),
    );
  }

  private async insertCoreTransactionFacts(
    pending: CoreDogecoinBlockApplication[],
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    const createdOutputs = coreCreatedOutputsByKey(pending);
    const currentOutputs = await this.getCurrentUtxoOutputMap(
      externalCoreSpendKeys(pending),
      requestContext,
    );
    const facts = pending.flatMap((application) =>
      coreApplicationTransactionFacts(application, createdOutputs, currentOutputs),
    );

    await this.insertRows(
      analyticsTransactionsTable,
      facts.map(toAnalyticsTransactionFactInsertRow),
      requestContext,
    );
  }

  private async applyCoreCurrentStateWindowIfEnabled(
    pending: CoreDogecoinBlockApplication[],
    context: CoreDogecoinApplyContext | undefined,
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    if (!shouldUpdateCoreCurrentState(context)) {
      return;
    }

    await this.applyCoreCurrentStateWindow(pending, requestContext);
  }

  private async applyCoreCurrentStateWindow(
    pending: CoreDogecoinBlockApplication[],
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    const currentState = await this.buildCoreCurrentStateWindow(pending, requestContext);
    if (!currentState) {
      return;
    }

    await this.insertCoreCurrentStateWindow(currentState, pending, requestContext);
  }

  private async buildCoreCurrentStateWindow(
    pending: CoreDogecoinBlockApplication[],
    requestContext: ClickHouseRequestContext,
  ): Promise<CoreCurrentStateWindow | null> {
    const windowEnd = coreWindowEnd(pending);
    if (windowEnd < 0) {
      return null;
    }

    const currentOutputs = await this.getCurrentUtxoOutputMap(
      coreWindowSpendKeys(pending),
      requestContext,
    );
    const mutation = applyCoreCurrentStateMutations(pending, currentOutputs);
    const currentBalances = await this.getBalanceRowsByKeys(
      [...mutation.balanceDeltas.keys()],
      requestContext,
    );

    return {
      nextBalances: coreCurrentBalanceRows(windowEnd, mutation.balanceDeltas, currentBalances),
      nextOutputs: mutation.nextOutputs,
      windowEnd,
    };
  }

  private async insertCoreCurrentStateWindow(
    currentState: CoreCurrentStateWindow,
    pending: CoreDogecoinBlockApplication[],
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    await this.insertRows(
      utxoCurrentStateTable,
      [...currentState.nextOutputs.values()].map((row) =>
        toUtxoInsertRow(row, coreCurrentUtxoVersion(row)),
      ),
      requestContext,
    );
    await this.insertRows(
      balancesTable,
      currentState.nextBalances.map((row) => toBalanceInsertRow(row, row.version)),
      requestContext,
    );
    await this.insertRows(
      analyticsBalancesCurrentTable,
      currentState.nextBalances.map((row) => toAnalyticsBalanceCurrentInsertRow(row, row.version)),
      requestContext,
    );
    await this.insertRows(
      appliedBlocksTable,
      toProjectionAppliedBlocks(pending).map(toAppliedBlockInsertRow),
      requestContext,
    );
  }

  public async materializeCoreDogecoinCurrentState(
    asOfBlockHeight: number,
    context?: CoreDogecoinApplyContext,
  ): Promise<void> {
    await this.clearCoreDogecoinCurrentState({
      currentUtxosTable: utxoCurrentStateTable,
      currentUtxosByAddressTable: utxoCurrentStateByAddressTable,
      balancesTable,
      appliedBlocksTable,
      ...coreApplyContextOption(context),
    });
    await this.insertCoreCurrentStateMaterialization({
      asOfBlockHeight,
      createsTable: coreUtxoCreatesTable,
      spendsTable: coreUtxoSpendsTable,
      currentUtxosTable: utxoCurrentStateTable,
      currentUtxosByAddressTable: utxoCurrentStateByAddressTable,
      balancesTable,
      appliedBlocksTable,
      processedBlocksTable: coreProcessedBlocksTable,
      ...coreApplyContextOption(context),
    });
  }

  private async clearCoreDogecoinCurrentState(input: {
    appliedBlocksTable: string;
    balancesTable: string;
    context?: CoreDogecoinApplyContext;
    currentUtxosByAddressTable: string;
    currentUtxosTable: string;
  }): Promise<void> {
    const materializationSettings = clickHouseCoreMaterializationSettings(input.context);
    const mutationSettings = {
      ...materializationSettings,
      mutations_sync: '2',
    };
    for (const table of [
      input.currentUtxosTable,
      input.currentUtxosByAddressTable,
      input.balancesTable,
      input.appliedBlocksTable,
    ]) {
      await this.executeCommand({
        query: `ALTER TABLE ${table} DELETE WHERE 1 = 1`,
        clickhouse_settings: mutationSettings,
      });
    }
    await this.executeCommand({
      query: `ALTER TABLE ${analyticsBalancesCurrentTable} DELETE WHERE 1 = 1`,
      clickhouse_settings: mutationSettings,
    });
  }

  public async resetCoreDogecoinStorage(): Promise<void> {
    for (const table of clickHouseDestructiveResetTables) {
      await this.executeCommand({
        query: `DROP TABLE IF EXISTS ${table} SYNC`,
      });
    }
    if (!this.schemaLock) {
      throw new Error('ClickHouse warehouse reset requires a metadata schema lock');
    }
    await runClickHouseMigrations(this.settings, this.schemaLock);
  }

  public async resetCoreDogecoinBenchmarkStorage(
    prefix = 'core_backfill_benchmark',
  ): Promise<void> {
    await this.dropCoreDogecoinBenchmarkTables(prefix);
    await this.createCoreDogecoinBenchmarkStorage(prefix);
  }

  public async dropCoreDogecoinBenchmarkStorage(prefix = 'core_backfill_benchmark'): Promise<void> {
    await this.dropCoreDogecoinBenchmarkTables(prefix);
  }

  private async dropCoreDogecoinBenchmarkTables(prefix: string): Promise<void> {
    const tables = coreBenchmarkTableNames(prefix);
    for (const table of Object.values(tables).reverse()) {
      await this.executeCommand({
        query: `DROP TABLE IF EXISTS ${table} SYNC`,
      });
    }
  }

  public async insertCoreDogecoinBenchmarkWindow(
    input: CoreDogecoinBlockApplication[],
    prefix = 'core_backfill_benchmark',
  ): Promise<{ rowsInserted: number }> {
    if (input.length === 0) {
      return { rowsInserted: 0 };
    }

    this.assertCoreWindowShape(input);
    const tables = coreBenchmarkTableNames(prefix);
    const rows = coreBenchmarkRows(input);
    await this.insertRows(tables.creates, rows.createRows.map(toCoreUtxoCreateInsertRow));
    await this.insertRows(tables.spends, rows.spendRows.map(toCoreUtxoSpendInsertRow));
    await this.insertRows(tables.processedBlocks, input.map(toCoreProcessedBlockInsertRow));

    return {
      rowsInserted: coreBenchmarkRowsInserted(rows, input),
    };
  }

  public async materializeCoreDogecoinBenchmarkCurrentState(
    prefix: string,
    asOfBlockHeight: number,
  ): Promise<void> {
    const tables = coreBenchmarkTableNames(prefix);
    await this.executeCommand({
      query: `TRUNCATE TABLE ${tables.currentUtxos}`,
    });
    await this.executeCommand({ query: `TRUNCATE TABLE ${tables.balances}` });
    await this.executeCommand({
      query: `TRUNCATE TABLE ${tables.appliedBlocks}`,
    });
    await this.insertCoreCurrentStateMaterialization({
      asOfBlockHeight,
      createsTable: tables.creates,
      spendsTable: tables.spends,
      currentUtxosTable: tables.currentUtxos,
      currentUtxosByAddressTable: tables.currentUtxos,
      balancesTable: tables.balances,
      appliedBlocksTable: tables.appliedBlocks,
      processedBlocksTable: tables.processedBlocks,
    });
  }

  private async createCoreDogecoinBenchmarkStorage(prefix: string): Promise<void> {
    for (const statement of coreBenchmarkBootstrapStatements(coreBenchmarkTableNames(prefix))) {
      await this.executeCommand({ query: statement });
    }
  }

  private async insertCoreCurrentStateMaterialization(input: {
    appliedBlocksTable: string;
    asOfBlockHeight: number;
    balancesTable: string;
    createsTable: string;
    currentUtxosByAddressTable: string;
    currentUtxosTable: string;
    context?: CoreDogecoinApplyContext;
    processedBlocksTable: string;
    spendsTable: string;
  }): Promise<void> {
    const materializationSettings = clickHouseCoreMaterializationSettings(input.context);
    for (const range of coreCurrentStateOutputKeyRanges) {
      await this.executeCommand({
        query: `
          INSERT INTO ${input.currentUtxosTable} (
            block_height,
            block_hash,
            block_time,
            txid,
            tx_index,
            vout,
            output_key,
            address,
            script_type,
            value_base,
            is_coinbase,
            is_spendable,
            spent_by_txid,
            spent_in_block,
            spent_input_index,
            version
          )
          SELECT
            c.block_height,
            c.block_hash,
            c.block_time,
            c.txid,
            c.tx_index,
            c.vout,
            c.output_key,
            c.address,
            c.script_type,
            c.value_base,
            c.is_coinbase,
            c.is_spendable,
            NULL,
            NULL,
            NULL,
            {asOfBlockHeight:UInt64}
          FROM (
            SELECT *
            FROM ${input.createsTable}
            WHERE
              version <= {asOfBlockHeight:UInt64}
              ${clickHouseStringRangeClause('output_key', range)}
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          ) AS c
          LEFT ANTI JOIN (
            SELECT spent_output_key
            FROM ${input.spendsTable}
            WHERE
              version <= {asOfBlockHeight:UInt64}
              ${clickHouseStringRangeClause('spent_output_key', range)}
            ORDER BY spent_output_key ASC, version DESC
            LIMIT 1 BY spent_output_key
          ) AS s
          ON c.output_key = s.spent_output_key
        `,
        query_params: {
          ...clickHouseStringRangeParams(range),
          asOfBlockHeight: input.asOfBlockHeight,
        },
        clickhouse_settings: materializationSettings,
      });
    }
    await this.executeCommand({
      query: `
        INSERT INTO ${input.balancesTable} (
          address,
          asset_address,
          balance,
          as_of_block_height,
          version
        )
        SELECT
          address,
          '',
          toString(sum(toInt256(value_base))),
          {asOfBlockHeight:UInt64},
          {asOfBlockHeight:UInt64}
        FROM ${input.currentUtxosByAddressTable}
        WHERE
          is_spendable = 1
          AND address != ''
          AND spent_by_txid IS NULL
        GROUP BY address
      `,
      query_params: { asOfBlockHeight: input.asOfBlockHeight },
      clickhouse_settings: {
        ...materializationSettings,
        optimize_aggregation_in_order: 1,
      },
    });
    await this.executeCommand({
      query: `
        INSERT INTO ${analyticsBalancesCurrentTable} (
          address,
          asset_address,
          balance,
          as_of_block_height,
          version
        )
        SELECT
          address,
          '',
          toString(sum(toInt256(value_base))),
          {asOfBlockHeight:UInt64},
          {asOfBlockHeight:UInt64}
        FROM ${input.currentUtxosByAddressTable}
        WHERE
          is_spendable = 1
          AND address != ''
          AND spent_by_txid IS NULL
        GROUP BY address
      `,
      query_params: { asOfBlockHeight: input.asOfBlockHeight },
      clickhouse_settings: {
        ...materializationSettings,
        optimize_aggregation_in_order: 1,
      },
    });
    await this.executeCommand({
      query: `
        INSERT INTO ${input.appliedBlocksTable} (block_height, block_hash)
        SELECT block_height, block_hash
        FROM (
          SELECT block_height, block_hash, version
          FROM ${input.processedBlocksTable}
          WHERE
            block_height <= {asOfBlockHeight:UInt64}
          ORDER BY block_height ASC, version DESC
          LIMIT 1 BY block_height
        )
      `,
      query_params: { asOfBlockHeight: input.asOfBlockHeight },
      clickhouse_settings: materializationSettings,
    });
  }

  public async getCurrentAddressSummary(address: string) {
    const summary = await this.getAddressSummary(address);
    if (!summary) {
      return null;
    }

    return {
      balance: summary.balance,
      utxoCount: summary.utxoCount,
    };
  }

  public async listAppliedBlocks(offset = 0, limit?: number) {
    const pagination = clickHousePagination(offset, limit);
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
    }>({
      query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash"
          FROM ${appliedBlocksTable}
          ORDER BY block_height DESC
          ${pagination.limitClause}
          ${pagination.offsetClause}
        `,
      query_params: {
        ...pagination.queryParams,
      },
      format: 'JSONEachRow',
    });

    return rows;
  }

  public async getAppliedBlockByHash(blockHash: string) {
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
    }>({
      query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash"
          FROM ${appliedBlocksTable}
          WHERE block_hash = {blockHash:String}
          LIMIT 1
        `,
      query_params: { blockHash },
      format: 'JSONEachRow',
    });

    return rows[0] ?? null;
  }

  public async getTransactionRef(txid: string) {
    const indexedRef = await this.getIndexedTransactionRef(txid);
    if (indexedRef) {
      return indexedRef;
    }

    const coreRef = await this.getCoreTransactionRef(txid);
    if (coreRef) {
      return coreRef;
    }

    return this.getCurrentTransactionRef(txid);
  }

  public async upsertTransactionRefs(
    refs: Array<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      source: 'raw_sync' | 'core_process';
      txIndex: number;
      txid: string;
      version: number;
    }>,
  ): Promise<void> {
    if (refs.length === 0) {
      return;
    }

    await this.insertRows(
      transactionRefsTable,
      refs.map((ref) => ({
        txid: ref.txid,
        block_height: ref.blockHeight,
        block_hash: ref.blockHash,
        block_time: ref.blockTime,
        tx_index: ref.txIndex,
        source: ref.source === 'raw_sync' ? 1 : 2,
        version: ref.version,
      })),
    );
  }

  private async getIndexedTransactionRef(txid: string) {
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      txIndex: number;
    }>({
      query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash",
            block_time AS "blockTime",
            tx_index AS "txIndex"
          FROM ${transactionRefsTable}
          WHERE txid = {txid:String}
          ORDER BY version DESC
          LIMIT 1
        `,
      query_params: { txid },
      format: 'JSONEachRow',
    });

    return rows[0] ?? null;
  }

  private async getCurrentTransactionRef(txid: string) {
    const prefix = `${txid}:`;
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      txIndex: number;
    }>({
      query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash",
            block_time AS "blockTime",
            tx_index AS "txIndex"
          FROM ${utxoCurrentStateTable}
          WHERE
            output_key >= {prefix:String}
            AND output_key < {prefixEnd:String}
          ORDER BY output_key ASC, version DESC
          LIMIT 1 BY output_key
          LIMIT 1
        `,
      query_params: {
        prefix,
        prefixEnd: `${txid};`,
      },
      format: 'JSONEachRow',
    });

    return rows[0] ?? null;
  }

  private async getCoreTransactionRef(txid: string) {
    const prefix = `${txid}:`;
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      txIndex: number;
    }>({
      query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash",
            block_time AS "blockTime",
            tx_index AS "txIndex"
          FROM ${coreUtxoCreatesTable}
          WHERE
            output_key >= {prefix:String}
            AND output_key < {prefixEnd:String}
          ORDER BY output_key ASC, version DESC
          LIMIT 1 BY output_key
          LIMIT 1
        `,
      query_params: {
        prefix,
        prefixEnd: `${txid};`,
      },
      format: 'JSONEachRow',
    });

    return rows[0] ?? null;
  }

  public async getAddressSummary(address: string) {
    const [movementFromTable, balance, utxoCount] = await Promise.all([
      this.queryAddressMovementSummary(address),
      this.queryNativeBalance(address),
      this.querySpendableUtxoCount(address),
    ]);
    return buildClickHouseAddressSummary(movementFromTable, balance, utxoCount);
  }

  private async queryAddressMovementSummary(
    address: string,
  ): Promise<AddressMovementSummaryRow | undefined> {
    const rows = await this.queryRows<AddressMovementSummaryRow>({
      query: `
          SELECT
            CAST(sumIf(amount_base_i256, direction = 'credit') AS String) AS "receivedBase",
            CAST(sumIf(amount_base_i256, direction = 'debit') AS String) AS "sentBase",
            uniqExact(txid) AS "txCount"
          FROM ${addressMovementsByAddressTable}
          WHERE address = {address:String} AND asset_address = ''
        `,
      query_params: { address },
      format: 'JSONEachRow',
    });

    return rows[0];
  }

  private async queryNativeBalance(address: string): Promise<string> {
    const rows = await this.queryRows<{ balance: string }>({
      query: `
          SELECT CAST(sum(toInt256(value_base)) AS String) AS balance
          FROM (
            SELECT
              output_key,
              value_base,
              is_spendable,
              spent_by_txid
            FROM ${utxoCurrentStateByAddressTable}
            WHERE address = {address:String}
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          )
          WHERE is_spendable = 1 AND spent_by_txid IS NULL
        `,
      query_params: { address },
      format: 'JSONEachRow',
    });

    const first = rows[0];
    return first ? first.balance : '0';
  }

  private async querySpendableUtxoCount(address: string): Promise<number> {
    const rows = await this.queryRows<{ utxoCount: number }>({
      query: `
          SELECT count() AS "utxoCount"
          FROM (
            SELECT
              output_key,
              is_spendable,
              spent_by_txid
            FROM ${utxoCurrentStateByAddressTable}
            WHERE address = {address:String}
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          )
          WHERE is_spendable = 1 AND spent_by_txid IS NULL
        `,
      query_params: { address },
      format: 'JSONEachRow',
    });

    const first = rows[0];
    return first ? first.utxoCount : 0;
  }

  public async listAddressTransactions(address: string, offset = 0, limit?: number) {
    const pagination = clickHousePagination(offset, limit);
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      feeBase: string | null;
      inputCount: number;
      isCoinbase: boolean;
      outputCount: number;
      receivedBase: string;
      sentBase: string;
      totalInputBase: string;
      totalOutputBase: string;
      txIndex: number;
      txid: string;
    }>({
      query: `
          SELECT
            movements.block_height AS "blockHeight",
            movements.block_hash AS "blockHash",
            movements.block_time AS "blockTime",
            movements.txid,
            movements.tx_index AS "txIndex",
            CAST(movements.receivedBase AS String) AS "receivedBase",
            CAST(movements.sentBase AS String) AS "sentBase",
            facts.is_coinbase AS "isCoinbase",
            facts.input_count AS "inputCount",
            facts.output_count AS "outputCount",
            facts.total_input_base AS "totalInputBase",
            facts.gross_output_base AS "totalOutputBase",
            facts.fee_base AS "feeBase"
          FROM (
            SELECT
              block_height,
              block_hash,
              block_time,
              txid,
              tx_index,
              sumIf(amount_base_i256, direction = 'credit') AS receivedBase,
              sumIf(amount_base_i256, direction = 'debit') AS sentBase
            FROM ${addressMovementsByAddressTable}
            WHERE address = {address:String} AND asset_address = ''
            GROUP BY block_height, block_hash, block_time, txid, tx_index
          ) AS movements
          LEFT JOIN (
            SELECT
              block_height,
              block_hash,
              txid,
              argMax(is_coinbase, version) AS is_coinbase,
              argMax(input_count, version) AS input_count,
              argMax(output_count, version) AS output_count,
              argMax(total_input_base, version) AS total_input_base,
              argMax(gross_output_base, version) AS gross_output_base,
              argMax(fee_base, version) AS fee_base
            FROM ${analyticsTransactionsTable}
            GROUP BY block_height, block_hash, txid
          ) AS facts
            ON movements.block_height = facts.block_height
           AND movements.block_hash = facts.block_hash
           AND movements.txid = facts.txid
          ORDER BY movements.block_height DESC, movements.tx_index DESC, movements.txid DESC
          ${pagination.limitClause}
          ${pagination.offsetClause}
        `,
      query_params: {
        address,
        ...pagination.queryParams,
      },
      format: 'JSONEachRow',
    });

    return rows.map((row) => ({
      ...row,
      feeBase: row.feeBase ?? null,
      isCoinbase: Boolean(row.isCoinbase),
      totalInputBase: row.totalInputBase ?? '0',
      totalOutputBase: row.totalOutputBase ?? '0',
    }));
  }

  public async listAddressUtxos(address: string, offset = 0, limit?: number) {
    const pagination = clickHousePagination(offset, limit);
    return await this.queryRows<ProjectionUtxoOutput>({
      query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash",
            block_time AS "blockTime",
            txid,
            tx_index AS "txIndex",
            vout,
            output_key AS "outputKey",
            address,
            script_type AS "scriptType",
            value_base AS "valueBase",
            is_coinbase = 1 AS "isCoinbase",
            is_spendable = 1 AS "isSpendable",
            spent_by_txid AS "spentByTxid",
            spent_in_block AS "spentInBlock",
            spent_input_index AS "spentInputIndex"
          FROM (
            SELECT
              block_height,
              block_hash,
              block_time,
              txid,
              tx_index,
              vout,
              output_key,
              address,
              script_type,
              value_base,
              is_coinbase,
              is_spendable,
              spent_by_txid,
              spent_in_block,
              spent_input_index,
              version
            FROM ${utxoCurrentStateByAddressTable}
            WHERE address = {address:String}
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          )
          WHERE is_spendable = 1 AND spent_by_txid IS NULL
          ORDER BY block_height DESC, tx_index DESC, vout ASC
          ${pagination.limitClause}
          ${pagination.offsetClause}
        `,
      query_params: {
        address,
        ...pagination.queryParams,
      },
      format: 'JSONEachRow',
    });
  }

  public async getUtxoOutput(outputKey: string): Promise<ProjectionUtxoOutput | null> {
    return (await this.getUtxoOutputs([outputKey])).get(outputKey) ?? null;
  }

  public async getUtxoOutputs(outputKeys: string[]): Promise<Map<string, ProjectionUtxoOutput>> {
    if (outputKeys.length === 0) {
      return new Map();
    }

    const currentOutputs = await this.getCurrentUtxoOutputMap(outputKeys);
    await this.addMissingUtxoOutputs(outputKeys, currentOutputs);
    return currentOutputs;
  }

  public async getCreatedUtxoOutputs(
    outputKeys: string[],
  ): Promise<Map<string, ExplorerCreatedUtxoOutput>> {
    return this.queryCoreCreatedUtxoOutputs(outputKeys);
  }

  private async addMissingUtxoOutputs(
    outputKeys: string[],
    currentOutputs: Map<string, ProjectionUtxoOutput>,
  ): Promise<void> {
    const missingOutputKeys = missingUtxoOutputKeys(outputKeys, currentOutputs);
    if (missingOutputKeys.length === 0) {
      return;
    }

    await this.addFallbackUtxoOutputs(missingOutputKeys, currentOutputs);
    await this.addHistoricalUtxoOutputsIfMissing(outputKeys, currentOutputs);
  }

  private async addHistoricalUtxoOutputsIfMissing(
    outputKeys: string[],
    currentOutputs: Map<string, ProjectionUtxoOutput>,
  ): Promise<void> {
    const stillMissingOutputKeys = missingUtxoOutputKeys(outputKeys, currentOutputs);
    if (stillMissingOutputKeys.length === 0) {
      return;
    }

    await this.addCoreHistoricalUtxoOutputs(stillMissingOutputKeys, currentOutputs);
  }

  private async getCurrentUtxoOutputMap(
    outputKeys: string[],
    requestContext?: ClickHouseRequestContext,
  ): Promise<Map<string, ProjectionUtxoOutput>> {
    const currentRows = await this.queryUtxoOutputsFromTable(
      utxoCurrentStateTable,
      outputKeys,
      requestContext,
    );
    return new Map(currentRows.map((row) => [row.outputKey, row]));
  }

  private async addFallbackUtxoOutputs(
    missingOutputKeys: string[],
    currentOutputs: Map<string, ProjectionUtxoOutput>,
  ): Promise<void> {
    const fallbackRows = await this.queryUtxoOutputsFromTable(
      utxoCurrentStateTable,
      missingOutputKeys,
    );
    await this.insertFallbackUtxoOutputsIfPresent(fallbackRows, currentOutputs);
  }

  private async insertFallbackUtxoOutputsIfPresent(
    fallbackRows: ProjectionUtxoOutput[],
    currentOutputs: Map<string, ProjectionUtxoOutput>,
  ): Promise<void> {
    if (fallbackRows.length === 0) {
      return;
    }

    await this.insertRows(
      utxoCurrentStateTable,
      fallbackRows.map((row) => toUtxoInsertRow(row, fallbackUtxoVersion(row))),
    );
    this.addCurrentUtxoOutputs(fallbackRows, currentOutputs);
  }

  private addCurrentUtxoOutputs(
    rows: ProjectionUtxoOutput[],
    currentOutputs: Map<string, ProjectionUtxoOutput>,
  ): void {
    for (const row of rows) {
      currentOutputs.set(row.outputKey, row);
    }
  }

  private async addCoreHistoricalUtxoOutputs(
    missingOutputKeys: string[],
    currentOutputs: Map<string, ProjectionUtxoOutput>,
  ): Promise<void> {
    const rows = await this.queryCoreHistoricalUtxoOutputRows(missingOutputKeys);
    for (const row of rows) {
      currentOutputs.set(row.outputKey, row);
    }
  }

  private async queryCoreHistoricalUtxoOutputRows(
    outputKeys: string[],
    requestContext?: ClickHouseRequestContext,
    asOfBlockHeight?: number,
  ): Promise<ProjectionUtxoOutput[]> {
    if (outputKeys.length === 0 || (asOfBlockHeight !== undefined && asOfBlockHeight < 0)) {
      return [];
    }

    const rowChunks = await mapWithConcurrency(
      chunkQueryValues([...new Set(outputKeys)], {
        maxBytes: maxClickHouseCoreOutputKeyBytesPerChunk,
        maxValues: maxClickHouseCoreOutputKeyValuesPerChunk,
      }),
      maxClickHouseCoreOutputKeyQueryConcurrency,
      (chunk) =>
        this.queryRows<ProjectionUtxoOutput>(
          {
            query: `
              SELECT
                c.block_height AS "blockHeight",
                c.block_hash AS "blockHash",
                c.block_time AS "blockTime",
                c.txid,
                c.tx_index AS "txIndex",
                c.vout,
                c.output_key AS "outputKey",
                c.address,
                c.script_type AS "scriptType",
                c.value_base AS "valueBase",
                c.is_coinbase = 1 AS "isCoinbase",
                c.is_spendable = 1 AS "isSpendable",
                s.spent_by_txid AS "spentByTxid",
                s.spent_in_block AS "spentInBlock",
                s.spent_input_index AS "spentInputIndex"
              FROM (
                SELECT
                  block_height,
                  block_hash,
                  block_time,
                  txid,
                  tx_index,
                  vout,
                  output_key,
                  address,
                  script_type,
                  value_base,
                  is_coinbase,
                  is_spendable
                FROM ${coreUtxoCreatesTable}
                WHERE
                  output_key IN ({outputKeys:Array(String)})
                  ${coreAsOfBlockHeightClause('block_height', asOfBlockHeight)}
                ORDER BY output_key ASC, version DESC
                LIMIT 1 BY output_key
              ) AS c
              LEFT JOIN (
                SELECT
                  spent_output_key,
                  spent_by_txid,
                  spent_in_block,
                  spent_input_index
                FROM ${coreUtxoSpendsTable}
                WHERE
                  spent_output_key IN ({outputKeys:Array(String)})
                  ${coreAsOfBlockHeightClause('spent_in_block', asOfBlockHeight)}
                ORDER BY spent_output_key ASC, version DESC
                LIMIT 1 BY spent_output_key
              ) AS s
              ON c.output_key = s.spent_output_key
              SETTINGS join_use_nulls = 1
            `,
            query_params: coreHistoricalUtxoOutputParams(chunk, asOfBlockHeight),
            format: 'JSONEachRow',
          },
          requestContext,
        ),
    );

    return rowChunks.flat();
  }

  private async queryCoreCreatedUtxoOutputs(
    outputKeys: string[],
  ): Promise<Map<string, ExplorerCreatedUtxoOutput>> {
    return this.queryCoreOutputKeyRows<ExplorerCreatedUtxoOutput>(outputKeys, (chunk) => ({
      query: `
          SELECT
            output_key AS "outputKey",
            address,
            value_base AS "valueBase"
          FROM ${coreUtxoCreatesTable}
          WHERE output_key IN ({outputKeys:Array(String)})
          ORDER BY output_key ASC, version DESC
          LIMIT 1 BY output_key
        `,
      query_params: { outputKeys: chunk },
      format: 'JSONEachRow',
    }));
  }

  public async hasAppliedBlock(blockHeight: number, blockHash: string): Promise<boolean> {
    const rows = await this.queryRows<Record<string, unknown>>({
      query: `
          SELECT 1
          FROM ${appliedBlocksTable}
          WHERE
            block_height = {blockHeight:UInt64}
            AND block_hash = {blockHash:String}
          LIMIT 1
        `,
      query_params: { blockHeight, blockHash },
      format: 'JSONEachRow',
    });

    return rows.length > 0;
  }

  public async listAppliedBlockSet(
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>> {
    if (blocks.length === 0) {
      return new Set();
    }

    const heights = [...new Set(blocks.map((block) => block.blockHeight))];
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
    }>({
      query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash"
          FROM ${appliedBlocksTable}
          WHERE block_height IN ({heights:Array(UInt64)})
        `,
      query_params: { heights },
      format: 'JSONEachRow',
    });

    const requested = new Set(
      blocks.map((block) => projectionBlockIdentity(block.blockHeight, block.blockHash)),
    );
    return new Set(
      rows
        .map((row) => projectionBlockIdentity(row.blockHeight, row.blockHash))
        .filter((identity) => requested.has(identity)),
    );
  }

  public async getAppliedBlockTail(): Promise<number | null> {
    const rows = await this.queryRows<{ blockHeight: number | null }>({
      query: `
          SELECT max(block_height) AS "blockHeight"
          FROM ${appliedBlocksTable}
        `,
      format: 'JSONEachRow',
    });

    return appliedBlockTail(rows);
  }

  public async applyProjectionWindow(batches: BlockProjectionBatch[]): Promise<void> {
    const window = await resolvePendingProjectionWindow(batches, (blocks) =>
      this.listAppliedBlockSet(blocks),
    );
    if (window === null) {
      return;
    }

    await this.applyPendingProjectionWindow(window);
  }

  private async applyPendingProjectionWindow(window: {
    pendingBatches: BlockProjectionBatch[];
  }): Promise<void> {
    const windowEnd = pendingProjectionWindowEnd(window.pendingBatches);
    const { nextBalances, nextOutputs } = await buildProjectionStateChanges<
      string,
      VersionedBalanceRow
    >({
      batches: window.pendingBatches,
      keyForMovement: (movement) => balanceKey(movement.address, movement.assetAddress),
      loadBalances: (keys) => this.getBalanceRowsByKeys(keys),
      loadOutputs: (outputKeys) => this.getUtxoOutputs(outputKeys),
      toSnapshotKey: (key) => balanceKey(key.address, key.assetAddress),
      toStoredSnapshot: (snapshot) => ({
        ...snapshot,
        version: windowEnd,
      }),
    });

    await this.insertRows(
      addressMovementsTable,
      window.pendingBatches.flatMap((batch) =>
        batch.addressMovements.map(toAddressMovementInsertRow),
      ),
    );
    await this.insertRows(
      utxoCurrentStateTable,
      [...nextOutputs.values()].map((row) => toUtxoInsertRow(row, windowEnd)),
    );
    await this.insertRows(
      balancesTable,
      [...nextBalances.values()].map((row) => toBalanceInsertRow(row, row.version)),
    );
    await this.insertRows(
      analyticsBalancesCurrentTable,
      [...nextBalances.values()].map((row) => toAnalyticsBalanceCurrentInsertRow(row, row.version)),
    );
    await this.insertRows(
      appliedBlocksTable,
      toProjectionAppliedBlocks(window.pendingBatches).map(toAppliedBlockInsertRow),
    );
  }

  public async applyProjectionFacts(window: ProjectionFactWindow): Promise<void> {
    await this.insertRows(
      addressMovementsTable,
      window.addressMovements.map(toAddressMovementInsertRow),
    );
    await this.insertRows(
      utxoCurrentStateTable,
      window.utxoOutputs.map((row) => toUtxoInsertRow(row, row.spentInBlock ?? row.blockHeight)),
    );
    await this.insertRows(
      balancesTable,
      window.balances.map((row) => toBalanceInsertRow(row, row.asOfBlockHeight)),
    );
    await this.insertRows(
      analyticsBalancesCurrentTable,
      window.balances.map((row) => toAnalyticsBalanceCurrentInsertRow(row, row.asOfBlockHeight)),
    );
    await this.insertRows(appliedBlocksTable, window.appliedBlocks.map(toAppliedBlockInsertRow));
  }

  public async applyBlockProjection(batch: BlockProjectionBatch): Promise<void> {
    await this.applyProjectionWindow([batch]);
  }

  public async exportProjectionStateSnapshot(): Promise<ProjectionStateBootstrapSnapshot> {
    const [utxoOutputs, balances] = await Promise.all([
      this.queryRows<ProjectionUtxoOutput>({
        query: `
            SELECT
              block_height AS "blockHeight",
              block_hash AS "blockHash",
              block_time AS "blockTime",
              txid,
              tx_index AS "txIndex",
              vout,
              output_key AS "outputKey",
              address,
              script_type AS "scriptType",
              value_base AS "valueBase",
              is_coinbase = 1 AS "isCoinbase",
              is_spendable = 1 AS "isSpendable",
              spent_by_txid AS "spentByTxid",
              spent_in_block AS "spentInBlock",
              spent_input_index AS "spentInputIndex"
            FROM ${utxoCurrentStateTable}
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          `,
        format: 'JSONEachRow',
      }),
      this.queryRows<ProjectionBalanceSnapshot>({
        query: `
            SELECT
              address,
              asset_address AS "assetAddress",
              balance,
              as_of_block_height AS "asOfBlockHeight"
            FROM ${balancesTable}
            ORDER BY address ASC, asset_address ASC, version DESC
            LIMIT 1 BY address, asset_address
          `,
        format: 'JSONEachRow',
      }),
    ]);

    return {
      appliedBlocks: [],
      utxoOutputs,
      balances,
    };
  }

  public async listCurrentUtxoOutputsPage(
    cursorOutputKey: string | null,
    limit: number,
    context?: ProjectionPageRequestContext,
  ): Promise<ProjectionCurrentUtxoPage> {
    const rows = await this.queryCurrentUtxoOutputPageRows(cursorOutputKey, limit, context);
    return toCurrentUtxoPage(rows, limit);
  }

  private async queryCurrentUtxoOutputPageRows(
    cursorOutputKey: string | null,
    limit: number,
    context?: ProjectionPageRequestContext,
  ): Promise<ProjectionUtxoOutput[]> {
    const timeoutMs = queryTimeoutMs(context, this.requestTimeoutMs);
    return this.queryRowsWithDeadline<ProjectionUtxoOutput>(
      {
        query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash",
            block_time AS "blockTime",
            txid,
            tx_index AS "txIndex",
            vout,
            output_key AS "outputKey",
            address,
            script_type AS "scriptType",
            value_base AS "valueBase",
            is_coinbase = 1 AS "isCoinbase",
            is_spendable = 1 AS "isSpendable",
            spent_by_txid AS "spentByTxid",
            spent_in_block AS "spentInBlock",
            spent_input_index AS "spentInputIndex"
          FROM (
            SELECT
              block_height,
              block_hash,
              block_time,
              txid,
              tx_index,
              vout,
              output_key,
              address,
              script_type,
              value_base,
              is_coinbase,
              is_spendable,
              spent_by_txid,
              spent_in_block,
              spent_input_index,
              version
            FROM ${utxoCurrentStateTable}
            WHERE
              1 = 1
              ${clickHouseOutputKeyCursorClause(cursorOutputKey)}
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          )
          ORDER BY output_key ASC
          LIMIT {limit:UInt64}
        `,
        query_params: clickHouseOutputPageParams(cursorOutputKey, limit),
        format: 'JSONEachRow',
        clickhouse_settings: {
          max_execution_time: toClickHouseMaxExecutionTimeSeconds(timeoutMs),
        },
      },
      context,
    );
  }

  public async listCurrentBalancesPage(
    cursor: ProjectionBalanceCursor | null,
    limit: number,
    context?: ProjectionPageRequestContext,
  ): Promise<ProjectionCurrentBalancePage> {
    const timeoutMs = queryTimeoutMs(context, this.requestTimeoutMs);
    const rows = await this.queryRowsWithDeadline<ProjectionBalanceSnapshot>(
      {
        query: `
          SELECT
            address,
            asset_address AS "assetAddress",
            balance,
            as_of_block_height AS "asOfBlockHeight"
          FROM (
            SELECT
              address,
              asset_address,
              balance,
              as_of_block_height,
              version
            FROM ${balancesTable}
            WHERE
              1 = 1
              ${clickHouseBalanceCursorClause(cursor)}
            ORDER BY address ASC, asset_address ASC, version DESC
            LIMIT 1 BY address, asset_address
          )
          ORDER BY address ASC, asset_address ASC
          LIMIT {limit:UInt64}
        `,
        query_params: clickHouseBalancePageParams(cursor, limit),
        format: 'JSONEachRow',
        clickhouse_settings: {
          max_execution_time: toClickHouseMaxExecutionTimeSeconds(timeoutMs),
        },
      },
      context,
    );

    return toCurrentBalancePage(rows, limit);
  }

  private async getBalanceRowsByKeys(
    keys: string[],
    requestContext?: ClickHouseRequestContext,
  ): Promise<Map<string, VersionedBalanceRow>> {
    if (keys.length === 0) {
      return new Map();
    }

    const rowChunks: Array<
      Array<
        BalanceRow & {
          version: number;
        }
      >
    > = await Promise.all(
      chunkQueryValues(keys).map((chunk) =>
        this.queryRows<
          BalanceRow & {
            version: number;
          }
        >(
          {
            query: `
              SELECT
                address,
                asset_address AS "assetAddress",
                balance,
                as_of_block_height AS "asOfBlockHeight",
                version
              FROM ${balancesTable}
              WHERE (address, asset_address) IN ${formatBalanceTupleList(chunk)}
              ORDER BY address ASC, asset_address ASC, version DESC
              LIMIT 1 BY address, asset_address
          `,
            format: 'JSONEachRow',
          },
          requestContext,
        ),
      ),
    );
    const rows = rowChunks.flat();

    return new Map(
      rows.map((row) => [
        balanceKey(row.address, row.assetAddress),
        {
          address: row.address,
          assetAddress: row.assetAddress,
          balance: row.balance,
          asOfBlockHeight: row.asOfBlockHeight,
          version: row.version,
        },
      ]),
    );
  }

  private assertCoreWindowShape(applications: CoreDogecoinBlockApplication[]): void {
    const state = createCoreWindowShapeState();

    for (const application of applications) {
      assertCoreApplicationShape(application, state);
      rememberCoreApplicationShape(application, state);
    }
  }

  private async assertCoreWindowPreviousBlock(
    firstApplication: CoreDogecoinBlockApplication | undefined,
    requestContext?: ClickHouseRequestContext,
  ): Promise<void> {
    const previousHeight = previousCoreBlockHeight(firstApplication);
    if (previousHeight === null) {
      return;
    }

    const [previous] = await this.queryRows<CoreProcessedBlockRow>(
      {
        query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash"
          FROM ${coreProcessedBlocksTable}
          WHERE block_height = {blockHeight:UInt64}
          ORDER BY version DESC
          LIMIT 1
        `,
        query_params: {
          blockHeight: previousHeight,
        },
        format: 'JSONEachRow',
      },
      requestContext,
    );

    assertPreviousCoreBlockMatches(previous, firstApplication as CoreDogecoinBlockApplication);
  }

  private async assertCoreWindowPrevouts(
    applications: CoreDogecoinBlockApplication[],
    requestContext?: ClickHouseRequestContext,
  ): Promise<void> {
    const externalSpendKeys = externalCoreSpendKeys(applications);
    if (externalSpendKeys.length === 0) {
      return;
    }

    const [created, spent] = await Promise.all([
      this.getCoreUtxoCreateRows(externalSpendKeys, requestContext),
      this.getCoreUtxoSpendRows(externalSpendKeys, requestContext),
    ]);
    assertCorePrevoutsExist(externalSpendKeys, created);
    assertCorePrevoutsUnspent(externalSpendKeys, spent, coreSpendsInWindow(applications));
  }

  private async getCoreProcessedBlocks(
    blockHeights: number[],
    requestContext?: ClickHouseRequestContext,
  ): Promise<Map<number, CoreProcessedBlockRow>> {
    if (blockHeights.length === 0) {
      return new Map();
    }

    const rowChunks = await Promise.all(
      chunkQueryValues([...new Set(blockHeights)]).map((chunk) =>
        this.queryRows<CoreProcessedBlockRow>(
          {
            query: `
            SELECT
              block_height AS "blockHeight",
              block_hash AS "blockHash"
            FROM ${coreProcessedBlocksTable}
            WHERE block_height IN ({blockHeights:Array(UInt64)})
            ORDER BY block_height ASC, version DESC
            LIMIT 1 BY block_height
          `,
            query_params: { blockHeights: chunk },
            format: 'JSONEachRow',
          },
          requestContext,
        ),
      ),
    );

    return new Map(rowChunks.flat().map((row) => [row.blockHeight, row]));
  }

  private async getCoreUtxoCreateRows(
    outputKeys: string[],
    requestContext?: ClickHouseRequestContext,
  ): Promise<Map<string, CoreUtxoCreateRow>> {
    return this.queryCoreOutputKeyRows(
      outputKeys,
      (chunk) => ({
        query: `
            SELECT
              block_height AS "blockHeight",
              block_hash AS "blockHash",
              block_time AS "blockTime",
              txid,
              tx_index AS "txIndex",
              vout,
              output_key AS "outputKey"
            FROM ${coreUtxoCreatesTable}
            WHERE output_key IN ({outputKeys:Array(String)})
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          `,
        query_params: { outputKeys: chunk },
        format: 'JSONEachRow',
      }),
      requestContext,
    );
  }

  private async getCoreUtxoSpendRows(
    outputKeys: string[],
    requestContext?: ClickHouseRequestContext,
  ): Promise<Map<string, CoreUtxoSpendRow>> {
    return this.queryCoreOutputKeyRows(
      outputKeys,
      (chunk) => ({
        query: `
            SELECT
              spent_output_key AS "outputKey",
              spent_by_txid AS "spentByTxid",
              spent_in_block AS "spentInBlock",
              spent_input_index AS "spentInputIndex"
            FROM ${coreUtxoSpendsTable}
            WHERE spent_output_key IN ({outputKeys:Array(String)})
            ORDER BY spent_output_key ASC, version DESC
            LIMIT 1 BY spent_output_key
          `,
        query_params: { outputKeys: chunk },
        format: 'JSONEachRow',
      }),
      requestContext,
    );
  }

  private async queryCoreOutputKeyRows<T extends { outputKey: string }>(
    outputKeys: string[],
    queryForChunk: (chunk: string[]) => ClickHouseJsonQueryParameters,
    requestContext?: ClickHouseRequestContext,
  ): Promise<Map<string, T>> {
    if (outputKeys.length === 0) {
      return new Map();
    }

    const rowChunks = await mapWithConcurrency(
      chunkQueryValues([...new Set(outputKeys)], {
        maxBytes: maxClickHouseCoreOutputKeyBytesPerChunk,
        maxValues: maxClickHouseCoreOutputKeyValuesPerChunk,
      }),
      maxClickHouseCoreOutputKeyQueryConcurrency,
      (chunk) => this.queryRows<T>(queryForChunk(chunk), requestContext),
    );

    return new Map(rowChunks.flat().map((row) => [row.outputKey, row]));
  }

  private async insertRows(
    table: string,
    values: Record<string, unknown>[],
    requestContext?: ClickHouseRequestContext,
  ): Promise<void> {
    if (values.length === 0) {
      return;
    }

    await this.executeInsert(
      {
        table,
        values,
        format: 'JSONEachRow',
      },
      requestContext,
    );
  }

  public async insertMempoolSamples(rows: MempoolSampleRow[]): Promise<void> {
    await this.insertRows(mempoolSamplesTable, rows.map(toMempoolSampleInsertRow));
  }

  private async queryUtxoOutputsFromTable(
    table: string,
    outputKeys: string[],
    requestContext?: ClickHouseRequestContext,
  ): Promise<ProjectionUtxoOutput[]> {
    const rowChunks: ProjectionUtxoOutput[][] = await Promise.all(
      chunkQueryValues(outputKeys, {
        maxBytes: maxClickHouseHotOutputKeyBytesPerChunk,
        maxValues: maxClickHouseHotOutputKeyValuesPerChunk,
      }).map((chunk) =>
        this.queryRows<ProjectionUtxoOutput>(
          {
            query: `
              SELECT
                block_height AS "blockHeight",
                block_hash AS "blockHash",
                block_time AS "blockTime",
                txid,
                tx_index AS "txIndex",
                vout,
                output_key AS "outputKey",
                address,
                script_type AS "scriptType",
                value_base AS "valueBase",
                is_coinbase = 1 AS "isCoinbase",
                is_spendable = 1 AS "isSpendable",
                spent_by_txid AS "spentByTxid",
                spent_in_block AS "spentInBlock",
                spent_input_index AS "spentInputIndex"
              FROM ${table}
              WHERE output_key IN ({outputKeys:Array(String)})
              ORDER BY output_key ASC, version DESC
              LIMIT 1 BY output_key
          `,
            query_params: { outputKeys: chunk },
            format: 'JSONEachRow',
          },
          requestContext,
        ),
      ),
    );

    return rowChunks.flat();
  }

  public async insertAnalyticsTransactionFacts(rows: AnalyticsTransactionFact[]): Promise<void> {
    await this.insertRows(
      analyticsTransactionsTable,
      rows.map(toAnalyticsTransactionFactInsertRow),
    );
  }

  public async backfillAnalyticsTransactionFacts(input: {
    throughBlockHeight: number;
  }): Promise<{ rowsInserted: number | null; throughBlockHeight: number }> {
    await this.executeCommand({
      query: `
        ALTER TABLE ${analyticsTransactionsTable}
        DELETE WHERE block_height <= {throughBlockHeight:UInt64}
      `,
      query_params: input,
      clickhouse_settings: {
        mutations_sync: '2',
        max_execution_time: 300,
      },
    });
    await this.executeCommand({
      query: analyticsTransactionFactsBackfillSql(),
      query_params: input,
      clickhouse_settings: {
        max_execution_time: 300,
        max_bytes_before_external_group_by: '1073741824',
        max_bytes_before_external_sort: '1073741824',
      },
    });

    return { rowsInserted: null, throughBlockHeight: input.throughBlockHeight };
  }

  public async preflightAnalyticsQuery(input: {
    limits: AnalyticsQueryLimits;
    params: AnalyticsQueryParams;
    sql: string;
  }): Promise<AnalyticsQueryEstimate> {
    const rows = await this.queryAnalyticsRows<{
      bytes?: number;
      rows?: number;
    }>({
      query: `EXPLAIN ESTIMATE ${input.sql}`,
      query_params: analyticsQueryParamsRecord(input.params),
      format: 'JSONEachRow',
      clickhouse_settings: analyticsClickHouseSettings(input.limits),
    });

    return {
      estimatedRows: sumNullableNumbers(rows.map((row) => row.rows)),
      estimatedBytes: sumNullableNumbers(rows.map((row) => row.bytes)),
    };
  }

  public async executeAnalyticsQuery(input: {
    limits: AnalyticsQueryLimits;
    params: AnalyticsQueryParams;
    sql: string;
  }): Promise<AnalyticsQueryExecutionResult> {
    try {
      const result = await this.requireAnalyticsClient().query({
        query: input.sql,
        query_params: analyticsQueryParamsRecord(input.params),
        format: 'JSON',
        clickhouse_settings: analyticsClickHouseSettings(input.limits),
      });
      return analyticsExecutionResult(await result.json<ClickHouseJsonResult>());
    } catch (error) {
      throw this.toInfrastructureError(error);
    }
  }

  private async queryAnalyticsRows<T>(parameters: ClickHouseJsonQueryParameters): Promise<T[]> {
    try {
      const result = await this.requireAnalyticsClient().query(parameters);
      return (await result.json<T>()) as T[];
    } catch (error) {
      throw this.toInfrastructureError(error);
    }
  }

  private requireAnalyticsClient(): ReturnType<typeof createClient> {
    if (!this.analyticsClient) {
      throw new InfrastructureError(
        'analytics querying is unavailable: configure ONLYDOGE_ANALYTICS_WAREHOUSE_USER and ONLYDOGE_ANALYTICS_WAREHOUSE_PASSWORD',
      );
    }
    return this.analyticsClient;
  }

  private async queryRows<T>(
    parameters: ClickHouseJsonQueryParameters,
    requestContext?: ClickHouseRequestContext,
  ): Promise<T[]> {
    return this.queryRowsUnchecked<T>(parameters, requestContext).catch((error) => {
      throw this.toInfrastructureError(error);
    });
  }

  private async queryRowsUnchecked<T>(
    parameters: ClickHouseJsonQueryParameters,
    requestContext?: ClickHouseRequestContext,
  ): Promise<T[]> {
    if (requestContext) {
      return this.queryRowsWithRequestContext<T>(parameters, requestContext);
    }

    const result = await this.client.query(this.explorerQueryParameters(parameters));
    return (await result.json<T>()) as T[];
  }

  private async queryRowsWithDeadline<T>(
    parameters: ClickHouseJsonQueryParameters,
    context?: ProjectionPageRequestContext,
  ): Promise<T[]> {
    const timeoutMs = queryTimeoutMs(context, this.requestTimeoutMs);
    const requestContext = createAbortableRequestContext(context?.abortSignal, timeoutMs);

    return this.queryRowsWithRequestContext<T>(parameters, requestContext)
      .catch((error) => {
        throw this.toDeadlineInfrastructureError(error, requestContext, timeoutMs);
      })
      .finally(() => requestContext.cleanup());
  }

  private async queryRowsWithRequestContext<T>(
    parameters: ClickHouseJsonQueryParameters,
    requestContext: ReturnType<typeof createAbortableRequestContext>,
  ): Promise<T[]> {
    const result = await this.runWithRequestContext(requestContext, () =>
      this.client.query({
        ...this.explorerQueryParameters(parameters),
        abort_signal: requestContext.signal,
      }),
    );
    return (await this.runWithRequestContext(requestContext, () => result.json<T>())) as T[];
  }

  private explorerQueryParameters(
    parameters: ClickHouseJsonQueryParameters,
  ): ClickHouseJsonQueryParameters {
    if (!this.explorerReadContext.getStore()) {
      return parameters;
    }

    return {
      ...parameters,
      clickhouse_settings: {
        ...parameters.clickhouse_settings,
        ...explorerClickHouseSettings,
      },
    };
  }

  private toDeadlineInfrastructureError(
    error: unknown,
    requestContext: ReturnType<typeof createAbortableRequestContext>,
    timeoutMs: number,
  ): InfrastructureError {
    if (requestContext.didTimeout()) {
      return new InfrastructureError(`warehouse request timed out after ${timeoutMs}ms`, {
        cause: error,
      });
    }

    return this.toInfrastructureError(error);
  }

  private async executeCommand(parameters: ClickHouseCommandParameters): Promise<void> {
    try {
      await this.client.command(parameters);
    } catch (error) {
      throw this.toInfrastructureError(error);
    }
  }

  private async executeInsert(
    parameters: ClickHouseInsertParameters,
    requestContext?: ClickHouseRequestContext,
  ): Promise<void> {
    return this.executeInsertUnchecked(parameters, requestContext).catch((error) => {
      throw this.toInfrastructureError(error);
    });
  }

  private async executeInsertUnchecked(
    parameters: ClickHouseInsertParameters,
    requestContext?: ClickHouseRequestContext,
  ): Promise<void> {
    if (requestContext) {
      await this.runWithRequestContext(requestContext, () =>
        this.client.insert({
          ...parameters,
          abort_signal: requestContext.signal,
        }),
      );
      return;
    }

    await this.client.insert(parameters);
  }

  private async runWithRequestContext<T>(
    requestContext: ClickHouseRequestContext,
    work: () => Promise<T>,
  ): Promise<T> {
    if (requestContext.signal.aborted) {
      throw abortReason(requestContext.signal);
    }

    let listener: (() => void) | null = null;
    return Promise.race([
      work(),
      new Promise<never>((_resolve, reject) => {
        listener = () => reject(abortReason(requestContext.signal));
        requestContext.signal.addEventListener('abort', listener, {
          once: true,
        });
      }),
    ]).finally(() => {
      removeAbortListener(requestContext.signal, listener);
    });
  }

  private toInfrastructureError(error: unknown): InfrastructureError {
    if (error instanceof InfrastructureError) {
      return error;
    }

    return new InfrastructureError(warehouseInfrastructureMessage(error), {
      cause: error,
    });
  }
}

export async function createWarehouse(
  settings: WarehouseSettings,
  schemaLock?: SchemaLockPort,
  logger: ServiceLogger = noopServiceLogger(),
): Promise<ProjectionWarehousePort & ExplorerWarehousePort & MempoolSampleWarehousePort> {
  if (settings.driver === 'clickhouse') {
    const adapter = new ClickHouseWarehouseAdapter(settings, schemaLock, logger);
    await adapter.boot();
    return adapter;
  }

  const adapter = new DuckDbWarehouseAdapter(settings.location);
  await adapter.boot();
  return adapter;
}

export async function createFactWarehouse(
  settings: WarehouseSettings,
  schemaLock?: SchemaLockPort,
  logger: ServiceLogger = noopServiceLogger(),
): Promise<
  AnalyticsWarehousePort &
    MempoolSampleWarehousePort &
    ProjectionFactWarehousePort &
    Pick<
      ProjectionStateStorePort,
      | 'getCurrentAddressSummary'
      | 'getBalanceSnapshots'
      | 'getUtxoOutputs'
      | 'hasAppliedBlock'
      | 'listAddressUtxos'
      | 'listAppliedBlockSet'
    > &
    ProjectionWarehousePort &
    ExplorerWarehousePort
> {
  return createWarehouse(settings, schemaLock, logger) as Promise<
    AnalyticsWarehousePort &
      MempoolSampleWarehousePort &
      ProjectionFactWarehousePort &
      Pick<
        ProjectionStateStorePort,
        | 'getCurrentAddressSummary'
        | 'getBalanceSnapshots'
        | 'getUtxoOutputs'
        | 'hasAppliedBlock'
        | 'listAddressUtxos'
        | 'listAppliedBlockSet'
      > &
      ProjectionWarehousePort &
      ExplorerWarehousePort
  >;
}

export class CompositeWarehouseAdapter
  implements ExplorerWarehousePort, Pick<ProjectionWarehousePort, 'getUtxoOutputs'>
{
  public constructor(
    private readonly stateStore: Pick<
      ProjectionStateStorePort,
      'getCurrentAddressSummary' | 'getUtxoOutputs' | 'listAddressUtxos'
    >,
    private readonly historyWarehouse: ExplorerWarehousePort,
  ) {}

  public getUtxoOutputs(outputKeys: string[]) {
    return runExplorerRead(this.stateStore, () => this.stateStore.getUtxoOutputs(outputKeys));
  }

  public getCreatedUtxoOutputs(outputKeys: string[]) {
    return runExplorerRead(this.historyWarehouse, () =>
      this.historyWarehouse.getCreatedUtxoOutputs(outputKeys),
    );
  }

  public async getAddressSummary(address: string) {
    const [current, historical] = await Promise.all([
      runExplorerRead(this.stateStore, () => this.stateStore.getCurrentAddressSummary(address)),
      runExplorerRead(this.historyWarehouse, () =>
        this.historyWarehouse.getAddressSummary(address),
      ),
    ]);
    return combineAddressSummary(current, historical);
  }

  public getAppliedBlockByHash(blockHash: string) {
    return runExplorerRead(this.historyWarehouse, () =>
      this.historyWarehouse.getAppliedBlockByHash(blockHash),
    );
  }

  public getTransactionRef(txid: string) {
    return runExplorerRead(this.historyWarehouse, () =>
      this.historyWarehouse.getTransactionRef(txid),
    );
  }

  public listAddressTransactions(address: string, offset?: number, limit?: number) {
    return runExplorerRead(this.historyWarehouse, () =>
      this.historyWarehouse.listAddressTransactions(address, offset, limit),
    );
  }

  public listAddressUtxos(address: string, offset?: number, limit?: number) {
    return runExplorerRead(this.stateStore, () =>
      this.stateStore.listAddressUtxos(address, offset, limit),
    );
  }

  public listAppliedBlocks(offset?: number, limit?: number) {
    return runExplorerRead(this.historyWarehouse, () =>
      this.historyWarehouse.listAppliedBlocks(offset, limit),
    );
  }
}

function runExplorerRead<T>(warehouse: object, work: () => Promise<T>): Promise<T> {
  return warehouse instanceof ClickHouseWarehouseAdapter ? warehouse.runExplorerRead(work) : work();
}

function combineAddressSummary(
  current: {
    balance: string;
    utxoCount: number;
  } | null,
  historical: {
    balance: string;
    receivedBase: string;
    sentBase: string;
    txCount: number;
    utxoCount: number;
  } | null,
) {
  if (!current) {
    return historical;
  }

  return combineCurrentAddressSummary(current, historical);
}

function combineCurrentAddressSummary(
  current: {
    balance: string;
    utxoCount: number;
  },
  historical: {
    balance: string;
    receivedBase: string;
    sentBase: string;
    txCount: number;
    utxoCount: number;
  } | null,
) {
  if (!historical) {
    return {
      balance: current.balance,
      receivedBase: '0',
      sentBase: '0',
      txCount: 0,
      utxoCount: current.utxoCount,
    };
  }

  return {
    balance: current.balance,
    receivedBase: historical.receivedBase,
    sentBase: historical.sentBase,
    txCount: historical.txCount,
    utxoCount: current.utxoCount,
  };
}

function buildClickHouseAddressSummary(
  movement: AddressMovementSummaryRow | undefined,
  balance: string,
  utxoCount: number,
) {
  if (movement) {
    return addressSummaryWithMovement(movement, balance, utxoCount);
  }

  return addressSummaryWithoutMovement(balance, utxoCount);
}

function addressSummaryWithMovement(
  movement: AddressMovementSummaryRow,
  balance: string,
  utxoCount: number,
) {
  return {
    balance,
    receivedBase: movement.receivedBase,
    sentBase: movement.sentBase,
    txCount: movement.txCount,
    utxoCount,
  };
}

function addressSummaryWithoutMovement(balance: string, utxoCount: number) {
  if (isEmptyCurrentAddressSummary(balance, utxoCount)) {
    return null;
  }

  return {
    balance,
    receivedBase: '0',
    sentBase: '0',
    txCount: 0,
    utxoCount,
  };
}

function toMempoolSampleInsertRow(row: MempoolSampleRow): Record<string, unknown> {
  return {
    sampled_at: row.sampledAt,
    txid: row.txid,
    entry_time: row.entryTime,
    height: row.height,
    size_bytes: row.sizeBytes,
    fee_base: row.feeBase,
    fee_rate_base_per_kilobyte: row.feeRateBasePerKilobyte,
    raw_json: row.rawJson,
  };
}

export class MirroredProjectionStateStore implements ProjectionStateStorePort {
  public constructor(
    private readonly primary: ProjectionStateStorePort,
    private readonly fallback?: Pick<
      ProjectionStateStorePort,
      | 'getCurrentAddressSummary'
      | 'getBalanceSnapshots'
      | 'getUtxoOutputs'
      | 'hasAppliedBlock'
      | 'listAddressUtxos'
      | 'listAppliedBlockSet'
    >,
  ) {}

  public applyProjectionWindow(batches: BlockProjectionBatch[]) {
    return this.primary.applyProjectionWindow(batches);
  }

  public clearProjectionBootstrapState() {
    return this.primary.clearProjectionBootstrapState();
  }

  public finalizeProjectionBootstrap(processTail: number) {
    return this.primary.finalizeProjectionBootstrap(processTail);
  }

  public async getCurrentAddressSummary(address: string) {
    const primary = await this.primary.getCurrentAddressSummary(address);
    if (primary) {
      return primary;
    }

    return this.fallbackCurrentAddressSummary(address);
  }

  private async fallbackCurrentAddressSummary(address: string) {
    if (!this.fallback) {
      return null;
    }

    return this.fallback.getCurrentAddressSummary(address);
  }

  public getBalanceSnapshots(
    keys: Array<{
      address: string;
      assetAddress: string;
    }>,
  ) {
    return this.withFallbackMap(
      this.primary.getBalanceSnapshots(keys),
      keys,
      (missingKeys) =>
        this.fallback ? this.fallback.getBalanceSnapshots(missingKeys) : Promise.resolve(new Map()),
      ({ address, assetAddress }) => projectionBalanceSnapshotKey(address, assetAddress),
    );
  }

  public getProjectionBootstrapTail() {
    return this.primary.getProjectionBootstrapTail();
  }

  public getUtxoOutputs(outputKeys: string[]) {
    return this.withFallbackMap(
      this.primary.getUtxoOutputs(outputKeys),
      outputKeys,
      (missingKeys) =>
        this.fallback ? this.fallback.getUtxoOutputs(missingKeys) : Promise.resolve(new Map()),
      (outputKey) => outputKey,
    );
  }

  public async hasAppliedBlock(blockHeight: number, blockHash: string) {
    if (await this.primary.hasAppliedBlock(blockHeight, blockHash)) {
      return true;
    }

    return this.fallbackHasAppliedBlock(blockHeight, blockHash);
  }

  private async fallbackHasAppliedBlock(blockHeight: number, blockHash: string): Promise<boolean> {
    if (!this.fallback) {
      return false;
    }

    return this.fallback.hasAppliedBlock(blockHeight, blockHash);
  }

  public async listAppliedBlockSet(
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>> {
    const primaryRows = await this.primary.listAppliedBlockSet(blocks);
    const fallbackRows = this.fallback
      ? await this.fallback.listAppliedBlockSet(blocks)
      : new Set<string>();

    return new Set([...fallbackRows, ...primaryRows]);
  }

  public hasProjectionState() {
    return this.primary.hasProjectionState();
  }

  public importProjectionStateSnapshot(
    snapshot: ProjectionStateBootstrapSnapshot,
    processTail: number,
  ) {
    return this.primary.importProjectionStateSnapshot(snapshot, processTail);
  }

  public upsertProjectionBootstrapBalances(rows: ProjectionBalanceSnapshot[]) {
    return this.primary.upsertProjectionBootstrapBalances(rows);
  }

  public upsertProjectionBootstrapUtxoOutputs(rows: ProjectionUtxoOutput[]) {
    return this.primary.upsertProjectionBootstrapUtxoOutputs(rows);
  }

  public async listAddressUtxos(address: string, offset?: number, limit?: number) {
    const primaryRows = await this.primary.listAddressUtxos(address, offset, limit);
    const fallback = this.fallback;
    if (shouldUsePrimaryAddressUtxos(primaryRows, fallback)) {
      return primaryRows;
    }

    return fallbackAddressUtxos(fallback, address, offset, limit);
  }

  private async withFallbackMap<TKey, TValue>(
    primaryPromise: Promise<Map<string, TValue>>,
    keys: TKey[],
    fallbackLoader: (missingKeys: TKey[]) => Promise<Map<string, TValue>>,
    toKey: (key: TKey) => string,
  ): Promise<Map<string, TValue>> {
    const primaryRows = await primaryPromise;
    if (shouldSkipFallbackMap(this.fallback, keys)) {
      return primaryRows;
    }

    return this.withFallbackMissingMap(primaryRows, keys, fallbackLoader, toKey);
  }

  private async withFallbackMissingMap<TKey, TValue>(
    primaryRows: Map<string, TValue>,
    keys: TKey[],
    fallbackLoader: (missingKeys: TKey[]) => Promise<Map<string, TValue>>,
    toKey: (key: TKey) => string,
  ): Promise<Map<string, TValue>> {
    const missingKeys = keys.filter((key) => !primaryRows.has(toKey(key)));
    if (missingKeys.length === 0) {
      return primaryRows;
    }

    const fallbackRows = await fallbackLoader(missingKeys);
    return new Map([...fallbackRows, ...primaryRows]);
  }
}

function balanceKey(address: string, assetAddress: string): string {
  return `${address}:${assetAddress}`;
}

function coreApplyContextOption(context: CoreDogecoinApplyContext | undefined): {
  context?: CoreDogecoinApplyContext;
} {
  if (!context) {
    return {};
  }

  return { context };
}

function shouldValidateCorePrevouts(context: CoreDogecoinApplyContext | undefined): boolean {
  if (!context) {
    return true;
  }

  return context.validatePrevouts !== false;
}

async function runCoreWindowInsertStageHook(
  context: CoreDogecoinApplyContext | undefined,
  stage: CoreWindowInsertStage,
): Promise<void> {
  await context?.testHooks?.afterStage?.(stage);
}

function shouldUpdateCoreCurrentState(context: CoreDogecoinApplyContext | undefined): boolean {
  if (!context) {
    return false;
  }

  return context.updateCurrentState === true;
}

function shouldRecoverMissingCoreCurrentPrevout(
  error: unknown,
  context: CoreDogecoinApplyContext | undefined,
): error is MissingCoreCurrentPrevoutError {
  return shouldUpdateCoreCurrentState(context) && error instanceof MissingCoreCurrentPrevoutError;
}

function coreBenchmarkRows(input: CoreDogecoinBlockApplication[]): {
  createRows: ProjectionUtxoOutput[];
  spendRows: CoreDogecoinBlockApplication['utxoSpends'];
} {
  return {
    createRows: input.flatMap((application) => application.utxoCreates),
    spendRows: input.flatMap((application) => application.utxoSpends),
  };
}

function coreBenchmarkRowsInserted(
  rows: {
    createRows: ProjectionUtxoOutput[];
    spendRows: CoreDogecoinBlockApplication['utxoSpends'];
  },
  input: CoreDogecoinBlockApplication[],
): number {
  return rows.createRows.length + rows.spendRows.length + input.length;
}

function missingUtxoOutputKeys(
  outputKeys: string[],
  currentOutputs: Map<string, ProjectionUtxoOutput>,
): string[] {
  return outputKeys.filter((outputKey) => !currentOutputs.has(outputKey));
}

function assertCoreCurrentPrevoutRepairs(
  missingOutputKeys: string[],
  repairedRows: ProjectionUtxoOutput[],
): void {
  const repairedByKey = new Map(repairedRows.map((row) => [row.outputKey, row]));
  const missing = missingOutputKeys.find((outputKey) => !repairedByKey.has(outputKey));
  if (missing) {
    throw new MissingCoreCurrentPrevoutError(missing);
  }

  const spent = repairedRows.find((row) => row.spentInBlock !== null);
  if (spent) {
    throw new Error(
      `core dogecoin prevout already spent before current repair: ${spent.outputKey}`,
    );
  }
}

function unspentCoreCurrentRepair(row: ProjectionUtxoOutput): ProjectionUtxoOutput {
  return {
    ...row,
    spentByTxid: null,
    spentInBlock: null,
    spentInputIndex: null,
  };
}

function coreAsOfBlockHeightClause(column: string, asOfBlockHeight: number | undefined): string {
  if (asOfBlockHeight === undefined) {
    return '';
  }

  return `AND ${column} <= {asOfBlockHeight:UInt64}`;
}

function coreHistoricalUtxoOutputParams(
  outputKeys: string[],
  asOfBlockHeight: number | undefined,
): Record<string, number | string[]> {
  if (asOfBlockHeight === undefined) {
    return { outputKeys };
  }

  return { outputKeys, asOfBlockHeight };
}

function fallbackUtxoVersion(row: ProjectionUtxoOutput): number {
  if (row.spentInBlock === null) {
    return row.blockHeight;
  }

  return row.spentInBlock;
}

function appliedBlockTail(rows: Array<{ blockHeight: number | null }>): number | null {
  const [row] = rows;
  if (!row) {
    return null;
  }

  return blockHeightOrNull(row.blockHeight);
}

function blockHeightOrNull(blockHeight: number | null): number | null {
  if (blockHeight === null) {
    return null;
  }

  return Number(blockHeight);
}

function pendingProjectionWindowEnd(pendingBatches: BlockProjectionBatch[]): number {
  const last = pendingBatches.at(-1);
  if (!last) {
    return 0;
  }

  return last.blockHeight;
}

function shouldUsePrimaryAddressUtxos(
  primaryRows: ProjectionUtxoOutput[],
  fallback: Pick<ProjectionStateStorePort, 'listAddressUtxos'> | undefined,
): boolean {
  return [primaryRows.length > 0, fallback === undefined].includes(true);
}

async function fallbackAddressUtxos(
  fallback: Pick<ProjectionStateStorePort, 'listAddressUtxos'> | undefined,
  address: string,
  offset?: number,
  limit?: number,
): Promise<ProjectionUtxoOutput[]> {
  if (!fallback) {
    return [];
  }

  return fallback.listAddressUtxos(address, offset, limit);
}

function shouldSkipFallbackMap<TKey>(fallback: unknown, keys: TKey[]): boolean {
  return [fallback === undefined, keys.length === 0].includes(true);
}

function removeAbortListener(signal: AbortSignal, listener: (() => void) | null): void {
  if (!listener) {
    return;
  }

  signal.removeEventListener('abort', listener);
}

function clickHouseBool(value: boolean): number {
  if (value) {
    return 1;
  }

  return 0;
}

function abortReason(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) {
    return signal.reason;
  }

  return new Error('warehouse request aborted');
}

function toCoreUtxoCreateInsertRow(output: ProjectionUtxoOutput): Record<string, unknown> {
  return {
    block_height: output.blockHeight,
    block_hash: output.blockHash,
    block_time: output.blockTime,
    txid: output.txid,
    tx_index: output.txIndex,
    vout: output.vout,
    output_key: output.outputKey,
    address: output.address,
    script_type: output.scriptType,
    value_base: output.valueBase,
    is_coinbase: clickHouseBool(output.isCoinbase),
    is_spendable: clickHouseBool(output.isSpendable),
    version: output.blockHeight,
  };
}

function toCoreUtxoSpendInsertRow(
  spend: CoreDogecoinBlockApplication['utxoSpends'][number],
): Record<string, unknown> {
  return {
    spent_output_key: spend.outputKey,
    spent_by_txid: spend.spentByTxid,
    spent_in_block: spend.spentInBlock,
    spent_input_index: spend.spentInputIndex,
    version: spend.spentInBlock,
  };
}

function toCoreProcessedBlockInsertRow(
  input: CoreDogecoinBlockApplication,
): Record<string, unknown> {
  return {
    block_height: input.blockHeight,
    block_hash: input.blockHash,
    block_time: input.blockTime,
    tx_count: input.txCount,
    version: input.blockHeight,
  };
}

function toAnalyticsTransactionFactInsertRow(
  row: AnalyticsTransactionFact,
): Record<string, unknown> {
  return {
    block_height: row.blockHeight,
    block_hash: row.blockHash,
    block_time: row.blockTime,
    txid: row.txid,
    tx_index: row.txIndex,
    is_coinbase: clickHouseBool(row.isCoinbase),
    input_count: row.inputCount,
    output_count: row.outputCount,
    total_input_base: row.totalInputBase,
    gross_output_base: row.grossOutputBase,
    fee_base: row.feeBase,
    version: row.version,
  };
}

function unappliedCoreWindowResult(input: CoreDogecoinBlockApplication[]): CoreDogecoinApplyResult {
  return {
    applied: false,
    processTail: coreWindowEnd(input),
  };
}

function appliedCoreWindowResult(
  input: CoreDogecoinBlockApplication[],
  pending: CoreDogecoinBlockApplication[],
): CoreDogecoinApplyResult {
  return {
    applied: true,
    processTail: coreWindowResultTail(pending, input),
  };
}

function coreWindowTimeoutMs(
  context: CoreDogecoinApplyContext | undefined,
  defaultTimeoutMs: number,
): number {
  if (!context) {
    return defaultTimeoutMs;
  }

  return contextStatementTimeoutMs(context, defaultTimeoutMs);
}

function contextStatementTimeoutMs(
  context: CoreDogecoinApplyContext,
  defaultTimeoutMs: number,
): number {
  return context.statementTimeoutMs ?? defaultTimeoutMs;
}

function coreWindowResultTail(
  pending: CoreDogecoinBlockApplication[],
  input: CoreDogecoinBlockApplication[],
): number {
  const pendingTail = coreWindowEnd(pending);
  return pendingTail >= 0 ? pendingTail : coreWindowEnd(input);
}

function isPendingCoreApplication(
  application: CoreDogecoinBlockApplication,
  existing: CoreProcessedBlockRow | undefined,
): boolean {
  if (!existing) {
    return true;
  }

  assertPendingCoreApplicationHash(application, existing);
  return false;
}

function firstCoreReorgHeight(
  applications: CoreDogecoinBlockApplication[],
  processedBlocks: Map<number, CoreProcessedBlockRow>,
): number | null {
  for (const application of applications) {
    const existing = processedBlocks.get(application.blockHeight);
    if (existing && existing.blockHash !== application.blockHash) {
      return application.blockHeight;
    }
  }

  return null;
}

function assertPendingCoreApplicationHash(
  application: CoreDogecoinBlockApplication,
  existing: CoreProcessedBlockRow,
): void {
  if (existing.blockHash === application.blockHash) {
    return;
  }

  throw new Error(
    `core block hash mismatch height=${application.blockHeight} existing=${existing.blockHash} next=${application.blockHash}`,
  );
}

function coreWindowEnd(pending: CoreDogecoinBlockApplication[]): number {
  const last = pending.at(-1);
  if (!last) {
    return -1;
  }

  return last.blockHeight;
}

function coreWindowStart(pending: CoreDogecoinBlockApplication[]): number {
  const first = pending.at(0);
  if (!first) {
    return -1;
  }

  return first.blockHeight;
}

function coreWindowSpendKeys(pending: CoreDogecoinBlockApplication[]): string[] {
  return [
    ...new Set(pending.flatMap((application) => application.utxoSpends.map(coreSpendOutputKey))),
  ];
}

function externalCoreSpendKeys(applications: CoreDogecoinBlockApplication[]): string[] {
  const createdInWindow = new Set(
    applications.flatMap((application) =>
      application.utxoCreates.map((output) => output.outputKey),
    ),
  );
  return [
    ...new Set(
      applications
        .flatMap((application) => application.utxoSpends.map((spend) => spend.outputKey))
        .filter((outputKey) => !createdInWindow.has(outputKey)),
    ),
  ];
}

function coreCreatedOutputsByKey(
  applications: CoreDogecoinBlockApplication[],
): Map<string, ProjectionUtxoOutput> {
  return new Map(
    applications
      .flatMap((application) => application.utxoCreates)
      .map((output) => [output.outputKey, output]),
  );
}

function coreApplicationAddressMovements(
  application: CoreDogecoinBlockApplication,
  createdOutputs: Map<string, ProjectionUtxoOutput>,
  currentOutputs: Map<string, ProjectionUtxoOutput>,
): AddressMovement[] {
  return [
    ...application.utxoCreates.flatMap(coreCreateAddressMovement),
    ...application.utxoSpends.flatMap((spend) =>
      coreSpendAddressMovement(application, spend, createdOutputs, currentOutputs),
    ),
  ];
}

function coreCreateAddressMovement(output: ProjectionUtxoOutput): AddressMovement[] {
  if (!isCoreAddressMovementOutput(output)) {
    return [];
  }

  return [
    {
      movementId: `core-credit:${output.outputKey}`,
      blockHeight: output.blockHeight,
      blockHash: output.blockHash,
      blockTime: output.blockTime,
      txid: output.txid,
      txIndex: output.txIndex,
      entryIndex: output.vout,
      address: output.address,
      assetAddress: '',
      direction: 'credit',
      amountBase: output.valueBase,
      outputKey: output.outputKey,
      derivationMethod: 'core_utxo',
    },
  ];
}

function coreSpendAddressMovement(
  application: CoreDogecoinBlockApplication,
  spend: CoreDogecoinSpend,
  createdOutputs: Map<string, ProjectionUtxoOutput>,
  currentOutputs: Map<string, ProjectionUtxoOutput>,
): AddressMovement[] {
  const output = createdOutputs.get(spend.outputKey) ?? currentOutputs.get(spend.outputKey);
  if (!output || !isCoreAddressMovementOutput(output)) {
    return [];
  }

  return [
    {
      movementId: `core-debit:${spend.outputKey}:${spend.spentByTxid}:${spend.spentInputIndex}`,
      blockHeight: application.blockHeight,
      blockHash: application.blockHash,
      blockTime: application.blockTime,
      txid: spend.spentByTxid,
      txIndex: 0,
      entryIndex: spend.spentInputIndex,
      address: output.address,
      assetAddress: '',
      direction: 'debit',
      amountBase: output.valueBase,
      outputKey: spend.outputKey,
      derivationMethod: 'core_utxo',
    },
  ];
}

function isCoreAddressMovementOutput(output: ProjectionUtxoOutput): boolean {
  return output.isSpendable && output.address.length > 0;
}

function coreApplicationTransactionFacts(
  application: CoreDogecoinBlockApplication,
  createdOutputs: Map<string, ProjectionUtxoOutput>,
  currentOutputs: Map<string, ProjectionUtxoOutput>,
): AnalyticsTransactionFact[] {
  const outputsByTxid = groupCoreOutputsByTxid(application.utxoCreates);
  const spendsByTxid = groupCoreSpendsByTxid(application.utxoSpends);

  return [...outputsByTxid.entries()].map(([txid, outputs]) =>
    coreTransactionFact(application, txid, outputs, spendsByTxid.get(txid) ?? [], {
      createdOutputs,
      currentOutputs,
    }),
  );
}

function groupCoreOutputsByTxid(
  outputs: ProjectionUtxoOutput[],
): Map<string, ProjectionUtxoOutput[]> {
  const grouped = new Map<string, ProjectionUtxoOutput[]>();
  for (const output of outputs) {
    grouped.set(output.txid, [...(grouped.get(output.txid) ?? []), output]);
  }

  return grouped;
}

function groupCoreSpendsByTxid(spends: CoreDogecoinSpend[]): Map<string, CoreDogecoinSpend[]> {
  const grouped = new Map<string, CoreDogecoinSpend[]>();
  for (const spend of spends) {
    grouped.set(spend.spentByTxid, [...(grouped.get(spend.spentByTxid) ?? []), spend]);
  }

  return grouped;
}

function coreTransactionFact(
  application: CoreDogecoinBlockApplication,
  txid: string,
  outputs: ProjectionUtxoOutput[],
  spends: CoreDogecoinSpend[],
  outputMaps: {
    createdOutputs: Map<string, ProjectionUtxoOutput>;
    currentOutputs: Map<string, ProjectionUtxoOutput>;
  },
): AnalyticsTransactionFact {
  const sortedOutputs = [...outputs].sort((left, right) => left.vout - right.vout);
  const firstOutput = requireAnalyticsOutput(sortedOutputs);
  const totalInput = spends.reduce((sum, spend) => sum + coreSpendValue(spend, outputMaps), 0n);
  const grossOutput = sortedOutputs.reduce((sum, output) => sum + BigInt(output.valueBase), 0n);
  const isCoinbase = sortedOutputs.some((output) => output.isCoinbase);

  return {
    blockHeight: application.blockHeight,
    blockHash: application.blockHash,
    blockTime: application.blockTime,
    txid,
    txIndex: firstOutput.txIndex,
    isCoinbase,
    inputCount: spends.length,
    outputCount: sortedOutputs.length,
    totalInputBase: totalInput.toString(),
    grossOutputBase: grossOutput.toString(),
    feeBase: analyticsFeeBase(isCoinbase, totalInput, grossOutput),
    version: application.blockHeight,
  };
}

function coreSpendValue(
  spend: CoreDogecoinSpend,
  outputMaps: {
    createdOutputs: Map<string, ProjectionUtxoOutput>;
    currentOutputs: Map<string, ProjectionUtxoOutput>;
  },
): bigint {
  const output =
    outputMaps.createdOutputs.get(spend.outputKey) ??
    outputMaps.currentOutputs.get(spend.outputKey);
  return output ? BigInt(output.valueBase) : 0n;
}

function assertCorePrevoutsExist(
  externalSpendKeys: string[],
  created: Map<string, CoreUtxoCreateRow>,
): void {
  const missing = externalSpendKeys.find((outputKey) => !created.has(outputKey));
  if (missing) {
    throw new Error(`missing core dogecoin prevout: ${missing}`);
  }
}

function coreSpendsInWindow(
  applications: CoreDogecoinBlockApplication[],
): Map<string, CoreDogecoinSpend> {
  return new Map(
    applications
      .flatMap((application) => application.utxoSpends)
      .map((spend) => [spend.outputKey, spend]),
  );
}

function assertCorePrevoutsUnspent(
  externalSpendKeys: string[],
  spent: Map<string, CoreUtxoSpendRow>,
  spendsInWindow: Map<string, CoreDogecoinSpend>,
): void {
  const alreadySpent = externalSpendKeys.find((outputKey) =>
    isConflictingCoreSpend(spent.get(outputKey), spendsInWindow.get(outputKey)),
  );
  if (alreadySpent) {
    throw new Error(`core dogecoin prevout already spent: ${alreadySpent}`);
  }
}

function coreSpendOutputKey(spend: CoreDogecoinSpend): string {
  return spend.outputKey;
}

function applyCoreCurrentStateMutations(
  pending: CoreDogecoinBlockApplication[],
  currentOutputs: Map<string, ProjectionUtxoOutput>,
): CoreCurrentStateMutation {
  const mutation = createCoreCurrentStateMutation();
  for (const application of pending) {
    applyCoreCurrentStateApplication(mutation, application, currentOutputs);
  }
  return mutation;
}

function createCoreCurrentStateMutation(): CoreCurrentStateMutation {
  return {
    balanceDeltas: new Map<string, { address: string; amount: bigint; assetAddress: string }>(),
    nextOutputs: new Map<string, ProjectionUtxoOutput>(),
  };
}

function applyCoreCurrentStateApplication(
  mutation: CoreCurrentStateMutation,
  application: CoreDogecoinBlockApplication,
  currentOutputs: Map<string, ProjectionUtxoOutput>,
): void {
  applyCoreCurrentStateCreates(mutation, application);
  applyCoreCurrentStateSpends(mutation, application, currentOutputs);
}

function applyCoreCurrentStateCreates(
  mutation: CoreCurrentStateMutation,
  application: CoreDogecoinBlockApplication,
): void {
  for (const output of application.utxoCreates) {
    applyCoreCurrentStateCreate(mutation, output);
  }
}

function applyCoreCurrentStateSpends(
  mutation: CoreCurrentStateMutation,
  application: CoreDogecoinBlockApplication,
  currentOutputs: Map<string, ProjectionUtxoOutput>,
): void {
  for (const spend of application.utxoSpends) {
    applyCoreCurrentStateSpend(mutation, spend, currentOutputs);
  }
}

function applyCoreCurrentStateCreate(
  mutation: CoreCurrentStateMutation,
  output: ProjectionUtxoOutput,
): void {
  mutation.nextOutputs.set(output.outputKey, { ...output });
  addCoreBalanceDelta(mutation.balanceDeltas, output, BigInt(output.valueBase));
}

function applyCoreCurrentStateSpend(
  mutation: CoreCurrentStateMutation,
  spend: CoreDogecoinSpend,
  currentOutputs: Map<string, ProjectionUtxoOutput>,
): void {
  const current = currentCoreOutput(mutation, spend, currentOutputs);
  mutation.nextOutputs.set(spend.outputKey, spentCoreOutput(current, spend));
  addCoreBalanceDelta(mutation.balanceDeltas, current, -BigInt(current.valueBase));
}

function currentCoreOutput(
  mutation: CoreCurrentStateMutation,
  spend: CoreDogecoinSpend,
  currentOutputs: Map<string, ProjectionUtxoOutput>,
): ProjectionUtxoOutput {
  const current = currentCoreWindowOutput(mutation, spend);
  if (current) {
    return current;
  }

  return requireCurrentCoreOutput(currentOutputs, spend.outputKey);
}

function currentCoreWindowOutput(
  mutation: CoreCurrentStateMutation,
  spend: CoreDogecoinSpend,
): ProjectionUtxoOutput | undefined {
  return mutation.nextOutputs.get(spend.outputKey);
}

function requireCurrentCoreOutput(
  currentOutputs: Map<string, ProjectionUtxoOutput>,
  outputKey: string,
): ProjectionUtxoOutput {
  const current = currentOutputs.get(outputKey);
  if (!current) {
    throw new MissingCoreCurrentPrevoutError(outputKey);
  }

  return current;
}

function spentCoreOutput(
  current: ProjectionUtxoOutput,
  spend: CoreDogecoinSpend,
): ProjectionUtxoOutput {
  return {
    ...current,
    spentByTxid: spend.spentByTxid,
    spentInBlock: spend.spentInBlock,
    spentInputIndex: spend.spentInputIndex,
  };
}

function coreCurrentBalanceRows(
  windowEnd: number,
  balanceDeltas: Map<string, { address: string; amount: bigint; assetAddress: string }>,
  currentBalances: Map<string, BalanceRow>,
): VersionedBalanceRow[] {
  const nextBalances: VersionedBalanceRow[] = [];
  for (const [key, delta] of balanceDeltas) {
    nextBalances.push(coreCurrentBalanceRow(windowEnd, key, delta, currentBalances));
  }
  return nextBalances;
}

function coreCurrentBalanceRow(
  windowEnd: number,
  key: string,
  delta: { address: string; amount: bigint; assetAddress: string },
  currentBalances: Map<string, BalanceRow>,
): VersionedBalanceRow {
  const currentBalance = coreCurrentBalanceAmount(currentBalances, key);
  const nextBalance = currentBalance + delta.amount;
  if (nextBalance < 0n) {
    throw new Error(`negative balance for ${delta.address}:${delta.assetAddress}`);
  }

  return {
    address: delta.address,
    assetAddress: delta.assetAddress,
    balance: formatAmountBase(nextBalance),
    asOfBlockHeight: windowEnd,
    version: coreCurrentBalanceVersion(windowEnd),
  };
}

function coreCurrentBalanceAmount(currentBalances: Map<string, BalanceRow>, key: string): bigint {
  const current = currentBalances.get(key);
  if (!current) {
    return 0n;
  }

  return BigInt(current.balance);
}

function createCoreWindowShapeState(): CoreWindowShapeState {
  return {
    created: new Set<string>(),
    previousHash: null,
    previousHeight: null,
    spent: new Set<string>(),
  };
}

function assertCoreApplicationShape(
  application: CoreDogecoinBlockApplication,
  state: CoreWindowShapeState,
): void {
  assertCoreApplicationHeight(application, state.previousHeight);
  assertCoreApplicationPreviousHash(application, state);
  assertUniqueCoreOutputs(application, state.created);
  assertUniqueCoreSpends(application, state.spent);
}

function assertCoreApplicationHeight(
  application: CoreDogecoinBlockApplication,
  previousHeight: number | null,
): void {
  if (previousHeight === null) {
    return;
  }

  assertNextCoreApplicationHeight(application, previousHeight);
}

function assertNextCoreApplicationHeight(
  application: CoreDogecoinBlockApplication,
  previousHeight: number,
): void {
  if (application.blockHeight !== previousHeight + 1) {
    throw new Error(
      `non-contiguous core dogecoin window previous=${previousHeight} next=${application.blockHeight}`,
    );
  }
}

function assertCoreApplicationPreviousHash(
  application: CoreDogecoinBlockApplication,
  state: CoreWindowShapeState,
): void {
  if (state.previousHash === null) {
    return;
  }

  assertNextCoreApplicationPreviousHash(application, state);
}

function assertNextCoreApplicationPreviousHash(
  application: CoreDogecoinBlockApplication,
  state: CoreWindowShapeState,
): void {
  if (application.previousBlockHash !== state.previousHash) {
    throw new Error(
      `non-contiguous core dogecoin chain previous_height=${state.previousHeight} previous_hash=${state.previousHash} next_height=${application.blockHeight} next_previous=${formatNullableHash(application.previousBlockHash)}`,
    );
  }
}

function assertUniqueCoreOutputs(
  application: CoreDogecoinBlockApplication,
  created: Set<string>,
): void {
  for (const output of application.utxoCreates) {
    assertUniqueCoreKey(created, output.outputKey, 'duplicate dogecoin output in core window');
  }
}

function assertUniqueCoreSpends(
  application: CoreDogecoinBlockApplication,
  spent: Set<string>,
): void {
  for (const spend of application.utxoSpends) {
    assertUniqueCoreKey(spent, spend.outputKey, 'duplicate dogecoin spend in core window');
  }
}

function assertUniqueCoreKey(seen: Set<string>, key: string, message: string): void {
  if (seen.has(key)) {
    throw new Error(`${message}: ${key}`);
  }
  seen.add(key);
}

function rememberCoreApplicationShape(
  application: CoreDogecoinBlockApplication,
  state: CoreWindowShapeState,
): void {
  state.previousHeight = application.blockHeight;
  state.previousHash = application.blockHash;
}

function previousCoreBlockHeight(
  firstApplication: CoreDogecoinBlockApplication | undefined,
): number | null {
  if (!firstApplication) {
    return null;
  }

  return previousCoreApplicationHeight(firstApplication);
}

function previousCoreApplicationHeight(
  firstApplication: CoreDogecoinBlockApplication,
): number | null {
  if (firstApplication.blockHeight === 0) {
    return null;
  }

  return firstApplication.blockHeight - 1;
}

function assertPreviousCoreBlockMatches(
  previous: CoreProcessedBlockRow | undefined,
  firstApplication: CoreDogecoinBlockApplication,
): void {
  if (!previous) {
    return;
  }

  assertPreviousCoreBlockHash(previous, firstApplication);
}

function assertPreviousCoreBlockHash(
  previous: CoreProcessedBlockRow,
  firstApplication: CoreDogecoinBlockApplication,
): void {
  if (previous.blockHash === firstApplication.previousBlockHash) {
    return;
  }

  throw new Error(
    `non-contiguous core dogecoin chain previous_height=${previous.blockHeight} previous_hash=${previous.blockHash} next_height=${firstApplication.blockHeight} next_previous=${formatNullableHash(firstApplication.previousBlockHash)}`,
  );
}

function formatNullableHash(hash: string | null): string {
  return hash ?? 'null';
}

function isConflictingCoreSpend(
  row: CoreUtxoSpendRow | undefined,
  retrySpend: CoreDogecoinSpend | undefined,
): boolean {
  if (!row) {
    return false;
  }

  return isDifferentCoreSpend(row, retrySpend);
}

function isDifferentCoreSpend(
  row: CoreUtxoSpendRow,
  retrySpend: CoreDogecoinSpend | undefined,
): boolean {
  if (!retrySpend) {
    return true;
  }

  return !isSameCoreSpend(row, retrySpend);
}

function isSameCoreSpend(row: CoreUtxoSpendRow, spend: CoreDogecoinSpend): boolean {
  return [
    row.spentByTxid === spend.spentByTxid,
    row.spentInBlock === spend.spentInBlock,
    row.spentInputIndex === spend.spentInputIndex,
  ].every(Boolean);
}

function clickHouseCoreMaterializationSettings(
  context: CoreDogecoinApplyContext | undefined,
): ClickHouseCommandSettings {
  const timeoutMs = coreWindowTimeoutMs(context, 300_000);
  return {
    max_execution_time: toClickHouseMaxExecutionTimeSeconds(timeoutMs),
    max_block_size: '65536',
    max_bytes_before_external_group_by: '1073741824',
    max_bytes_before_external_sort: '1073741824',
    max_insert_block_size: '65536',
    min_insert_block_size_bytes: '0',
    min_insert_block_size_rows: '0',
  };
}

function analyticsClickHouseSettings(limits: AnalyticsQueryLimits): ClickHouseCommandSettings {
  return {
    max_execution_time: limits.maxExecutionSeconds,
    max_rows_to_read: String(limits.maxRowsToRead),
    max_bytes_to_read: String(limits.maxBytesToRead),
    max_result_rows: String(limits.maxResultRows),
    result_overflow_mode: 'break',
    timeout_before_checking_execution_speed: 0,
  };
}

function analyticsClickHouseCredentials(
  settings: WarehouseSettings,
): { password: string; user: string } | null {
  if (!settings.analyticsUser && !settings.analyticsPassword) {
    return null;
  }
  if (!settings.analyticsUser || !settings.analyticsPassword) {
    throw new InfrastructureError(
      'analytics warehouse credentials must configure both ONLYDOGE_ANALYTICS_WAREHOUSE_USER and ONLYDOGE_ANALYTICS_WAREHOUSE_PASSWORD',
    );
  }
  return { password: settings.analyticsPassword, user: settings.analyticsUser };
}

function analyticsQueryParamsRecord(params: AnalyticsQueryParams): Record<string, unknown> {
  return { ...params };
}

function analyticsExecutionResult(payload: ClickHouseJsonResult): AnalyticsQueryExecutionResult {
  return {
    columns: payload.meta ?? [],
    rows: payload.data ?? [],
    statistics: {
      elapsed: payload.statistics?.elapsed ?? null,
      rowsRead: payload.statistics?.rows_read ?? null,
      bytesRead: payload.statistics?.bytes_read ?? null,
    },
    warnings: [],
  };
}

function sumNullableNumbers(values: Array<number | undefined>): number | null {
  const numbers = values.filter((value): value is number => typeof value === 'number');
  if (numbers.length === 0) {
    return null;
  }

  return numbers.reduce((sum, value) => sum + value, 0);
}

function analyticsTransactionFactsBackfillSql(): string {
  return `
    INSERT INTO ${analyticsTransactionsTable} (
      block_height,
      block_hash,
      block_time,
      txid,
      tx_index,
      is_coinbase,
      input_count,
      output_count,
      total_input_base,
      gross_output_base,
      fee_base,
      version
    )
    WITH
    latest_outputs AS (
      SELECT
        block_height,
        block_hash,
        block_time,
        txid,
        tx_index,
        vout,
        output_key,
        value_base,
        is_coinbase,
        version
      FROM ${coreUtxoCreatesTable}
      WHERE block_height <= {throughBlockHeight:UInt64}
      ORDER BY output_key ASC, version DESC
      LIMIT 1 BY output_key
    ),
    latest_spends AS (
      SELECT
        spent_output_key,
        spent_by_txid,
        version
      FROM ${coreUtxoSpendsTable}
      WHERE spent_in_block <= {throughBlockHeight:UInt64}
      ORDER BY spent_output_key ASC, version DESC
      LIMIT 1 BY spent_output_key
    ),
    input_totals AS (
      SELECT
        s.spent_by_txid AS txid,
        count() AS input_count,
        sum(toInt256(o.value_base)) AS total_input_base_i256
      FROM latest_spends AS s
      INNER JOIN latest_outputs AS o
      ON o.output_key = s.spent_output_key
      GROUP BY s.spent_by_txid
    )
    SELECT
      any(o.block_height) AS block_height,
      any(o.block_hash) AS block_hash,
      any(o.block_time) AS block_time,
      o.txid AS txid,
      min(o.tx_index) AS tx_index,
      max(o.is_coinbase) AS is_coinbase,
      toUInt64(coalesce(any(i.input_count), 0)) AS input_count,
      count() AS output_count,
      toString(coalesce(any(i.total_input_base_i256), 0)) AS total_input_base,
      toString(sum(toInt256(o.value_base))) AS gross_output_base,
      if(
        max(o.is_coinbase) = 1 OR coalesce(any(i.total_input_base_i256), 0) = 0,
        NULL,
        toString(coalesce(any(i.total_input_base_i256), 0) - sum(toInt256(o.value_base)))
      ) AS fee_base,
      max(o.block_height) AS version
    FROM latest_outputs AS o
    LEFT JOIN input_totals AS i
    ON i.txid = o.txid
    GROUP BY o.txid
  `;
}

function addCoreBalanceDelta(
  deltas: Map<string, { address: string; amount: bigint; assetAddress: string }>,
  output: ProjectionUtxoOutput,
  amount: bigint,
): void {
  if (!isBalanceAffectingCoreOutput(output)) {
    return;
  }

  const assetAddress = '';
  const key = balanceKey(output.address, assetAddress);
  applyCoreBalanceDelta(deltas, key, output.address, amount, assetAddress);
}

function isBalanceAffectingCoreOutput(output: ProjectionUtxoOutput): boolean {
  return [output.isSpendable, output.address !== ''].every(Boolean);
}

function applyCoreBalanceDelta(
  deltas: Map<string, { address: string; amount: bigint; assetAddress: string }>,
  key: string,
  address: string,
  amount: bigint,
  assetAddress: string,
): void {
  const current = deltas.get(key);
  if (current) {
    current.amount += amount;
    return;
  }

  deltas.set(key, { address, amount, assetAddress });
}

function coreCurrentUtxoVersion(row: ProjectionUtxoOutput): number {
  return row.spentInBlock === null
    ? coreCurrentCreateVersion(row.blockHeight)
    : coreCurrentSpendVersion(row.spentInBlock);
}

function coreCurrentCreateVersion(blockHeight: number): number {
  return blockHeight * 2;
}

function coreCurrentSpendVersion(blockHeight: number): number {
  return blockHeight * 2 + 1;
}

function coreCurrentBalanceVersion(blockHeight: number): number {
  return coreCurrentSpendVersion(blockHeight);
}

function coreBenchmarkTableNames(prefix: string): CoreBenchmarkTables {
  const safePrefix = assertClickHouseIdentifier(prefix);
  return {
    creates: `${safePrefix}_utxo_creates_v1`,
    spends: `${safePrefix}_utxo_spends_v1`,
    processedBlocks: `${safePrefix}_processed_blocks_v1`,
    currentUtxos: `${safePrefix}_utxo_current_v1`,
    balances: `${safePrefix}_balances_v1`,
    appliedBlocks: `${safePrefix}_applied_blocks_v1`,
  };
}

function assertClickHouseIdentifier(value: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/u.test(value)) {
    throw new Error(`invalid ClickHouse identifier: ${value}`);
  }
  return value;
}

function coreBenchmarkBootstrapStatements(tables: CoreBenchmarkTables): string[] {
  return [
    `
      CREATE TABLE IF NOT EXISTS ${tables.creates}
      (
        block_height UInt64,
        block_hash String,
        block_time UInt64,
        txid String,
        tx_index UInt64,
        vout UInt64,
        output_key String,
        address String,
        script_type String,
        value_base String,
        is_coinbase UInt8,
        is_spendable UInt8,
        version UInt64
      )
      ENGINE = MergeTree
      ORDER BY (output_key)
    `,
    `
      CREATE TABLE IF NOT EXISTS ${tables.spends}
      (
        spent_output_key String,
        spent_by_txid String,
        spent_in_block UInt64,
        spent_input_index UInt64,
        version UInt64
      )
      ENGINE = MergeTree
      ORDER BY (spent_output_key)
    `,
    `
      CREATE TABLE IF NOT EXISTS ${tables.processedBlocks}
      (
        block_height UInt64,
        block_hash String,
        block_time UInt64,
        tx_count UInt64,
        version UInt64
      )
      ENGINE = MergeTree
      ORDER BY (block_height)
    `,
    `
      CREATE TABLE IF NOT EXISTS ${tables.currentUtxos}
      (
        block_height UInt64,
        block_hash String,
        block_time UInt64,
        txid String,
        tx_index UInt64,
        vout UInt64,
        output_key String,
        address String,
        script_type String,
        value_base String,
        is_coinbase UInt8,
        is_spendable UInt8,
        spent_by_txid Nullable(String),
        spent_in_block Nullable(UInt64),
        spent_input_index Nullable(UInt64),
        version UInt64
      )
      ENGINE = MergeTree
      ORDER BY (output_key)
    `,
    `
      CREATE TABLE IF NOT EXISTS ${tables.balances}
      (
        address String,
        asset_address String,
        balance String,
        as_of_block_height UInt64,
        version UInt64
      )
      ENGINE = MergeTree
      ORDER BY (address, asset_address)
    `,
    `
      CREATE TABLE IF NOT EXISTS ${tables.appliedBlocks}
      (
        block_height UInt64,
        block_hash String
      )
      ENGINE = MergeTree
      ORDER BY (block_height, block_hash)
    `,
  ];
}

const clickHouseDestructiveResetTables = [
  'onlydoge_schema_migrations',
  `${utxoCurrentStateByAddressTable}_mv`,
  `${addressMovementsByAddressTable}_mv`,
  'applied_blocks',
  'utxo_outputs',
  'address_movements',
  'balances',
  'applied_blocks_v2',
  'utxo_outputs_v2',
  'utxo_outputs_current_v2',
  'utxo_outputs_current_by_address_v2',
  'address_movements_v2',
  'address_movements_by_address_v2',
  'balances_v2',
  'transfers',
  'transfers_v2',
  'direct_links',
  'direct_links_v2',
  'source_links',
  appliedBlocksTable,
  utxoCurrentStateTable,
  utxoCurrentStateByAddressTable,
  addressMovementsTable,
  addressMovementsByAddressTable,
  balancesTable,
  coreUtxoCreatesTable,
  coreUtxoSpendsTable,
  coreProcessedBlocksTable,
  analyticsTransactionsTable,
  'analytics_balances_current_v1',
  'mempool_samples_v1',
];
