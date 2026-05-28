import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';

import { createClient } from '@clickhouse/client';
import type { ExplorerWarehousePort } from '@onlydoge/explorer-query';
import {
  type AddressMovement,
  applyDirectLinkDeltasToSnapshots,
  type BlockProjectionBatch,
  buildProjectionStateChanges,
  type CoreDogecoinApplyContext,
  type CoreDogecoinApplyResult,
  type CoreDogecoinBlockApplication,
  collectProjectionDirectLinkSnapshotKeys,
  type DirectLinkRecord,
  formatAmountBase,
  mapWithConcurrency,
  mergeDirectLinkDelta,
  type ProjectionAppliedBlock,
  type ProjectionBalanceCursor,
  type ProjectionBalanceSnapshot,
  type ProjectionCurrentBalancePage,
  type ProjectionCurrentUtxoPage,
  type ProjectionDirectLinkBatch,
  type ProjectionFactWarehousePort,
  type ProjectionFactWindow,
  type ProjectionPageRequestContext,
  type ProjectionStateBootstrapSnapshot,
  type ProjectionStateStorePort,
  type ProjectionUtxoOutput,
  type ProjectionWarehousePort,
  parseProjectionDirectLinkSnapshotKey,
  projectionBalanceSnapshotKey,
  projectionBlockIdentity,
  projectionDirectLinkSnapshotKey,
  resolvePendingProjectionWindow,
  type SourceLinkRecord,
  toProjectionAppliedBlocks,
} from '@onlydoge/indexing-pipeline';
import type { InvestigationWarehousePort } from '@onlydoge/investigation-query';
import { InfrastructureError, type PrimaryId } from '@onlydoge/shared-kernel';

import {
  buildCoreCurrentStateOutputKeyRanges,
  clickHouseCoreDogecoinTables,
  clickHouseStringRangeClause,
  clickHouseStringRangeParams,
} from './clickhouse-core-dogecoin';
import type { ClickHouseCoreDogecoinStore } from './core-dogecoin-state-store';
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
  formatDirectLinkTupleList,
  queryTimeoutMs,
  toAddressMovementInsertRow,
  toAppliedBlockInsertRow,
  toBalanceInsertRow,
  toClickHouseMaxExecutionTimeSeconds,
  toCurrentBalancePage,
  toCurrentUtxoPage,
  toDirectLinkInsertRow,
  toTransferInsertRow,
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

interface VersionedDirectLinkRow extends DirectLinkRecord {
  version: number;
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

const maxClickHouseHotOutputKeyValuesPerChunk = 128;
const maxClickHouseHotOutputKeyBytesPerChunk = 6_000;
const maxClickHouseCoreOutputKeyValuesPerChunk = 512;
const maxClickHouseCoreOutputKeyBytesPerChunk = 48_000;
const maxClickHouseCoreOutputKeyQueryConcurrency = 4;
const addressMovementsByAddressTable = 'address_movements_by_address_v2';
const appliedBlocksTable = clickHouseCoreDogecoinTables.appliedBlocks;
const balancesTable = clickHouseCoreDogecoinTables.balances;
const coreProcessedBlocksTable = clickHouseCoreDogecoinTables.coreProcessedBlocks;
const coreUtxoCreatesTable = clickHouseCoreDogecoinTables.coreUtxoCreates;
const coreUtxoSpendsTable = clickHouseCoreDogecoinTables.coreUtxoSpends;
const utxoCurrentStateTable = clickHouseCoreDogecoinTables.currentUtxos;
const utxoCurrentStateByAddressTable = clickHouseCoreDogecoinTables.currentUtxosByAddress;
const coreCurrentStateOutputKeyRanges = buildCoreCurrentStateOutputKeyRanges();
type ClickHouseClient = ReturnType<typeof createClient>;
type ClickHouseCommandParameters = Parameters<ClickHouseClient['command']>[0];
type ClickHouseCommandSettings = NonNullable<ClickHouseCommandParameters['clickhouse_settings']>;
type ClickHouseInsertParameters = Parameters<ClickHouseClient['insert']>[0];
type ClickHouseJsonQueryParameters = Parameters<ClickHouseClient['query']>[0];
type ClickHouseRequestContext = ReturnType<typeof createAbortableRequestContext>;
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
    InvestigationWarehousePort,
    ProjectionWarehousePort,
    ProjectionFactWarehousePort,
    ExplorerWarehousePort
{
  protected state: WarehouseState = emptyWarehouseState();
  private readonly bootstrapTails = new Map<PrimaryId, number>();

  public async getBalancesByAddresses(addresses: string[]) {
    return this.state.balances.filter((balance) => addresses.includes(balance.address));
  }

  public async getTokensByAddresses(addresses: string[]) {
    return this.state.tokens.filter((token) => addresses.includes(token.address));
  }

  public async getDistinctLinksByAddresses(addresses: string[]) {
    return this.state.sourceLinks
      .filter((link) => addresses.includes(link.toAddress))
      .map((link) => ({
        networkId: link.networkId,
        fromAddress: link.sourceAddress,
        toAddress: link.toAddress,
        transferCount: link.hopCount,
      }));
  }

  public async getBalanceSnapshots(
    networkId: PrimaryId,
    keys: Array<{
      address: string;
      assetAddress: string;
    }>,
  ): Promise<Map<string, ProjectionBalanceSnapshot>> {
    const keySet = new Set(
      keys.map((key) => projectionBalanceSnapshotKey(key.address, key.assetAddress)),
    );
    const rows = this.state.balances.filter(
      (balance) =>
        balance.networkId === networkId &&
        keySet.has(projectionBalanceSnapshotKey(balance.address, balance.assetAddress)),
    );

    return new Map(
      rows.map((row) => [projectionBalanceSnapshotKey(row.address, row.assetAddress), { ...row }]),
    );
  }

  public async getDirectLinkSnapshots(
    networkId: PrimaryId,
    keys: Array<{
      assetAddress: string;
      fromAddress: string;
      toAddress: string;
    }>,
  ): Promise<Map<string, DirectLinkRecord>> {
    const keySet = new Set(
      keys.map((key) =>
        projectionDirectLinkSnapshotKey(key.fromAddress, key.toAddress, key.assetAddress),
      ),
    );
    const rows = this.state.directLinks.filter(
      (link) =>
        link.networkId === networkId &&
        keySet.has(
          projectionDirectLinkSnapshotKey(link.fromAddress, link.toAddress, link.assetAddress),
        ),
    );

    return new Map(
      rows.map((row) => [
        projectionDirectLinkSnapshotKey(row.fromAddress, row.toAddress, row.assetAddress),
        { ...row },
      ]),
    );
  }

  public async clearProjectionBootstrapState(networkId: PrimaryId): Promise<void> {
    this.state.utxoOutputs = this.state.utxoOutputs.filter((row) => row.networkId !== networkId);
    this.state.balances = this.state.balances.filter((row) => row.networkId !== networkId);
    this.state.appliedBlocks = this.state.appliedBlocks.filter(
      (row) => row.networkId !== networkId,
    );
    this.bootstrapTails.delete(networkId);
  }

  public async finalizeProjectionBootstrap(
    networkId: PrimaryId,
    processTail: number,
  ): Promise<void> {
    this.bootstrapTails.set(networkId, processTail);
  }

  public async getProjectionBootstrapTail(networkId: PrimaryId): Promise<number | null> {
    return this.bootstrapTails.get(networkId) ?? null;
  }

  public async listCurrentUtxoOutputsPage(
    networkId: PrimaryId,
    cursorOutputKey: string | null,
    limit: number,
    _context?: ProjectionPageRequestContext,
  ): Promise<ProjectionCurrentUtxoPage> {
    const rows = this.state.utxoOutputs
      .filter((row) => isCurrentUtxoOutputPageRow(row, networkId, cursorOutputKey))
      .sort((left, right) => left.outputKey.localeCompare(right.outputKey))
      .slice(0, limit)
      .map((row) => ({ ...row }));

    return {
      rows,
      nextCursor: currentUtxoOutputNextCursor(rows, limit),
    };
  }

  public async listCurrentBalancesPage(
    networkId: PrimaryId,
    cursor: ProjectionBalanceCursor | null,
    limit: number,
    _context?: ProjectionPageRequestContext,
  ): Promise<ProjectionCurrentBalancePage> {
    const rows = currentBalancePageRows(this.state.balances, networkId, cursor, limit);

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

  public async getCurrentAddressSummary(networkId: PrimaryId, address: string) {
    const balance = this.getNativeBalance(networkId, address);
    const utxoCount = this.countSpendableUtxos(networkId, address);

    return currentAddressSummaryOrNull(balance, utxoCount);
  }

  public async listAppliedBlocks(networkId: PrimaryId, offset = 0, limit?: number) {
    const rows = this.state.appliedBlocks
      .filter((block) => block.networkId === networkId)
      .sort((left, right) => right.blockHeight - left.blockHeight);

    return rows.slice(offset, limit === undefined ? undefined : offset + limit);
  }

  public async getAppliedBlockByHash(networkId: PrimaryId, blockHash: string) {
    return (
      this.state.appliedBlocks.find(
        (block) => block.networkId === networkId && block.blockHash === blockHash,
      ) ?? null
    );
  }

  public async getTransactionRef(networkId: PrimaryId, txid: string) {
    const output = this.state.utxoOutputs.find(
      (candidate) => candidate.networkId === networkId && candidate.txid === txid,
    );
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

  public async getAddressSummary(networkId: PrimaryId, address: string) {
    const balance = this.getNativeBalance(networkId, address);
    const movements = this.getNativeMovements(networkId, address);
    const utxoCount = this.countSpendableUtxos(networkId, address);
    const totals = summarizeNativeMovements(movements);

    return inMemoryAddressSummary(balance, totals, utxoCount);
  }

  private getNativeBalance(networkId: PrimaryId, address: string): string {
    return balanceOrZero(
      this.state.balances.find((candidate) => isNativeBalance(candidate, networkId, address)),
    );
  }

  private getNativeMovements(networkId: PrimaryId, address: string): AddressMovement[] {
    return this.state.addressMovements.filter((candidate) =>
      isNativeMovement(candidate, networkId, address),
    );
  }

  private countSpendableUtxos(networkId: PrimaryId, address: string): number {
    return this.state.utxoOutputs.filter((candidate) =>
      isSpendableAddressUtxo(candidate, networkId, address),
    ).length;
  }

  public async listAddressTransactions(
    networkId: PrimaryId,
    address: string,
    offset = 0,
    limit?: number,
  ) {
    const aggregates = aggregateAddressTransactions(this.getNativeMovements(networkId, address));
    return paginateAddressTransactions(aggregates, offset, limit);
  }

  public async listAddressUtxos(networkId: PrimaryId, address: string, offset = 0, limit?: number) {
    return this.state.utxoOutputs
      .filter((candidate) => isSpendableAddressUtxo(candidate, networkId, address))
      .sort(compareAddressUtxos)
      .slice(offset, limit === undefined ? undefined : offset + limit);
  }

  public async getUtxoOutput(
    networkId: PrimaryId,
    outputKey: string,
  ): Promise<ProjectionUtxoOutput | null> {
    return (
      this.state.utxoOutputs.find(
        (output) => output.networkId === networkId && output.outputKey === outputKey,
      ) ?? null
    );
  }

  public async getUtxoOutputs(
    networkId: PrimaryId,
    outputKeys: string[],
  ): Promise<Map<string, ProjectionUtxoOutput>> {
    const outputs = this.state.utxoOutputs.filter((output) =>
      isRequestedUtxoOutput(output, networkId, outputKeys),
    );

    return new Map(outputs.map((output) => [output.outputKey, output]));
  }

  public async hasAppliedBlock(
    networkId: PrimaryId,
    blockHeight: number,
    blockHash: string,
  ): Promise<boolean> {
    return this.state.appliedBlocks.some((candidate) =>
      isAppliedBlockRecord(candidate, networkId, blockHeight, blockHash),
    );
  }

  public async listAppliedBlockSet(
    networkId: PrimaryId,
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>> {
    return new Set(
      blocks
        .filter((block) =>
          this.state.appliedBlocks.some((candidate) =>
            isAppliedBlockRecord(candidate, networkId, block.blockHeight, block.blockHash),
          ),
        )
        .map((block) => projectionBlockIdentity(networkId, block.blockHeight, block.blockHash)),
    );
  }

  public async hasProjectionState(networkId: PrimaryId): Promise<boolean> {
    return this.state.appliedBlocks.some((candidate) => candidate.networkId === networkId);
  }

  public async getAppliedBlockTail(networkId: PrimaryId): Promise<number | null> {
    const tail = this.state.appliedBlocks
      .filter((candidate) => candidate.networkId === networkId)
      .reduce<number | null>(
        (current, candidate) =>
          current === null ? candidate.blockHeight : Math.max(current, candidate.blockHeight),
        null,
      );
    return tail;
  }

  public async importProjectionStateSnapshot(
    networkId: PrimaryId,
    snapshot: ProjectionStateBootstrapSnapshot,
  ): Promise<void> {
    this.state.appliedBlocks = [
      ...this.state.appliedBlocks.filter((row) => row.networkId !== networkId),
      ...snapshot.appliedBlocks,
    ];
    this.state.utxoOutputs = [
      ...this.state.utxoOutputs.filter((row) => row.networkId !== networkId),
      ...snapshot.utxoOutputs,
    ];
    this.state.balances = [
      ...this.state.balances.filter((row) => row.networkId !== networkId),
      ...snapshot.balances,
    ];
    this.state.directLinks = [
      ...this.state.directLinks.filter((row) => row.networkId !== networkId),
      ...snapshot.directLinks,
    ];
    this.state.sourceLinks = [
      ...this.state.sourceLinks.filter((row) => row.networkId !== networkId),
      ...snapshot.sourceLinks,
    ];
    await this.afterMutation();
  }

  public async listDirectLinksFromAddresses(networkId: PrimaryId, fromAddresses: string[]) {
    return this.state.directLinks.filter(
      (link) => link.networkId === networkId && fromAddresses.includes(link.fromAddress),
    );
  }

  public async listSourceSeedIdsReachingAddresses(
    networkId: PrimaryId,
    addresses: string[],
  ): Promise<PrimaryId[]> {
    return [
      ...new Set(
        this.state.sourceLinks
          .filter((row) => row.networkId === networkId && addresses.includes(row.toAddress))
          .map((row) => row.sourceAddressId),
      ),
    ];
  }

  public async applyProjectionWindow(batches: BlockProjectionBatch[]): Promise<void> {
    for (const batch of batches) {
      await this.applyBlockProjection(batch);
    }
  }

  public async applyDirectLinkDeltasWindow(batches: ProjectionDirectLinkBatch[]): Promise<void> {
    for (const batch of batches) {
      this.applyDirectLinkDeltaBatchIfPending(batch);
    }

    await this.afterMutation();
  }

  private applyDirectLinkDeltaBatchIfPending(batch: ProjectionDirectLinkBatch): void {
    if (this.hasAppliedDirectLinkBlock(batch)) {
      return;
    }

    this.applyDirectLinkDeltaBatch(batch);
  }

  private applyDirectLinkDeltaBatch(batch: ProjectionDirectLinkBatch): void {
    for (const delta of batch.directLinkDeltas) {
      this.mergeDirectLinkDelta(delta);
    }

    this.state.directLinkAppliedBlocks.push({
      networkId: batch.networkId,
      blockHeight: batch.blockHeight,
      blockHash: batch.blockHash,
    });
  }

  public async applyProjectionFacts(window: ProjectionFactWindow): Promise<void> {
    this.applyProjectionFactOutputs(window.utxoOutputs);
    this.applyProjectionFactMovements(window.addressMovements);
    this.applyProjectionFactTransfers(window.transfers);
    this.applyProjectionFactBalances(window.balances);
    this.applyProjectionFactDirectLinks(window.directLinks);
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

  private applyProjectionFactTransfers(transfers: BlockProjectionBatch['transfers']): void {
    for (const transfer of transfers) {
      this.appendProjectionTransferIfUnique(transfer);
    }
  }

  private appendProjectionTransferIfUnique(
    transfer: BlockProjectionBatch['transfers'][number],
  ): void {
    if (this.appendUniqueTransfer(transfer)) {
      this.state.transfers.push(transfer);
    }
  }

  private applyProjectionFactBalances(balances: ProjectionBalanceSnapshot[]): void {
    for (const balance of balances) {
      this.upsertBalance(balance);
    }
  }

  private applyProjectionFactDirectLinks(links: DirectLinkRecord[]): void {
    for (const link of links) {
      this.upsertDirectLink(link);
    }
  }

  private applyProjectionFactBlocks(blocks: ProjectionAppliedBlock[]): void {
    for (const block of blocks) {
      this.appendAppliedBlock(block);
    }
  }

  public async exportProjectionStateSnapshot(
    networkId: PrimaryId,
  ): Promise<ProjectionStateBootstrapSnapshot> {
    return {
      appliedBlocks: this.state.appliedBlocks.filter((row) => row.networkId === networkId),
      utxoOutputs: this.state.utxoOutputs.filter((row) => row.networkId === networkId),
      balances: this.state.balances.filter((row) => row.networkId === networkId),
      directLinks: this.state.directLinks.filter((row) => row.networkId === networkId),
      sourceLinks: this.state.sourceLinks.filter((row) => row.networkId === networkId),
    };
  }

  public async applyBlockProjection(batch: BlockProjectionBatch): Promise<void> {
    const alreadyApplied = await this.hasAppliedBlock(
      batch.networkId,
      batch.blockHeight,
      batch.blockHash,
    );
    if (alreadyApplied) {
      return;
    }

    this.applyBatchUtxoCreates(batch.utxoCreates);
    this.applyBatchUtxoSpends(batch.networkId, batch.utxoSpends);
    this.applyBatchAddressMovements(batch.addressMovements, batch.blockHeight);
    this.applyBatchTransfers(batch.transfers);
    this.applyBatchDirectLinkDeltas(batch.directLinkDeltas);
    this.appendAppliedBlock({
      networkId: batch.networkId,
      blockHeight: batch.blockHeight,
      blockHash: batch.blockHash,
    });
    await this.afterMutation();
  }

  public async replaceSourceLinks(
    networkId: PrimaryId,
    sourceAddressId: PrimaryId,
    rows: SourceLinkRecord[],
  ): Promise<void> {
    this.state.sourceLinks = this.state.sourceLinks.filter(
      (row) => !(row.networkId === networkId && row.sourceAddressId === sourceAddressId),
    );
    this.state.sourceLinks.push(...rows);
    await this.afterMutation();
  }

  protected async afterMutation(): Promise<void> {}

  private applyBatchUtxoCreates(outputs: ProjectionUtxoOutput[]): void {
    for (const output of outputs) {
      this.upsertUtxoOutput(output, false);
    }
  }

  private applyBatchUtxoSpends(
    networkId: PrimaryId,
    spends: BlockProjectionBatch['utxoSpends'],
  ): void {
    for (const spend of spends) {
      const output = this.requireUtxoOutput(networkId, spend.outputKey);
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

  private applyBatchTransfers(transfers: BlockProjectionBatch['transfers']): void {
    for (const transfer of transfers) {
      this.appendProjectionTransferIfUnique(transfer);
    }
  }

  private applyBatchDirectLinkDeltas(deltas: BlockProjectionBatch['directLinkDeltas']): void {
    for (const delta of deltas) {
      this.mergeDirectLinkDelta(delta);
    }
  }

  private requireUtxoOutput(networkId: PrimaryId, outputKey: string): ProjectionUtxoOutput {
    const output = this.state.utxoOutputs.find(
      (candidate) => candidate.networkId === networkId && candidate.outputKey === outputKey,
    );
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
      (candidate) =>
        candidate.networkId === movement.networkId && candidate.movementId === movement.movementId,
    );
  }

  private appendUniqueTransfer(transfer: BlockProjectionBatch['transfers'][number]): boolean {
    return !this.state.transfers.some(
      (candidate) =>
        candidate.networkId === transfer.networkId && candidate.transferId === transfer.transferId,
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

  private upsertDirectLink(link: DirectLinkRecord): void {
    const existing = this.findDirectLink(link);
    if (existing) {
      Object.assign(existing, link);
    } else {
      this.state.directLinks.push({ ...link });
    }
  }

  private mergeDirectLinkDelta(delta: DirectLinkRecord): void {
    const current = this.findDirectLink(delta);
    if (current) {
      Object.assign(current, mergeDirectLinkDelta(current, delta));
    } else {
      this.state.directLinks.push({ ...delta });
    }
  }

  private findDirectLink(link: DirectLinkRecord): DirectLinkRecord | undefined {
    return this.state.directLinks.find((candidate) => isDirectLinkRecord(candidate, link));
  }

  private appendAppliedBlock(block: ProjectionAppliedBlock): void {
    if (!this.hasAppliedBlockRecord(block, this.state.appliedBlocks)) {
      this.state.appliedBlocks.push({ ...block });
    }
  }

  private hasAppliedDirectLinkBlock(block: ProjectionAppliedBlock): boolean {
    return this.hasAppliedBlockRecord(block, this.state.directLinkAppliedBlocks);
  }

  private hasAppliedBlockRecord(
    block: ProjectionAppliedBlock,
    blocks: ProjectionAppliedBlock[],
  ): boolean {
    return blocks.some((candidate) =>
      isAppliedBlockRecord(candidate, block.networkId, block.blockHeight, block.blockHash),
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
      networkId: movement.networkId,
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
  networkId: PrimaryId,
  cursorOutputKey: string | null,
): boolean {
  return [row.networkId === networkId, isAfterOutputCursor(row.outputKey, cursorOutputKey)].every(
    Boolean,
  );
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
    candidate.networkId === balance.networkId,
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

function isNativeBalance(
  candidate: ProjectionBalanceSnapshot,
  networkId: PrimaryId,
  address: string,
): boolean {
  return [
    candidate.networkId === networkId,
    candidate.address === address,
    candidate.assetAddress === '',
  ].every(Boolean);
}

function isNativeMovement(
  candidate: AddressMovement,
  networkId: PrimaryId,
  address: string,
): boolean {
  return [
    candidate.networkId === networkId,
    candidate.address === address,
    candidate.assetAddress === '',
  ].every(Boolean);
}

function isSpendableAddressUtxo(
  candidate: ProjectionUtxoOutput,
  networkId: PrimaryId,
  address: string,
): boolean {
  return [
    candidate.networkId === networkId,
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

function isRequestedUtxoOutput(
  output: ProjectionUtxoOutput,
  networkId: PrimaryId,
  outputKeys: string[],
): boolean {
  return [output.networkId === networkId, outputKeys.includes(output.outputKey)].every(Boolean);
}

function isAppliedBlockRecord(
  candidate: ProjectionAppliedBlock,
  networkId: PrimaryId,
  blockHeight: number,
  blockHash: string,
): boolean {
  return [
    candidate.networkId === networkId,
    candidate.blockHeight === blockHeight,
    candidate.blockHash === blockHash,
  ].every(Boolean);
}

function isSameUtxoOutput(candidate: ProjectionUtxoOutput, output: ProjectionUtxoOutput): boolean {
  return [candidate.networkId === output.networkId, candidate.outputKey === output.outputKey].every(
    Boolean,
  );
}

function isDirectLinkRecord(candidate: DirectLinkRecord, link: DirectLinkRecord): boolean {
  return [
    candidate.networkId === link.networkId,
    candidate.fromAddress === link.fromAddress,
    candidate.toAddress === link.toAddress,
    candidate.assetAddress === link.assetAddress,
  ].every(Boolean);
}

function isMovementBalanceSnapshot(candidate: BalanceRow, movement: AddressMovement): boolean {
  return [
    candidate.networkId === movement.networkId,
    candidate.address === movement.address,
    candidate.assetAddress === movement.assetAddress,
  ].every(Boolean);
}

function spendableOutputByKey(
  outputsByKey: Map<string, ProjectionUtxoOutput>,
  outputKey: string,
): ProjectionUtxoOutput[] {
  const output = outputsByKey.get(outputKey);
  if (!isUnspentSpendableOutput(output)) {
    return [];
  }

  return [output];
}

function isUnspentSpendableOutput(
  output: ProjectionUtxoOutput | undefined,
): output is ProjectionUtxoOutput {
  if (!output) {
    return false;
  }

  return [output.isSpendable, output.spentByTxid === null].every(Boolean);
}

function utxoOutputForWrite(output: ProjectionUtxoOutput, clone: boolean): ProjectionUtxoOutput {
  if (clone) {
    return { ...output };
  }

  return output;
}

export class ClickHouseWarehouseAdapter
  implements
    InvestigationWarehousePort,
    ProjectionWarehousePort,
    ProjectionFactWarehousePort,
    ExplorerWarehousePort,
    ClickHouseCoreDogecoinStore
{
  private readonly client: ReturnType<typeof createClient>;
  private readonly requestTimeoutMs: number;

  public constructor(settings: WarehouseSettings) {
    this.requestTimeoutMs = settings.requestTimeoutMs ?? 30_000;
    this.client = createClient(clickHouseClientOptions(settings, this.requestTimeoutMs));
  }

  public async boot(): Promise<void> {
    await this.migrate();
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
    const networkId = coreWindowNetworkId(input);
    const pending = await this.pendingCoreWindowApplications(
      networkId,
      input,
      context,
      requestContext,
    );

    if (pending.length === 0) {
      return unappliedCoreWindowResult(input);
    }

    await this.validatePendingCoreWindow(networkId, pending, context, requestContext);
    await this.insertPendingCoreWindow(networkId, pending, context, requestContext);
    return appliedCoreWindowResult(input, pending);
  }

  private async pendingCoreWindowApplications(
    networkId: PrimaryId,
    input: CoreDogecoinBlockApplication[],
    context: CoreDogecoinApplyContext | undefined,
    requestContext: ClickHouseRequestContext,
  ): Promise<CoreDogecoinBlockApplication[]> {
    const processedBlocks = await this.getCoreProcessedBlocks(
      networkId,
      input.map((application) => application.blockHeight),
      requestContext,
    );
    const reorgHeight = firstCoreReorgHeight(input, processedBlocks);
    if (reorgHeight !== null) {
      await this.rewindCoreDogecoinWindow(networkId, reorgHeight, context);
      return input.filter((application) => application.blockHeight >= reorgHeight);
    }

    return input.filter((application) =>
      isPendingCoreApplication(
        networkId,
        application,
        processedBlocks.get(application.blockHeight),
      ),
    );
  }

  private async rewindCoreDogecoinWindow(
    networkId: PrimaryId,
    fromBlockHeight: number,
    context: CoreDogecoinApplyContext | undefined,
  ): Promise<void> {
    await this.deleteCoreDogecoinTail(networkId, fromBlockHeight, context);
    if (!shouldUpdateCoreCurrentState(context)) {
      return;
    }

    await this.rematerializeCoreCurrentStateAt(networkId, fromBlockHeight - 1, context);
  }

  private async deleteCoreDogecoinTail(
    networkId: PrimaryId,
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
        table: 'address_movements_v2',
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
    ];

    for (const deletion of deletes) {
      await this.executeCommand({
        query: `ALTER TABLE ${deletion.table} DELETE WHERE network_id = {networkId:UInt64} AND ${deletion.heightColumn} >= {fromBlockHeight:UInt64}`,
        query_params: { networkId, fromBlockHeight },
        clickhouse_settings: settings,
      });
    }
  }

  private async rematerializeCoreCurrentStateAt(
    networkId: PrimaryId,
    asOfBlockHeight: number,
    context: CoreDogecoinApplyContext | undefined,
  ): Promise<void> {
    await this.clearCoreDogecoinCurrentStateForNetwork({
      networkId,
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
      networkId,
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
    networkId: PrimaryId,
    pending: CoreDogecoinBlockApplication[],
    context: CoreDogecoinApplyContext | undefined,
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    this.assertCoreWindowShape(networkId, pending);
    await this.assertCoreWindowPreviousBlock(networkId, pending[0], requestContext);
    await this.assertCoreWindowPrevoutsIfEnabled(networkId, pending, context, requestContext);
  }

  private async assertCoreWindowPrevoutsIfEnabled(
    networkId: PrimaryId,
    pending: CoreDogecoinBlockApplication[],
    context: CoreDogecoinApplyContext | undefined,
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    if (!shouldValidateCorePrevouts(context)) {
      return;
    }

    await this.assertCoreWindowPrevouts(networkId, pending, requestContext);
  }

  private async insertPendingCoreWindow(
    networkId: PrimaryId,
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
    await this.insertRows(
      coreUtxoSpendsTable,
      spendRows.map((spend) => toCoreUtxoSpendInsertRow(networkId, spend)),
      requestContext,
    );
    await this.insertCoreAddressMovements(networkId, pending, requestContext);
    await this.applyCoreCurrentStateWindowIfEnabled(networkId, pending, context, requestContext);
    await this.insertRows(
      coreProcessedBlocksTable,
      pending.map(toCoreProcessedBlockInsertRow),
      requestContext,
    );
  }

  private async insertCoreAddressMovements(
    networkId: PrimaryId,
    pending: CoreDogecoinBlockApplication[],
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    const movements = await this.buildCoreAddressMovements(networkId, pending, requestContext);
    await this.insertRows(
      'address_movements_v2',
      movements.map(toAddressMovementInsertRow),
      requestContext,
    );
  }

  private async buildCoreAddressMovements(
    networkId: PrimaryId,
    pending: CoreDogecoinBlockApplication[],
    requestContext: ClickHouseRequestContext,
  ): Promise<AddressMovement[]> {
    const createdOutputs = coreCreatedOutputsByKey(pending);
    const currentOutputs = await this.getCurrentUtxoOutputMap(
      networkId,
      externalCoreSpendKeys(pending),
      requestContext,
    );

    return pending.flatMap((application) =>
      coreApplicationAddressMovements(application, createdOutputs, currentOutputs),
    );
  }

  private async applyCoreCurrentStateWindowIfEnabled(
    networkId: PrimaryId,
    pending: CoreDogecoinBlockApplication[],
    context: CoreDogecoinApplyContext | undefined,
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    if (!shouldUpdateCoreCurrentState(context)) {
      return;
    }

    await this.applyCoreCurrentStateWindow(networkId, pending, requestContext);
  }

  private async applyCoreCurrentStateWindow(
    networkId: PrimaryId,
    pending: CoreDogecoinBlockApplication[],
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    const currentState = await this.buildCoreCurrentStateWindow(networkId, pending, requestContext);
    if (!currentState) {
      return;
    }

    await this.insertCoreCurrentStateWindow(currentState, pending, requestContext);
  }

  private async buildCoreCurrentStateWindow(
    networkId: PrimaryId,
    pending: CoreDogecoinBlockApplication[],
    requestContext: ClickHouseRequestContext,
  ): Promise<CoreCurrentStateWindow | null> {
    const windowEnd = coreWindowEnd(pending);
    if (windowEnd < 0) {
      return null;
    }

    const currentOutputs = await this.getCurrentUtxoOutputMap(
      networkId,
      coreWindowSpendKeys(pending),
      requestContext,
    );
    const mutation = applyCoreCurrentStateMutations(pending, currentOutputs);
    const currentBalances = await this.getBalanceRowsByKeys(
      networkId,
      [...mutation.balanceDeltas.keys()],
      requestContext,
    );

    return {
      nextBalances: coreCurrentBalanceRows(
        networkId,
        windowEnd,
        mutation.balanceDeltas,
        currentBalances,
      ),
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
      appliedBlocksTable,
      toProjectionAppliedBlocks(pending).map(toAppliedBlockInsertRow),
      requestContext,
    );
  }

  public async materializeCoreDogecoinCurrentState(
    networkId: PrimaryId,
    asOfBlockHeight: number,
    context?: CoreDogecoinApplyContext,
  ): Promise<void> {
    await this.clearCoreDogecoinCurrentStateForNetwork({
      networkId,
      currentUtxosTable: utxoCurrentStateTable,
      currentUtxosByAddressTable: utxoCurrentStateByAddressTable,
      balancesTable,
      appliedBlocksTable,
      ...coreApplyContextOption(context),
    });
    await this.insertCoreCurrentStateMaterialization({
      networkId,
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

  private async clearCoreDogecoinCurrentStateForNetwork(input: {
    appliedBlocksTable: string;
    balancesTable: string;
    context?: CoreDogecoinApplyContext;
    currentUtxosByAddressTable: string;
    currentUtxosTable: string;
    networkId: PrimaryId;
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
        query: `ALTER TABLE ${table} DELETE WHERE network_id = {networkId:UInt64}`,
        query_params: { networkId: input.networkId },
        clickhouse_settings: mutationSettings,
      });
    }
  }

  public async resetCoreDogecoinStorage(): Promise<void> {
    for (const table of clickHouseDestructiveResetTables) {
      await this.executeCommand({ query: `DROP TABLE IF EXISTS ${table} SYNC` });
    }
    await this.migrate();
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
      await this.executeCommand({ query: `DROP TABLE IF EXISTS ${table} SYNC` });
    }
  }

  public async insertCoreDogecoinBenchmarkWindow(
    input: CoreDogecoinBlockApplication[],
    prefix = 'core_backfill_benchmark',
  ): Promise<{ rowsInserted: number }> {
    if (input.length === 0) {
      return { rowsInserted: 0 };
    }

    const networkId = coreWindowNetworkId(input);
    this.assertCoreWindowShape(networkId, input);
    const tables = coreBenchmarkTableNames(prefix);
    const rows = coreBenchmarkRows(input);
    await this.insertRows(tables.creates, rows.createRows.map(toCoreUtxoCreateInsertRow));
    await this.insertRows(
      tables.spends,
      rows.spendRows.map((spend) => toCoreUtxoSpendInsertRow(networkId, spend)),
    );
    await this.insertRows(tables.processedBlocks, input.map(toCoreProcessedBlockInsertRow));

    return {
      rowsInserted: coreBenchmarkRowsInserted(rows, input),
    };
  }

  public async materializeCoreDogecoinBenchmarkCurrentState(
    prefix: string,
    networkId: PrimaryId,
    asOfBlockHeight: number,
  ): Promise<void> {
    const tables = coreBenchmarkTableNames(prefix);
    await this.executeCommand({ query: `TRUNCATE TABLE ${tables.currentUtxos}` });
    await this.executeCommand({ query: `TRUNCATE TABLE ${tables.balances}` });
    await this.executeCommand({ query: `TRUNCATE TABLE ${tables.appliedBlocks}` });
    await this.insertCoreCurrentStateMaterialization({
      networkId,
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
    networkId: PrimaryId;
    processedBlocksTable: string;
    spendsTable: string;
  }): Promise<void> {
    const materializationSettings = clickHouseCoreMaterializationSettings(input.context);
    for (const range of coreCurrentStateOutputKeyRanges) {
      await this.executeCommand({
        query: `
          INSERT INTO ${input.currentUtxosTable} (
            network_id,
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
            c.network_id,
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
              network_id = {networkId:UInt64}
              AND version <= {asOfBlockHeight:UInt64}
              ${clickHouseStringRangeClause('output_key', range)}
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          ) AS c
          LEFT ANTI JOIN (
            SELECT network_id, spent_output_key
            FROM ${input.spendsTable}
            WHERE
              network_id = {networkId:UInt64}
              AND version <= {asOfBlockHeight:UInt64}
              ${clickHouseStringRangeClause('spent_output_key', range)}
            ORDER BY spent_output_key ASC, version DESC
            LIMIT 1 BY spent_output_key
          ) AS s
          ON c.network_id = s.network_id AND c.output_key = s.spent_output_key
        `,
        query_params: {
          ...clickHouseStringRangeParams(range),
          networkId: input.networkId,
          asOfBlockHeight: input.asOfBlockHeight,
        },
        clickhouse_settings: materializationSettings,
      });
    }
    await this.executeCommand({
      query: `
        INSERT INTO ${input.balancesTable} (
          network_id,
          address,
          asset_address,
          balance,
          as_of_block_height,
          version
        )
        SELECT
          network_id,
          address,
          '',
          toString(sum(toInt256(value_base))),
          {asOfBlockHeight:UInt64},
          {asOfBlockHeight:UInt64}
        FROM ${input.currentUtxosByAddressTable}
        WHERE
          network_id = {networkId:UInt64}
          AND is_spendable = 1
          AND address != ''
          AND spent_by_txid IS NULL
        GROUP BY network_id, address
      `,
      query_params: { networkId: input.networkId, asOfBlockHeight: input.asOfBlockHeight },
      clickhouse_settings: {
        ...materializationSettings,
        optimize_aggregation_in_order: 1,
      },
    });
    await this.executeCommand({
      query: `
        INSERT INTO ${input.appliedBlocksTable} (network_id, block_height, block_hash)
        SELECT network_id, block_height, block_hash
        FROM (
          SELECT network_id, block_height, block_hash, version
          FROM ${input.processedBlocksTable}
          WHERE
            network_id = {networkId:UInt64}
            AND block_height <= {asOfBlockHeight:UInt64}
          ORDER BY block_height ASC, version DESC
          LIMIT 1 BY block_height
        )
      `,
      query_params: { networkId: input.networkId, asOfBlockHeight: input.asOfBlockHeight },
      clickhouse_settings: materializationSettings,
    });
  }

  public async getCurrentAddressSummary(networkId: PrimaryId, address: string) {
    const summary = await this.getAddressSummary(networkId, address);
    if (!summary) {
      return null;
    }

    return {
      balance: summary.balance,
      utxoCount: summary.utxoCount,
    };
  }

  public async getBalancesByAddresses(addresses: string[]) {
    return this.queryRowsByAddressChunks<BalanceRow>(
      addresses,
      `
        SELECT
          network_id AS "networkId",
          asset_address AS "assetAddress",
          address,
          balance,
          as_of_block_height AS "asOfBlockHeight"
        FROM balances_v2
        WHERE address IN ({addresses:Array(String)})
        ORDER BY network_id ASC, asset_address ASC, address ASC, version DESC
        LIMIT 1 BY network_id, asset_address, address
      `,
    );
  }

  public async getTokensByAddresses() {
    return [];
  }

  public async getDistinctLinksByAddresses(addresses: string[]) {
    return this.queryRowsByAddressChunks<{
      fromAddress: string;
      networkId: PrimaryId;
      toAddress: string;
      transferCount: number;
    }>(
      addresses,
      `
        SELECT network_id AS "networkId", source_address AS "fromAddress", to_address AS "toAddress", hop_count AS "transferCount"
        FROM source_links
        WHERE to_address IN ({addresses:Array(String)})
      `,
    );
  }

  private async queryRowsByAddressChunks<T>(addresses: string[], query: string): Promise<T[]> {
    if (addresses.length === 0) {
      return [];
    }

    const rowChunks = await Promise.all(
      chunkQueryValues(addresses).map((chunk) =>
        this.queryRows<T>({
          query,
          query_params: { addresses: chunk },
          format: 'JSONEachRow',
        }),
      ),
    );
    return rowChunks.flat();
  }

  public async listAppliedBlocks(networkId: PrimaryId, offset = 0, limit?: number) {
    const pagination = clickHousePagination(offset, limit);
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
    }>({
      query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash"
          FROM applied_blocks_v2
          WHERE network_id = {networkId:UInt64}
          ORDER BY block_height DESC
          ${pagination.limitClause}
          ${pagination.offsetClause}
        `,
      query_params: {
        networkId,
        ...pagination.queryParams,
      },
      format: 'JSONEachRow',
    });

    return rows;
  }

  public async getAppliedBlockByHash(networkId: PrimaryId, blockHash: string) {
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
    }>({
      query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash"
          FROM applied_blocks_v2
          WHERE network_id = {networkId:UInt64} AND block_hash = {blockHash:String}
          LIMIT 1
        `,
      query_params: { networkId, blockHash },
      format: 'JSONEachRow',
    });

    return rows[0] ?? null;
  }

  public async getTransactionRef(networkId: PrimaryId, txid: string) {
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
          FROM utxo_outputs_v2
          WHERE network_id = {networkId:UInt64} AND txid = {txid:String}
          ORDER BY version DESC
          LIMIT 1
        `,
      query_params: { networkId, txid },
      format: 'JSONEachRow',
    });

    return rows[0] ?? (await this.getCoreTransactionRef(networkId, txid));
  }

  private async getCoreTransactionRef(networkId: PrimaryId, txid: string) {
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
            network_id = {networkId:UInt64}
            AND output_key >= {prefix:String}
            AND output_key < {prefixEnd:String}
          ORDER BY output_key ASC, version DESC
          LIMIT 1 BY output_key
          LIMIT 1
        `,
      query_params: {
        networkId,
        prefix,
        prefixEnd: `${txid};`,
      },
      format: 'JSONEachRow',
    });

    return rows[0] ?? null;
  }

  public async getAddressSummary(networkId: PrimaryId, address: string) {
    const [movementFromTable, balance, utxoCount] = await Promise.all([
      this.queryAddressMovementSummary(networkId, address),
      this.queryNativeBalance(networkId, address),
      this.querySpendableUtxoCount(networkId, address),
    ]);
    const movement = hasAddressMovementSummary(movementFromTable)
      ? movementFromTable
      : await this.queryCoreAddressMovementSummary(networkId, address);

    return buildClickHouseAddressSummary(movement, balance, utxoCount);
  }

  private async queryAddressMovementSummary(
    networkId: PrimaryId,
    address: string,
  ): Promise<AddressMovementSummaryRow | undefined> {
    const rows = await this.queryRows<AddressMovementSummaryRow>({
      query: `
          SELECT
            CAST(sumIf(amount_base_i256, direction = 'credit') AS String) AS "receivedBase",
            CAST(sumIf(amount_base_i256, direction = 'debit') AS String) AS "sentBase",
            uniqExact(txid) AS "txCount"
          FROM ${addressMovementsByAddressTable}
          WHERE network_id = {networkId:UInt64} AND address = {address:String} AND asset_address = ''
        `,
      query_params: { networkId, address },
      format: 'JSONEachRow',
    });

    return rows[0];
  }

  private async queryCoreAddressMovementSummary(
    networkId: PrimaryId,
    address: string,
  ): Promise<AddressMovementSummaryRow | undefined> {
    const rows = await this.queryRows<AddressMovementSummaryRow>({
      query: `
          WITH address_outputs AS (
            SELECT
              output_key,
              txid,
              value_base
            FROM ${coreUtxoCreatesTable}
            WHERE
              network_id = {networkId:UInt64}
              AND address = {address:String}
              AND is_spendable = 1
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          )
          SELECT
            CAST(sumIf(amount_base_i256, direction = 'credit') AS String) AS "receivedBase",
            CAST(sumIf(amount_base_i256, direction = 'debit') AS String) AS "sentBase",
            uniqExact(txid) AS "txCount"
          FROM (
            SELECT
              txid,
              toInt256(value_base) AS amount_base_i256,
              'credit' AS direction
            FROM address_outputs
            UNION ALL
            SELECT
              s.spent_by_txid AS txid,
              toInt256(c.value_base) AS amount_base_i256,
              'debit' AS direction
            FROM address_outputs AS c
            INNER JOIN (
              SELECT
                spent_output_key,
                spent_by_txid
              FROM ${coreUtxoSpendsTable}
              WHERE
                network_id = {networkId:UInt64}
                AND spent_output_key IN (SELECT output_key FROM address_outputs)
              ORDER BY spent_output_key ASC, version DESC
              LIMIT 1 BY spent_output_key
            ) AS s
            ON c.output_key = s.spent_output_key
          )
        `,
      query_params: { networkId, address },
      format: 'JSONEachRow',
    });

    return rows[0];
  }

  private async queryNativeBalance(networkId: PrimaryId, address: string): Promise<string> {
    const rows = await this.queryRows<{ balance: string }>({
      query: `
          SELECT balance
          FROM balances_v2
          WHERE network_id = {networkId:UInt64} AND address = {address:String} AND asset_address = ''
          ORDER BY version DESC
          LIMIT 1
        `,
      query_params: { networkId, address },
      format: 'JSONEachRow',
    });

    const first = rows[0];
    return first ? first.balance : '0';
  }

  private async querySpendableUtxoCount(networkId: PrimaryId, address: string): Promise<number> {
    const rows = await this.queryRows<{ utxoCount: number }>({
      query: `
          SELECT count() AS "utxoCount"
          FROM (
            SELECT
              output_key,
              is_spendable,
              spent_by_txid
            FROM ${utxoCurrentStateByAddressTable}
            WHERE network_id = {networkId:UInt64} AND address = {address:String}
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          )
          WHERE is_spendable = 1 AND spent_by_txid IS NULL
        `,
      query_params: { networkId, address },
      format: 'JSONEachRow',
    });

    const first = rows[0];
    return first ? first.utxoCount : 0;
  }

  public async listAddressTransactions(
    networkId: PrimaryId,
    address: string,
    offset = 0,
    limit?: number,
  ) {
    const pagination = clickHousePagination(offset, limit);
    const rows = await this.queryRows<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      receivedBase: string;
      sentBase: string;
      txIndex: number;
      txid: string;
    }>({
      query: `
          SELECT
            block_height AS "blockHeight",
            block_hash AS "blockHash",
            block_time AS "blockTime",
            txid,
            tx_index AS "txIndex",
            CAST(sumIf(amount_base_i256, direction = 'credit') AS String) AS "receivedBase",
            CAST(sumIf(amount_base_i256, direction = 'debit') AS String) AS "sentBase"
          FROM ${addressMovementsByAddressTable}
          WHERE network_id = {networkId:UInt64} AND address = {address:String} AND asset_address = ''
          GROUP BY block_height, block_hash, block_time, txid, tx_index
          ORDER BY block_height DESC, tx_index DESC, txid DESC
          ${pagination.limitClause}
          ${pagination.offsetClause}
        `,
      query_params: {
        networkId,
        address,
        ...pagination.queryParams,
      },
      format: 'JSONEachRow',
    });

    return rows.length > 0
      ? rows
      : await this.listCoreAddressTransactions(networkId, address, offset, limit);
  }

  private async listCoreAddressTransactions(
    networkId: PrimaryId,
    address: string,
    offset = 0,
    limit?: number,
  ) {
    const pagination = clickHousePagination(offset, limit);
    return await this.queryRows<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      receivedBase: string;
      sentBase: string;
      txIndex: number;
      txid: string;
    }>({
      query: `
          WITH
          address_outputs AS (
            SELECT
              block_height,
              block_hash,
              block_time,
              txid,
              tx_index,
              output_key,
              value_base
            FROM ${coreUtxoCreatesTable}
            WHERE
              network_id = {networkId:UInt64}
              AND address = {address:String}
              AND is_spendable = 1
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          ),
          address_spends AS (
            SELECT
              spent_output_key,
              spent_by_txid,
              spent_in_block
            FROM ${coreUtxoSpendsTable}
            WHERE
              network_id = {networkId:UInt64}
              AND spent_output_key IN (SELECT output_key FROM address_outputs)
            ORDER BY spent_output_key ASC, version DESC
            LIMIT 1 BY spent_output_key
          ),
          spend_blocks AS (
            SELECT
              block_height,
              block_hash,
              block_time
            FROM ${coreProcessedBlocksTable}
            WHERE
              network_id = {networkId:UInt64}
              AND block_height IN (SELECT spent_in_block FROM address_spends)
            ORDER BY block_height ASC, version DESC
            LIMIT 1 BY block_height
          )
          SELECT
            block_height AS "blockHeight",
            any(block_hash) AS "blockHash",
            any(block_time) AS "blockTime",
            txid,
            max(tx_index) AS "txIndex",
            CAST(sumIf(amount_base_i256, direction = 'credit') AS String) AS "receivedBase",
            CAST(sumIf(amount_base_i256, direction = 'debit') AS String) AS "sentBase"
          FROM (
            SELECT
              block_height,
              block_hash,
              block_time,
              txid,
              tx_index,
              toInt256(value_base) AS amount_base_i256,
              'credit' AS direction
            FROM address_outputs
            UNION ALL
            SELECT
              s.spent_in_block AS block_height,
              b.block_hash AS block_hash,
              b.block_time AS block_time,
              s.spent_by_txid AS txid,
              0 AS tx_index,
              toInt256(c.value_base) AS amount_base_i256,
              'debit' AS direction
            FROM address_outputs AS c
            INNER JOIN address_spends AS s
            ON c.output_key = s.spent_output_key
            INNER JOIN spend_blocks AS b
            ON b.block_height = s.spent_in_block
          )
          GROUP BY block_height, txid
          ORDER BY block_height DESC, max(tx_index) DESC, txid DESC
          ${pagination.limitClause}
          ${pagination.offsetClause}
        `,
      query_params: {
        networkId,
        address,
        ...pagination.queryParams,
      },
      format: 'JSONEachRow',
    });
  }

  public async listAddressUtxos(networkId: PrimaryId, address: string, offset = 0, limit?: number) {
    const pagination = clickHousePagination(offset, limit);
    const pageRows = await this.queryRows<{
      blockHeight: number;
      outputKey: string;
      txIndex: number;
      vout: number;
    }>({
      query: `
          SELECT
            output_key AS "outputKey",
            block_height AS "blockHeight",
            tx_index AS "txIndex",
            vout,
            is_spendable,
            spent_by_txid
          FROM (
            SELECT
              output_key,
              block_height,
              tx_index,
              vout,
              is_spendable,
              spent_by_txid
            FROM ${utxoCurrentStateByAddressTable}
            WHERE network_id = {networkId:UInt64} AND address = {address:String}
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          )
          WHERE is_spendable = 1 AND spent_by_txid IS NULL
          ORDER BY block_height DESC, tx_index DESC, vout ASC
          ${pagination.limitClause}
          ${pagination.offsetClause}
        `,
      query_params: {
        networkId,
        address,
        ...pagination.queryParams,
      },
      format: 'JSONEachRow',
    });

    if (pageRows.length === 0) {
      return [];
    }

    const outputsByKey = await this.getUtxoOutputs(
      networkId,
      pageRows.map((row) => row.outputKey),
    );

    return pageRows.flatMap((row) => spendableOutputByKey(outputsByKey, row.outputKey));
  }

  public async getUtxoOutput(
    networkId: PrimaryId,
    outputKey: string,
  ): Promise<ProjectionUtxoOutput | null> {
    return (await this.getUtxoOutputs(networkId, [outputKey])).get(outputKey) ?? null;
  }

  public async getUtxoOutputs(
    networkId: PrimaryId,
    outputKeys: string[],
  ): Promise<Map<string, ProjectionUtxoOutput>> {
    if (outputKeys.length === 0) {
      return new Map();
    }

    const currentOutputs = await this.getCurrentUtxoOutputMap(networkId, outputKeys);
    await this.addMissingUtxoOutputs(networkId, outputKeys, currentOutputs);
    return currentOutputs;
  }

  private async addMissingUtxoOutputs(
    networkId: PrimaryId,
    outputKeys: string[],
    currentOutputs: Map<string, ProjectionUtxoOutput>,
  ): Promise<void> {
    const missingOutputKeys = missingUtxoOutputKeys(outputKeys, currentOutputs);
    if (missingOutputKeys.length === 0) {
      return;
    }

    await this.addFallbackUtxoOutputs(networkId, missingOutputKeys, currentOutputs);
    await this.addHistoricalUtxoOutputsIfMissing(networkId, outputKeys, currentOutputs);
  }

  private async addHistoricalUtxoOutputsIfMissing(
    networkId: PrimaryId,
    outputKeys: string[],
    currentOutputs: Map<string, ProjectionUtxoOutput>,
  ): Promise<void> {
    const stillMissingOutputKeys = missingUtxoOutputKeys(outputKeys, currentOutputs);
    if (stillMissingOutputKeys.length === 0) {
      return;
    }

    await this.addCoreHistoricalUtxoOutputs(networkId, stillMissingOutputKeys, currentOutputs);
  }

  private async getCurrentUtxoOutputMap(
    networkId: PrimaryId,
    outputKeys: string[],
    requestContext?: ClickHouseRequestContext,
  ): Promise<Map<string, ProjectionUtxoOutput>> {
    const currentRows = await this.queryUtxoOutputsFromTable(
      utxoCurrentStateTable,
      networkId,
      outputKeys,
      requestContext,
    );
    return new Map(currentRows.map((row) => [row.outputKey, row]));
  }

  private async addFallbackUtxoOutputs(
    networkId: PrimaryId,
    missingOutputKeys: string[],
    currentOutputs: Map<string, ProjectionUtxoOutput>,
  ): Promise<void> {
    const fallbackRows = await this.queryUtxoOutputsFromTable(
      'utxo_outputs_v2',
      networkId,
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
    networkId: PrimaryId,
    missingOutputKeys: string[],
    currentOutputs: Map<string, ProjectionUtxoOutput>,
  ): Promise<void> {
    const rowChunks = await mapWithConcurrency(
      chunkQueryValues([...new Set(missingOutputKeys)], {
        maxBytes: maxClickHouseCoreOutputKeyBytesPerChunk,
        maxValues: maxClickHouseCoreOutputKeyValuesPerChunk,
      }),
      maxClickHouseCoreOutputKeyQueryConcurrency,
      (chunk) =>
        this.queryRows<ProjectionUtxoOutput>({
          query: `
              SELECT
                c.network_id AS "networkId",
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
                  network_id,
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
                  network_id = {networkId:UInt64}
                  AND output_key IN ({outputKeys:Array(String)})
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
                  network_id = {networkId:UInt64}
                  AND spent_output_key IN ({outputKeys:Array(String)})
                ORDER BY spent_output_key ASC, version DESC
                LIMIT 1 BY spent_output_key
              ) AS s
              ON c.output_key = s.spent_output_key
            `,
          query_params: { networkId, outputKeys: chunk },
          format: 'JSONEachRow',
        }),
    );

    for (const row of rowChunks.flat()) {
      currentOutputs.set(row.outputKey, row);
    }
  }

  public async hasAppliedBlock(
    networkId: PrimaryId,
    blockHeight: number,
    blockHash: string,
  ): Promise<boolean> {
    const rows = await this.queryRows<Record<string, unknown>>({
      query: `
          SELECT 1
          FROM applied_blocks_v2
          WHERE
            network_id = {networkId:UInt64}
            AND block_height = {blockHeight:UInt64}
            AND block_hash = {blockHash:String}
          LIMIT 1
        `,
      query_params: { networkId, blockHeight, blockHash },
      format: 'JSONEachRow',
    });

    return rows.length > 0;
  }

  public async listAppliedBlockSet(
    networkId: PrimaryId,
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
      networkId: PrimaryId;
    }>({
      query: `
          SELECT
            network_id AS "networkId",
            block_height AS "blockHeight",
            block_hash AS "blockHash"
          FROM applied_blocks_v2
          WHERE network_id = {networkId:UInt64} AND block_height IN ({heights:Array(UInt64)})
        `,
      query_params: { networkId, heights },
      format: 'JSONEachRow',
    });

    const requested = new Set(
      blocks.map((block) => projectionBlockIdentity(networkId, block.blockHeight, block.blockHash)),
    );
    return new Set(
      rows
        .map((row) => projectionBlockIdentity(row.networkId, row.blockHeight, row.blockHash))
        .filter((identity) => requested.has(identity)),
    );
  }

  public async getAppliedBlockTail(networkId: PrimaryId): Promise<number | null> {
    const rows = await this.queryRows<{ blockHeight: number | null }>({
      query: `
          SELECT max(block_height) AS "blockHeight"
          FROM applied_blocks_v2
          WHERE network_id = {networkId:UInt64}
        `,
      query_params: { networkId },
      format: 'JSONEachRow',
    });

    return appliedBlockTail(rows);
  }

  public async listDirectLinksFromAddresses(networkId: PrimaryId, fromAddresses: string[]) {
    if (fromAddresses.length === 0) {
      return [];
    }

    const rowChunks: DirectLinkRecord[][] = await Promise.all(
      chunkQueryValues(fromAddresses).map((chunk) =>
        this.queryRows<DirectLinkRecord>({
          query: `
              SELECT
                {networkId:UInt64} AS "networkId",
                from_address AS "fromAddress",
                to_address AS "toAddress",
                asset_address AS "assetAddress",
                transfer_count AS "transferCount",
                total_amount_base AS "totalAmountBase",
                first_seen_block_height AS "firstSeenBlockHeight",
                last_seen_block_height AS "lastSeenBlockHeight"
              FROM direct_links_v2
              WHERE network_id = {networkId:UInt64} AND from_address IN ({fromAddresses:Array(String)})
              ORDER BY from_address ASC, to_address ASC, asset_address ASC, version DESC
              LIMIT 1 BY network_id, from_address, to_address, asset_address
            `,
          query_params: { networkId, fromAddresses: chunk },
          format: 'JSONEachRow',
        }),
      ),
    );
    const rows = rowChunks.flat();

    return rows;
  }

  public async listSourceSeedIdsReachingAddresses(
    networkId: PrimaryId,
    addresses: string[],
  ): Promise<PrimaryId[]> {
    if (addresses.length === 0) {
      return [];
    }

    const rowChunks: Array<Array<{ sourceAddressId: PrimaryId }>> = await Promise.all(
      chunkQueryValues(addresses).map((chunk) =>
        this.queryRows<{ sourceAddressId: PrimaryId }>({
          query: `
              SELECT DISTINCT source_address_id AS "sourceAddressId"
              FROM source_links
              WHERE network_id = {networkId:UInt64} AND to_address IN ({addresses:Array(String)})
            `,
          query_params: { networkId, addresses: chunk },
          format: 'JSONEachRow',
        }),
      ),
    );
    const rows = rowChunks.flat();

    return [...new Set(rows.map((row) => row.sourceAddressId))];
  }

  public async applyProjectionWindow(batches: BlockProjectionBatch[]): Promise<void> {
    const window = await resolvePendingProjectionWindow(batches, (networkId, blocks) =>
      this.listAppliedBlockSet(networkId, blocks),
    );
    if (window === null) {
      return;
    }

    await this.applyPendingProjectionWindow(window);
  }

  private async applyPendingProjectionWindow(window: {
    networkId: PrimaryId;
    pendingBatches: BlockProjectionBatch[];
  }): Promise<void> {
    const windowEnd = pendingProjectionWindowEnd(window.pendingBatches);
    const { nextBalances, nextOutputs } = await buildProjectionStateChanges<
      string,
      VersionedBalanceRow
    >({
      batches: window.pendingBatches,
      keyForMovement: (movement) =>
        balanceKey(movement.networkId, movement.address, movement.assetAddress),
      loadBalances: (keys) => this.getBalanceRowsByKeys(window.networkId, keys),
      loadOutputs: (networkId, outputKeys) => this.getUtxoOutputs(networkId, outputKeys),
      networkId: window.networkId,
      toSnapshotKey: (key) => balanceKey(window.networkId, key.address, key.assetAddress),
      toStoredSnapshot: (snapshot) => ({
        ...snapshot,
        version: windowEnd,
      }),
    });

    const directLinkKeys = collectProjectionDirectLinkSnapshotKeys(window.pendingBatches)
      .map(parseProjectionDirectLinkSnapshotKey)
      .map((key) =>
        directLinkKey(window.networkId, key.fromAddress, key.toAddress, key.assetAddress),
      );
    const currentDirectLinks = await this.getDirectLinkRowsByKeys(window.networkId, directLinkKeys);
    const nextDirectLinks = new Map<string, VersionedDirectLinkRow>();
    applyDirectLinkDeltasToSnapshots({
      currentDirectLinks,
      directLinkDeltas: window.pendingBatches.flatMap((batch) => batch.directLinkDeltas),
      keyForDelta: (delta) =>
        directLinkKey(delta.networkId, delta.fromAddress, delta.toAddress, delta.assetAddress),
      nextDirectLinks,
      toStoredRecord: (record) => ({
        ...record,
        version: windowEnd,
      }),
    });

    await this.insertRows(
      'address_movements_v2',
      window.pendingBatches.flatMap((batch) =>
        batch.addressMovements.map(toAddressMovementInsertRow),
      ),
    );
    await this.insertRows(
      'transfers_v2',
      window.pendingBatches.flatMap((batch) => batch.transfers.map(toTransferInsertRow)),
    );
    await this.insertRows(
      'utxo_outputs_v2',
      [...nextOutputs.values()].map((row) => toUtxoInsertRow(row, windowEnd)),
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
      'direct_links_v2',
      [...nextDirectLinks.values()].map((row) => toDirectLinkInsertRow(row, row.version)),
    );
    await this.insertRows(
      appliedBlocksTable,
      toProjectionAppliedBlocks(window.pendingBatches).map(toAppliedBlockInsertRow),
    );
  }

  public async applyProjectionFacts(window: ProjectionFactWindow): Promise<void> {
    await this.insertRows(
      'address_movements_v2',
      window.addressMovements.map(toAddressMovementInsertRow),
    );
    await this.insertRows('transfers_v2', window.transfers.map(toTransferInsertRow));
    await this.insertRows(
      'utxo_outputs_v2',
      window.utxoOutputs.map((row) => toUtxoInsertRow(row, row.spentInBlock ?? row.blockHeight)),
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
      'direct_links_v2',
      window.directLinks.map((row) => toDirectLinkInsertRow(row, row.lastSeenBlockHeight)),
    );
    await this.insertRows(appliedBlocksTable, window.appliedBlocks.map(toAppliedBlockInsertRow));
  }

  public async applyBlockProjection(batch: BlockProjectionBatch): Promise<void> {
    await this.applyProjectionWindow([batch]);
  }

  public async replaceSourceLinks(
    networkId: PrimaryId,
    sourceAddressId: PrimaryId,
    rows: SourceLinkRecord[],
  ): Promise<void> {
    await this.executeCommand({
      query: `
        ALTER TABLE source_links
        DELETE WHERE network_id = {networkId:UInt64} AND source_address_id = {sourceAddressId:UInt64}
      `,
      query_params: { networkId, sourceAddressId },
    });
    await this.insertRows(
      'source_links',
      rows.map((row) => ({
        network_id: row.networkId,
        source_address_id: row.sourceAddressId,
        source_address: row.sourceAddress,
        to_address: row.toAddress,
        hop_count: row.hopCount,
        path_transfer_count: row.pathTransferCount,
        path_addresses: row.pathAddresses,
        first_seen_block_height: row.firstSeenBlockHeight,
        last_seen_block_height: row.lastSeenBlockHeight,
      })),
    );
  }

  public async exportProjectionStateSnapshot(
    networkId: PrimaryId,
  ): Promise<ProjectionStateBootstrapSnapshot> {
    const [utxoOutputs, balances, directLinks, sourceLinks] = await Promise.all([
      this.queryRows<ProjectionUtxoOutput>({
        query: `
            SELECT
              network_id AS "networkId",
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
            WHERE network_id = {networkId:UInt64}
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          `,
        query_params: { networkId },
        format: 'JSONEachRow',
      }),
      this.queryRows<ProjectionBalanceSnapshot>({
        query: `
            SELECT
              network_id AS "networkId",
              address,
              asset_address AS "assetAddress",
              balance,
              as_of_block_height AS "asOfBlockHeight"
            FROM balances_v2
            WHERE network_id = {networkId:UInt64}
            ORDER BY address ASC, asset_address ASC, version DESC
            LIMIT 1 BY network_id, address, asset_address
          `,
        query_params: { networkId },
        format: 'JSONEachRow',
      }),
      this.queryRows<DirectLinkRecord>({
        query: `
            SELECT
              network_id AS "networkId",
              from_address AS "fromAddress",
              to_address AS "toAddress",
              asset_address AS "assetAddress",
              transfer_count AS "transferCount",
              total_amount_base AS "totalAmountBase",
              first_seen_block_height AS "firstSeenBlockHeight",
              last_seen_block_height AS "lastSeenBlockHeight"
            FROM direct_links_v2
            WHERE network_id = {networkId:UInt64}
            ORDER BY from_address ASC, to_address ASC, asset_address ASC, version DESC
            LIMIT 1 BY network_id, from_address, to_address, asset_address
          `,
        query_params: { networkId },
        format: 'JSONEachRow',
      }),
      this.queryRows<SourceLinkRecord>({
        query: `
            SELECT
              network_id AS "networkId",
              source_address_id AS "sourceAddressId",
              source_address AS "sourceAddress",
              to_address AS "toAddress",
              hop_count AS "hopCount",
              path_transfer_count AS "pathTransferCount",
              path_addresses AS "pathAddresses",
              first_seen_block_height AS "firstSeenBlockHeight",
              last_seen_block_height AS "lastSeenBlockHeight"
            FROM source_links
            WHERE network_id = {networkId:UInt64}
          `,
        query_params: { networkId },
        format: 'JSONEachRow',
      }),
    ]);

    return {
      appliedBlocks: [],
      utxoOutputs,
      balances,
      directLinks,
      sourceLinks: sourceLinks.map((row) => ({
        ...row,
        pathAddresses: Array.isArray(row.pathAddresses) ? row.pathAddresses : [],
      })),
    };
  }

  public async listCurrentUtxoOutputsPage(
    networkId: PrimaryId,
    cursorOutputKey: string | null,
    limit: number,
    context?: ProjectionPageRequestContext,
  ): Promise<ProjectionCurrentUtxoPage> {
    const rows = await this.queryCurrentUtxoOutputPageRows(
      networkId,
      cursorOutputKey,
      limit,
      context,
    );
    return toCurrentUtxoPage(rows, limit);
  }

  private async queryCurrentUtxoOutputPageRows(
    networkId: PrimaryId,
    cursorOutputKey: string | null,
    limit: number,
    context?: ProjectionPageRequestContext,
  ): Promise<ProjectionUtxoOutput[]> {
    const timeoutMs = queryTimeoutMs(context, this.requestTimeoutMs);
    return this.queryRowsWithDeadline<ProjectionUtxoOutput>(
      {
        query: `
          SELECT
            {networkId:UInt64} AS "networkId",
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
              network_id = {networkId:UInt64}
              ${clickHouseOutputKeyCursorClause(cursorOutputKey)}
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          )
          ORDER BY output_key ASC
          LIMIT {limit:UInt64}
        `,
        query_params: clickHouseOutputPageParams(networkId, cursorOutputKey, limit),
        format: 'JSONEachRow',
        clickhouse_settings: {
          max_execution_time: toClickHouseMaxExecutionTimeSeconds(timeoutMs),
        },
      },
      context,
    );
  }

  public async listCurrentBalancesPage(
    networkId: PrimaryId,
    cursor: ProjectionBalanceCursor | null,
    limit: number,
    context?: ProjectionPageRequestContext,
  ): Promise<ProjectionCurrentBalancePage> {
    const timeoutMs = queryTimeoutMs(context, this.requestTimeoutMs);
    const rows = await this.queryRowsWithDeadline<ProjectionBalanceSnapshot>(
      {
        query: `
          SELECT
            network_id AS "networkId",
            address,
            asset_address AS "assetAddress",
            balance,
            as_of_block_height AS "asOfBlockHeight"
          FROM (
            SELECT
              network_id,
              address,
              asset_address,
              balance,
              as_of_block_height,
              version
            FROM balances_v2
            WHERE
              network_id = {networkId:UInt64}
              ${clickHouseBalanceCursorClause(cursor)}
            ORDER BY address ASC, asset_address ASC, version DESC
            LIMIT 1 BY network_id, address, asset_address
          )
          ORDER BY address ASC, asset_address ASC
          LIMIT {limit:UInt64}
        `,
        query_params: clickHouseBalancePageParams(networkId, cursor, limit),
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
    networkId: PrimaryId,
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
                network_id AS "networkId",
                address,
                asset_address AS "assetAddress",
                balance,
                as_of_block_height AS "asOfBlockHeight",
                version
              FROM balances_v2
              WHERE network_id = {networkId:UInt64}
                AND (address, asset_address) IN ${formatBalanceTupleList(chunk)}
              ORDER BY address ASC, asset_address ASC, version DESC
              LIMIT 1 BY network_id, address, asset_address
          `,
            query_params: { networkId },
            format: 'JSONEachRow',
          },
          requestContext,
        ),
      ),
    );
    const rows = rowChunks.flat();

    return new Map(
      rows.map((row) => [
        balanceKey(row.networkId, row.address, row.assetAddress),
        {
          networkId: row.networkId,
          address: row.address,
          assetAddress: row.assetAddress,
          balance: row.balance,
          asOfBlockHeight: row.asOfBlockHeight,
          version: row.version,
        },
      ]),
    );
  }

  private async getDirectLinkRowsByKeys(
    networkId: PrimaryId,
    keys: string[],
  ): Promise<Map<string, VersionedDirectLinkRow>> {
    if (keys.length === 0) {
      return new Map();
    }

    const rowChunks: Array<
      Array<
        DirectLinkRecord & {
          version: number;
        }
      >
    > = await Promise.all(
      chunkQueryValues(keys).map((chunk) =>
        this.queryRows<
          DirectLinkRecord & {
            version: number;
          }
        >({
          query: `
              SELECT
                network_id AS "networkId",
                from_address AS "fromAddress",
                to_address AS "toAddress",
                asset_address AS "assetAddress",
                transfer_count AS "transferCount",
                total_amount_base AS "totalAmountBase",
                first_seen_block_height AS "firstSeenBlockHeight",
                last_seen_block_height AS "lastSeenBlockHeight",
                version
              FROM direct_links_v2
              WHERE network_id = {networkId:UInt64}
                AND (from_address, to_address, asset_address) IN ${formatDirectLinkTupleList(chunk)}
              ORDER BY from_address ASC, to_address ASC, asset_address ASC, version DESC
              LIMIT 1 BY network_id, from_address, to_address, asset_address
            `,
          query_params: { networkId },
          format: 'JSONEachRow',
        }),
      ),
    );
    const rows = rowChunks.flat();

    return new Map(
      rows.map((row) => [
        directLinkKey(row.networkId, row.fromAddress, row.toAddress, row.assetAddress),
        {
          networkId: row.networkId,
          fromAddress: row.fromAddress,
          toAddress: row.toAddress,
          assetAddress: row.assetAddress,
          transferCount: row.transferCount,
          totalAmountBase: row.totalAmountBase,
          firstSeenBlockHeight: row.firstSeenBlockHeight,
          lastSeenBlockHeight: row.lastSeenBlockHeight,
          version: row.version,
        },
      ]),
    );
  }

  private assertCoreWindowShape(
    networkId: PrimaryId,
    applications: CoreDogecoinBlockApplication[],
  ): void {
    const state = createCoreWindowShapeState();

    for (const application of applications) {
      assertCoreApplicationShape(networkId, application, state);
      rememberCoreApplicationShape(application, state);
    }
  }

  private async assertCoreWindowPreviousBlock(
    networkId: PrimaryId,
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
          WHERE network_id = {networkId:UInt64} AND block_height = {blockHeight:UInt64}
          ORDER BY version DESC
          LIMIT 1
        `,
        query_params: {
          networkId,
          blockHeight: previousHeight,
        },
        format: 'JSONEachRow',
      },
      requestContext,
    );

    assertPreviousCoreBlockMatches(previous, firstApplication as CoreDogecoinBlockApplication);
  }

  private async assertCoreWindowPrevouts(
    networkId: PrimaryId,
    applications: CoreDogecoinBlockApplication[],
    requestContext?: ClickHouseRequestContext,
  ): Promise<void> {
    const externalSpendKeys = externalCoreSpendKeys(applications);
    if (externalSpendKeys.length === 0) {
      return;
    }

    const [created, spent] = await Promise.all([
      this.getCoreUtxoCreateRows(networkId, externalSpendKeys, requestContext),
      this.getCoreUtxoSpendRows(networkId, externalSpendKeys, requestContext),
    ]);
    assertCorePrevoutsExist(externalSpendKeys, created);
    assertCorePrevoutsUnspent(externalSpendKeys, spent, coreSpendsInWindow(applications));
  }

  private async getCoreProcessedBlocks(
    networkId: PrimaryId,
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
            WHERE network_id = {networkId:UInt64} AND block_height IN ({blockHeights:Array(UInt64)})
            ORDER BY block_height ASC, version DESC
            LIMIT 1 BY block_height
          `,
            query_params: { networkId, blockHeights: chunk },
            format: 'JSONEachRow',
          },
          requestContext,
        ),
      ),
    );

    return new Map(rowChunks.flat().map((row) => [row.blockHeight, row]));
  }

  private async getCoreUtxoCreateRows(
    networkId: PrimaryId,
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
            WHERE network_id = {networkId:UInt64} AND output_key IN ({outputKeys:Array(String)})
            ORDER BY output_key ASC, version DESC
            LIMIT 1 BY output_key
          `,
        query_params: { networkId, outputKeys: chunk },
        format: 'JSONEachRow',
      }),
      requestContext,
    );
  }

  private async getCoreUtxoSpendRows(
    networkId: PrimaryId,
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
            WHERE network_id = {networkId:UInt64} AND spent_output_key IN ({outputKeys:Array(String)})
            ORDER BY spent_output_key ASC, version DESC
            LIMIT 1 BY spent_output_key
          `,
        query_params: { networkId, outputKeys: chunk },
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

  private async queryUtxoOutputsFromTable(
    table: string,
    networkId: PrimaryId,
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
                {networkId:UInt64} AS "networkId",
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
              WHERE network_id = {networkId:UInt64} AND output_key IN ({outputKeys:Array(String)})
              ORDER BY output_key ASC, version DESC
              LIMIT 1 BY output_key
          `,
            query_params: { networkId, outputKeys: chunk },
            format: 'JSONEachRow',
          },
          requestContext,
        ),
      ),
    );

    return rowChunks.flat();
  }

  private async migrate(): Promise<void> {
    for (const statement of clickHouseWarehouseBootstrapStatements) {
      await this.executeCommand({ query: statement });
    }

    await this.backfillTableIfEmpty(
      utxoCurrentStateByAddressTable,
      `
        INSERT INTO ${utxoCurrentStateByAddressTable} (
          network_id,
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
          network_id,
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
      `,
    );
    await this.backfillTableIfEmpty(
      addressMovementsByAddressTable,
      `
        INSERT INTO ${addressMovementsByAddressTable} (
          movement_id,
          network_id,
          block_height,
          block_hash,
          block_time,
          txid,
          tx_index,
          entry_index,
          address,
          asset_address,
          direction,
          amount_base,
          output_key,
          derivation_method
        )
        SELECT
          movement_id,
          network_id,
          block_height,
          block_hash,
          block_time,
          txid,
          tx_index,
          entry_index,
          address,
          asset_address,
          direction,
          amount_base,
          output_key,
          derivation_method
        FROM address_movements_v2
      `,
    );
  }

  private async backfillTableIfEmpty(table: string, statement: string): Promise<void> {
    if (await this.tableHasRows(table)) {
      return;
    }

    await this.executeCommand({ query: statement });
  }

  private async tableHasRows(table: string): Promise<boolean> {
    const rows = await this.queryRows<{ present: number }>({
      query: `
        SELECT 1 AS present
        FROM ${table}
        LIMIT 1
      `,
      format: 'JSONEachRow',
    });

    return rows.length > 0;
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

    const result = await this.client.query(parameters);
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
        ...parameters,
        abort_signal: requestContext.signal,
      }),
    );
    return (await this.runWithRequestContext(requestContext, () => result.json<T>())) as T[];
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
        this.client.insert({ ...parameters, abort_signal: requestContext.signal }),
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
        requestContext.signal.addEventListener('abort', listener, { once: true });
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
): Promise<InvestigationWarehousePort & ProjectionWarehousePort & ExplorerWarehousePort> {
  if (settings.driver === 'clickhouse') {
    const adapter = new ClickHouseWarehouseAdapter(settings);
    await adapter.boot();
    return adapter;
  }

  const adapter = new DuckDbWarehouseAdapter(settings.location);
  await adapter.boot();
  return adapter;
}

export async function createFactWarehouse(
  settings: WarehouseSettings,
): Promise<
  ProjectionFactWarehousePort &
    Pick<
      ProjectionStateStorePort,
      | 'getCurrentAddressSummary'
      | 'getBalanceSnapshots'
      | 'getDirectLinkSnapshots'
      | 'getDistinctLinksByAddresses'
      | 'getBalancesByAddresses'
      | 'getUtxoOutputs'
      | 'hasAppliedBlock'
      | 'listAddressUtxos'
      | 'listAppliedBlockSet'
      | 'listDirectLinksFromAddresses'
      | 'listSourceSeedIdsReachingAddresses'
    > &
    InvestigationWarehousePort &
    ProjectionWarehousePort &
    ExplorerWarehousePort
> {
  return createWarehouse(settings) as Promise<
    ProjectionFactWarehousePort &
      Pick<
        ProjectionStateStorePort,
        | 'getCurrentAddressSummary'
        | 'getBalanceSnapshots'
        | 'getDirectLinkSnapshots'
        | 'getDistinctLinksByAddresses'
        | 'getBalancesByAddresses'
        | 'getUtxoOutputs'
        | 'hasAppliedBlock'
        | 'listAddressUtxos'
        | 'listAppliedBlockSet'
        | 'listDirectLinksFromAddresses'
        | 'listSourceSeedIdsReachingAddresses'
      > &
      InvestigationWarehousePort &
      ProjectionWarehousePort &
      ExplorerWarehousePort
  >;
}

export class CompositeWarehouseAdapter
  implements
    InvestigationWarehousePort,
    ExplorerWarehousePort,
    Pick<ProjectionWarehousePort, 'getUtxoOutputs'>
{
  public constructor(
    private readonly stateStore: Pick<
      ProjectionStateStorePort,
      | 'getBalancesByAddresses'
      | 'getCurrentAddressSummary'
      | 'getDistinctLinksByAddresses'
      | 'getUtxoOutputs'
      | 'listAddressUtxos'
    >,
    private readonly historyWarehouse: InvestigationWarehousePort & ExplorerWarehousePort,
  ) {}

  public getBalancesByAddresses(addresses: string[]) {
    return this.stateStore.getBalancesByAddresses(addresses);
  }

  public getDistinctLinksByAddresses(addresses: string[]) {
    return this.stateStore.getDistinctLinksByAddresses(addresses);
  }

  public getTokensByAddresses(addresses: string[]) {
    return this.historyWarehouse.getTokensByAddresses(addresses);
  }

  public getUtxoOutputs(networkId: PrimaryId, outputKeys: string[]) {
    return this.stateStore.getUtxoOutputs(networkId, outputKeys);
  }

  public async getAddressSummary(networkId: PrimaryId, address: string) {
    const [current, historical] = await Promise.all([
      this.stateStore.getCurrentAddressSummary(networkId, address),
      this.historyWarehouse.getAddressSummary(networkId, address),
    ]);
    return combineAddressSummary(current, historical);
  }

  public getAppliedBlockByHash(networkId: PrimaryId, blockHash: string) {
    return this.historyWarehouse.getAppliedBlockByHash(networkId, blockHash);
  }

  public getTransactionRef(networkId: PrimaryId, txid: string) {
    return this.historyWarehouse.getTransactionRef(networkId, txid);
  }

  public listAddressTransactions(
    networkId: PrimaryId,
    address: string,
    offset?: number,
    limit?: number,
  ) {
    return this.historyWarehouse.listAddressTransactions(networkId, address, offset, limit);
  }

  public listAddressUtxos(networkId: PrimaryId, address: string, offset?: number, limit?: number) {
    return this.stateStore.listAddressUtxos(networkId, address, offset, limit);
  }

  public listAppliedBlocks(networkId: PrimaryId, offset?: number, limit?: number) {
    return this.historyWarehouse.listAppliedBlocks(networkId, offset, limit);
  }
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

function hasAddressMovementSummary(
  movement: AddressMovementSummaryRow | undefined,
): movement is AddressMovementSummaryRow {
  if (movement === undefined) {
    return false;
  }

  return hasAddressMovementValues(movement);
}

function hasAddressMovementValues(movement: AddressMovementSummaryRow): boolean {
  return [
    Number(movement.txCount) > 0,
    movement.receivedBase !== '0',
    movement.sentBase !== '0',
  ].includes(true);
}

export class MirroredProjectionStateStore implements ProjectionStateStorePort {
  public constructor(
    private readonly primary: ProjectionStateStorePort,
    private readonly fallback?: Pick<
      ProjectionStateStorePort,
      | 'getCurrentAddressSummary'
      | 'getBalanceSnapshots'
      | 'getBalancesByAddresses'
      | 'getDirectLinkSnapshots'
      | 'getDistinctLinksByAddresses'
      | 'getUtxoOutputs'
      | 'hasAppliedBlock'
      | 'listAddressUtxos'
      | 'listAppliedBlockSet'
      | 'listDirectLinksFromAddresses'
      | 'listSourceSeedIdsReachingAddresses'
    >,
    private readonly mirror?: Pick<ProjectionWarehousePort, 'replaceSourceLinks'>,
  ) {}

  public applyProjectionWindow(batches: BlockProjectionBatch[]) {
    return this.primary.applyProjectionWindow(batches);
  }

  public applyDirectLinkDeltasWindow(batches: ProjectionDirectLinkBatch[]) {
    return this.primary.applyDirectLinkDeltasWindow(batches);
  }

  public clearProjectionBootstrapState(networkId: PrimaryId) {
    return this.primary.clearProjectionBootstrapState(networkId);
  }

  public finalizeProjectionBootstrap(networkId: PrimaryId, processTail: number) {
    return this.primary.finalizeProjectionBootstrap(networkId, processTail);
  }

  public async getCurrentAddressSummary(networkId: PrimaryId, address: string) {
    const primary = await this.primary.getCurrentAddressSummary(networkId, address);
    if (primary) {
      return primary;
    }

    return this.fallbackCurrentAddressSummary(networkId, address);
  }

  private async fallbackCurrentAddressSummary(networkId: PrimaryId, address: string) {
    if (!this.fallback) {
      return null;
    }

    return this.fallback.getCurrentAddressSummary(networkId, address);
  }

  public getBalanceSnapshots(
    networkId: PrimaryId,
    keys: Array<{
      address: string;
      assetAddress: string;
    }>,
  ) {
    return this.withFallbackMap(
      this.primary.getBalanceSnapshots(networkId, keys),
      keys,
      (missingKeys) =>
        this.fallback
          ? this.fallback.getBalanceSnapshots(networkId, missingKeys)
          : Promise.resolve(new Map()),
      ({ address, assetAddress }) => projectionBalanceSnapshotKey(address, assetAddress),
    );
  }

  public getDirectLinkSnapshots(
    networkId: PrimaryId,
    keys: Array<{
      assetAddress: string;
      fromAddress: string;
      toAddress: string;
    }>,
  ) {
    return this.withFallbackMap(
      this.primary.getDirectLinkSnapshots(networkId, keys),
      keys,
      (missingKeys) =>
        this.fallback
          ? this.fallback.getDirectLinkSnapshots(networkId, missingKeys)
          : Promise.resolve(new Map()),
      ({ fromAddress, toAddress, assetAddress }) =>
        projectionDirectLinkSnapshotKey(fromAddress, toAddress, assetAddress),
    );
  }

  public async getDistinctLinksByAddresses(addresses: string[]) {
    const primaryRows = await this.primary.getDistinctLinksByAddresses(addresses);
    const fallbackRows = this.fallback
      ? await this.fallback.getDistinctLinksByAddresses(addresses)
      : [];

    return dedupeRecords(
      [...fallbackRows, ...primaryRows],
      (row) => `${row.networkId}:${row.fromAddress}:${row.toAddress}:${row.transferCount}`,
    );
  }

  public async getBalancesByAddresses(addresses: string[]) {
    const primaryRows = await this.primary.getBalancesByAddresses(addresses);
    const fallbackRows = this.fallback ? await this.fallback.getBalancesByAddresses(addresses) : [];

    return dedupeRecords(
      [...fallbackRows, ...primaryRows],
      (row) => `${row.networkId}:${row.assetAddress}:${row.balance}`,
    );
  }

  public getProjectionBootstrapTail(networkId: PrimaryId) {
    return this.primary.getProjectionBootstrapTail(networkId);
  }

  public getUtxoOutputs(networkId: PrimaryId, outputKeys: string[]) {
    return this.withFallbackMap(
      this.primary.getUtxoOutputs(networkId, outputKeys),
      outputKeys,
      (missingKeys) =>
        this.fallback
          ? this.fallback.getUtxoOutputs(networkId, missingKeys)
          : Promise.resolve(new Map()),
      (outputKey) => outputKey,
    );
  }

  public async hasAppliedBlock(networkId: PrimaryId, blockHeight: number, blockHash: string) {
    if (await this.primary.hasAppliedBlock(networkId, blockHeight, blockHash)) {
      return true;
    }

    return this.fallbackHasAppliedBlock(networkId, blockHeight, blockHash);
  }

  private async fallbackHasAppliedBlock(
    networkId: PrimaryId,
    blockHeight: number,
    blockHash: string,
  ): Promise<boolean> {
    if (!this.fallback) {
      return false;
    }

    return this.fallback.hasAppliedBlock(networkId, blockHeight, blockHash);
  }

  public async listAppliedBlockSet(
    networkId: PrimaryId,
    blocks: Array<{
      blockHash: string;
      blockHeight: number;
    }>,
  ): Promise<Set<string>> {
    const primaryRows = await this.primary.listAppliedBlockSet(networkId, blocks);
    const fallbackRows = this.fallback
      ? await this.fallback.listAppliedBlockSet(networkId, blocks)
      : new Set<string>();

    return new Set([...fallbackRows, ...primaryRows]);
  }

  public hasProjectionState(networkId: PrimaryId) {
    return this.primary.hasProjectionState(networkId);
  }

  public importProjectionStateSnapshot(
    networkId: PrimaryId,
    snapshot: ProjectionStateBootstrapSnapshot,
    processTail: number,
  ) {
    return this.primary.importProjectionStateSnapshot(networkId, snapshot, processTail);
  }

  public async listDirectLinksFromAddresses(networkId: PrimaryId, fromAddresses: string[]) {
    const primaryRows = await this.primary.listDirectLinksFromAddresses(networkId, fromAddresses);
    const fallbackRows = this.fallback
      ? await this.fallback.listDirectLinksFromAddresses(networkId, fromAddresses)
      : [];

    return dedupeRecords([...fallbackRows, ...primaryRows], (row) =>
      directLinkKey(row.networkId, row.fromAddress, row.toAddress, row.assetAddress),
    );
  }

  public async listSourceSeedIdsReachingAddresses(networkId: PrimaryId, addresses: string[]) {
    const primaryIds = await this.primary.listSourceSeedIdsReachingAddresses(networkId, addresses);
    const fallbackIds = this.fallback
      ? await this.fallback.listSourceSeedIdsReachingAddresses(networkId, addresses)
      : [];

    return [...new Set([...fallbackIds, ...primaryIds])];
  }

  public async replaceSourceLinks(
    networkId: PrimaryId,
    sourceAddressId: PrimaryId,
    rows: SourceLinkRecord[],
  ) {
    await this.primary.replaceSourceLinks(networkId, sourceAddressId, rows);
    const mirror = this.mirror;
    if (mirror) {
      await mirror.replaceSourceLinks(networkId, sourceAddressId, rows);
    }
  }

  public upsertProjectionBootstrapBalances(rows: ProjectionBalanceSnapshot[]) {
    return this.primary.upsertProjectionBootstrapBalances(rows);
  }

  public upsertProjectionBootstrapUtxoOutputs(rows: ProjectionUtxoOutput[]) {
    return this.primary.upsertProjectionBootstrapUtxoOutputs(rows);
  }

  public async listAddressUtxos(
    networkId: PrimaryId,
    address: string,
    offset?: number,
    limit?: number,
  ) {
    const primaryRows = await this.primary.listAddressUtxos(networkId, address, offset, limit);
    const fallback = this.fallback;
    if (shouldUsePrimaryAddressUtxos(primaryRows, fallback)) {
      return primaryRows;
    }

    return fallbackAddressUtxos(fallback, networkId, address, offset, limit);
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

function balanceKey(networkId: PrimaryId, address: string, assetAddress: string): string {
  return `${networkId}:${address}:${assetAddress}`;
}

function directLinkKey(
  networkId: PrimaryId,
  fromAddress: string,
  toAddress: string,
  assetAddress: string,
): string {
  return `${networkId}:${fromAddress}:${toAddress}:${assetAddress}`;
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

function shouldUpdateCoreCurrentState(context: CoreDogecoinApplyContext | undefined): boolean {
  if (!context) {
    return false;
  }

  return context.updateCurrentState === true;
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
  networkId: PrimaryId,
  address: string,
  offset?: number,
  limit?: number,
): Promise<ProjectionUtxoOutput[]> {
  if (!fallback) {
    return [];
  }

  return fallback.listAddressUtxos(networkId, address, offset, limit);
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

function dedupeRecords<T>(rows: T[], keyFor: (row: T) => string): T[] {
  const deduped = new Map<string, T>();
  for (const row of rows) {
    deduped.set(keyFor(row), row);
  }

  return [...deduped.values()];
}

function abortReason(signal: AbortSignal): Error {
  if (signal.reason instanceof Error) {
    return signal.reason;
  }

  return new Error('warehouse request aborted');
}

function toCoreUtxoCreateInsertRow(output: ProjectionUtxoOutput): Record<string, unknown> {
  return {
    network_id: output.networkId,
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
  networkId: PrimaryId,
  spend: CoreDogecoinBlockApplication['utxoSpends'][number],
): Record<string, unknown> {
  return {
    network_id: networkId,
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
    network_id: input.networkId,
    block_height: input.blockHeight,
    block_hash: input.blockHash,
    block_time: input.blockTime,
    tx_count: input.txCount,
    version: input.blockHeight,
  };
}

function coreWindowNetworkId(input: CoreDogecoinBlockApplication[]): PrimaryId {
  const [application] = input;
  if (!application) {
    return 0;
  }

  return application.networkId;
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
  networkId: PrimaryId,
  application: CoreDogecoinBlockApplication,
  existing: CoreProcessedBlockRow | undefined,
): boolean {
  if (!existing) {
    return true;
  }

  assertPendingCoreApplicationHash(networkId, application, existing);
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
  networkId: PrimaryId,
  application: CoreDogecoinBlockApplication,
  existing: CoreProcessedBlockRow,
): void {
  if (existing.blockHash === application.blockHash) {
    return;
  }

  throw new Error(
    `core block hash mismatch network=${networkId} height=${application.blockHeight} existing=${existing.blockHash} next=${application.blockHash}`,
  );
}

function coreWindowEnd(pending: CoreDogecoinBlockApplication[]): number {
  const last = pending.at(-1);
  if (!last) {
    return -1;
  }

  return last.blockHeight;
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
      movementId: `core-credit:${output.networkId}:${output.outputKey}`,
      networkId: output.networkId,
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
      movementId: `core-debit:${application.networkId}:${spend.outputKey}:${spend.spentByTxid}:${spend.spentInputIndex}`,
      networkId: application.networkId,
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
    throw new Error(`missing current dogecoin prevout: ${outputKey}`);
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
  networkId: PrimaryId,
  windowEnd: number,
  balanceDeltas: Map<string, { address: string; amount: bigint; assetAddress: string }>,
  currentBalances: Map<string, BalanceRow>,
): VersionedBalanceRow[] {
  const nextBalances: VersionedBalanceRow[] = [];
  for (const [key, delta] of balanceDeltas) {
    nextBalances.push(coreCurrentBalanceRow(networkId, windowEnd, key, delta, currentBalances));
  }
  return nextBalances;
}

function coreCurrentBalanceRow(
  networkId: PrimaryId,
  windowEnd: number,
  key: string,
  delta: { address: string; amount: bigint; assetAddress: string },
  currentBalances: Map<string, BalanceRow>,
): VersionedBalanceRow {
  const currentBalance = coreCurrentBalanceAmount(currentBalances, key);
  const nextBalance = currentBalance + delta.amount;
  if (nextBalance < 0n) {
    throw new Error(`negative balance for ${networkId}:${delta.address}:${delta.assetAddress}`);
  }

  return {
    networkId,
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
  networkId: PrimaryId,
  application: CoreDogecoinBlockApplication,
  state: CoreWindowShapeState,
): void {
  assertCoreApplicationNetwork(networkId, application);
  assertCoreApplicationHeight(application, state.previousHeight);
  assertCoreApplicationPreviousHash(application, state);
  assertUniqueCoreOutputs(application, state.created);
  assertUniqueCoreSpends(application, state.spent);
}

function assertCoreApplicationNetwork(
  networkId: PrimaryId,
  application: CoreDogecoinBlockApplication,
): void {
  if (application.networkId !== networkId) {
    throw new Error(
      `mixed core dogecoin networks in window expected=${networkId} actual=${application.networkId}`,
    );
  }
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

function addCoreBalanceDelta(
  deltas: Map<string, { address: string; amount: bigint; assetAddress: string }>,
  output: ProjectionUtxoOutput,
  amount: bigint,
): void {
  if (!isBalanceAffectingCoreOutput(output)) {
    return;
  }

  const assetAddress = '';
  const key = balanceKey(output.networkId, output.address, assetAddress);
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
        network_id UInt64,
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
      ORDER BY (network_id, output_key)
    `,
    `
      CREATE TABLE IF NOT EXISTS ${tables.spends}
      (
        network_id UInt64,
        spent_output_key String,
        spent_by_txid String,
        spent_in_block UInt64,
        spent_input_index UInt64,
        version UInt64
      )
      ENGINE = MergeTree
      ORDER BY (network_id, spent_output_key)
    `,
    `
      CREATE TABLE IF NOT EXISTS ${tables.processedBlocks}
      (
        network_id UInt64,
        block_height UInt64,
        block_hash String,
        block_time UInt64,
        tx_count UInt64,
        version UInt64
      )
      ENGINE = MergeTree
      ORDER BY (network_id, block_height)
    `,
    `
      CREATE TABLE IF NOT EXISTS ${tables.currentUtxos}
      (
        network_id UInt64,
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
      ORDER BY (network_id, output_key)
    `,
    `
      CREATE TABLE IF NOT EXISTS ${tables.balances}
      (
        network_id UInt64,
        address String,
        asset_address String,
        balance String,
        as_of_block_height UInt64,
        version UInt64
      )
      ENGINE = MergeTree
      ORDER BY (network_id, address, asset_address)
    `,
    `
      CREATE TABLE IF NOT EXISTS ${tables.appliedBlocks}
      (
        network_id UInt64,
        block_height UInt64,
        block_hash String
      )
      ENGINE = MergeTree
      ORDER BY (network_id, block_height, block_hash)
    `,
  ];
}

const clickHouseWarehouseBootstrapStatements = [
  `
    CREATE TABLE IF NOT EXISTS applied_blocks_v2
    (
      network_id UInt64,
      block_height UInt64,
      block_hash String
    )
    ENGINE = MergeTree
    ORDER BY (network_id, block_height, block_hash)
  `,
  `
    CREATE TABLE IF NOT EXISTS utxo_outputs_v2
    (
      network_id UInt64,
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
    ENGINE = ReplacingMergeTree(version)
    ORDER BY (network_id, output_key)
  `,
  `
    CREATE TABLE IF NOT EXISTS ${utxoCurrentStateTable}
    (
      network_id UInt64,
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
    ENGINE = ReplacingMergeTree(version)
    ORDER BY (network_id, output_key)
    SETTINGS old_parts_lifetime = 0
  `,
  `
    CREATE TABLE IF NOT EXISTS ${utxoCurrentStateByAddressTable}
    (
      network_id UInt64,
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
    ENGINE = ReplacingMergeTree(version)
    ORDER BY (network_id, address, output_key)
    SETTINGS old_parts_lifetime = 0
  `,
  `
    CREATE MATERIALIZED VIEW IF NOT EXISTS ${utxoCurrentStateByAddressTable}_mv
    TO ${utxoCurrentStateByAddressTable}
    AS
    SELECT
      network_id,
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
  `,
  `
    CREATE TABLE IF NOT EXISTS address_movements_v2
    (
      movement_id String,
      network_id UInt64,
      block_height UInt64,
      block_hash String,
      block_time UInt64,
      txid String,
      tx_index UInt64,
      entry_index UInt64,
      address String,
      asset_address String,
      direction String,
      amount_base String,
      output_key Nullable(String),
      derivation_method String
    )
    ENGINE = MergeTree
    ORDER BY (network_id, movement_id)
  `,
  `
    CREATE TABLE IF NOT EXISTS ${addressMovementsByAddressTable}
    (
      movement_id String,
      network_id UInt64,
      block_height UInt64,
      block_hash String,
      block_time UInt64,
      txid String,
      tx_index UInt64,
      entry_index UInt64,
      address String,
      asset_address String,
      direction String,
      amount_base String,
      amount_base_i256 Int256 MATERIALIZED toInt256(amount_base),
      output_key Nullable(String),
      derivation_method String
    )
    ENGINE = MergeTree
    ORDER BY (network_id, address, block_height, tx_index, entry_index, movement_id)
  `,
  `
    CREATE MATERIALIZED VIEW IF NOT EXISTS ${addressMovementsByAddressTable}_mv
    TO ${addressMovementsByAddressTable}
    AS
    SELECT
      movement_id,
      network_id,
      block_height,
      block_hash,
      block_time,
      txid,
      tx_index,
      entry_index,
      address,
      asset_address,
      direction,
      amount_base,
      output_key,
      derivation_method
    FROM address_movements_v2
  `,
  `
    CREATE TABLE IF NOT EXISTS transfers_v2
    (
      transfer_id String,
      network_id UInt64,
      block_height UInt64,
      block_hash String,
      block_time UInt64,
      txid String,
      tx_index UInt64,
      transfer_index UInt64,
      asset_address String,
      from_address String,
      to_address String,
      amount_base String,
      derivation_method String,
      confidence Float64,
      is_change UInt8,
      input_address_count UInt64,
      output_address_count UInt64
    )
    ENGINE = MergeTree
    ORDER BY (network_id, transfer_id)
  `,
  `
    CREATE TABLE IF NOT EXISTS balances_v2
    (
      network_id UInt64,
      address String,
      asset_address String,
      balance String,
      as_of_block_height UInt64,
      version UInt64
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY (network_id, address, asset_address)
  `,
  `
    CREATE TABLE IF NOT EXISTS direct_links_v2
    (
      network_id UInt64,
      from_address String,
      to_address String,
      asset_address String,
      transfer_count UInt64,
      total_amount_base String,
      first_seen_block_height UInt64,
      last_seen_block_height UInt64,
      version UInt64
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY (network_id, from_address, to_address, asset_address)
  `,
  `
    CREATE TABLE IF NOT EXISTS source_links
    (
      network_id UInt64,
      source_address_id UInt64,
      source_address String,
      to_address String,
      hop_count UInt64,
      path_transfer_count UInt64,
      path_addresses Array(String),
      first_seen_block_height UInt64,
      last_seen_block_height UInt64
    )
    ENGINE = MergeTree
    ORDER BY (network_id, source_address_id, to_address)
  `,
  `
    CREATE TABLE IF NOT EXISTS ${coreUtxoCreatesTable}
    (
      network_id UInt64,
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
    ENGINE = ReplacingMergeTree(version)
    ORDER BY (network_id, output_key)
  `,
  `
    ALTER TABLE ${coreUtxoCreatesTable}
    ADD INDEX IF NOT EXISTS core_utxo_creates_address_idx address TYPE bloom_filter(0.01) GRANULARITY 4
  `,
  `
    CREATE TABLE IF NOT EXISTS ${coreUtxoSpendsTable}
    (
      network_id UInt64,
      spent_output_key String,
      spent_by_txid String,
      spent_in_block UInt64,
      spent_input_index UInt64,
      version UInt64
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY (network_id, spent_output_key)
  `,
  `
    CREATE TABLE IF NOT EXISTS ${coreProcessedBlocksTable}
    (
      network_id UInt64,
      block_height UInt64,
      block_hash String,
      block_time UInt64,
      tx_count UInt64,
      version UInt64
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY (network_id, block_height)
  `,
];

const clickHouseDestructiveResetTables = [
  `${utxoCurrentStateByAddressTable}_mv`,
  `${addressMovementsByAddressTable}_mv`,
  'applied_blocks',
  'utxo_outputs',
  'address_movements',
  'transfers',
  'balances',
  'direct_links',
  appliedBlocksTable,
  'utxo_outputs_v2',
  utxoCurrentStateTable,
  utxoCurrentStateByAddressTable,
  'address_movements_v2',
  addressMovementsByAddressTable,
  'transfers_v2',
  balancesTable,
  'direct_links_v2',
  'source_links',
  coreUtxoCreatesTable,
  coreUtxoSpendsTable,
  coreProcessedBlocksTable,
];
