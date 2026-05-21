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

const maxClickHouseHotOutputKeyValuesPerChunk = 128;
const maxClickHouseHotOutputKeyBytesPerChunk = 6_000;
const maxClickHouseHotOutputKeyQueryConcurrency = 4;
const maxClickHouseBalanceKeyQueryConcurrency = 4;
const maxClickHouseCoreOutputKeyValuesPerChunk = 512;
const maxClickHouseCoreOutputKeyBytesPerChunk = 48_000;
const maxClickHouseCoreOutputKeyQueryConcurrency = 4;
const maxCoreCurrentStateApplyBlocksPerChunk = 20;
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
      .filter(
        (row) =>
          row.networkId === networkId &&
          (cursorOutputKey === null || row.outputKey > cursorOutputKey),
      )
      .sort((left, right) => left.outputKey.localeCompare(right.outputKey))
      .slice(0, limit)
      .map((row) => ({ ...row }));

    return {
      rows,
      nextCursor: rows.length === limit ? (rows.at(-1)?.outputKey ?? null) : null,
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
      const index = this.state.balances.findIndex(
        (candidate) =>
          candidate.networkId === row.networkId &&
          candidate.address === row.address &&
          candidate.assetAddress === row.assetAddress,
      );
      if (index >= 0) {
        this.state.balances[index] = { ...row };
      } else {
        this.state.balances.push({ ...row });
      }
    }
  }

  public async upsertProjectionBootstrapUtxoOutputs(rows: ProjectionUtxoOutput[]): Promise<void> {
    for (const row of rows) {
      this.upsertUtxoOutput(row);
    }
  }

  public async getCurrentAddressSummary(networkId: PrimaryId, address: string) {
    const balance = this.getNativeBalance(networkId, address);
    const utxoCount = this.countSpendableUtxos(networkId, address);

    if (balance === '0' && utxoCount === 0) {
      return null;
    }

    return {
      balance,
      utxoCount,
    };
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
    return (
      this.state.balances.find(
        (candidate) =>
          candidate.networkId === networkId &&
          candidate.address === address &&
          candidate.assetAddress === '',
      )?.balance ?? '0'
    );
  }

  private getNativeMovements(networkId: PrimaryId, address: string): AddressMovement[] {
    return this.state.addressMovements.filter(
      (candidate) =>
        candidate.networkId === networkId &&
        candidate.address === address &&
        candidate.assetAddress === '',
    );
  }

  private countSpendableUtxos(networkId: PrimaryId, address: string): number {
    return this.state.utxoOutputs.filter(
      (candidate) =>
        candidate.networkId === networkId &&
        candidate.address === address &&
        candidate.isSpendable &&
        candidate.spentByTxid === null,
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
      .filter(
        (candidate) =>
          candidate.networkId === networkId &&
          candidate.address === address &&
          candidate.isSpendable &&
          candidate.spentByTxid === null,
      )
      .sort(
        (left, right) =>
          right.blockHeight - left.blockHeight ||
          right.txIndex - left.txIndex ||
          left.vout - right.vout,
      )
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
    const outputs = this.state.utxoOutputs.filter(
      (output) => output.networkId === networkId && outputKeys.includes(output.outputKey),
    );

    return new Map(outputs.map((output) => [output.outputKey, output]));
  }

  public async hasAppliedBlock(
    networkId: PrimaryId,
    blockHeight: number,
    blockHash: string,
  ): Promise<boolean> {
    return this.state.appliedBlocks.some(
      (candidate) =>
        candidate.networkId === networkId &&
        candidate.blockHeight === blockHeight &&
        candidate.blockHash === blockHash,
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
          this.state.appliedBlocks.some(
            (candidate) =>
              candidate.networkId === networkId &&
              candidate.blockHeight === block.blockHeight &&
              candidate.blockHash === block.blockHash,
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
      if (this.hasAppliedDirectLinkBlock(batch)) {
        continue;
      }

      for (const delta of batch.directLinkDeltas) {
        this.mergeDirectLinkDelta(delta);
      }

      this.state.directLinkAppliedBlocks.push({
        networkId: batch.networkId,
        blockHeight: batch.blockHeight,
        blockHash: batch.blockHash,
      });
    }

    await this.afterMutation();
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
      if (this.appendUniqueTransfer(transfer)) {
        this.state.transfers.push(transfer);
      }
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
      if (this.appendUniqueAddressMovement(movement)) {
        this.state.addressMovements.push(movement);
        this.applyBalanceDelta(movement, blockHeight);
      }
    }
  }

  private applyBatchTransfers(transfers: BlockProjectionBatch['transfers']): void {
    for (const transfer of transfers) {
      if (this.appendUniqueTransfer(transfer)) {
        this.state.transfers.push(transfer);
      }
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
    const existingIndex = this.state.utxoOutputs.findIndex(
      (candidate) =>
        candidate.networkId === output.networkId && candidate.outputKey === output.outputKey,
    );
    const next = clone ? { ...output } : output;
    if (existingIndex >= 0) {
      this.state.utxoOutputs[existingIndex] = next;
    } else {
      this.state.utxoOutputs.push(next);
    }
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
    const existing = this.state.balances.find(
      (candidate) =>
        candidate.networkId === balance.networkId &&
        candidate.address === balance.address &&
        candidate.assetAddress === balance.assetAddress,
    );
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
    return this.state.directLinks.find(
      (candidate) =>
        candidate.networkId === link.networkId &&
        candidate.fromAddress === link.fromAddress &&
        candidate.toAddress === link.toAddress &&
        candidate.assetAddress === link.assetAddress,
    );
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
    return blocks.some(
      (candidate) =>
        candidate.networkId === block.networkId &&
        candidate.blockHeight === block.blockHeight &&
        candidate.blockHash === block.blockHash,
    );
  }

  private applyBalanceDelta(movement: AddressMovement, blockHeight: number): void {
    const current = this.findBalanceSnapshot(movement);
    const nextAmount = nextBalanceAmount(current?.balance, movement);
    assertNonNegativeBalance(movement, nextAmount);
    this.writeBalanceSnapshot(movement, blockHeight, current, nextAmount);
  }

  private findBalanceSnapshot(movement: AddressMovement): BalanceRow | undefined {
    return this.state.balances.find(
      (candidate) =>
        candidate.networkId === movement.networkId &&
        candidate.address === movement.address &&
        candidate.assetAddress === movement.assetAddress,
    );
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

    const timeoutMs = context?.statementTimeoutMs ?? this.requestTimeoutMs;
    const requestContext = createAbortableRequestContext(context?.abortSignal, timeoutMs);
    try {
      return await this.applyCoreDogecoinWindowWithRequestContext(input, requestContext, context);
    } catch (error) {
      if (requestContext.didTimeout()) {
        throw this.toDeadlineInfrastructureError(error, requestContext, timeoutMs);
      }
      throw error;
    } finally {
      requestContext.cleanup();
    }
  }

  private async applyCoreDogecoinWindowWithRequestContext(
    input: CoreDogecoinBlockApplication[],
    requestContext: ClickHouseRequestContext,
    context?: CoreDogecoinApplyContext,
  ): Promise<CoreDogecoinApplyResult> {
    if (input.length === 0) {
      return { applied: false, processTail: -1 };
    }

    const networkId = input[0]?.networkId ?? 0;
    const processedBlocks = await this.getCoreProcessedBlocks(
      networkId,
      input.map((application) => application.blockHeight),
      requestContext,
    );
    const pending = input.filter((application) => {
      const existing = processedBlocks.get(application.blockHeight);
      if (!existing) {
        return true;
      }
      if (existing.blockHash !== application.blockHash) {
        throw new Error(
          `core block hash mismatch network=${networkId} height=${application.blockHeight} existing=${existing.blockHash} next=${application.blockHash}`,
        );
      }
      return false;
    });

    if (pending.length === 0) {
      return {
        applied: false,
        processTail: input.at(-1)?.blockHeight ?? -1,
      };
    }

    this.assertCoreWindowShape(networkId, pending);
    if (context?.validatePrevouts !== false) {
      await this.assertCoreWindowPrevouts(networkId, pending, requestContext);
    }

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
    if (context?.updateCurrentState === true) {
      for (let index = 0; index < pending.length; index += maxCoreCurrentStateApplyBlocksPerChunk) {
        await this.applyCoreCurrentStateWindow(
          networkId,
          pending.slice(index, index + maxCoreCurrentStateApplyBlocksPerChunk),
          requestContext,
        );
      }
    }
    await this.insertRows(
      coreProcessedBlocksTable,
      pending.map(toCoreProcessedBlockInsertRow),
      requestContext,
    );

    return {
      applied: true,
      processTail: pending.at(-1)?.blockHeight ?? input.at(-1)?.blockHeight ?? -1,
    };
  }

  private async applyCoreCurrentStateWindow(
    networkId: PrimaryId,
    pending: CoreDogecoinBlockApplication[],
    requestContext: ClickHouseRequestContext,
  ): Promise<void> {
    const windowEnd = pending.at(-1)?.blockHeight ?? -1;
    if (windowEnd < 0) {
      return;
    }

    const currentOutputs = await this.getCurrentUtxoOutputMap(
      networkId,
      [
        ...new Set(
          pending.flatMap((application) => [
            ...application.utxoCreates.map((output) => output.outputKey),
            ...application.utxoSpends.map((spend) => spend.outputKey),
          ]),
        ),
      ],
      requestContext,
    );
    const nextOutputs = new Map<string, ProjectionUtxoOutput>();
    const balanceDeltas = new Map<
      string,
      {
        address: string;
        amount: bigint;
        assetAddress: string;
      }
    >();

    for (const application of pending) {
      for (const output of application.utxoCreates) {
        const current = nextOutputs.get(output.outputKey) ?? currentOutputs.get(output.outputKey);
        if (current) {
          continue;
        }
        nextOutputs.set(output.outputKey, { ...output });
        addCoreBalanceDelta(balanceDeltas, output, BigInt(output.valueBase));
      }

      for (const spend of application.utxoSpends) {
        const current = nextOutputs.get(spend.outputKey) ?? currentOutputs.get(spend.outputKey);
        if (!current) {
          console.warn(
            `[onlydoge] missing current dogecoin prevout ignored output_key=${spend.outputKey} spent_by_txid=${spend.spentByTxid} spent_in_block=${spend.spentInBlock}`,
          );
          continue;
        }
        if (current.spentByTxid !== null || current.spentInBlock !== null) {
          console.warn(
            `[onlydoge] already spent current dogecoin prevout ignored output_key=${spend.outputKey} existing_spent_by_txid=${current.spentByTxid ?? ''} existing_spent_in_block=${current.spentInBlock ?? ''} spent_by_txid=${spend.spentByTxid} spent_in_block=${spend.spentInBlock}`,
          );
          continue;
        }
        nextOutputs.set(spend.outputKey, {
          ...current,
          spentByTxid: spend.spentByTxid,
          spentInBlock: spend.spentInBlock,
          spentInputIndex: spend.spentInputIndex,
        });
        addCoreBalanceDelta(balanceDeltas, current, -BigInt(current.valueBase));
      }
    }

    const balanceKeys = [...balanceDeltas.keys()];
    const currentBalances = await this.getBalanceRowsByKeys(networkId, balanceKeys, requestContext);
    const nextBalances: VersionedBalanceRow[] = [];
    for (const [key, delta] of balanceDeltas) {
      const currentBalance = BigInt(currentBalances.get(key)?.balance ?? '0');
      const nextBalance = currentBalance + delta.amount;
      if (nextBalance < 0n) {
        throw new Error(`negative balance for ${networkId}:${delta.address}:${delta.assetAddress}`);
      }
      nextBalances.push({
        networkId,
        address: delta.address,
        assetAddress: delta.assetAddress,
        balance: formatAmountBase(nextBalance),
        asOfBlockHeight: windowEnd,
        version: coreCurrentBalanceVersion(windowEnd),
      });
    }

    await this.insertRows(
      utxoCurrentStateTable,
      [...nextOutputs.values()].map((row) => toUtxoInsertRow(row, coreCurrentUtxoVersion(row))),
      requestContext,
    );
    await this.insertRows(
      balancesTable,
      nextBalances.map((row) => toBalanceInsertRow(row, row.version)),
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
      ...(context ? { context } : {}),
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
      ...(context ? { context } : {}),
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

    const networkId = input[0]?.networkId ?? 0;
    this.assertCoreWindowShape(networkId, input);
    const tables = coreBenchmarkTableNames(prefix);
    const createRows = input.flatMap((application) => application.utxoCreates);
    const spendRows = input.flatMap((application) => application.utxoSpends);
    await this.insertRows(tables.creates, createRows.map(toCoreUtxoCreateInsertRow));
    await this.insertRows(
      tables.spends,
      spendRows.map((spend) => toCoreUtxoSpendInsertRow(networkId, spend)),
    );
    await this.insertRows(tables.processedBlocks, input.map(toCoreProcessedBlockInsertRow));

    return {
      rowsInserted: createRows.length + spendRows.length + input.length,
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
            script_pub_key,
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
            c.script_pub_key,
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
          WITH address_outputs AS (
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
            INNER JOIN (
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
            ) AS s
            ON c.output_key = s.spent_output_key
            INNER JOIN (
              SELECT
                block_height,
                block_hash,
                block_time
              FROM ${coreProcessedBlocksTable}
              WHERE network_id = {networkId:UInt64}
              ORDER BY block_height ASC, version DESC
              LIMIT 1 BY block_height
            ) AS b
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

    return pageRows.flatMap((row) => {
      const output = outputsByKey.get(row.outputKey);
      if (!output || !output.isSpendable || output.spentByTxid !== null) {
        return [];
      }

      return [output];
    });
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
    const missingOutputKeys = outputKeys.filter((outputKey) => !currentOutputs.has(outputKey));
    if (missingOutputKeys.length === 0) {
      return currentOutputs;
    }

    await this.addFallbackUtxoOutputs(networkId, missingOutputKeys, currentOutputs);
    const stillMissingOutputKeys = outputKeys.filter((outputKey) => !currentOutputs.has(outputKey));
    if (stillMissingOutputKeys.length > 0) {
      await this.addCoreHistoricalUtxoOutputs(networkId, stillMissingOutputKeys, currentOutputs);
    }
    return currentOutputs;
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
    if (fallbackRows.length > 0) {
      await this.insertRows(
        utxoCurrentStateTable,
        fallbackRows.map((row) => toUtxoInsertRow(row, row.spentInBlock ?? row.blockHeight)),
      );
      for (const row of fallbackRows) {
        currentOutputs.set(row.outputKey, row);
      }
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
                c.script_pub_key AS "scriptPubKey",
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
                  script_pub_key,
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

    const blockHeight = rows[0]?.blockHeight;
    return blockHeight === null || blockHeight === undefined ? null : Number(blockHeight);
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

    const windowEnd = window.pendingBatches.at(-1)?.blockHeight ?? 0;
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
              script_pub_key AS "scriptPubKey",
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
    const timeoutMs = context?.timeoutMs ?? this.requestTimeoutMs;
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
            script_pub_key AS "scriptPubKey",
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
              script_pub_key,
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
    const timeoutMs = context?.timeoutMs ?? this.requestTimeoutMs;
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
    > = await mapWithConcurrency(
      chunkQueryValues(keys),
      maxClickHouseBalanceKeyQueryConcurrency,
      (chunk) =>
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
    const created = new Set<string>();
    const spent = new Set<string>();
    let previousHeight: number | null = null;

    for (const application of applications) {
      if (application.networkId !== networkId) {
        throw new Error(
          `mixed core dogecoin networks in window expected=${networkId} actual=${application.networkId}`,
        );
      }
      if (previousHeight !== null && application.blockHeight !== previousHeight + 1) {
        throw new Error(
          `non-contiguous core dogecoin window previous=${previousHeight} next=${application.blockHeight}`,
        );
      }
      previousHeight = application.blockHeight;

      for (const output of application.utxoCreates) {
        if (created.has(output.outputKey)) {
          throw new Error(`duplicate dogecoin output in core window: ${output.outputKey}`);
        }
        created.add(output.outputKey);
      }
      for (const spend of application.utxoSpends) {
        if (spent.has(spend.outputKey)) {
          throw new Error(`duplicate dogecoin spend in core window: ${spend.outputKey}`);
        }
        spent.add(spend.outputKey);
      }
    }
  }

  private async assertCoreWindowPrevouts(
    networkId: PrimaryId,
    applications: CoreDogecoinBlockApplication[],
    requestContext?: ClickHouseRequestContext,
  ): Promise<void> {
    const createdInWindow = new Set(
      applications.flatMap((application) =>
        application.utxoCreates.map((output) => output.outputKey),
      ),
    );
    const externalSpendKeys = [
      ...new Set(
        applications
          .flatMap((application) => application.utxoSpends.map((spend) => spend.outputKey))
          .filter((outputKey) => !createdInWindow.has(outputKey)),
      ),
    ];
    if (externalSpendKeys.length === 0) {
      return;
    }

    const [created, spent] = await Promise.all([
      this.getCoreUtxoCreateRows(networkId, externalSpendKeys, requestContext),
      this.getCoreUtxoSpendRows(networkId, externalSpendKeys, requestContext),
    ]);
    const missing = externalSpendKeys.find((outputKey) => !created.has(outputKey));
    if (missing) {
      throw new Error(`missing core dogecoin prevout: ${missing}`);
    }

    const spendsInWindow = new Map(
      applications
        .flatMap((application) => application.utxoSpends)
        .map((spend) => [spend.outputKey, spend]),
    );
    const alreadySpent = externalSpendKeys.find((outputKey) => {
      const row = spent.get(outputKey);
      if (!row) {
        return false;
      }

      const retrySpend = spendsInWindow.get(outputKey);
      return (
        !retrySpend ||
        row.spentByTxid !== retrySpend.spentByTxid ||
        row.spentInBlock !== retrySpend.spentInBlock ||
        row.spentInputIndex !== retrySpend.spentInputIndex
      );
    });
    if (alreadySpent) {
      throw new Error(`core dogecoin prevout already spent: ${alreadySpent}`);
    }
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
    const outputKeyChunks = chunkQueryValues(outputKeys, {
      maxBytes: maxClickHouseHotOutputKeyBytesPerChunk,
      maxValues: maxClickHouseHotOutputKeyValuesPerChunk,
    });
    const rowChunks: ProjectionUtxoOutput[][] = await mapWithConcurrency(
      outputKeyChunks,
      maxClickHouseHotOutputKeyQueryConcurrency,
      (chunk) =>
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
                script_pub_key AS "scriptPubKey",
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
          script_pub_key,
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
          script_pub_key,
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
    try {
      if (requestContext) {
        return await this.queryRowsWithRequestContext<T>(parameters, requestContext);
      }
      const result = await this.client.query(parameters);
      return (await result.json<T>()) as T[];
    } catch (error) {
      throw this.toInfrastructureError(error);
    }
  }

  private async queryRowsWithDeadline<T>(
    parameters: ClickHouseJsonQueryParameters,
    context?: ProjectionPageRequestContext,
  ): Promise<T[]> {
    const timeoutMs = queryTimeoutMs(context, this.requestTimeoutMs);
    const requestContext = createAbortableRequestContext(context?.abortSignal, timeoutMs);

    try {
      return await this.queryRowsWithRequestContext<T>(parameters, requestContext);
    } catch (error) {
      throw this.toDeadlineInfrastructureError(error, requestContext, timeoutMs);
    } finally {
      requestContext.cleanup();
    }
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
    try {
      if (requestContext) {
        await this.runWithRequestContext(requestContext, () =>
          this.client.insert({ ...parameters, abort_signal: requestContext.signal }),
        );
        return;
      }

      await this.client.insert(parameters);
    } catch (error) {
      throw this.toInfrastructureError(error);
    }
  }

  private async runWithRequestContext<T>(
    requestContext: ClickHouseRequestContext,
    work: () => Promise<T>,
  ): Promise<T> {
    if (requestContext.signal.aborted) {
      throw abortReason(requestContext.signal);
    }

    let listener: (() => void) | null = null;
    try {
      return await Promise.race([
        work(),
        new Promise<never>((_resolve, reject) => {
          listener = () => reject(abortReason(requestContext.signal));
          requestContext.signal.addEventListener('abort', listener, { once: true });
        }),
      ]);
    } finally {
      if (listener) {
        requestContext.signal.removeEventListener('abort', listener);
      }
    }
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
    return {
      balance,
      receivedBase: movement.receivedBase,
      sentBase: movement.sentBase,
      txCount: movement.txCount,
      utxoCount,
    };
  }

  if (balance === '0' && utxoCount === 0) {
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
  return (
    movement !== undefined &&
    (Number(movement.txCount) > 0 || movement.receivedBase !== '0' || movement.sentBase !== '0')
  );
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

    return this.fallback?.getCurrentAddressSummary(networkId, address) ?? null;
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

    return this.fallback?.hasAppliedBlock(networkId, blockHeight, blockHash) ?? false;
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
    if (this.mirror) {
      await this.mirror.replaceSourceLinks(networkId, sourceAddressId, rows);
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
    if (primaryRows.length > 0 || !this.fallback) {
      return primaryRows;
    }

    return this.fallback.listAddressUtxos(networkId, address, offset, limit);
  }

  private async withFallbackMap<TKey, TValue>(
    primaryPromise: Promise<Map<string, TValue>>,
    keys: TKey[],
    fallbackLoader: (missingKeys: TKey[]) => Promise<Map<string, TValue>>,
    toKey: (key: TKey) => string,
  ): Promise<Map<string, TValue>> {
    const primaryRows = await primaryPromise;
    if (!this.fallback || keys.length === 0) {
      return primaryRows;
    }

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
    script_pub_key: output.scriptPubKey,
    script_type: output.scriptType,
    value_base: output.valueBase,
    is_coinbase: output.isCoinbase ? 1 : 0,
    is_spendable: output.isSpendable ? 1 : 0,
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

function clickHouseCoreMaterializationSettings(
  context: CoreDogecoinApplyContext | undefined,
): ClickHouseCommandSettings {
  const timeoutMs = context?.statementTimeoutMs ?? 300_000;
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
  if (!output.isSpendable || output.address === '') {
    return;
  }

  const assetAddress = '';
  const key = balanceKey(output.networkId, output.address, assetAddress);
  const current = deltas.get(key);
  if (current) {
    current.amount += amount;
    return;
  }

  deltas.set(key, {
    address: output.address,
    amount,
    assetAddress,
  });
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
        script_pub_key String,
        value_base String,
        is_coinbase UInt8,
        is_spendable UInt8,
        version UInt64
      )
      ENGINE = MergeTree
      ORDER BY (network_id, output_key)
    `,
    `
      ALTER TABLE ${tables.creates}
      ADD COLUMN IF NOT EXISTS script_pub_key String AFTER script_type
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
        script_pub_key String,
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
      ALTER TABLE ${tables.currentUtxos}
      ADD COLUMN IF NOT EXISTS script_pub_key String AFTER script_type
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
      script_pub_key String,
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
      script_pub_key String,
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
      script_pub_key String,
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
    DROP VIEW IF EXISTS ${utxoCurrentStateByAddressTable}_mv
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
      script_pub_key,
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
      script_pub_key String,
      value_base String,
      is_coinbase UInt8,
      is_spendable UInt8,
      version UInt64
    )
    ENGINE = ReplacingMergeTree(version)
    ORDER BY (network_id, output_key)
  `,
  `
    ALTER TABLE utxo_outputs_v2
    ADD COLUMN IF NOT EXISTS script_pub_key String AFTER script_type
  `,
  `
    ALTER TABLE ${utxoCurrentStateTable}
    ADD COLUMN IF NOT EXISTS script_pub_key String AFTER script_type
  `,
  `
    ALTER TABLE ${utxoCurrentStateByAddressTable}
    ADD COLUMN IF NOT EXISTS script_pub_key String AFTER script_type
  `,
  `
    ALTER TABLE ${coreUtxoCreatesTable}
    ADD COLUMN IF NOT EXISTS script_pub_key String AFTER script_type
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
