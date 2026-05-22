import type { PrimaryId } from '@onlydoge/shared-kernel';

import { addAmountBase, formatAmountBase, parseAmountBase } from './amounts';

export interface ProjectionUtxoOutput {
  address: string;
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  isCoinbase: boolean;
  isSpendable: boolean;
  networkId: PrimaryId;
  outputKey: string;
  scriptType: string;
  spentByTxid: string | null;
  spentInBlock: number | null;
  spentInputIndex: number | null;
  txIndex: number;
  txid: string;
  valueBase: string;
  vout: number;
}

export interface ProjectionUtxoSpend {
  outputKey: string;
  spentByTxid: string;
  spentInBlock: number;
  spentInputIndex: number;
}

export function applyProjectionUtxoSpends(
  nextOutputs: Map<string, ProjectionUtxoOutput>,
  currentOutputs: Map<string, ProjectionUtxoOutput>,
  spends: ProjectionUtxoSpend[],
): void {
  for (const spend of spends) {
    nextOutputs.set(
      spend.outputKey,
      spentProjectionUtxoOutput(
        requireProjectionUtxoSpendOutput(nextOutputs, currentOutputs, spend),
        spend,
      ),
    );
  }
}

function requireProjectionUtxoSpendOutput(
  nextOutputs: Map<string, ProjectionUtxoOutput>,
  currentOutputs: Map<string, ProjectionUtxoOutput>,
  spend: ProjectionUtxoSpend,
): ProjectionUtxoOutput {
  const next = nextOutputs.get(spend.outputKey);
  if (next) {
    return next;
  }

  return requireCurrentProjectionUtxoOutput(currentOutputs, spend.outputKey);
}

function requireCurrentProjectionUtxoOutput(
  currentOutputs: Map<string, ProjectionUtxoOutput>,
  outputKey: string,
): ProjectionUtxoOutput {
  const current = currentOutputs.get(outputKey);
  if (!current) {
    throw new Error(`missing utxo output: ${outputKey}`);
  }

  return current;
}

function spentProjectionUtxoOutput(
  current: ProjectionUtxoOutput,
  spend: ProjectionUtxoSpend,
): ProjectionUtxoOutput {
  return {
    ...current,
    spentByTxid: spend.spentByTxid,
    spentInBlock: spend.spentInBlock,
    spentInputIndex: spend.spentInputIndex,
  };
}

export interface AddressMovement {
  address: string;
  amountBase: string;
  assetAddress: string;
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  derivationMethod: string;
  direction: 'credit' | 'debit';
  entryIndex: number;
  movementId: string;
  networkId: PrimaryId;
  outputKey: string | null;
  txIndex: number;
  txid: string;
}

export interface TransferFact {
  amountBase: string;
  assetAddress: string;
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  confidence: number;
  derivationMethod: string;
  fromAddress: string;
  inputAddressCount: number;
  isChange: boolean;
  networkId: PrimaryId;
  outputAddressCount: number;
  toAddress: string;
  transferId: string;
  transferIndex: number;
  txIndex: number;
  txid: string;
}

export interface DirectLinkDelta {
  assetAddress: string;
  firstSeenBlockHeight: number;
  fromAddress: string;
  lastSeenBlockHeight: number;
  networkId: PrimaryId;
  toAddress: string;
  totalAmountBase: string;
  transferCount: number;
}

export interface DirectLinkRecord extends DirectLinkDelta {}

export function mergeDirectLinkDelta<T extends DirectLinkRecord>(
  current: T,
  delta: DirectLinkDelta,
): T {
  return {
    ...current,
    transferCount: current.transferCount + delta.transferCount,
    totalAmountBase: addAmountBase(current.totalAmountBase, delta.totalAmountBase),
    firstSeenBlockHeight: Math.min(current.firstSeenBlockHeight, delta.firstSeenBlockHeight),
    lastSeenBlockHeight: Math.max(current.lastSeenBlockHeight, delta.lastSeenBlockHeight),
  };
}

export function applyDirectLinkDeltasToSnapshots<T extends DirectLinkRecord>(input: {
  currentDirectLinks: Map<string, T>;
  directLinkDeltas: DirectLinkDelta[];
  keyForDelta: (delta: DirectLinkDelta) => string;
  nextDirectLinks: Map<string, T>;
  toStoredRecord?: (record: DirectLinkRecord, delta: DirectLinkDelta) => T;
}): void {
  for (const delta of input.directLinkDeltas) {
    const key = input.keyForDelta(delta);
    input.nextDirectLinks.set(key, nextDirectLinkRecord(input, key, delta));
  }
}

function nextDirectLinkRecord<T extends DirectLinkRecord>(
  input: {
    currentDirectLinks: Map<string, T>;
    nextDirectLinks: Map<string, T>;
    toStoredRecord?: (record: DirectLinkRecord, delta: DirectLinkDelta) => T;
  },
  key: string,
  delta: DirectLinkDelta,
): T {
  return storedDirectLinkRecord(input, mergedDirectLinkRecord(input, key, delta), delta);
}

function mergedDirectLinkRecord<T extends DirectLinkRecord>(
  input: {
    currentDirectLinks: Map<string, T>;
    nextDirectLinks: Map<string, T>;
  },
  key: string,
  delta: DirectLinkDelta,
): DirectLinkRecord {
  const current = nextDirectLinkCurrent(input, key);
  if (current) {
    return mergeDirectLinkDelta(current, delta);
  }

  return { ...delta };
}

function nextDirectLinkCurrent<T extends DirectLinkRecord>(
  input: {
    currentDirectLinks: Map<string, T>;
    nextDirectLinks: Map<string, T>;
  },
  key: string,
): T | undefined {
  const next = input.nextDirectLinks.get(key);
  if (next) {
    return next;
  }

  return input.currentDirectLinks.get(key);
}

function storedDirectLinkRecord<T extends DirectLinkRecord>(
  input: {
    toStoredRecord?: (record: DirectLinkRecord, delta: DirectLinkDelta) => T;
  },
  record: DirectLinkRecord,
  delta: DirectLinkDelta,
): T {
  if (input.toStoredRecord) {
    return input.toStoredRecord(record, delta);
  }

  return record as T;
}

export function buildDirectLinkDeltas(
  networkId: PrimaryId,
  blockHeight: number,
  transfers: TransferFact[],
  options?: { includeChange?: boolean },
): DirectLinkDelta[] {
  const result = new Map<string, DirectLinkDelta>();
  for (const transfer of transfers) {
    applyDirectLinkTransferIfIncluded(result, networkId, blockHeight, transfer, options);
  }

  return [...result.values()];
}

function shouldSkipDirectLinkTransfer(
  transfer: TransferFact,
  options?: { includeChange?: boolean },
): boolean {
  return [transfer.isChange, !includeDirectLinkChange(options)].every(Boolean);
}

function includeDirectLinkChange(options?: { includeChange?: boolean }): boolean {
  return options?.includeChange === true;
}

function applyDirectLinkTransferIfIncluded(
  result: Map<string, DirectLinkDelta>,
  networkId: PrimaryId,
  blockHeight: number,
  transfer: TransferFact,
  options?: { includeChange?: boolean },
): void {
  if (shouldSkipDirectLinkTransfer(transfer, options)) {
    return;
  }

  applyDirectLinkTransfer(result, networkId, blockHeight, transfer);
}

function applyDirectLinkTransfer(
  result: Map<string, DirectLinkDelta>,
  networkId: PrimaryId,
  blockHeight: number,
  transfer: TransferFact,
): void {
  const key = [transfer.fromAddress, transfer.toAddress, transfer.assetAddress].join(':');
  const current = result.get(key);
  if (current) {
    current.transferCount += 1;
    current.totalAmountBase = addAmountBase(current.totalAmountBase, transfer.amountBase);
    current.lastSeenBlockHeight = blockHeight;
    return;
  }

  result.set(key, newDirectLinkDelta(networkId, blockHeight, transfer));
}

function newDirectLinkDelta(
  networkId: PrimaryId,
  blockHeight: number,
  transfer: TransferFact,
): DirectLinkDelta {
  return {
    networkId,
    fromAddress: transfer.fromAddress,
    toAddress: transfer.toAddress,
    assetAddress: transfer.assetAddress,
    transferCount: 1,
    totalAmountBase: transfer.amountBase,
    firstSeenBlockHeight: blockHeight,
    lastSeenBlockHeight: blockHeight,
  };
}

export interface ProjectionBalanceSnapshot {
  address: string;
  assetAddress: string;
  asOfBlockHeight: number;
  balance: string;
  networkId: PrimaryId;
}

export function applyAddressMovementsToBalances<
  T extends ProjectionBalanceSnapshot = ProjectionBalanceSnapshot,
>(input: {
  asOfBlockHeight: number;
  currentBalances: Map<string, T>;
  keyForMovement: (movement: AddressMovement) => string;
  movements: AddressMovement[];
  nextBalances: Map<string, T>;
  toStoredSnapshot?: (snapshot: ProjectionBalanceSnapshot, movement: AddressMovement) => T;
}): void {
  for (const movement of input.movements) {
    applyAddressMovementToBalances(input, movement);
  }
}

function applyAddressMovementToBalances<T extends ProjectionBalanceSnapshot>(
  input: {
    asOfBlockHeight: number;
    currentBalances: Map<string, T>;
    keyForMovement: (movement: AddressMovement) => string;
    nextBalances: Map<string, T>;
    toStoredSnapshot?: (snapshot: ProjectionBalanceSnapshot, movement: AddressMovement) => T;
  },
  movement: AddressMovement,
): void {
  const key = input.keyForMovement(movement);
  const nextAmount = nextBalanceAmount(currentBalanceAmount(input, key), movement);
  assertNonNegativeBalance(nextAmount, movement);
  const snapshot = balanceSnapshot(input.asOfBlockHeight, movement, nextAmount);
  input.nextBalances.set(key, storedBalanceSnapshot(input, snapshot, movement));
}

function currentBalanceAmount<T extends ProjectionBalanceSnapshot>(
  input: {
    currentBalances: Map<string, T>;
    nextBalances: Map<string, T>;
  },
  key: string,
): bigint {
  return parseAmountBase(currentBalanceValue(input, key));
}

function currentBalanceValue<T extends ProjectionBalanceSnapshot>(
  input: {
    currentBalances: Map<string, T>;
    nextBalances: Map<string, T>;
  },
  key: string,
): string {
  const current = currentBalanceSnapshot(input, key);
  if (current) {
    return current.balance;
  }

  return '0';
}

function currentBalanceSnapshot<T extends ProjectionBalanceSnapshot>(
  input: {
    currentBalances: Map<string, T>;
    nextBalances: Map<string, T>;
  },
  key: string,
): T | undefined {
  const next = input.nextBalances.get(key);
  if (next) {
    return next;
  }

  return input.currentBalances.get(key);
}

function nextBalanceAmount(currentAmount: bigint, movement: AddressMovement): bigint {
  const movementAmount = parseAmountBase(movement.amountBase);
  return movement.direction === 'credit'
    ? currentAmount + movementAmount
    : currentAmount - movementAmount;
}

function assertNonNegativeBalance(amount: bigint, movement: AddressMovement): void {
  if (amount < 0n) {
    throw new Error(
      `negative balance for ${movement.networkId}:${movement.address}:${movement.assetAddress}`,
    );
  }
}

function balanceSnapshot(
  asOfBlockHeight: number,
  movement: AddressMovement,
  amount: bigint,
): ProjectionBalanceSnapshot {
  return {
    networkId: movement.networkId,
    address: movement.address,
    assetAddress: movement.assetAddress,
    balance: formatAmountBase(amount),
    asOfBlockHeight,
  };
}

function storedBalanceSnapshot<T extends ProjectionBalanceSnapshot>(
  input: {
    toStoredSnapshot?: (snapshot: ProjectionBalanceSnapshot, movement: AddressMovement) => T;
  },
  snapshot: ProjectionBalanceSnapshot,
  movement: AddressMovement,
): T {
  return input.toStoredSnapshot ? input.toStoredSnapshot(snapshot, movement) : (snapshot as T);
}

export interface ProjectionBalanceCursor {
  address: string;
  assetAddress: string;
}

export interface ProjectionPageRequestContext {
  abortSignal?: AbortSignal;
  timeoutMs?: number;
}

export interface ProjectionCurrentUtxoPage {
  nextCursor: string | null;
  rows: ProjectionUtxoOutput[];
}

export interface ProjectionCurrentBalancePage {
  nextCursor: ProjectionBalanceCursor | null;
  rows: ProjectionBalanceSnapshot[];
}

export interface ProjectionAppliedBlock {
  blockHash: string;
  blockHeight: number;
  networkId: PrimaryId;
}

export interface ProjectionDirectLinkBatch {
  blockHash: string;
  blockHeight: number;
  directLinkDeltas: DirectLinkDelta[];
  networkId: PrimaryId;
}

export interface SourceLinkRecord {
  firstSeenBlockHeight: number;
  hopCount: number;
  lastSeenBlockHeight: number;
  networkId: PrimaryId;
  pathAddresses: string[];
  pathTransferCount: number;
  sourceAddress: string;
  sourceAddressId: PrimaryId;
  toAddress: string;
}

export interface BlockProjectionBatch {
  addressMovements: AddressMovement[];
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  directLinkDeltas: DirectLinkDelta[];
  networkId: PrimaryId;
  transfers: TransferFact[];
  utxoCreates: ProjectionUtxoOutput[];
  utxoSpends: ProjectionUtxoSpend[];
}

export function projectionBlockIdentity(
  networkId: PrimaryId,
  blockHeight: number,
  blockHash: string,
): string {
  return `${networkId}:${blockHeight}:${blockHash}`;
}

export function projectionBalanceSnapshotKey(address: string, assetAddress: string): string {
  return `${address}:${assetAddress}`;
}

export function parseProjectionBalanceSnapshotKey(key: string): {
  address: string;
  assetAddress: string;
} {
  const [address, ...assetAddressParts] = key.split(':');
  return {
    address: address ?? '',
    assetAddress: assetAddressParts.join(':'),
  };
}

export function projectionDirectLinkSnapshotKey(
  fromAddress: string,
  toAddress: string,
  assetAddress: string,
): string {
  return `${fromAddress}:${toAddress}:${assetAddress}`;
}

export function parseProjectionDirectLinkSnapshotKey(key: string): {
  assetAddress: string;
  fromAddress: string;
  toAddress: string;
} {
  const [fromAddress = '', toAddress = '', ...assetAddressParts] = key.split(':');
  return {
    fromAddress,
    toAddress,
    assetAddress: assetAddressParts.join(':'),
  };
}

export function orderProjectionBatches<T extends { blockHeight: number }>(batches: T[]): T[] {
  return [...batches].sort((left, right) => left.blockHeight - right.blockHeight);
}

export function toProjectionAppliedBlocks<T extends ProjectionAppliedBlock>(
  batches: T[],
): ProjectionAppliedBlock[] {
  return batches.map((batch) => ({
    networkId: batch.networkId,
    blockHeight: batch.blockHeight,
    blockHash: batch.blockHash,
  }));
}

export function pendingProjectionBatches<T extends ProjectionAppliedBlock>(
  batches: T[],
  appliedBlocks: Set<string>,
): T[] {
  return batches.filter(
    (batch) =>
      !appliedBlocks.has(
        projectionBlockIdentity(batch.networkId, batch.blockHeight, batch.blockHash),
      ),
  );
}

export interface PendingProjectionWindow<T extends ProjectionAppliedBlock> {
  networkId: PrimaryId;
  orderedBatches: T[];
  pendingBatches: T[];
}

export async function resolvePendingProjectionWindow<T extends ProjectionAppliedBlock>(
  batches: T[],
  listAppliedBlocks: (
    networkId: PrimaryId,
    blocks: ProjectionAppliedBlock[],
  ) => Promise<Set<string>>,
): Promise<PendingProjectionWindow<T> | null> {
  if (batches.length === 0) {
    return null;
  }

  const orderedBatches = orderProjectionBatches(batches);
  const networkId = requireFirstProjectionBatch(orderedBatches).networkId;

  const appliedBlocks = await listAppliedBlocks(
    networkId,
    toProjectionAppliedBlocks(orderedBatches),
  );
  const pendingBatches = pendingProjectionBatches(orderedBatches, appliedBlocks);
  return pendingProjectionWindow(networkId, orderedBatches, pendingBatches);
}

function requireFirstProjectionBatch<T extends ProjectionAppliedBlock>(batches: T[]): T {
  const [batch] = batches;
  if (!batch) {
    throw new Error('empty projection window');
  }

  return batch;
}

function pendingProjectionWindow<T extends ProjectionAppliedBlock>(
  networkId: PrimaryId,
  orderedBatches: T[],
  pendingBatches: T[],
): PendingProjectionWindow<T> | null {
  if (pendingBatches.length === 0) {
    return null;
  }

  return { networkId, orderedBatches, pendingBatches };
}

export function collectProjectionSpendOutputKeys(batches: BlockProjectionBatch[]): string[] {
  return [...new Set(batches.flatMap((batch) => batch.utxoSpends.map((spend) => spend.outputKey)))];
}

export function collectProjectionTouchedOutputKeys(batches: BlockProjectionBatch[]): string[] {
  return [
    ...new Set(
      batches.flatMap((batch) => [
        ...batch.utxoCreates.map((output) => output.outputKey),
        ...batch.utxoSpends.map((spend) => spend.outputKey),
      ]),
    ),
  ];
}

export function buildNextProjectionUtxoOutputs(
  batches: BlockProjectionBatch[],
  currentOutputs: Map<string, ProjectionUtxoOutput>,
): Map<string, ProjectionUtxoOutput> {
  const nextOutputs = new Map<string, ProjectionUtxoOutput>();
  for (const batch of batches) {
    applyProjectionUtxoBatch(nextOutputs, currentOutputs, batch);
  }

  return nextOutputs;
}

function applyProjectionUtxoBatch(
  nextOutputs: Map<string, ProjectionUtxoOutput>,
  currentOutputs: Map<string, ProjectionUtxoOutput>,
  batch: BlockProjectionBatch,
): void {
  for (const output of batch.utxoCreates) {
    nextOutputs.set(output.outputKey, { ...output });
  }

  applyProjectionUtxoSpends(nextOutputs, currentOutputs, batch.utxoSpends);
}

export async function buildProjectionUtxoWindow(
  networkId: PrimaryId,
  batches: BlockProjectionBatch[],
  loadOutputs: (
    networkId: PrimaryId,
    outputKeys: string[],
  ) => Promise<Map<string, ProjectionUtxoOutput>>,
): Promise<Map<string, ProjectionUtxoOutput>> {
  const spendKeys = collectProjectionSpendOutputKeys(batches);
  const currentOutputs = await loadOutputs(networkId, spendKeys);
  return buildNextProjectionUtxoOutputs(batches, currentOutputs);
}

export function collectProjectionBalanceSnapshotKeys(batches: BlockProjectionBatch[]): string[] {
  return [
    ...new Set(
      batches.flatMap((batch) =>
        batch.addressMovements.map((movement) =>
          projectionBalanceSnapshotKey(movement.address, movement.assetAddress),
        ),
      ),
    ),
  ];
}

export async function buildProjectionBalanceWindow<
  TKey,
  TSnapshot extends ProjectionBalanceSnapshot = ProjectionBalanceSnapshot,
>(input: {
  batches: BlockProjectionBatch[];
  keyForMovement: (movement: AddressMovement) => string;
  loadBalances: (keys: TKey[]) => Promise<Map<string, TSnapshot>>;
  toSnapshotKey: (key: { address: string; assetAddress: string }) => TKey;
  toStoredSnapshot?: (snapshot: ProjectionBalanceSnapshot, movement: AddressMovement) => TSnapshot;
}): Promise<Map<string, TSnapshot>> {
  const balanceKeys = collectProjectionBalanceSnapshotKeys(input.batches)
    .map(parseProjectionBalanceSnapshotKey)
    .map(input.toSnapshotKey);
  const currentBalances = await input.loadBalances(balanceKeys);
  const nextBalances = new Map<string, TSnapshot>();

  for (const batch of input.batches) {
    applyProjectionBalanceBatch(input, currentBalances, nextBalances, batch);
  }

  return nextBalances;
}

function applyProjectionBalanceBatch<
  TSnapshot extends ProjectionBalanceSnapshot = ProjectionBalanceSnapshot,
>(
  input: {
    keyForMovement: (movement: AddressMovement) => string;
    toStoredSnapshot?: (
      snapshot: ProjectionBalanceSnapshot,
      movement: AddressMovement,
    ) => TSnapshot;
  },
  currentBalances: Map<string, TSnapshot>,
  nextBalances: Map<string, TSnapshot>,
  batch: BlockProjectionBatch,
): void {
  const balanceInput = {
    asOfBlockHeight: batch.blockHeight,
    currentBalances,
    keyForMovement: input.keyForMovement,
    movements: batch.addressMovements,
    nextBalances,
  };
  if (input.toStoredSnapshot) {
    applyAddressMovementsToBalances({
      ...balanceInput,
      toStoredSnapshot: input.toStoredSnapshot,
    });
    return;
  }

  applyAddressMovementsToBalances(balanceInput);
}

export async function buildProjectionStateChanges<
  TKey,
  TSnapshot extends ProjectionBalanceSnapshot = ProjectionBalanceSnapshot,
>(input: {
  batches: BlockProjectionBatch[];
  keyForMovement: (movement: AddressMovement) => string;
  loadBalances: (keys: TKey[]) => Promise<Map<string, TSnapshot>>;
  loadOutputs: (
    networkId: PrimaryId,
    outputKeys: string[],
  ) => Promise<Map<string, ProjectionUtxoOutput>>;
  networkId: PrimaryId;
  toSnapshotKey: (key: { address: string; assetAddress: string }) => TKey;
  toStoredSnapshot?: (snapshot: ProjectionBalanceSnapshot, movement: AddressMovement) => TSnapshot;
}): Promise<{
  nextBalances: Map<string, TSnapshot>;
  nextOutputs: Map<string, ProjectionUtxoOutput>;
}> {
  const [nextOutputs, nextBalances] = await Promise.all([
    buildProjectionUtxoWindow(input.networkId, input.batches, input.loadOutputs),
    buildProjectionBalanceWindow({
      batches: input.batches,
      keyForMovement: input.keyForMovement,
      loadBalances: input.loadBalances,
      toSnapshotKey: input.toSnapshotKey,
      ...(input.toStoredSnapshot ? { toStoredSnapshot: input.toStoredSnapshot } : {}),
    }),
  ]);

  return { nextOutputs, nextBalances };
}

export function collectProjectionDirectLinkSnapshotKeys(
  batches: Array<{ directLinkDeltas: DirectLinkDelta[] }>,
): string[] {
  return [
    ...new Set(
      batches.flatMap((batch) =>
        batch.directLinkDeltas.map((delta) =>
          projectionDirectLinkSnapshotKey(delta.fromAddress, delta.toAddress, delta.assetAddress),
        ),
      ),
    ),
  ];
}

export interface ProjectionFactWindow {
  addressMovements: AddressMovement[];
  appliedBlocks: ProjectionAppliedBlock[];
  balances: ProjectionBalanceSnapshot[];
  directLinks: DirectLinkRecord[];
  networkId: PrimaryId;
  transfers: TransferFact[];
  utxoOutputs: ProjectionUtxoOutput[];
}

export interface ProjectionStateBootstrapSnapshot {
  appliedBlocks: ProjectionAppliedBlock[];
  balances: ProjectionBalanceSnapshot[];
  directLinks: DirectLinkRecord[];
  sourceLinks: SourceLinkRecord[];
  utxoOutputs: ProjectionUtxoOutput[];
}

export interface TrackedAddress {
  address: string;
  addressId: PrimaryId;
}

export type CoreIndexerStage = 'sync_backfill' | 'process_backfill' | 'online';

export interface CoreIndexerState {
  lastError: string | null;
  networkId: PrimaryId;
  onlineTip: number;
  processTail: number;
  stage: CoreIndexerStage;
  syncTail: number;
  updatedAt: string;
}

export interface CoreBlockRecord {
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  fetchedAt: string;
  networkId: PrimaryId;
  previousBlockHash: string | null;
  processedAt: string | null;
  rawStorageKey: string;
  txCount: number;
}

export interface CoreDogecoinSpend {
  outputKey: string;
  spentByTxid: string;
  spentInBlock: number;
  spentInputIndex: number;
}

export interface CoreDogecoinBlockApplication {
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  networkId: PrimaryId;
  previousBlockHash: string | null;
  rawStorageKey: string;
  txCount: number;
  utxoCreates: ProjectionUtxoOutput[];
  utxoSpends: CoreDogecoinSpend[];
}

export interface CoreDogecoinApplyResult {
  applied: boolean;
  processTail: number;
}
