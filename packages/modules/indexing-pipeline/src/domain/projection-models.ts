import { formatAmountBase, parseAmountBase } from './amounts';

export interface ProjectionUtxoOutput {
  address: string;
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  isCoinbase: boolean;
  isSpendable: boolean;
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
  outputKey: string | null;
  txIndex: number;
  txid: string;
}

export interface ProjectionBalanceSnapshot {
  address: string;
  assetAddress: string;
  asOfBlockHeight: number;
  balance: string;
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
    throw new Error(`negative balance for ${movement.address}:${movement.assetAddress}`);
  }
}

function balanceSnapshot(
  asOfBlockHeight: number,
  movement: AddressMovement,
  amount: bigint,
): ProjectionBalanceSnapshot {
  return {
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
}

export interface BlockProjectionBatch {
  addressMovements: AddressMovement[];
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  utxoCreates: ProjectionUtxoOutput[];
  utxoSpends: ProjectionUtxoSpend[];
}

export function projectionBlockIdentity(blockHeight: number, blockHash: string): string {
  return `${blockHeight}:${blockHash}`;
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

export function orderProjectionBatches<T extends { blockHeight: number }>(batches: T[]): T[] {
  return [...batches].sort((left, right) => left.blockHeight - right.blockHeight);
}

export function toProjectionAppliedBlocks<T extends ProjectionAppliedBlock>(
  batches: T[],
): ProjectionAppliedBlock[] {
  return batches.map((batch) => ({
    blockHeight: batch.blockHeight,
    blockHash: batch.blockHash,
  }));
}

export function pendingProjectionBatches<T extends ProjectionAppliedBlock>(
  batches: T[],
  appliedBlocks: Set<string>,
): T[] {
  return batches.filter(
    (batch) => !appliedBlocks.has(projectionBlockIdentity(batch.blockHeight, batch.blockHash)),
  );
}

export interface PendingProjectionWindow<T extends ProjectionAppliedBlock> {
  orderedBatches: T[];
  pendingBatches: T[];
}

export async function resolvePendingProjectionWindow<T extends ProjectionAppliedBlock>(
  batches: T[],
  listAppliedBlocks: (blocks: ProjectionAppliedBlock[]) => Promise<Set<string>>,
): Promise<PendingProjectionWindow<T> | null> {
  if (batches.length === 0) {
    return null;
  }

  const orderedBatches = orderProjectionBatches(batches);
  requireFirstProjectionBatch(orderedBatches);

  const appliedBlocks = await listAppliedBlocks(toProjectionAppliedBlocks(orderedBatches));
  const pendingBatches = pendingProjectionBatches(orderedBatches, appliedBlocks);
  return pendingProjectionWindow(orderedBatches, pendingBatches);
}

function requireFirstProjectionBatch<T extends ProjectionAppliedBlock>(batches: T[]): T {
  const [batch] = batches;
  if (!batch) {
    throw new Error('empty projection window');
  }

  return batch;
}

function pendingProjectionWindow<T extends ProjectionAppliedBlock>(
  orderedBatches: T[],
  pendingBatches: T[],
): PendingProjectionWindow<T> | null {
  if (pendingBatches.length === 0) {
    return null;
  }

  return { orderedBatches, pendingBatches };
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
  batches: BlockProjectionBatch[],
  loadOutputs: (outputKeys: string[]) => Promise<Map<string, ProjectionUtxoOutput>>,
): Promise<Map<string, ProjectionUtxoOutput>> {
  const spendKeys = collectProjectionSpendOutputKeys(batches);
  const currentOutputs = await loadOutputs(spendKeys);
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
  loadOutputs: (outputKeys: string[]) => Promise<Map<string, ProjectionUtxoOutput>>;
  toSnapshotKey: (key: { address: string; assetAddress: string }) => TKey;
  toStoredSnapshot?: (snapshot: ProjectionBalanceSnapshot, movement: AddressMovement) => TSnapshot;
}): Promise<{
  nextBalances: Map<string, TSnapshot>;
  nextOutputs: Map<string, ProjectionUtxoOutput>;
}> {
  const [nextOutputs, nextBalances] = await Promise.all([
    buildProjectionUtxoWindow(input.batches, input.loadOutputs),
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

export interface ProjectionFactWindow {
  addressMovements: AddressMovement[];
  appliedBlocks: ProjectionAppliedBlock[];
  balances: ProjectionBalanceSnapshot[];
  utxoOutputs: ProjectionUtxoOutput[];
}

export interface ProjectionStateBootstrapSnapshot {
  appliedBlocks: ProjectionAppliedBlock[];
  balances: ProjectionBalanceSnapshot[];
  utxoOutputs: ProjectionUtxoOutput[];
}

export type CoreIndexerStage = 'sync_backfill' | 'process_backfill' | 'online';

export interface CoreIndexerState {
  lastError: string | null;
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
