import {
  configKeyIndexerProcessTail,
  configKeyIndexerSyncTail,
  type DogecoinTransaction,
  type DogecoinVin,
  type DogecoinVout,
  extractDogecoinOutputAddress,
  formatAmountBase,
  fromDecimalUnits,
  isDogecoinTransaction,
  type ParsedDogecoinBlock,
  type ProjectionUtxoOutput,
  parseAmountBase,
} from '@onlydoge/indexing-pipeline';
import { NotFoundError, TooEarlyError, ValidationError } from '@onlydoge/shared-kernel';

import type {
  ExplorerConfigPort,
  ExplorerCoreBlockPort,
  ExplorerDogecoinConfigPort,
  ExplorerMempoolRpcPort,
  ExplorerRawBlockPort,
  ExplorerWarehousePort,
} from '../contracts/ports';
import type {
  ExplorerAddressDetail,
  ExplorerAddressTransactionSummary,
  ExplorerAddressUtxo,
  ExplorerBlockSummary,
  ExplorerMempoolResponse,
  ExplorerMempoolTransaction,
  ExplorerSearchResult,
  ExplorerTransactionDetail,
  ExplorerTransactionInput,
  ExplorerTransactionOutput,
  ExplorerTransactionSummary,
} from '../domain/query-models';
import {
  addressDetail,
  addressSearchResult,
  outputIndex,
  outputScriptType,
  spentByTxid,
  spentInBlock,
} from './explorer-response-builders';

type ExplorerDogecoinRef = {
  architecture: 'dogecoin';
  rpcEndpoint: string;
  rps: number;
};

type CachedMempoolSnapshot = Omit<ExplorerMempoolResponse, 'limit' | 'offset' | 'returnedCount'>;

type MempoolCacheEntry = {
  expiresAtMs: number;
  promise: Promise<CachedMempoolSnapshot>;
};

const defaultMempoolLimit = 100;
const maxMempoolLimit = 500;
const rawTransactionRefLookupLimit = 1_000;
const mempoolCacheTtlMs = 1_000;

export class ExplorerQueryService {
  private readonly mempoolSnapshots = new Map<string, MempoolCacheEntry>();

  public constructor(
    private readonly dogecoin: ExplorerDogecoinConfigPort,
    private readonly warehouse: ExplorerWarehousePort,
    private readonly rawBlocks: ExplorerRawBlockPort,
    private readonly configs: ExplorerConfigPort,
    private readonly coreBlocks: ExplorerCoreBlockPort,
    private readonly mempoolRpc: ExplorerMempoolRpcPort,
  ) {}

  public async search(query: string | undefined): Promise<{ matches: ExplorerSearchResult[] }> {
    const q = normalizeRequiredQuery(query);
    await this.resolveDogecoin();
    await this.assertHistoryReady();

    if (/^\d+$/u.test(q)) {
      const block = await this.getBlockByHeight(Number(q));
      return { matches: block ? [blockSearchResult(block)] : [] };
    }

    const txMatch = await this.searchTransaction(q);
    const blockMatch = await this.searchBlockHash(q);
    const addressMatch = await this.searchAddress(q);
    return { matches: [txMatch, blockMatch, addressMatch].filter(isExplorerSearchResult) };
  }

  public async listBlocks(
    offset?: number,
    limit?: number,
  ): Promise<{ blocks: ExplorerBlockSummary[] }> {
    await this.resolveDogecoin();
    const syncTail = await this.configNumberOrDefault(configKeyIndexerSyncTail(), -1);
    if (syncTail < 0) {
      return this.listAppliedBlocks(offset, limit);
    }

    const heights = descendingBlockHeights(syncTail, offset ?? 0, limit ?? 20);
    const blocks = await Promise.all(heights.map(async (height) => this.getBlockByHeight(height)));

    return { blocks: blocks.filter((block): block is ExplorerBlockSummary => Boolean(block)) };
  }

  public async listMempool(offset?: number, limit?: number): Promise<ExplorerMempoolResponse> {
    const dogecoin = await this.resolveDogecoin();
    const page = mempoolPage(offset, limit);
    const snapshot = await this.getCachedMempoolSnapshot(dogecoin);
    const transactions = snapshot.transactions.slice(page.offset, page.offset + page.limit);

    return {
      ...snapshot,
      offset: page.offset,
      limit: page.limit,
      returnedCount: transactions.length,
      transactions,
    };
  }

  public async getBlock(
    ref: string,
  ): Promise<{ block: ExplorerBlockSummary; transactions: ExplorerTransactionSummary[] }> {
    await this.resolveDogecoin();
    await this.assertHistoryReady();
    const parsed = await this.resolveBlockSnapshot(ref);
    const inputMap = await this.loadResolvedInputs(parsed.tx);

    return {
      block: this.serializeBlock(parsed),
      transactions: parsed.tx.map((transaction, txIndex) =>
        this.serializeTransactionSummary(parsed, transaction, txIndex, inputMap),
      ),
    };
  }

  public async getTransaction(txid: string): Promise<ExplorerTransactionDetail> {
    const normalizedTxid = txid.trim();
    await this.resolveDogecoin();
    await this.assertHistoryReady();
    const txRef = await this.requireTransactionRef(normalizedTxid);
    const block = await this.loadBlockSnapshot(txRef.blockHeight);
    const transaction = this.requireTransaction(block, normalizedTxid);
    const inputMap = await this.loadResolvedInputs([transaction]);
    const summary = this.serializeTransactionSummary(block, transaction, txRef.txIndex, inputMap);
    const outputs = this.readOutputs(transaction.vout);
    const outputKeys = outputs.map((_output, index) => `${normalizedTxid}:${index}`);
    const currentOutputs = await this.warehouse.getUtxoOutputs(outputKeys);
    const addresses = new Set<string>();
    const inputs = this.serializeTransactionInputs(transaction, inputMap, addresses);
    const serializedOutputs = this.serializeTransactionOutputs(
      normalizedTxid,
      outputs,
      currentOutputs,
      addresses,
    );

    return {
      transaction: summary,
      inputs,
      outputs: serializedOutputs,
    };
  }

  public async getAddress(address: string): Promise<ExplorerAddressDetail> {
    const normalizedAddress = requireExplorerAddress(address);
    await this.resolveDogecoin();
    const summary = await this.warehouse.getAddressSummary(normalizedAddress);
    assertAddressExists(summary);

    return {
      address: addressDetail(normalizedAddress, summary),
    };
  }

  public async listAddressTransactions(
    address: string,
    offset?: number,
    limit?: number,
  ): Promise<{ transactions: ExplorerAddressTransactionSummary[] }> {
    const normalizedAddress = requireExplorerAddress(address);
    await this.resolveDogecoin();
    await this.assertHistoryReady();
    const rows = await this.warehouse.listAddressTransactions(
      normalizedAddress,
      offset,
      defaultAddressPageLimit(limit),
    );
    const snapshotsByHeight = await this.loadSnapshotsByHeight([
      ...new Set(rows.map((row) => row.blockHeight)),
    ]);

    return {
      transactions: rows
        .flatMap((row) => this.addressTransactionSummary(row, snapshotsByHeight))
        .sort(compareAddressTransactionSummaries),
    };
  }

  public async listAddressUtxos(
    address: string,
    offset?: number,
    limit?: number,
  ): Promise<{ utxos: ExplorerAddressUtxo[] }> {
    const normalizedAddress = requireExplorerAddress(address);
    await this.resolveDogecoin();
    const utxos = await this.warehouse.listAddressUtxos(
      normalizedAddress,
      offset,
      defaultAddressPageLimit(limit),
    );

    return {
      utxos: utxos.map((utxo) => ({
        address: utxo.address,
        blockHash: utxo.blockHash,
        blockHeight: utxo.blockHeight,
        blockTime: utxo.blockTime,
        outputKey: utxo.outputKey,
        scriptType: utxo.scriptType,
        spentByTxid: utxo.spentByTxid,
        spentInBlock: utxo.spentInBlock,
        txid: utxo.txid,
        txIndex: utxo.txIndex,
        valueBase: utxo.valueBase,
        vout: utxo.vout,
      })),
    };
  }

  private async resolveDogecoin(): Promise<ExplorerDogecoinRef> {
    const dogecoin = await this.dogecoin.getDogecoinConfig();
    if (!dogecoin) {
      throw new NotFoundError('dogecoin config not found');
    }

    return {
      architecture: 'dogecoin',
      rpcEndpoint: dogecoin.rpcEndpoint,
      rps: dogecoin.rps,
    };
  }

  private async configNumberOrDefault(key: string, fallback: number): Promise<number> {
    const value = await this.configs.getJsonValue<number>(key);
    return value ?? fallback;
  }

  private async assertHistoryReady(): Promise<void> {
    if (!(await this.configs.canReadDogecoinHistory())) {
      throw new TooEarlyError('dogecoin history index is not ready');
    }
  }

  private async listAppliedBlocks(
    offset?: number,
    limit?: number,
  ): Promise<{ blocks: ExplorerBlockSummary[] }> {
    const refs = await this.warehouse.listAppliedBlocks(offset, limit ?? 20);
    const blocks = await Promise.all(
      refs.map(async (ref) => this.getBlockByHeight(ref.blockHeight)),
    );

    return { blocks: blocks.filter((block): block is ExplorerBlockSummary => Boolean(block)) };
  }

  private async searchTransaction(q: string): Promise<ExplorerSearchResult | null> {
    const txRef = await this.getTransactionRef(q);
    if (!txRef) {
      return null;
    }

    return {
      type: 'transaction',
      txid: q,
      blockHeight: txRef.blockHeight,
      blockHash: txRef.blockHash,
      blockTime: txRef.blockTime,
    };
  }

  private async searchBlockHash(q: string): Promise<ExplorerSearchResult | null> {
    const blockRef = await this.warehouse.getAppliedBlockByHash(q);
    if (!blockRef) {
      return null;
    }

    const block = await this.getBlockByHeight(blockRef.blockHeight);
    return block ? blockSearchResult(block) : null;
  }

  private async searchAddress(q: string): Promise<ExplorerSearchResult | null> {
    const summary = await this.warehouse.getAddressSummary(q);
    return addressSearchResult(q, summary);
  }

  private async getCachedMempoolSnapshot(
    dogecoin: ExplorerDogecoinRef,
  ): Promise<CachedMempoolSnapshot> {
    const now = Date.now();
    const cached = this.mempoolSnapshots.get('dogecoin');
    if (isFreshMempoolCacheEntry(cached, now)) {
      return cached.promise;
    }

    const promise = this.loadMempoolSnapshot(dogecoin).catch((error) => {
      this.deleteMempoolSnapshotIfCurrent(promise);
      throw error;
    });
    this.mempoolSnapshots.set('dogecoin', {
      expiresAtMs: now + mempoolCacheTtlMs,
      promise,
    });

    return promise;
  }

  private deleteMempoolSnapshotIfCurrent(promise: Promise<CachedMempoolSnapshot>): void {
    const cached = this.mempoolSnapshots.get('dogecoin');
    if (isCurrentMempoolPromise(cached, promise)) {
      this.mempoolSnapshots.delete('dogecoin');
    }
  }

  private async loadMempoolSnapshot(dogecoin: ExplorerDogecoinRef): Promise<CachedMempoolSnapshot> {
    const snapshot = await this.mempoolRpc.getMempoolSnapshot(dogecoin);
    const transactions = Object.entries(snapshot.entries)
      .map(([txid, entry]) => this.serializeMempoolTransaction(txid, entry))
      .sort(compareMempoolTransactions);

    return {
      ...this.serializeMempoolInfo(snapshot.info),
      fetchedAt: snapshot.fetchedAt,
      totalCount: transactions.length,
      transactions,
    };
  }

  private serializeMempoolInfo(
    info: Record<string, unknown>,
  ): Pick<
    CachedMempoolSnapshot,
    | 'bytes'
    | 'maxMempoolBytes'
    | 'mempoolMinFeeBasePerKilobyte'
    | 'minRelayFeeBasePerKilobyte'
    | 'usageBytes'
  > {
    return {
      bytes: readNonNegativeInteger(info.bytes),
      usageBytes: readNonNegativeInteger(info.usage),
      maxMempoolBytes: readNonNegativeInteger(info.maxmempool),
      mempoolMinFeeBasePerKilobyte: readDecimalAmountBase(info.mempoolminfee),
      minRelayFeeBasePerKilobyte: readDecimalAmountBase(info.minrelaytxfee),
    };
  }

  private serializeMempoolTransaction(
    txid: string,
    entry: Record<string, unknown>,
  ): ExplorerMempoolTransaction {
    const fees = readOptionalRecord(entry.fees);
    const sizeBytes = readMempoolSizeBytes(entry);
    const feeBase = readMempoolAmount(entry, fees, 'fee', 'base');

    return {
      txid,
      time: readNonNegativeInteger(entry.time),
      height: readNonNegativeInteger(entry.height),
      sizeBytes,
      feeBase,
      modifiedFeeBase: readMempoolAmount(entry, fees, 'modifiedfee', 'modified'),
      feeRateBasePerKilobyte: feeRateBasePerKilobyte(feeBase, sizeBytes),
      ancestorCount: readNonNegativeInteger(entry.ancestorcount),
      ancestorSizeBytes: readNonNegativeInteger(entry.ancestorsize),
      ancestorFeesBase: readMempoolAmount(entry, fees, 'ancestorfees', 'ancestor'),
      descendantCount: readNonNegativeInteger(entry.descendantcount),
      descendantSizeBytes: readNonNegativeInteger(entry.descendantsize),
      descendantFeesBase: readMempoolAmount(entry, fees, 'descendantfees', 'descendant'),
      depends: readStringArray(entry.depends),
    };
  }

  private async requireTransactionRef(txid: string) {
    const txRef = await this.getTransactionRef(txid);
    if (!txRef) {
      throw new NotFoundError('transaction not found');
    }

    return txRef;
  }

  private async getTransactionRef(txid: string) {
    return (await this.warehouse.getTransactionRef(txid)) ?? this.findRawTransactionRef(txid);
  }

  private async findRawTransactionRef(txid: string) {
    const syncTail = await this.configNumberOrDefault(configKeyIndexerSyncTail(), -1);
    if (syncTail < 0) {
      return null;
    }

    const processTail = await this.configNumberOrDefault(configKeyIndexerProcessTail(), -1);
    const earliestHeight = Math.max(
      0,
      processTail + 1,
      syncTail - rawTransactionRefLookupLimit + 1,
    );

    for (let blockHeight = syncTail; blockHeight >= earliestHeight; blockHeight -= 1) {
      const snapshot = await this.rawBlocks.getPart<Record<string, unknown>>(blockHeight, 'block');
      if (!snapshot) {
        continue;
      }

      const block = this.parseBlock(snapshot);
      const txIndex = block.tx.findIndex((candidate) => this.readString(candidate.txid) === txid);
      if (txIndex >= 0) {
        return {
          blockHash: block.hash,
          blockHeight: block.height,
          blockTime: block.time,
          txIndex,
        };
      }
    }

    return null;
  }

  private requireTransaction(block: ParsedDogecoinBlock, txid: string): DogecoinTransaction {
    const transaction = block.tx.find((candidate) => this.readString(candidate.txid) === txid);
    if (!transaction) {
      throw new NotFoundError('transaction not found');
    }

    return transaction;
  }

  private serializeTransactionInputs(
    transaction: DogecoinTransaction,
    inputMap: Map<string, ProjectionUtxoOutput>,
    addresses: Set<string>,
  ): ExplorerTransactionInput[] {
    return this.readInputs(transaction.vin).flatMap((input) =>
      this.serializeTransactionInput(input, inputMap, addresses),
    );
  }

  private serializeTransactionInput(
    input: DogecoinVin,
    inputMap: Map<string, ProjectionUtxoOutput>,
    addresses: Set<string>,
  ): ExplorerTransactionInput[] {
    if (input.coinbase) {
      return [];
    }

    const outputKey = `${this.requireString(input.txid, 'vin.txid')}:${this.requireNumber(input.vout, 'vin.vout')}`;
    const resolved = inputMap.get(outputKey);
    if (!hasResolvedAddress(resolved)) {
      return [];
    }

    addresses.add(resolved.address);
    return [
      {
        address: resolved.address,
        outputKey,
        valueBase: resolved.valueBase,
      },
    ];
  }

  private serializeTransactionOutputs(
    txid: string,
    outputs: DogecoinVout[],
    currentOutputs: Map<string, ProjectionUtxoOutput>,
    addresses: Set<string>,
  ): ExplorerTransactionOutput[] {
    return outputs.map((output, index) =>
      this.serializeTransactionOutput(txid, output, index, currentOutputs, addresses),
    );
  }

  private serializeTransactionOutput(
    txid: string,
    output: DogecoinVout,
    index: number,
    currentOutputs: Map<string, ProjectionUtxoOutput>,
    addresses: Set<string>,
  ): ExplorerTransactionOutput {
    const outputKey = `${txid}:${index}`;
    const current = currentOutputs.get(outputKey);
    const address = extractDogecoinOutputAddress(output);
    if (address) {
      addresses.add(address);
    }

    return {
      address,
      vout: this.requireNumber(outputIndex(output, index), 'vout.n'),
      outputKey,
      valueBase: this.requireAmountBase(output.value),
      scriptType: outputScriptType(output),
      isSpendable: Boolean(address),
      spentByTxid: spentByTxid(current),
      spentInBlock: spentInBlock(current),
    };
  }

  private addressTransactionSummary(
    row: {
      blockHeight: number;
      receivedBase: string;
      sentBase: string;
      txid: string;
    },
    snapshotsByHeight: Map<number, ParsedDogecoinBlock>,
  ): ExplorerAddressTransactionSummary[] {
    const block = snapshotsByHeight.get(row.blockHeight);
    if (!block) {
      return [];
    }

    const txIndex = block.tx.findIndex((candidate) => this.readString(candidate.txid) === row.txid);
    if (txIndex < 0) {
      return [];
    }

    return [
      {
        transaction: this.serializeTransactionSummary(
          block,
          requireDogecoinTransactionAt(block, txIndex),
          txIndex,
        ),
        receivedBase: row.receivedBase,
        sentBase: row.sentBase,
      },
    ];
  }

  private async resolveBlockSnapshot(ref: string): Promise<ParsedDogecoinBlock> {
    const normalized = ref.trim();
    if (/^\d+$/u.test(normalized)) {
      return this.loadBlockSnapshot(Number(normalized));
    }

    const blockRef =
      (await this.warehouse.getAppliedBlockByHash(normalized)) ??
      (await this.coreBlocks.getCoreBlockByHash(normalized));
    if (!blockRef) {
      throw new NotFoundError('block not found');
    }

    return this.loadBlockSnapshot(blockRef.blockHeight);
  }

  private async getBlockByHeight(blockHeight: number): Promise<ExplorerBlockSummary | null> {
    const snapshot = await this.rawBlocks.getPart<Record<string, unknown>>(blockHeight, 'block');
    if (!snapshot) {
      return null;
    }

    return this.serializeBlock(this.parseBlock(snapshot));
  }

  private async loadBlockSnapshot(blockHeight: number): Promise<ParsedDogecoinBlock> {
    const snapshot = await this.rawBlocks.getPart<Record<string, unknown>>(blockHeight, 'block');
    if (!snapshot) {
      throw new NotFoundError('block not found');
    }

    return this.parseBlock(snapshot);
  }

  private async loadSnapshotsByHeight(
    heights: number[],
  ): Promise<Map<number, ParsedDogecoinBlock>> {
    const snapshots = await Promise.all(
      heights.map(
        async (height) =>
          [height, await this.rawBlocks.getPart<Record<string, unknown>>(height, 'block')] as const,
      ),
    );

    return new Map(
      snapshots
        .filter((entry): entry is readonly [number, Record<string, unknown>] => Boolean(entry[1]))
        .map(([height, snapshot]) => [height, this.parseBlock(snapshot)]),
    );
  }

  private serializeBlock(block: ParsedDogecoinBlock): ExplorerBlockSummary {
    return {
      hash: block.hash,
      height: block.height,
      time: block.time,
      txCount: block.tx.length,
    };
  }

  private async loadResolvedInputs(
    transactions: DogecoinTransaction[],
  ): Promise<Map<string, ProjectionUtxoOutput>> {
    const outputKeys = [
      ...new Set(
        transactions.flatMap((transaction) =>
          this.readInputs(transaction.vin)
            .filter((input) => !input.coinbase)
            .map(
              (input) =>
                `${this.requireString(input.txid, 'vin.txid')}:${this.requireNumber(input.vout, 'vin.vout')}`,
            ),
        ),
      ),
    ];

    return this.warehouse.getUtxoOutputs(outputKeys);
  }

  private serializeTransactionSummary(
    block: ParsedDogecoinBlock,
    transaction: DogecoinTransaction,
    txIndex: number,
    resolvedInputs?: Map<string, ProjectionUtxoOutput>,
  ): ExplorerTransactionSummary {
    const txid = this.requireString(transaction.txid, 'tx.txid');
    const inputs = this.readInputs(transaction.vin);
    const outputs = this.readOutputs(transaction.vout);
    const isCoinbase = inputs.some((input) => Boolean(input.coinbase));
    const totalInput = this.totalResolvedInputBase(inputs, resolvedInputs);
    const totalOutput = this.totalOutputBase(outputs);

    return {
      txid,
      txIndex,
      blockHeight: block.height,
      blockHash: block.hash,
      blockTime: block.time,
      isCoinbase,
      inputCount: inputs.length,
      outputCount: outputs.length,
      totalInputBase: formatAmountBase(totalInput),
      totalOutputBase: formatAmountBase(totalOutput),
      feeBase: transactionFeeBase(isCoinbase, totalInput, totalOutput),
    };
  }

  private totalResolvedInputBase(
    inputs: DogecoinVin[],
    resolvedInputs?: Map<string, ProjectionUtxoOutput>,
  ): bigint {
    let totalInput = 0n;
    for (const input of inputs) {
      totalInput += this.resolvedInputValue(input, resolvedInputs);
    }

    return totalInput;
  }

  private resolvedInputValue(
    input: DogecoinVin,
    resolvedInputs?: Map<string, ProjectionUtxoOutput>,
  ): bigint {
    if (input.coinbase) {
      return 0n;
    }

    const outputKey = `${this.requireString(input.txid, 'vin.txid')}:${this.requireNumber(input.vout, 'vin.vout')}`;
    const resolved = resolvedInputs?.get(outputKey);
    return resolvedInputAmount(resolved);
  }

  private totalOutputBase(outputs: DogecoinVout[]): bigint {
    return outputs.reduce(
      (sum, output) => sum + parseAmountBase(this.requireAmountBase(output.value)),
      0n,
    );
  }

  private parseBlock(snapshot: Record<string, unknown>): ParsedDogecoinBlock {
    const candidate = this.requireRecord(snapshot.block, 'block');
    return {
      hash: this.requireString(this.readString(candidate.hash), 'block.hash'),
      height: this.requireNumber(this.readNumber(candidate.height), 'block.height'),
      time: this.requireNumber(this.readNumber(candidate.time), 'block.time'),
      tx: Array.isArray(candidate.tx) ? candidate.tx.filter(isDogecoinTransaction) : [],
    };
  }

  private readInputs(value: DogecoinTransaction['vin']): DogecoinVin[] {
    return Array.isArray(value) ? value : [];
  }

  private readOutputs(value: DogecoinTransaction['vout']): DogecoinVout[] {
    return Array.isArray(value) ? value : [];
  }

  private requireAmountBase(value: number | string | undefined): string {
    if (value === undefined) {
      throw new ValidationError('missing output value');
    }

    return typeof value === 'number'
      ? decimalPartsToBase(value.toFixed(8).split('.'))
      : stringDecimalToBase(value);
  }

  private requireNumber(value: number | undefined, field: string): number {
    if (!isNonNegativeInteger(value)) {
      throw new ValidationError(`invalid parameter for \`${field}\`: ${invalidNumberValue(value)}`);
    }

    return value;
  }

  private requireRecord(value: unknown, field: string): Record<string, unknown> {
    if (!isPlainRecord(value)) {
      throw new ValidationError(`invalid parameter for \`${field}\`: `);
    }

    return Object.fromEntries(Object.entries(value));
  }

  private requireString(value: string | undefined, field: string): string {
    const trimmed = value?.trim();
    if (!trimmed) {
      throw new ValidationError(`invalid parameter for \`${field}\`: `);
    }

    return trimmed;
  }

  private readNumber(value: unknown): number | undefined {
    return typeof value === 'number' ? value : undefined;
  }

  private readString(value: unknown): string | undefined {
    return typeof value === 'string' ? value : undefined;
  }
}

function normalizeRequiredQuery(query: string | undefined): string {
  const q = query?.trim() ?? '';
  if (!q) {
    throw new ValidationError('missing input params');
  }

  return q;
}

function descendingBlockHeights(tail: number, offset: number, limit: number): number[] {
  if (tail < 0 || limit <= 0) {
    return [];
  }

  const start = tail - offset;
  if (start < 0) {
    return [];
  }

  const count = Math.min(limit, start + 1);
  return Array.from({ length: count }, (_value, index) => start - index);
}

function isExplorerSearchResult(
  result: ExplorerSearchResult | null,
): result is ExplorerSearchResult {
  return result !== null;
}

function blockSearchResult(block: ExplorerBlockSummary): ExplorerSearchResult {
  return {
    type: 'block',
    blockHeight: block.height,
    blockHash: block.hash,
    blockTime: block.time,
  };
}

function mempoolPage(
  offset: number | undefined,
  limit: number | undefined,
): { limit: number; offset: number } {
  return {
    offset: offset ?? 0,
    limit: Math.min(limit ?? defaultMempoolLimit, maxMempoolLimit),
  };
}

function defaultAddressPageLimit(limit: number | undefined): number {
  return limit ?? 50;
}

function requireExplorerAddress(address: string): string {
  const normalizedAddress = address.trim();
  if (!normalizedAddress) {
    throw new ValidationError('missing input params');
  }

  return normalizedAddress;
}

function isFreshMempoolCacheEntry(
  cached: MempoolCacheEntry | undefined,
  now: number,
): cached is MempoolCacheEntry {
  return cached !== undefined && cached.expiresAtMs > now;
}

function isCurrentMempoolPromise(
  cached: MempoolCacheEntry | undefined,
  promise: Promise<CachedMempoolSnapshot>,
): cached is MempoolCacheEntry {
  return cached !== undefined && cached.promise === promise;
}

function hasResolvedAddress(
  resolved: ProjectionUtxoOutput | undefined,
): resolved is ProjectionUtxoOutput & { address: string } {
  return resolved !== undefined && resolved.address !== '';
}

function resolvedInputAmount(resolved: ProjectionUtxoOutput | undefined): bigint {
  return resolved ? parseAmountBase(resolved.valueBase) : 0n;
}

function compareAddressTransactionSummaries(
  left: ExplorerAddressTransactionSummary,
  right: ExplorerAddressTransactionSummary,
): number {
  return firstNonZero([
    right.transaction.blockHeight - left.transaction.blockHeight,
    right.transaction.txIndex - left.transaction.txIndex,
    right.transaction.txid.localeCompare(left.transaction.txid),
  ]);
}

function firstNonZero(values: number[]): number {
  return values.find((value) => value !== 0) ?? 0;
}

function requireDogecoinTransactionAt(
  block: ParsedDogecoinBlock,
  txIndex: number,
): DogecoinTransaction {
  const transaction = block.tx[txIndex];
  if (!transaction) {
    throw new NotFoundError('transaction not found');
  }

  return transaction;
}

function assertAddressExists(summary: unknown): void {
  if (summary) {
    return;
  }

  throw new NotFoundError('address not found');
}

function transactionFeeBase(
  isCoinbase: boolean,
  totalInput: bigint,
  totalOutput: bigint,
): string | null {
  if (isCoinbase || totalInput === 0n) {
    return null;
  }

  return formatAmountBase(totalInput - totalOutput);
}

function stringDecimalToBase(value: string): string {
  const [whole = '', fraction = ''] = value.trim().split('.');
  assertDecimalParts(value, whole, fraction);
  return decimalPartsToBase([whole, fraction.slice(0, 8)]);
}

function decimalPartsToBase(parts: string[]): string {
  const [whole = '', fraction = ''] = parts;
  return `${whole}${fraction.padEnd(8, '0')}`.replace(/^0+(?=\d)/u, '') || '0';
}

function assertDecimalParts(raw: string, whole: string, fraction: string): void {
  if (!/^\d+$/u.test(whole) || !/^\d*$/u.test(fraction)) {
    throw new ValidationError(`invalid decimal amount: ${raw}`);
  }
}

function isNonNegativeInteger(value: number | undefined): value is number {
  return value !== undefined && Number.isInteger(value) && value >= 0;
}

function invalidNumberValue(value: number | undefined): string {
  return value === undefined ? '' : String(value);
}

function compareMempoolTransactions(
  left: ExplorerMempoolTransaction,
  right: ExplorerMempoolTransaction,
): number {
  return firstNonZero([
    mempoolTime(right) - mempoolTime(left),
    left.txid.localeCompare(right.txid),
  ]);
}

function mempoolTime(transaction: ExplorerMempoolTransaction): number {
  return transaction.time ?? -1;
}

function readNonNegativeInteger(value: unknown): number | null {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0 ? value : null;
}

function readOptionalRecord(value: unknown): Record<string, unknown> | null {
  if (!isPlainRecord(value)) {
    return null;
  }

  return Object.fromEntries(Object.entries(value));
}

function readMempoolSizeBytes(entry: Record<string, unknown>): number | null {
  return readNonNegativeInteger(entry.size) ?? readNonNegativeInteger(entry.vsize);
}

function readMempoolAmount(
  entry: Record<string, unknown>,
  fees: Record<string, unknown> | null,
  entryKey: string,
  feeKey: string,
): string | null {
  return readDecimalAmountBase(readMempoolAmountValue(entry, fees, entryKey, feeKey));
}

function readMempoolAmountValue(
  entry: Record<string, unknown>,
  fees: Record<string, unknown> | null,
  entryKey: string,
  feeKey: string,
): unknown {
  if (entry[entryKey] !== undefined) {
    return entry[entryKey];
  }

  return fees?.[feeKey];
}

function readDecimalAmountBase(value: unknown): string | null {
  if (typeof value !== 'number' && typeof value !== 'string') {
    return null;
  }

  try {
    return fromDecimalUnits(value, 8);
  } catch {
    return null;
  }
}

function readStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value.filter((item): item is string => typeof item === 'string' && Boolean(item.trim()));
}

function feeRateBasePerKilobyte(feeBase: string | null, sizeBytes: number | null): string | null {
  if (!feeBase || !sizeBytes || sizeBytes <= 0) {
    return null;
  }

  return formatAmountBase((parseAmountBase(feeBase) * 1000n) / BigInt(sizeBytes));
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}
