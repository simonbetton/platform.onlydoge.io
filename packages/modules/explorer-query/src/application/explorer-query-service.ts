import type { AuthenticatedApiKey } from '@onlydoge/access-control';
import {
  type DogecoinTransaction,
  type DogecoinVin,
  type DogecoinVout,
  extractDogecoinOutputAddress,
  formatAmountBase,
  fromDecimalUnits,
  isDogecoinTransaction,
  type ParsedDogecoinBlock,
  type ProjectionUtxoOutput,
  type ProjectionWarehousePort,
  parseAmountBase,
} from '@onlydoge/indexing-pipeline';
import {
  buildInfoResponse,
  type InfoResponse,
  type InvestigationWarehousePort,
} from '@onlydoge/investigation-query';
import {
  NotFoundError,
  type PrimaryId,
  type RiskLevel,
  TooEarlyError,
  ValidationError,
} from '@onlydoge/shared-kernel';

import type {
  ExplorerActiveNetworkPort,
  ExplorerConfigPort,
  ExplorerMempoolRpcPort,
  ExplorerMetadataPort,
  ExplorerRawBlockPort,
  ExplorerWarehousePort,
} from '../contracts/ports';
import type {
  ExplorerAddressDetail,
  ExplorerAddressTransactionSummary,
  ExplorerAddressUtxo,
  ExplorerBlockSummary,
  ExplorerLabelRef,
  ExplorerMempoolResponse,
  ExplorerMempoolTransaction,
  ExplorerNetworkSummary,
  ExplorerSearchResult,
  ExplorerTransactionDetail,
  ExplorerTransactionInput,
  ExplorerTransactionOutput,
  ExplorerTransactionSummary,
} from '../domain/query-models';
import {
  addressDetail,
  addressSearchResult,
  buildExplorerTransferBasis,
  type ExplorerProjectedTransfer,
  outputIndex,
  outputScriptType,
  projectExplorerTransfers,
  spentByTxid,
  spentInBlock,
  type WarehouseAddressSummary,
  withTransactionLabels,
} from './explorer-response-builders';

export type ExplorerWarehouse = ExplorerWarehousePort &
  InvestigationWarehousePort &
  Pick<ProjectionWarehousePort, 'getUtxoOutputs'>;

type ExplorerNetworkRef = {
  architecture: 'dogecoin';
  id: string;
  name: string;
  networkId: PrimaryId;
  rpcEndpoint: string;
  rps: number;
};

type CachedMempoolSnapshot = Omit<
  ExplorerMempoolResponse,
  'limit' | 'network' | 'offset' | 'returnedCount'
>;

type MempoolCacheEntry = {
  expiresAtMs: number;
  promise: Promise<CachedMempoolSnapshot>;
};

const defaultMempoolLimit = 100;
const maxMempoolLimit = 500;
const mempoolCacheTtlMs = 1_000;

export class ExplorerQueryService {
  private readonly mempoolSnapshots = new Map<PrimaryId, MempoolCacheEntry>();

  public constructor(
    private readonly networks: ExplorerActiveNetworkPort,
    private readonly metadata: ExplorerMetadataPort,
    private readonly warehouse: ExplorerWarehouse,
    private readonly rawBlocks: ExplorerRawBlockPort,
    private readonly configs: ExplorerConfigPort,
    private readonly mempoolRpc: ExplorerMempoolRpcPort,
  ) {}

  public async listNetworks(): Promise<{
    networks: ExplorerNetworkSummary[];
  }> {
    const activeNetworks = (await this.networks.listActiveNetworks()).filter(
      (network) => network.architecture === 'dogecoin',
    );
    const isSingleNetwork = activeNetworks.length === 1;

    return {
      networks: await Promise.all(
        activeNetworks.map((network) => this.networkSummary(network, isSingleNetwork)),
      ),
    };
  }

  private async networkSummary(
    network: Awaited<ReturnType<ExplorerActiveNetworkPort['listActiveNetworks']>>[number],
    isSingleNetwork: boolean,
  ): Promise<ExplorerNetworkSummary> {
    return {
      id: network.id,
      name: network.name,
      chainId: network.chainId,
      blockTime: network.blockTime,
      blockHeight: await this.configNumberOrZero(`block_height_n${network.networkId}`),
      synced: await this.configNumberOrZero(`indexer_sync_progress_n${network.networkId}`),
      processed: await this.configNumberOrZero(`indexer_process_progress_n${network.networkId}`),
      isDefault: isSingleNetwork,
    };
  }

  private async configNumberOrZero(key: string): Promise<number> {
    const value = await this.configs.getJsonValue<number>(key);
    return value ?? 0;
  }

  public async search(
    actor: AuthenticatedApiKey,
    query: string | undefined,
    networkId?: string,
  ): Promise<{
    matches: ExplorerSearchResult[];
  }> {
    const q = normalizeRequiredQuery(query);
    const network = await this.resolveNetwork(networkId);
    await this.assertHistoryReady(network.networkId);

    if (/^\d+$/u.test(q)) {
      return { matches: await this.searchBlockHeight(network, Number(q)) };
    }

    return { matches: await this.searchNonNumeric(actor, network, q) };
  }

  private async searchBlockHeight(
    network: ExplorerNetworkRef,
    blockHeight: number,
  ): Promise<ExplorerSearchResult[]> {
    const block = await this.getBlockByHeight(network.networkId, network.id, blockHeight);
    return block ? [blockSearchResult(block)] : [];
  }

  private async searchNonNumeric(
    actor: AuthenticatedApiKey,
    network: ExplorerNetworkRef,
    q: string,
  ): Promise<ExplorerSearchResult[]> {
    const txMatch = await this.searchTransaction(network, q);
    const blockMatch = await this.searchBlockHash(network, q);
    const addressMatch = await this.searchAddress(actor, network, q);
    return [txMatch, blockMatch, addressMatch].filter(isExplorerSearchResult);
  }

  private async searchTransaction(
    network: ExplorerNetworkRef,
    q: string,
  ): Promise<ExplorerSearchResult | null> {
    const txRef = await this.warehouse.getTransactionRef(network.networkId, q);
    if (!txRef) {
      return null;
    }

    return {
      type: 'transaction',
      network: network.id,
      txid: q,
      blockHeight: txRef.blockHeight,
      blockHash: txRef.blockHash,
      blockTime: txRef.blockTime,
    };
  }

  private async searchBlockHash(
    network: ExplorerNetworkRef,
    q: string,
  ): Promise<ExplorerSearchResult | null> {
    const blockRef = await this.warehouse.getAppliedBlockByHash(network.networkId, q);
    if (!blockRef) {
      return null;
    }

    return this.searchResultForBlockHeight(network, blockRef.blockHeight);
  }

  private async searchResultForBlockHeight(
    network: ExplorerNetworkRef,
    blockHeight: number,
  ): Promise<ExplorerSearchResult | null> {
    const block = await this.getBlockByHeight(network.networkId, network.id, blockHeight);
    return blockSearchResultOrNull(block);
  }

  private async searchAddress(
    actor: AuthenticatedApiKey,
    network: ExplorerNetworkRef,
    q: string,
  ): Promise<ExplorerSearchResult | null> {
    const [summary, labelMap] = await Promise.all([
      this.warehouse.getAddressSummary(network.networkId, q),
      this.buildLabelMap(actor, network.networkId, [q]),
    ]);

    return addressSearchResult(network.id, q, summary, labelMap.get(q));
  }

  public async listBlocks(
    networkId?: string,
    offset?: number,
    limit?: number,
  ): Promise<{
    blocks: ExplorerBlockSummary[];
  }> {
    const network = await this.resolveNetwork(networkId);
    const refs = await this.warehouse.listAppliedBlocks(network.networkId, offset, limit ?? 20);
    const blocks = await Promise.all(
      refs.map(async (ref) =>
        this.getBlockByHeight(network.networkId, network.id, ref.blockHeight),
      ),
    );

    return {
      blocks: blocks.filter((block): block is ExplorerBlockSummary => Boolean(block)),
    };
  }

  public async listMempool(
    networkId?: string,
    offset?: number,
    limit?: number,
  ): Promise<ExplorerMempoolResponse> {
    const network = await this.resolveNetwork(networkId);
    const page = mempoolPage(offset, limit);
    const snapshot = await this.getCachedMempoolSnapshot(network);
    const transactions = snapshot.transactions.slice(page.offset, page.offset + page.limit);

    return {
      ...snapshot,
      network: network.id,
      offset: page.offset,
      limit: page.limit,
      returnedCount: transactions.length,
      transactions,
    };
  }

  private async getCachedMempoolSnapshot(
    network: ExplorerNetworkRef,
  ): Promise<CachedMempoolSnapshot> {
    const now = Date.now();
    const cached = this.mempoolSnapshots.get(network.networkId);
    if (isFreshMempoolCacheEntry(cached, now)) {
      return cached.promise;
    }

    const promise = this.loadMempoolSnapshot(network).catch((error) => {
      this.deleteMempoolSnapshotIfCurrent(network, promise);
      throw error;
    });
    this.mempoolSnapshots.set(network.networkId, {
      expiresAtMs: now + mempoolCacheTtlMs,
      promise,
    });

    return promise;
  }

  private deleteMempoolSnapshotIfCurrent(
    network: ExplorerNetworkRef,
    promise: Promise<CachedMempoolSnapshot>,
  ): void {
    const cached = this.mempoolSnapshots.get(network.networkId);
    if (isCurrentMempoolPromise(cached, promise)) {
      this.mempoolSnapshots.delete(network.networkId);
    }
  }

  private async loadMempoolSnapshot(network: ExplorerNetworkRef): Promise<CachedMempoolSnapshot> {
    const snapshot = await this.mempoolRpc.getMempoolSnapshot(network);
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

  public async getBlock(
    ref: string,
    networkId?: string,
  ): Promise<{
    block: ExplorerBlockSummary;
    transactions: ExplorerTransactionSummary[];
  }> {
    const network = await this.resolveNetwork(networkId);
    await this.assertHistoryReady(network.networkId);
    const parsed = await this.resolveBlockSnapshot(network.networkId, ref);
    const inputMap = await this.loadResolvedInputs(network.networkId, parsed.tx);

    return {
      block: this.serializeBlock(network.id, parsed),
      transactions: parsed.tx.map((transaction, txIndex) =>
        this.serializeTransactionSummary(network.id, parsed, transaction, txIndex, inputMap),
      ),
    };
  }

  public async getTransaction(
    actor: AuthenticatedApiKey,
    txid: string,
    networkId?: string,
  ): Promise<ExplorerTransactionDetail> {
    const normalizedTxid = txid.trim();
    const network = await this.resolveNetwork(networkId);
    await this.assertHistoryReady(network.networkId);
    const txRef = await this.requireTransactionRef(network.networkId, normalizedTxid);
    const block = await this.loadBlockSnapshot(network.networkId, txRef.blockHeight);
    const transaction = this.requireTransaction(block, normalizedTxid);
    const inputMap = await this.loadResolvedInputs(network.networkId, [transaction]);
    const summary = this.serializeTransactionSummary(
      network.id,
      block,
      transaction,
      txRef.txIndex,
      inputMap,
    );
    const outputs = this.readOutputs(transaction.vout);
    const outputKeys = outputs.map((_output, index) => `${normalizedTxid}:${index}`);
    const currentOutputs = await this.warehouse.getUtxoOutputs(network.networkId, outputKeys);
    const addresses = new Set<string>();
    const inputs = this.serializeTransactionInputs(transaction, inputMap, addresses);
    const serializedOutputs = this.serializeTransactionOutputs(
      normalizedTxid,
      outputs,
      currentOutputs,
      addresses,
    );

    const labelMap = await this.buildLabelMap(actor, network.networkId, [...addresses]);
    const labeled = withTransactionLabels(inputs, serializedOutputs, labelMap);

    const transfers = this.projectTransfers(summary, labeled.inputs, labeled.outputs);

    return {
      transaction: summary,
      inputs: labeled.inputs,
      outputs: labeled.outputs,
      transfers,
      overlay: {
        labels: [...new Map([...labelMap.values()].map((label) => [label.entity, label])).values()],
      },
    };
  }

  private async requireTransactionRef(networkId: PrimaryId, txid: string) {
    const txRef = await this.warehouse.getTransactionRef(networkId, txid);
    if (!txRef) {
      throw new NotFoundError('transaction not found');
    }

    return txRef;
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

    return this.resolvedTransactionInput(input, inputMap, addresses);
  }

  private resolvedTransactionInput(
    input: DogecoinVin,
    inputMap: Map<string, ProjectionUtxoOutput>,
    addresses: Set<string>,
  ): ExplorerTransactionInput[] {
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

  public async getAddress(
    actor: AuthenticatedApiKey,
    address: string,
    networkId?: string,
  ): Promise<ExplorerAddressDetail> {
    const normalizedAddress = address.trim();
    if (!normalizedAddress) {
      throw new ValidationError('missing input params');
    }

    const network = await this.resolveNetwork(networkId);
    const [summary, overlay, labeledRecord] = await Promise.all([
      this.warehouse.getAddressSummary(network.networkId, normalizedAddress),
      this.buildAddressOverlay(actor, network.networkId, normalizedAddress),
      this.findLabeledAddress(actor, network.networkId, normalizedAddress),
    ]);

    assertAddressExists(summary, labeledRecord);

    return {
      address: addressDetail(network.id, normalizedAddress, summary),
      overlay,
    };
  }

  private async findLabeledAddress(
    actor: AuthenticatedApiKey,
    networkId: PrimaryId,
    address: string,
  ) {
    return (await this.metadata.listAddressesByValues([address])).find(
      (candidate) => candidate.networkId === networkId && canReadOwner(candidate, actor),
    );
  }

  public async listAddressTransactions(
    address: string,
    networkId?: string,
    offset?: number,
    limit?: number,
  ): Promise<{
    transactions: ExplorerAddressTransactionSummary[];
  }> {
    const normalizedAddress = requireExplorerAddress(address);

    const network = await this.resolveNetwork(networkId);
    await this.assertHistoryReady(network.networkId);
    const rows = await this.warehouse.listAddressTransactions(
      network.networkId,
      normalizedAddress,
      offset,
      defaultAddressPageLimit(limit),
    );
    const snapshotsByHeight = await this.loadSnapshotsByHeight(network.networkId, [
      ...new Set(rows.map((row) => row.blockHeight)),
    ]);

    return {
      transactions: rows
        .flatMap((row) => this.addressTransactionSummary(network.id, row, snapshotsByHeight))
        .sort(compareAddressTransactionSummaries),
    };
  }

  private addressTransactionSummary(
    networkId: string,
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

    return this.addressTransactionSummaryForBlock(networkId, row, block);
  }

  private addressTransactionSummaryForBlock(
    networkId: string,
    row: {
      receivedBase: string;
      sentBase: string;
      txid: string;
    },
    block: ParsedDogecoinBlock,
  ): ExplorerAddressTransactionSummary[] {
    const txIndex = block.tx.findIndex((candidate) => this.readString(candidate.txid) === row.txid);
    if (txIndex < 0) {
      return [];
    }

    return [
      {
        transaction: this.serializeTransactionSummary(
          networkId,
          block,
          requireDogecoinTransactionAt(block, txIndex),
          txIndex,
        ),
        receivedBase: row.receivedBase,
        sentBase: row.sentBase,
      },
    ];
  }

  public async listAddressUtxos(
    address: string,
    networkId?: string,
    offset?: number,
    limit?: number,
  ): Promise<{
    utxos: ExplorerAddressUtxo[];
  }> {
    const normalizedAddress = requireExplorerAddress(address);

    const network = await this.resolveNetwork(networkId);
    const utxos = await this.warehouse.listAddressUtxos(
      network.networkId,
      normalizedAddress,
      offset,
      defaultAddressPageLimit(limit),
    );

    return {
      utxos: utxos.map((utxo) => ({
        network: network.id,
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

  private async resolveNetwork(networkId?: string): Promise<ExplorerNetworkRef> {
    const activeNetworks = (await this.networks.listActiveNetworks()).filter(
      (network) => network.architecture === 'dogecoin',
    );

    if (networkId) {
      return explorerNetworkRef(requireExplorerNetwork(activeNetworks, networkId));
    }

    return explorerNetworkRef(requireDefaultExplorerNetwork(activeNetworks));
  }

  private async assertHistoryReady(networkId: PrimaryId): Promise<void> {
    if (!(await this.configs.canReadDogecoinHistory(networkId))) {
      throw new TooEarlyError('dogecoin history index is not ready');
    }
  }

  private async resolveBlockSnapshot(
    networkId: PrimaryId,
    ref: string,
  ): Promise<ParsedDogecoinBlock> {
    const normalized = ref.trim();
    if (/^\d+$/u.test(normalized)) {
      return this.loadBlockSnapshot(networkId, Number(normalized));
    }

    return this.resolveBlockHashSnapshot(networkId, normalized);
  }

  private async resolveBlockHashSnapshot(
    networkId: PrimaryId,
    blockHash: string,
  ): Promise<ParsedDogecoinBlock> {
    const blockRef = await this.warehouse.getAppliedBlockByHash(networkId, blockHash);
    if (!blockRef) {
      throw new NotFoundError('block not found');
    }

    return this.loadBlockSnapshot(networkId, blockRef.blockHeight);
  }

  private async getBlockByHeight(
    networkId: PrimaryId,
    externalNetworkId: string,
    blockHeight: number,
  ): Promise<ExplorerBlockSummary | null> {
    const snapshot = await this.rawBlocks.getPart<Record<string, unknown>>(
      networkId,
      blockHeight,
      'block',
    );
    if (!snapshot) {
      return null;
    }

    return this.serializeBlock(externalNetworkId, this.parseBlock(snapshot));
  }

  private async loadBlockSnapshot(
    networkId: PrimaryId,
    blockHeight: number,
  ): Promise<ParsedDogecoinBlock> {
    const snapshot = await this.rawBlocks.getPart<Record<string, unknown>>(
      networkId,
      blockHeight,
      'block',
    );
    if (!snapshot) {
      throw new NotFoundError('block not found');
    }

    return this.parseBlock(snapshot);
  }

  private async loadSnapshotsByHeight(
    networkId: PrimaryId,
    heights: number[],
  ): Promise<Map<number, ParsedDogecoinBlock>> {
    const snapshots = await Promise.all(
      heights.map(
        async (height) =>
          [
            height,
            await this.rawBlocks.getPart<Record<string, unknown>>(networkId, height, 'block'),
          ] as const,
      ),
    );

    return new Map(
      snapshots
        .filter((entry): entry is readonly [number, Record<string, unknown>] => Boolean(entry[1]))
        .map(([height, snapshot]) => [height, this.parseBlock(snapshot)]),
    );
  }

  private serializeBlock(network: string, block: ParsedDogecoinBlock): ExplorerBlockSummary {
    return {
      network,
      hash: block.hash,
      height: block.height,
      time: block.time,
      txCount: block.tx.length,
    };
  }

  private async loadResolvedInputs(
    networkId: PrimaryId,
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

    return this.warehouse.getUtxoOutputs(networkId, outputKeys);
  }

  private serializeTransactionSummary(
    network: string,
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
      network,
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

    return this.resolvedNonCoinbaseInputValue(input, resolvedInputs);
  }

  private resolvedNonCoinbaseInputValue(
    input: DogecoinVin,
    resolvedInputs?: Map<string, ProjectionUtxoOutput>,
  ): bigint {
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

  private projectTransfers(
    summary: ExplorerTransactionSummary,
    inputs: ExplorerTransactionInput[],
    outputs: ExplorerTransactionOutput[],
  ): ExplorerProjectedTransfer[] {
    const basis = buildExplorerTransferBasis(summary, inputs, outputs);
    if (!basis) {
      return [];
    }

    return projectExplorerTransfers(outputs, basis);
  }

  private async buildLabelMap(
    actor: AuthenticatedApiKey,
    networkId: PrimaryId,
    addresses: string[],
  ): Promise<Map<string, ExplorerLabelRef>> {
    const addressRecords = (
      await this.metadata.listAddressesByValues([...new Set(addresses)])
    ).filter((address) => isReadableNetworkAddress(address, actor, networkId));
    if (addressRecords.length === 0) {
      return new Map();
    }

    const entityIds = [...new Set(addressRecords.map((address) => address.entityId))];
    const [entities, joinedTags] = await Promise.all([
      this.metadata.listEntitiesByIds(entityIds),
      this.metadata.listTagsByEntityIds(entityIds),
    ]);
    const entityById = new Map(entities.map((entity) => [entity.entityId, entity]));
    const tagsByEntityId = tagsByExplorerEntityId(joinedTags);

    return new Map(
      addressRecords.flatMap((record) => explorerLabelMapEntry(record, entityById, tagsByEntityId)),
    );
  }

  private async buildAddressOverlay(
    actor: AuthenticatedApiKey,
    networkId: PrimaryId,
    address: string,
  ): Promise<InfoResponse> {
    const balances = (await this.warehouse.getBalancesByAddresses([address])).filter(
      (balance) => balance.networkId === networkId,
    );
    const links = (await this.warehouse.getDistinctLinksByAddresses([address])).filter(
      (link) => link.networkId === networkId,
    );
    const addressRecords = (
      await this.metadata.listAddressesByValues([
        ...new Set([address, ...links.map((link) => link.fromAddress)]),
      ])
    ).filter((candidate) => candidate.networkId === networkId && canReadOwner(candidate, actor));
    const entityIds = [...new Set(addressRecords.map((candidate) => candidate.entityId))];
    const [entities, joinedTags, networks] = await Promise.all([
      this.metadata
        .listEntitiesByIds(entityIds)
        .then((records) => records.filter((record) => canReadOwner(record, actor))),
      this.metadata.listTagsByEntityIds(entityIds),
      this.metadata.listNetworksByInternalIds(
        addressRecords.map((candidate) => candidate.networkId),
      ),
    ]);

    const tokens = (
      await this.warehouse.getTokensByAddresses(
        balances.map((balance) => balance.assetAddress).filter(Boolean),
      )
    ).filter((token) => token.networkId === networkId);

    return buildInfoResponse({
      addresses: [address],
      addressRecords,
      balances,
      entities,
      joinedTags,
      links,
      networks,
      tokens,
    });
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

    return this.definedAmountBase(value);
  }

  private definedAmountBase(value: number | string): string {
    if (typeof value === 'number') {
      return decimalPartsToBase(value.toFixed(8).split('.'));
    }

    return stringDecimalToBase(value);
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
  const q = trimOptionalString(query);
  if (!q) {
    throw new ValidationError('missing input params');
  }

  return q;
}

function trimOptionalString(value: string | undefined): string {
  if (!value) {
    return '';
  }

  return value.trim();
}

function isExplorerSearchResult(
  result: ExplorerSearchResult | null,
): result is ExplorerSearchResult {
  return result !== null;
}

function blockSearchResultOrNull(block: ExplorerBlockSummary | null): ExplorerSearchResult | null {
  if (!block) {
    return null;
  }

  return blockSearchResult(block);
}

function mempoolPage(
  offset: number | undefined,
  limit: number | undefined,
): { limit: number; offset: number } {
  return {
    offset: optionalOffset(offset),
    limit: Math.min(optionalMempoolLimit(limit), maxMempoolLimit),
  };
}

function optionalOffset(offset: number | undefined): number {
  return offset ?? 0;
}

function optionalMempoolLimit(limit: number | undefined): number {
  return limit ?? defaultMempoolLimit;
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
  if (!cached) {
    return false;
  }

  return cached.expiresAtMs > now;
}

function isCurrentMempoolPromise(
  cached: MempoolCacheEntry | undefined,
  promise: Promise<CachedMempoolSnapshot>,
): cached is MempoolCacheEntry {
  if (!cached) {
    return false;
  }

  return cached.promise === promise;
}

function hasResolvedAddress(
  resolved: ProjectionUtxoOutput | undefined,
): resolved is ProjectionUtxoOutput & { address: string } {
  if (!resolved) {
    return false;
  }

  return resolved.address !== '';
}

function resolvedInputAmount(resolved: ProjectionUtxoOutput | undefined): bigint {
  if (!resolved) {
    return 0n;
  }

  return parseAmountBase(resolved.valueBase);
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

function isReadableNetworkAddress(
  address: { networkId: PrimaryId; ownerApiKeyId: PrimaryId },
  actor: AuthenticatedApiKey,
  networkId: PrimaryId,
): boolean {
  return [address.networkId === networkId, canReadOwner(address, actor)].every(Boolean);
}

function tagsByExplorerEntityId(
  joinedTags: Array<{ entityId: PrimaryId; id: string; riskLevel: RiskLevel }>,
): Map<PrimaryId, Array<{ id: string; riskLevel: RiskLevel }>> {
  const tagsByEntityId = new Map<PrimaryId, Array<{ id: string; riskLevel: RiskLevel }>>();
  for (const tag of joinedTags) {
    addExplorerTag(tagsByEntityId, tag);
  }

  return tagsByEntityId;
}

function addExplorerTag(
  tagsByEntityId: Map<PrimaryId, Array<{ id: string; riskLevel: RiskLevel }>>,
  tag: { entityId: PrimaryId; id: string; riskLevel: RiskLevel },
): void {
  const current = tagsByEntityId.get(tag.entityId);
  if (current) {
    current.push({ id: tag.id, riskLevel: tag.riskLevel });
    return;
  }

  tagsByEntityId.set(tag.entityId, [{ id: tag.id, riskLevel: tag.riskLevel }]);
}

function explorerLabelMapEntry(
  record: { address: string; entityId: PrimaryId },
  entityById: Map<PrimaryId, { id: string; name: string | null }>,
  tagsByEntityId: Map<PrimaryId, Array<{ id: string; riskLevel: RiskLevel }>>,
): Array<readonly [string, ExplorerLabelRef]> {
  const entity = entityById.get(record.entityId);
  if (!entity) {
    return [];
  }

  return [
    [record.address, explorerLabelRef(entity, tagsForEntity(tagsByEntityId, record.entityId))],
  ];
}

function tagsForEntity(
  tagsByEntityId: Map<PrimaryId, Array<{ id: string; riskLevel: RiskLevel }>>,
  entityId: PrimaryId,
): Array<{ id: string; riskLevel: RiskLevel }> {
  const tags = tagsByEntityId.get(entityId);
  if (!tags) {
    return [];
  }

  return tags;
}

function explorerLabelRef(
  entity: { id: string; name: string | null },
  tags: Array<{ id: string; riskLevel: RiskLevel }>,
): ExplorerLabelRef {
  return {
    entity: entity.id,
    name: entity.name,
    tags: tags.map((tag) => tag.id),
    riskLevel: tags.some((tag) => tag.riskLevel === 'high') ? 'high' : 'low',
  };
}

function blockSearchResult(block: ExplorerBlockSummary): ExplorerSearchResult {
  return {
    type: 'block',
    network: block.network,
    blockHeight: block.height,
    blockHash: block.hash,
    blockTime: block.time,
  };
}

function requireExplorerNetwork(
  networks: Array<ExplorerNetworkRef & { architecture?: string }>,
  networkId: string,
): ExplorerNetworkRef {
  const network = networks.find((candidate) => candidate.id === networkId);
  if (!network) {
    throw new ValidationError(`invalid parameter for \`network\`: ${networkId}`);
  }

  return network;
}

function requireDefaultExplorerNetwork(
  networks: Array<ExplorerNetworkRef & { architecture?: string }>,
): ExplorerNetworkRef {
  assertExplorerNetworkExists(networks);
  assertSingleExplorerNetwork(networks);
  return networks[0] as ExplorerNetworkRef;
}

function assertExplorerNetworkExists(
  networks: Array<ExplorerNetworkRef & { architecture?: string }>,
): void {
  if (networks.length === 0) {
    throw new NotFoundError('dogecoin network not found');
  }
}

function assertSingleExplorerNetwork(
  networks: Array<ExplorerNetworkRef & { architecture?: string }>,
): void {
  if (networks.length > 1) {
    throw new ValidationError('missing parameter for `network`');
  }
}

function explorerNetworkRef(network: ExplorerNetworkRef): ExplorerNetworkRef {
  return {
    architecture: network.architecture,
    id: network.id,
    name: network.name,
    networkId: network.networkId,
    rpcEndpoint: network.rpcEndpoint,
    rps: network.rps,
  };
}

function assertAddressExists(
  summary: WarehouseAddressSummary | null,
  labeledRecord: unknown,
): void {
  if (hasAddressEvidence(summary, labeledRecord)) {
    return;
  }

  throw new NotFoundError('address not found');
}

function hasAddressEvidence(
  summary: WarehouseAddressSummary | null,
  labeledRecord: unknown,
): boolean {
  return [Boolean(summary), Boolean(labeledRecord)].includes(true);
}

function canReadOwner(record: { ownerApiKeyId: PrimaryId }, actor: AuthenticatedApiKey): boolean {
  return [actor.role === 'admin', record.ownerApiKeyId === actor.apiKeyId].includes(true);
}

function transactionFeeBase(
  isCoinbase: boolean,
  totalInput: bigint,
  totalOutput: bigint,
): string | null {
  if (isCoinbase) {
    return null;
  }

  return nonCoinbaseTransactionFeeBase(totalInput, totalOutput);
}

function nonCoinbaseTransactionFeeBase(totalInput: bigint, totalOutput: bigint): string | null {
  if (totalInput === 0n) {
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
  assertDecimalWholePart(raw, whole);
  assertDecimalFractionPart(raw, fraction);
}

function assertDecimalWholePart(raw: string, whole: string): void {
  if (!/^\d+$/u.test(whole)) {
    throw new ValidationError(`invalid decimal amount: ${raw}`);
  }
}

function assertDecimalFractionPart(raw: string, fraction: string): void {
  if (!/^\d*$/u.test(fraction)) {
    throw new ValidationError(`invalid decimal amount: ${raw}`);
  }
}

function isNonNegativeInteger(value: number | undefined): value is number {
  if (value === undefined) {
    return false;
  }

  return isNonNegativeIntegerValue(value);
}

function isNonNegativeIntegerValue(value: number): boolean {
  return [Number.isInteger(value), value >= 0].every(Boolean);
}

function invalidNumberValue(value: number | undefined): string {
  if (value === undefined) {
    return '';
  }

  return String(value);
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
  if (typeof value !== 'number') {
    return null;
  }

  return nonNegativeIntegerOrNull(value);
}

function nonNegativeIntegerOrNull(value: number): number | null {
  if (!isNonNegativeIntegerValue(value)) {
    return null;
  }

  return value;
}

function readOptionalRecord(value: unknown): Record<string, unknown> | null {
  if (!isPlainRecord(value)) {
    return null;
  }

  return Object.fromEntries(Object.entries(value));
}

function readMempoolSizeBytes(entry: Record<string, unknown>): number | null {
  const size = readNonNegativeInteger(entry.size);
  if (size !== null) {
    return size;
  }

  return readNonNegativeInteger(entry.vsize);
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

  return readMempoolFeeValue(fees, feeKey);
}

function readMempoolFeeValue(fees: Record<string, unknown> | null, feeKey: string): unknown {
  if (!fees) {
    return undefined;
  }

  return fees[feeKey];
}

function readDecimalAmountBase(value: unknown): string | null {
  if (!isDecimalAmountInput(value)) {
    return null;
  }

  return decimalAmountBaseOrNull(value);
}

function decimalAmountBaseOrNull(value: number | string): string | null {
  try {
    return fromDecimalUnits(value, 8);
  } catch {
    return null;
  }
}

function isDecimalAmountInput(value: unknown): value is number | string {
  return [typeof value === 'number', typeof value === 'string'].includes(true);
}

function readStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value.filter((item): item is string => typeof item === 'string' && Boolean(item.trim()));
}

function feeRateBasePerKilobyte(feeBase: string | null, sizeBytes: number | null): string | null {
  if (!feeBase) {
    return null;
  }

  return feeRateForSizedTransaction(feeBase, sizeBytes);
}

function feeRateForSizedTransaction(feeBase: string, sizeBytes: number | null): string | null {
  if (!isPositiveSizeBytes(sizeBytes)) {
    return null;
  }

  return formatAmountBase((parseAmountBase(feeBase) * 1000n) / BigInt(sizeBytes));
}

function isPositiveSizeBytes(sizeBytes: number | null): sizeBytes is number {
  if (sizeBytes === null) {
    return false;
  }

  return sizeBytes > 0;
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return [Object(value) === value, !Array.isArray(value)].every(Boolean);
}
