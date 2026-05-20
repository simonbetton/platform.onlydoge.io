import {
  type DogecoinTransaction,
  type DogecoinVin,
  type DogecoinVout,
  extractDogecoinOutputAddress,
  formatAmountBase,
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
  ExplorerMetadataPort,
  ExplorerRawBlockPort,
  ExplorerWarehousePort,
} from '../contracts/ports';
import type {
  ExplorerAddressDetail,
  ExplorerAddressTransactionSummary,
  ExplorerAddressUtxo,
  ExplorerBlockSummary,
  ExplorerHdWalletBalance,
  ExplorerLabelRef,
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
import {
  hdWalletCacheKey,
  normalizeHdWalletRequest,
  scanHdWalletBalance,
} from './hd-wallet-balance';

export type ExplorerWarehouse = ExplorerWarehousePort &
  InvestigationWarehousePort &
  Pick<ProjectionWarehousePort, 'getUtxoOutputs'>;

type HdWalletBalanceInput = {
  chains?: number[];
  gapLimit?: number;
  network?: string;
  xpub: string;
};

type HdWalletBalanceCacheEntry = {
  expiresAtMs: number;
  result: Omit<ExplorerHdWalletBalance, 'cache'>;
};

const hdWalletBalanceCacheTtlMs = 60_000;

type ExplorerNetworkRef = {
  id: string;
  name: string;
  networkId: PrimaryId;
};

export class ExplorerQueryService {
  public constructor(
    private readonly networks: ExplorerActiveNetworkPort,
    private readonly metadata: ExplorerMetadataPort,
    private readonly warehouse: ExplorerWarehouse,
    private readonly rawBlocks: ExplorerRawBlockPort,
    private readonly configs: ExplorerConfigPort,
  ) {}

  private readonly hdWalletBalanceCache = new Map<string, HdWalletBalanceCacheEntry>();

  public async listNetworks(): Promise<{
    networks: ExplorerNetworkSummary[];
  }> {
    const activeNetworks = (await this.networks.listActiveNetworks()).filter(
      (network) => network.architecture === 'dogecoin',
    );
    const isSingleNetwork = activeNetworks.length === 1;

    return {
      networks: await Promise.all(
        activeNetworks.map(async (network) => ({
          id: network.id,
          name: network.name,
          chainId: network.chainId,
          blockTime: network.blockTime,
          blockHeight:
            (await this.configs.getJsonValue<number>(`block_height_n${network.networkId}`)) ?? 0,
          synced:
            (await this.configs.getJsonValue<number>(
              `indexer_sync_progress_n${network.networkId}`,
            )) ?? 0,
          processed:
            (await this.configs.getJsonValue<number>(
              `indexer_process_progress_n${network.networkId}`,
            )) ?? 0,
          isDefault: isSingleNetwork,
        })),
      ),
    };
  }

  public async search(
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

    return { matches: await this.searchNonNumeric(network, q) };
  }

  private async searchBlockHeight(
    network: ExplorerNetworkRef,
    blockHeight: number,
  ): Promise<ExplorerSearchResult[]> {
    const block = await this.getBlockByHeight(network.networkId, network.id, blockHeight);
    return block ? [blockSearchResult(block)] : [];
  }

  private async searchNonNumeric(
    network: ExplorerNetworkRef,
    q: string,
  ): Promise<ExplorerSearchResult[]> {
    const matches: ExplorerSearchResult[] = [];
    const txMatch = await this.searchTransaction(network, q);
    if (txMatch) {
      matches.push(txMatch);
    }

    const blockMatch = await this.searchBlockHash(network, q);
    if (blockMatch) {
      matches.push(blockMatch);
    }

    const addressMatch = await this.searchAddress(network, q);
    if (addressMatch) {
      matches.push(addressMatch);
    }

    return matches;
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

    const block = await this.getBlockByHeight(network.networkId, network.id, blockRef.blockHeight);
    return block ? blockSearchResult(block) : null;
  }

  private async searchAddress(
    network: ExplorerNetworkRef,
    q: string,
  ): Promise<ExplorerSearchResult | null> {
    const [summary, labelMap] = await Promise.all([
      this.warehouse.getAddressSummary(network.networkId, q),
      this.buildLabelMap(network.networkId, [q]),
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

    const labelMap = await this.buildLabelMap(network.networkId, [...addresses]);
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

    const outputKey = `${this.requireString(input.txid, 'vin.txid')}:${this.requireNumber(input.vout, 'vin.vout')}`;
    const resolved = inputMap.get(outputKey);
    if (!resolved?.address) {
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

  public async getAddress(address: string, networkId?: string): Promise<ExplorerAddressDetail> {
    const normalizedAddress = address.trim();
    if (!normalizedAddress) {
      throw new ValidationError('missing input params');
    }

    const network = await this.resolveNetwork(networkId);
    const [summary, overlay, labeledRecord] = await Promise.all([
      this.warehouse.getAddressSummary(network.networkId, normalizedAddress),
      this.buildAddressOverlay(network.networkId, normalizedAddress),
      this.findLabeledAddress(network.networkId, normalizedAddress),
    ]);

    assertAddressExists(summary, labeledRecord);

    return {
      address: addressDetail(network.id, normalizedAddress, summary),
      overlay,
    };
  }

  private async findLabeledAddress(networkId: PrimaryId, address: string) {
    return (await this.metadata.listAddressesByValues([address])).find(
      (candidate) => candidate.networkId === networkId,
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
    const normalizedAddress = address.trim();
    if (!normalizedAddress) {
      throw new ValidationError('missing input params');
    }

    const network = await this.resolveNetwork(networkId);
    await this.assertHistoryReady(network.networkId);
    const rows = await this.warehouse.listAddressTransactions(
      network.networkId,
      normalizedAddress,
      offset,
      limit ?? 50,
    );
    const snapshotsByHeight = await this.loadSnapshotsByHeight(network.networkId, [
      ...new Set(rows.map((row) => row.blockHeight)),
    ]);

    return {
      transactions: rows
        .flatMap((row) => {
          const block = snapshotsByHeight.get(row.blockHeight);
          if (!block) {
            return [];
          }
          const txIndex = block.tx.findIndex(
            (candidate) => this.readString(candidate.txid) === row.txid,
          );
          const transaction = txIndex >= 0 ? block.tx[txIndex] : undefined;
          if (!transaction) {
            return [];
          }
          const summary = this.serializeTransactionSummary(network.id, block, transaction, txIndex);
          return [
            {
              transaction: summary,
              receivedBase: row.receivedBase,
              sentBase: row.sentBase,
            },
          ];
        })
        .sort(
          (left, right) =>
            right.transaction.blockHeight - left.transaction.blockHeight ||
            right.transaction.txIndex - left.transaction.txIndex ||
            right.transaction.txid.localeCompare(left.transaction.txid),
        ),
    };
  }

  public async listAddressUtxos(
    address: string,
    networkId?: string,
    offset?: number,
    limit?: number,
  ): Promise<{
    utxos: ExplorerAddressUtxo[];
  }> {
    const normalizedAddress = address.trim();
    if (!normalizedAddress) {
      throw new ValidationError('missing input params');
    }

    const network = await this.resolveNetwork(networkId);
    const utxos = await this.warehouse.listAddressUtxos(
      network.networkId,
      normalizedAddress,
      offset,
      limit ?? 50,
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

  public async getHdWalletBalance(input: HdWalletBalanceInput): Promise<ExplorerHdWalletBalance> {
    const network = await this.resolveNetwork(input.network);
    await this.assertHistoryReady(network.networkId);
    const normalized = normalizeHdWalletRequest(input);
    const cacheKey = hdWalletCacheKey({
      chains: normalized.chains,
      gapLimit: normalized.gapLimit,
      network: network.id,
      xpub: normalized.xpub,
    });
    const nowMs = Date.now();
    const cached = this.hdWalletBalanceCache.get(cacheKey);

    if (cached && cached.expiresAtMs > nowMs) {
      return hdWalletBalanceWithCache(cached, true);
    }

    this.evictExpiredHdWalletCache(nowMs);
    const scanned = await scanHdWalletBalance(normalized, (address) =>
      this.warehouse.getAddressSummary(network.networkId, address),
    );
    const entry = {
      expiresAtMs: Date.now() + hdWalletBalanceCacheTtlMs,
      result: {
        network: network.id,
        ...scanned,
      },
    };
    this.hdWalletBalanceCache.set(cacheKey, entry);
    return hdWalletBalanceWithCache(entry, false);
  }

  private evictExpiredHdWalletCache(nowMs: number): void {
    for (const [key, entry] of this.hdWalletBalanceCache.entries()) {
      if (entry.expiresAtMs <= nowMs) {
        this.hdWalletBalanceCache.delete(key);
      }
    }
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

    const blockRef = await this.warehouse.getAppliedBlockByHash(networkId, normalized);
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

    const outputKey = `${this.requireString(input.txid, 'vin.txid')}:${this.requireNumber(input.vout, 'vin.vout')}`;
    const resolved = resolvedInputs?.get(outputKey);
    return resolved ? parseAmountBase(resolved.valueBase) : 0n;
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
    networkId: PrimaryId,
    addresses: string[],
  ): Promise<Map<string, ExplorerLabelRef>> {
    const addressRecords = (
      await this.metadata.listAddressesByValues([...new Set(addresses)])
    ).filter((address) => address.networkId === networkId);
    if (addressRecords.length === 0) {
      return new Map();
    }

    const entityIds = [...new Set(addressRecords.map((address) => address.entityId))];
    const [entities, joinedTags] = await Promise.all([
      this.metadata.listEntitiesByIds(entityIds),
      this.metadata.listTagsByEntityIds(entityIds),
    ]);
    const entityById = new Map(entities.map((entity) => [entity.entityId, entity]));
    const tagsByEntityId = new Map<PrimaryId, Array<{ id: string; riskLevel: RiskLevel }>>();
    for (const tag of joinedTags) {
      const current = tagsByEntityId.get(tag.entityId) ?? [];
      current.push({
        id: tag.id,
        riskLevel: tag.riskLevel,
      });
      tagsByEntityId.set(tag.entityId, current);
    }

    return new Map(
      addressRecords.flatMap((record) => {
        const entity = entityById.get(record.entityId);
        if (!entity) {
          return [];
        }
        const tags = tagsByEntityId.get(record.entityId) ?? [];
        return [
          [
            record.address,
            {
              entity: entity.id,
              name: entity.name,
              tags: tags.map((tag) => tag.id),
              riskLevel: tags.some((tag) => tag.riskLevel === 'high') ? 'high' : 'low',
            } satisfies ExplorerLabelRef,
          ] as const,
        ];
      }),
    );
  }

  private async buildAddressOverlay(networkId: PrimaryId, address: string): Promise<InfoResponse> {
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
    ).filter((candidate) => candidate.networkId === networkId);
    const entityIds = [...new Set(addressRecords.map((candidate) => candidate.entityId))];
    const [entities, joinedTags, networks] = await Promise.all([
      this.metadata.listEntitiesByIds(entityIds),
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

    if (typeof value === 'number') {
      return decimalPartsToBase(value.toFixed(8).split('.'));
    }

    return stringDecimalToBase(value);
  }

  private requireNumber(value: number | undefined, field: string): number {
    if (!isNonNegativeInteger(value)) {
      throw new ValidationError(`invalid parameter for \`${field}\`: ${value ?? ''}`);
    }

    return value;
  }

  private requireRecord(value: unknown, field: string): Record<string, unknown> {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
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

function hdWalletBalanceWithCache(
  entry: HdWalletBalanceCacheEntry,
  hit: boolean,
): ExplorerHdWalletBalance {
  return {
    ...entry.result,
    cache: {
      expiresAt: new Date(entry.expiresAtMs).toISOString(),
      hit,
      ttlSeconds: Math.max(0, Math.ceil((entry.expiresAtMs - Date.now()) / 1000)),
    },
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
  if (networks.length === 0) {
    throw new NotFoundError('dogecoin network not found');
  }
  if (networks.length > 1) {
    throw new ValidationError('missing parameter for `network`');
  }

  return networks[0] as ExplorerNetworkRef;
}

function explorerNetworkRef(network: ExplorerNetworkRef): ExplorerNetworkRef {
  return {
    id: network.id,
    name: network.name,
    networkId: network.networkId,
  };
}

function assertAddressExists(
  summary: WarehouseAddressSummary | null,
  labeledRecord: unknown,
): void {
  if (!summary && !labeledRecord) {
    throw new NotFoundError('address not found');
  }
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
  if (!/^\d+$/u.test(whole)) {
    throw new ValidationError(`invalid decimal amount: ${raw}`);
  }
  if (!/^\d*$/u.test(fraction)) {
    throw new ValidationError(`invalid decimal amount: ${raw}`);
  }
}

function isNonNegativeInteger(value: number | undefined): value is number {
  return value !== undefined && Number.isInteger(value) && value >= 0;
}
