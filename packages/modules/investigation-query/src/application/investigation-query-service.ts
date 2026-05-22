import type { AuthenticatedApiKey } from '@onlydoge/access-control';
import { ValidationError } from '@onlydoge/shared-kernel';
import type {
  ConfigReader,
  InvestigationMetadataPort,
  InvestigationWarehousePort,
} from '../contracts/ports';
import { buildInfoResponse, type InfoResponse } from '../domain/query-models';

type InvestigationNetwork = Awaited<
  ReturnType<InvestigationMetadataPort['listActiveNetworks']>
>[number];
export type InvestigationNetworkStats = {
  blockHeight: number;
  factTail: number | null;
  lastError: string | null;
  lastUpdatedAt: string | null;
  name: string;
  onlineTip: number | null;
  processTail: number;
  processed: number;
  stage: string | null;
  syncTail: number;
  synced: number;
};

export class InvestigationQueryService {
  public constructor(
    private readonly metadata: InvestigationMetadataPort,
    private readonly warehouse: InvestigationWarehousePort,
    private readonly configs: ConfigReader,
  ) {}

  public async heartbeat(): Promise<void> {}

  public async stats(): Promise<{ networks: InvestigationNetworkStats[] }> {
    const networks = await this.metadata.listActiveNetworks();
    const response = await Promise.all(networks.map((network) => this.networkStats(network)));

    return { networks: response };
  }

  private async networkStats(network: InvestigationNetwork): Promise<InvestigationNetworkStats> {
    const [coreState, legacy] = await Promise.all([
      this.coreIndexerState(network.networkId),
      this.legacyNetworkStats(network.networkId),
    ]);
    return networkStatsResponse(network, coreState, legacy);
  }

  private async coreIndexerState(networkId: number) {
    if (!this.metadata.getCoreIndexerState) {
      return null;
    }

    return this.metadata.getCoreIndexerState(networkId);
  }

  private async numberConfig(key: string, fallback: number): Promise<number> {
    return (await this.configs.getJsonValue<number>(key)) ?? fallback;
  }

  private async optionalNumberConfig(key: string): Promise<number | null> {
    return this.configs.getJsonValue<number>(key);
  }

  private async stringConfig(key: string): Promise<string | null> {
    return this.configs.getJsonValue<string>(key);
  }

  private async legacyNetworkStats(networkId: number) {
    return {
      blockHeight: await this.numberConfig(`block_height_n${networkId}`, 0),
      stage: await this.stringConfig(`indexer_stage_n${networkId}`),
      syncTail: await this.numberConfig(`indexer_sync_tail_n${networkId}`, -1),
      processTail: await this.numberConfig(`indexer_process_tail_n${networkId}`, -1),
      factTail: await this.optionalNumberConfig(`indexer_fact_tail_n${networkId}`),
      synced: await this.numberConfig(`indexer_sync_progress_n${networkId}`, 0),
      processed: await this.numberConfig(`indexer_process_progress_n${networkId}`, 0),
    };
  }

  public async info(actor: AuthenticatedApiKey, query: string | undefined): Promise<InfoResponse> {
    const q = requireInfoQuery(query);

    const entity = readableBy(await this.metadata.getEntityById(q), actor);
    const addresses = await this.infoAddresses(q, entity);

    const [balances, links] = await Promise.all([
      this.warehouse.getBalancesByAddresses(addresses),
      this.warehouse.getDistinctLinksByAddresses(addresses),
    ]);
    const addressRecords = await this.readableAddressRecords(
      actor,
      addresses,
      links.map((link) => link.fromAddress),
    );
    const metadata = await this.infoMetadata(actor, addressRecords);

    const tokens = await this.warehouse.getTokensByAddresses(
      balances.map((balance) => balance.assetAddress).filter(Boolean),
    );

    return buildInfoResponse({
      addresses,
      addressRecords,
      balances,
      entities: metadata.entities,
      joinedTags: metadata.joinedTags,
      links,
      networks: metadata.networks,
      tokens,
    });
  }

  private async infoMetadata(
    actor: AuthenticatedApiKey,
    addressRecords: Array<{ entityId: number; networkId: number }>,
  ) {
    const entityIds = uniqueEntityIds(addressRecords);
    const [entities, joinedTags, networks] = await Promise.all([
      this.readableEntities(entityIds, actor),
      this.metadata.listTagsByEntityIds(entityIds),
      this.metadata.listNetworksByInternalIds(networkIds(addressRecords)),
    ]);

    return { entities, joinedTags, networks };
  }

  private async readableEntities(entityIds: number[], actor: AuthenticatedApiKey) {
    const records = await this.metadata.listEntitiesByIds(entityIds);
    return records.filter((record) => canReadOwner(record, actor));
  }

  private async infoAddresses(
    query: string,
    entity: { entityId: number } | null,
  ): Promise<string[]> {
    if (!entity) {
      return [query];
    }

    return (await this.metadata.listAddressesByEntityIds([entity.entityId])).map(
      (address) => address.address,
    );
  }

  private async readableAddressRecords(
    actor: AuthenticatedApiKey,
    addresses: string[],
    linkedAddresses: string[],
  ) {
    return (
      await this.metadata.listAddressesByValues(uniqueAddresses(addresses, linkedAddresses))
    ).filter((record) => canReadOwner(record, actor));
  }
}

function requireInfoQuery(query: string | undefined): string {
  const q = trimmedQuery(query);
  if (!q) {
    throw new ValidationError('missing input params');
  }

  return q;
}

function trimmedQuery(query: string | undefined): string {
  if (!query) {
    return '';
  }

  return query.trim();
}

function uniqueAddresses(addresses: string[], linkedAddresses: string[]): string[] {
  return [...new Set([...addresses, ...linkedAddresses])];
}

function uniqueEntityIds(records: Array<{ entityId: number }>): number[] {
  return [...new Set(records.map((record) => record.entityId))];
}

function networkIds(records: Array<{ networkId: number }>): number[] {
  return records.map((record) => record.networkId);
}

type CoreInvestigationState = Awaited<
  ReturnType<NonNullable<InvestigationMetadataPort['getCoreIndexerState']>>
>;
type LegacyNetworkStats = {
  blockHeight: number;
  factTail: number | null;
  processTail: number;
  processed: number;
  stage: string | null;
  syncTail: number;
  synced: number;
};

function networkStatsResponse(
  network: InvestigationNetwork,
  coreState: CoreInvestigationState,
  legacy: LegacyNetworkStats,
): InvestigationNetworkStats {
  return {
    name: network.name,
    blockHeight: legacy.blockHeight,
    stage: coreStage(coreState, legacy.stage),
    syncTail: coreNumber(coreState, 'syncTail', legacy.syncTail),
    processTail: coreNumber(coreState, 'processTail', legacy.processTail),
    onlineTip: coreNumberOrNull(coreState, 'onlineTip'),
    lastError: coreStringOrNull(coreState, 'lastError'),
    lastUpdatedAt: coreStringOrNull(coreState, 'updatedAt'),
    factTail: legacy.factTail,
    synced: legacy.synced,
    processed: legacy.processed,
  };
}

function coreStage(coreState: CoreInvestigationState, fallback: string | null): string | null {
  if (!coreState) {
    return fallback;
  }

  return coreState.stage;
}

function coreNumber<T extends 'onlineTip' | 'processTail' | 'syncTail'>(
  coreState: CoreInvestigationState,
  key: T,
  fallback: number,
): number {
  if (!coreState) {
    return fallback;
  }

  return coreState[key];
}

function coreNumberOrNull<T extends 'onlineTip'>(
  coreState: CoreInvestigationState,
  key: T,
): number | null {
  if (!coreState) {
    return null;
  }

  return coreState[key];
}

function coreStringOrNull<T extends 'lastError' | 'updatedAt'>(
  coreState: CoreInvestigationState,
  key: T,
): string | null {
  if (!coreState) {
    return null;
  }

  return coreState[key];
}

function readableBy<T extends { ownerApiKeyId: number }>(
  record: T | null,
  actor: AuthenticatedApiKey,
): T | null {
  if (!record) {
    return null;
  }

  return readableRecord(record, actor);
}

function readableRecord<T extends { ownerApiKeyId: number }>(
  record: T,
  actor: AuthenticatedApiKey,
): T | null {
  if (!canReadOwner(record, actor)) {
    return null;
  }

  return record;
}

function canReadOwner(record: { ownerApiKeyId: number }, actor: AuthenticatedApiKey): boolean {
  return [actor.role === 'admin', record.ownerApiKeyId === actor.apiKeyId].includes(true);
}
