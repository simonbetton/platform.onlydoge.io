import { ValidationError } from '@onlydoge/shared-kernel';
import type {
  ConfigReader,
  InvestigationMetadataPort,
  InvestigationWarehousePort,
} from '../contracts/ports';
import { buildInfoResponse, type InfoResponse } from '../domain/query-models';

export class InvestigationQueryService {
  public constructor(
    private readonly metadata: InvestigationMetadataPort,
    private readonly warehouse: InvestigationWarehousePort,
    private readonly configs: ConfigReader,
  ) {}

  public async heartbeat(): Promise<void> {}

  public async stats(): Promise<{
    networks: Array<{
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
    }>;
  }> {
    const networks = await this.metadata.listActiveNetworks();
    const response = await Promise.all(
      networks.map(async (network) => {
        const coreState = this.metadata.getCoreIndexerState
          ? await this.metadata.getCoreIndexerState(network.networkId)
          : null;
        return {
          name: network.name,
          blockHeight:
            (await this.configs.getJsonValue<number>(`block_height_n${network.networkId}`)) ?? 0,
          stage:
            coreState?.stage ??
            (await this.configs.getJsonValue<string>(`indexer_stage_n${network.networkId}`)) ??
            null,
          syncTail:
            coreState?.syncTail ??
            (await this.configs.getJsonValue<number>(`indexer_sync_tail_n${network.networkId}`)) ??
            -1,
          processTail:
            coreState?.processTail ??
            (await this.configs.getJsonValue<number>(
              `indexer_process_tail_n${network.networkId}`,
            )) ??
            -1,
          onlineTip: coreState?.onlineTip ?? null,
          lastError: coreState?.lastError ?? null,
          lastUpdatedAt: coreState?.updatedAt ?? null,
          factTail:
            (await this.configs.getJsonValue<number>(`indexer_fact_tail_n${network.networkId}`)) ??
            null,
          synced:
            (await this.configs.getJsonValue<number>(
              `indexer_sync_progress_n${network.networkId}`,
            )) ?? 0,
          processed:
            (await this.configs.getJsonValue<number>(
              `indexer_process_progress_n${network.networkId}`,
            )) ?? 0,
        };
      }),
    );

    return { networks: response };
  }

  public async info(query: string | undefined): Promise<InfoResponse> {
    const q = query?.trim() ?? '';
    if (!q) {
      throw new ValidationError('missing input params');
    }

    const entity = await this.metadata.getEntityById(q);
    const addresses = entity
      ? (await this.metadata.listAddressesByEntityIds([entity.entityId])).map(
          (address) => address.address,
        )
      : [q];

    const [balances, links] = await Promise.all([
      this.warehouse.getBalancesByAddresses(addresses),
      this.warehouse.getDistinctLinksByAddresses(addresses),
    ]);
    const addressRecords = await this.metadata.listAddressesByValues([
      ...new Set([...addresses, ...links.map((link) => link.fromAddress)]),
    ]);
    const entityIds = [...new Set(addressRecords.map((address) => address.entityId))];
    const [entities, joinedTags, networks] = await Promise.all([
      this.metadata.listEntitiesByIds(entityIds),
      this.metadata.listTagsByEntityIds(entityIds),
      this.metadata.listNetworksByInternalIds(addressRecords.map((address) => address.networkId)),
    ]);

    const tokens = await this.warehouse.getTokensByAddresses(
      balances.map((balance) => balance.assetAddress).filter(Boolean),
    );

    return buildInfoResponse({
      addresses,
      addressRecords,
      balances,
      entities,
      joinedTags,
      links,
      networks,
      tokens,
    });
  }
}
