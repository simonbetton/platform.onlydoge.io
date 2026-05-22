import type { PrimaryId, RiskLevel } from '@onlydoge/shared-kernel';

export interface ConfigReader {
  getJsonValue<T>(key: string): Promise<T | null>;
}

export interface InvestigationMetadataPort {
  getEntityById(id: string): Promise<{
    data: Record<string, unknown>;
    description: string;
    entityId: PrimaryId;
    id: string;
    name: string | null;
  } | null>;
  listAddressesByEntityIds(entityIds: PrimaryId[]): Promise<
    Array<{
      address: string;
      entityId: PrimaryId;
      id: string;
      network: string;
      networkId: PrimaryId;
    }>
  >;
  listAddressesByValues(values: string[]): Promise<
    Array<{
      address: string;
      entityId: PrimaryId;
      id: string;
      network: string;
      networkId: PrimaryId;
    }>
  >;
  listEntitiesByIds(entityIds: PrimaryId[]): Promise<
    Array<{
      data: Record<string, unknown>;
      description: string;
      entityId: PrimaryId;
      id: string;
      name: string | null;
    }>
  >;
  listTagsByEntityIds(entityIds: PrimaryId[]): Promise<
    Array<{
      entityId: PrimaryId;
      id: string;
      name: string;
      riskLevel: RiskLevel;
    }>
  >;
  listNetworksByInternalIds(networkIds: PrimaryId[]): Promise<
    Array<{
      chainId: number;
      id: string;
      name: string;
      networkId: PrimaryId;
    }>
  >;
  listActiveNetworks(): Promise<
    Array<{
      name: string;
      networkId: PrimaryId;
    }>
  >;
  getCoreIndexerState?(networkId: PrimaryId): Promise<{
    lastError: string | null;
    networkId: PrimaryId;
    onlineTip: number;
    processTail: number;
    stage: string;
    syncTail: number;
    updatedAt: string;
  } | null>;
}

export interface InvestigationWarehousePort {
  getBalancesByAddresses(addresses: string[]): Promise<
    Array<{
      assetAddress: string;
      balance: string;
      networkId: PrimaryId;
    }>
  >;
  getTokensByAddresses(addresses: string[]): Promise<
    Array<{
      address: string;
      decimals: number;
      id: string;
      name: string;
      networkId: PrimaryId;
      symbol: string;
    }>
  >;
  getDistinctLinksByAddresses(addresses: string[]): Promise<
    Array<{
      fromAddress: string;
      networkId: PrimaryId;
      toAddress: string;
      transferCount: number;
    }>
  >;
}
