import type { ChainFamily, PrimaryId } from '@onlydoge/shared-kernel';

import type { NetworkRecord } from '../domain/network';
import type { TokenRecord } from '../domain/token';

export interface NetworkRepository {
  createNetwork(record: NetworkRecord): Promise<NetworkRecord>;
  getNetworkByArchitectureAndChainId(
    architecture: ChainFamily,
    chainId: number,
    includeDeleted?: boolean,
  ): Promise<NetworkRecord | null>;
  getNetworkById(id: string): Promise<NetworkRecord | null>;
  getNetworkByInternalId(id: PrimaryId): Promise<NetworkRecord | null>;
  getNetworkByName(name: string, includeDeleted?: boolean): Promise<NetworkRecord | null>;
  listNetworks(offset?: number, limit?: number): Promise<NetworkRecord[]>;
  updateNetworkRecord(record: NetworkRecord): Promise<void>;
  softDeleteNetworks(ids: string[]): Promise<NetworkRecord[]>;
}

export interface TokenRepository {
  createToken(record: TokenRecord): Promise<TokenRecord>;
  getTokenById(id: string): Promise<TokenRecord | null>;
  listTokens(offset?: number, limit?: number): Promise<TokenRecord[]>;
  listTokensByNetworkIds(networkIds: PrimaryId[]): Promise<TokenRecord[]>;
  getTokenByNetworkAndAddress(networkId: PrimaryId, address: string): Promise<TokenRecord | null>;
  deleteTokens(ids: string[]): Promise<void>;
}

export interface NetworkRpcGateway {
  assertHealthy(architecture: ChainFamily, rpcEndpoint: string): Promise<void>;
  getBlockHeight(network: Pick<NetworkRecord, 'architecture' | 'rpcEndpoint'>): Promise<number>;
}
