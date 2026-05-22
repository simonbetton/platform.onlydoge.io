import { type AuthenticatedApiKey, requireAdminApiKey } from '@onlydoge/access-control';
import {
  ConflictError,
  NotFoundError,
  type PrimaryId,
  TooEarlyError,
  ValidationError,
} from '@onlydoge/shared-kernel';
import type {
  NetworkRepository,
  NetworkRpcGateway,
  TokenRepository,
} from '../contracts/repositories';
import {
  type CreateNetworkInput,
  Network,
  type NetworkResponse,
  networkToResponse,
  updateNetworkRecord,
} from '../domain/network';
import { type CreateTokenInput, Token, type TokenResponse, tokenToResponse } from '../domain/token';

export class NetworkCatalogService {
  public constructor(
    private readonly networks: NetworkRepository,
    private readonly tokens: TokenRepository,
    private readonly rpc: NetworkRpcGateway,
    private readonly lifecycle?: {
      markNetworksUpdated(): Promise<void>;
      softDeleteAddressesByNetworkIds(networkIds: PrimaryId[]): Promise<void>;
    },
  ) {}

  public async createNetwork(actor: AuthenticatedApiKey, input: CreateNetworkInput) {
    requireAdminApiKey(actor);
    await this.assertNetworkIdAvailable(input.id);
    await this.assertNetworkNameAvailable(input.name);
    const chainId = input.chainId ?? 0;
    await this.assertChainIdAvailable(input.architecture, chainId);

    await this.rpc.assertHealthy(input.architecture, input.rpcEndpoint);

    const created = await this.networks.createNetwork(Network.create(input).record);
    await this.markNetworksUpdated();
    return networkToResponse(created);
  }

  public async listNetworks(
    offset?: number,
    limit?: number,
  ): Promise<{
    networks: NetworkResponse[];
  }> {
    return {
      networks: (await this.networks.listNetworks(offset, limit))
        .filter((network) => !network.isDeleted)
        .map(networkToResponse),
    };
  }

  public async getNetwork(id: string): Promise<{ network: NetworkResponse }> {
    const network = await this.networks.getNetworkById(id);
    assertActiveNetworkRecord(network);

    return {
      network: networkToResponse(network),
    };
  }

  public async updateNetwork(
    actor: AuthenticatedApiKey,
    id: string,
    input: Partial<CreateNetworkInput>,
  ): Promise<void> {
    requireAdminApiKey(actor);
    const network = await this.requireActiveNetwork(id);

    await this.assertNetworkNameAvailable(input.name, network.id);
    await this.assertUpdatedChainIdAvailable(network, input);
    await this.assertUpdatedRpcHealthy(network, input);

    const updated = updateNetworkRecord(network, input);
    await this.networks.updateNetworkRecord(updated);
    await this.markNetworksUpdated();
  }

  public async deleteNetworks(actor: AuthenticatedApiKey, ids: string[]): Promise<void> {
    requireAdminApiKey(actor);
    const deleted = await this.networks.softDeleteNetworks(ids);
    await this.softDeleteAddressesForNetworks(deleted.map((network) => network.networkId));
    await this.markNetworksUpdated();
  }

  public async createToken(
    actor: AuthenticatedApiKey,
    input: Omit<CreateTokenInput, 'networkId'> & { network: string },
  ) {
    requireAdminApiKey(actor);
    await this.assertTokenIdAvailable(input.id);
    const network = await this.requireTokenNetwork(input.network);
    const address = tokenAddress(input.address);
    await this.assertTokenAddressAvailable(network.networkId, address);

    const created = await this.tokens.createToken(
      Token.create({
        ...input,
        address,
        networkId: network.networkId,
      }).record,
    );

    return tokenToResponse(created);
  }

  public async listTokens(
    offset?: number,
    limit?: number,
  ): Promise<{
    networks: NetworkResponse[];
    tokens: TokenResponse[];
  }> {
    const tokens = await this.tokens.listTokens(offset, limit);
    const networkIds = [...new Set(tokens.map((token) => token.networkId))];
    const networks = (
      await Promise.all(
        networkIds.map((networkId) => this.networks.getNetworkByInternalId(networkId)),
      )
    )
      .filter((network): network is NonNullable<typeof network> => Boolean(network))
      .map(networkToResponse);

    return {
      tokens: tokens.map(tokenToResponse),
      networks,
    };
  }

  public async getToken(id: string): Promise<{
    token: TokenResponse;
    networks: NetworkResponse[];
  }> {
    const token = requireTokenRecord(await this.tokens.getTokenById(id));

    const network = await this.networks.getNetworkByInternalId(token.networkId);
    return {
      token: tokenToResponse(token),
      networks: network ? [networkToResponse(network)] : [],
    };
  }

  public async deleteTokens(actor: AuthenticatedApiKey, ids: string[]): Promise<void> {
    requireAdminApiKey(actor);
    await this.tokens.deleteTokens(ids);
  }

  private async assertNetworkNameAvailable(
    name: string | undefined,
    currentNetworkId?: string,
  ): Promise<void> {
    if (!name) {
      return;
    }

    await this.assertDeletedNetworkNameNotPending(name);
    await this.assertDuplicateNetworkNameAvailable(name, currentNetworkId);
  }

  private async assertNetworkIdAvailable(id: string | undefined): Promise<void> {
    if (!id) {
      return;
    }

    await this.assertNetworkIdDoesNotExist(id);
  }

  private async assertNetworkIdDoesNotExist(id: string): Promise<void> {
    if (await this.networks.getNetworkById(id)) {
      throw new ValidationError(`invalid parameter for \`id\`: ${id}`);
    }
  }

  private async assertChainIdAvailable(
    architecture: CreateNetworkInput['architecture'],
    chainId: number,
  ): Promise<void> {
    const duplicateNetwork = await this.networks.getNetworkByArchitectureAndChainId(
      architecture,
      chainId,
    );
    if (duplicateNetwork) {
      throw new ConflictError(`duplicate found at \`chainId\`: ${chainId}`);
    }
  }

  private async requireActiveNetwork(id: string) {
    const network = await this.networks.getNetworkById(id);
    assertActiveNetworkRecord(network);
    return network;
  }

  private async assertUpdatedChainIdAvailable(
    network: Awaited<ReturnType<NetworkRepository['getNetworkById']>> & {},
    input: Partial<CreateNetworkInput>,
  ): Promise<void> {
    if (input.chainId === undefined) {
      return;
    }

    const duplicateNetwork = await this.networks.getNetworkByArchitectureAndChainId(
      updatedNetworkArchitecture(network, input),
      input.chainId,
    );
    assertNoUpdatedChainIdConflict(duplicateNetwork, network.id, input.chainId);
  }

  private async assertUpdatedRpcHealthy(
    network: Awaited<ReturnType<NetworkRepository['getNetworkById']>> & {},
    input: Partial<CreateNetworkInput>,
  ): Promise<void> {
    if (!input.rpcEndpoint) {
      return;
    }

    await this.rpc.assertHealthy(updatedNetworkArchitecture(network, input), input.rpcEndpoint);
  }

  private async assertDeletedNetworkNameNotPending(name: string): Promise<void> {
    const deletedName = await this.networks.getNetworkByName(name, true);
    if (isDeletedNetworkRecord(deletedName)) {
      throw new TooEarlyError(`too early: network hasn't been deleted yet: ${name}`);
    }
  }

  private async assertDuplicateNetworkNameAvailable(
    name: string,
    currentNetworkId?: string,
  ): Promise<void> {
    const duplicateName = await this.networks.getNetworkByName(name);
    if (isDifferentNetwork(duplicateName, currentNetworkId)) {
      throw new ConflictError(`duplicate found at \`name\`: ${name}`);
    }
  }

  private async assertTokenIdAvailable(id: string | undefined): Promise<void> {
    if (!id) {
      return;
    }

    await this.assertTokenIdDoesNotExist(id);
  }

  private async assertTokenIdDoesNotExist(id: string): Promise<void> {
    if (await this.tokens.getTokenById(id)) {
      throw new ValidationError(`invalid parameter for \`id\`: ${id}`);
    }
  }

  private async requireTokenNetwork(id: string) {
    const network = await this.networks.getNetworkById(id);
    if (!isActiveNetworkRecord(network)) {
      throw new ValidationError(`invalid parameter for \`network\`: ${id}`);
    }

    return network;
  }

  private async assertTokenAddressAvailable(networkId: PrimaryId, address: string): Promise<void> {
    const duplicate = await this.tokens.getTokenByNetworkAndAddress(networkId, address);
    if (duplicate) {
      throw new ConflictError(`duplicate found at \`address\`: ${address}`);
    }
  }

  private async markNetworksUpdated(): Promise<void> {
    await this.lifecycle?.markNetworksUpdated();
  }

  private async softDeleteAddressesForNetworks(networkIds: PrimaryId[]): Promise<void> {
    await this.lifecycle?.softDeleteAddressesByNetworkIds(networkIds);
  }
}

function tokenAddress(address: string | undefined): string {
  return address?.trim() ?? '';
}

function assertActiveNetworkRecord(
  network: Awaited<ReturnType<NetworkRepository['getNetworkById']>>,
): asserts network is NonNullable<typeof network> {
  if (!isActiveNetworkRecord(network)) {
    throw new NotFoundError();
  }
}

function isActiveNetworkRecord(
  network: Awaited<ReturnType<NetworkRepository['getNetworkById']>>,
): network is NonNullable<typeof network> {
  return network !== null && !network.isDeleted;
}

function isDeletedNetworkRecord(
  network: Awaited<ReturnType<NetworkRepository['getNetworkByName']>>,
): boolean {
  return network?.isDeleted === true;
}

function requireTokenRecord(
  token: Awaited<ReturnType<TokenRepository['getTokenById']>>,
): NonNullable<typeof token> {
  if (!token) {
    throw new NotFoundError();
  }

  return token;
}

function assertNoUpdatedChainIdConflict(
  duplicateNetwork: Awaited<ReturnType<NetworkRepository['getNetworkByArchitectureAndChainId']>>,
  currentNetworkId: string,
  chainId: number,
): void {
  if (isDifferentNetwork(duplicateNetwork, currentNetworkId)) {
    throw new ConflictError(`duplicate found at \`chainId\`: ${chainId}`);
  }
}

function updatedNetworkArchitecture(
  network: NonNullable<Awaited<ReturnType<NetworkRepository['getNetworkById']>>>,
  input: Partial<CreateNetworkInput>,
): CreateNetworkInput['architecture'] {
  return input.architecture ?? network.architecture;
}

function isDifferentNetwork(
  network: Awaited<ReturnType<NetworkRepository['getNetworkById']>>,
  currentNetworkId: string | undefined,
): boolean {
  return Boolean(network && network.id !== currentNetworkId);
}
