import type { PrimaryId, RiskLevel, RiskReason } from '@onlydoge/shared-kernel';

export interface InfoResponse {
  addresses: string[];
  risk: {
    level: RiskLevel;
    reasons: RiskReason[];
  };
  assets: Array<{
    network: string;
    token?: string;
    balance: string;
  }>;
  tokens: Array<{
    id: string;
    name: string;
    symbol: string;
    address: string;
    decimals: number;
  }>;
  sources: Array<{
    network: string;
    entity: string;
    from: string;
    to: string;
    hops: number;
  }>;
  networks: Array<{
    id: string;
    name: string;
    chainId: number;
  }>;
  entities: Array<{
    id: string;
    name: string | null;
    description: string;
    data: Record<string, unknown>;
    tags?: string[];
  }>;
  tags: Array<{
    id: string;
    name: string;
    riskLevel: RiskLevel;
  }>;
}

export interface InfoResponseInput {
  addresses: string[];
  addressRecords: Array<{
    address: string;
    entityId: PrimaryId;
  }>;
  balances: Array<{
    assetAddress: string;
    balance: string;
    networkId: PrimaryId;
  }>;
  entities: Array<{
    data: Record<string, unknown>;
    description: string;
    entityId: PrimaryId;
    id: string;
    name: string | null;
  }>;
  joinedTags: Array<{
    entityId: PrimaryId;
    id: string;
    name: string;
    riskLevel: RiskLevel;
  }>;
  links: Array<{
    fromAddress: string;
    networkId: PrimaryId;
    toAddress: string;
    transferCount: number;
  }>;
  networks: Array<{
    chainId: number;
    id: string;
    name: string;
    networkId: PrimaryId;
  }>;
  tokens: Array<{
    address: string;
    decimals: number;
    id: string;
    name: string;
    networkId: PrimaryId;
    symbol: string;
  }>;
}

export function buildInfoResponse(input: InfoResponseInput): InfoResponse {
  const maps = infoResponseMaps(input);

  return {
    addresses: input.addresses,
    risk: riskSummary(input),
    assets: input.balances.map((balance) => assetSummary(balance, maps)),
    tokens: input.tokens.map((token) => ({
      id: token.id,
      name: token.name,
      symbol: token.symbol,
      address: token.address,
      decimals: token.decimals,
    })),
    sources: input.links.map((link) => sourceSummary(link, maps)),
    networks: input.networks.map((network) => ({
      id: network.id,
      name: network.name,
      chainId: network.chainId,
    })),
    entities: input.entities.map((entity) => ({
      id: entity.id,
      name: entity.name,
      description: entity.description,
      data: entity.data,
      tags: maps.tagIdsByEntityId.get(entity.entityId) ?? [],
    })),
    tags: input.joinedTags.map((tag) => ({
      id: tag.id,
      name: tag.name,
      riskLevel: tag.riskLevel,
    })),
  };
}

function riskSummary(input: InfoResponseInput): InfoResponse['risk'] {
  const reasons = new Set<RiskReason>();
  const level = riskLevelFromTags(input.joinedTags);
  if (input.joinedTags.length > 0) {
    reasons.add('entity');
  }
  if (input.links.length > 0) {
    reasons.add('source');
  }

  return { level, reasons: [...reasons] };
}

function riskLevelFromTags(input: InfoResponseInput['joinedTags']): RiskLevel {
  return input.some((tag) => tag.riskLevel === 'high') ? 'high' : 'low';
}

function infoResponseMaps(input: InfoResponseInput) {
  return {
    entityById: new Map(input.entities.map((entity) => [entity.entityId, entity])),
    entityIdsByAddress: new Map(
      input.addressRecords.map((record) => [record.address, record.entityId]),
    ),
    networkById: new Map(input.networks.map((network) => [network.networkId, network])),
    tagIdsByEntityId: tagIdsByEntityId(input.joinedTags),
    tokenByNetworkAndAddress: new Map(
      input.tokens.map((token) => [`${token.networkId}:${token.address}`, token]),
    ),
  };
}

function tagIdsByEntityId(input: InfoResponseInput['joinedTags']): Map<PrimaryId, string[]> {
  const tags = new Map<PrimaryId, string[]>();
  for (const tag of input) {
    const current = tags.get(tag.entityId) ?? [];
    current.push(tag.id);
    tags.set(tag.entityId, current);
  }

  return tags;
}

function assetSummary(
  balance: InfoResponseInput['balances'][number],
  maps: ReturnType<typeof infoResponseMaps>,
): InfoResponse['assets'][number] {
  const asset: InfoResponse['assets'][number] = {
    network: networkExternalId(balance.networkId, maps),
    balance: balance.balance,
  };
  applyAssetToken(asset, balance, maps);

  return asset;
}

function networkExternalId(
  networkId: PrimaryId,
  maps: ReturnType<typeof infoResponseMaps>,
): string {
  return maps.networkById.get(networkId)?.id ?? '';
}

function applyAssetToken(
  asset: InfoResponse['assets'][number],
  balance: InfoResponseInput['balances'][number],
  maps: ReturnType<typeof infoResponseMaps>,
): void {
  const tokenId = maps.tokenByNetworkAndAddress.get(
    `${balance.networkId}:${balance.assetAddress}`,
  )?.id;
  if (tokenId) {
    asset.token = tokenId;
  }
}

function sourceSummary(
  link: InfoResponseInput['links'][number],
  maps: ReturnType<typeof infoResponseMaps>,
): InfoResponse['sources'][number] {
  return {
    network: networkExternalId(link.networkId, maps),
    entity: sourceEntity(link.fromAddress, maps),
    from: link.fromAddress,
    to: link.toAddress,
    hops: link.transferCount,
  };
}

function sourceEntity(fromAddress: string, maps: ReturnType<typeof infoResponseMaps>): string {
  const entityId = maps.entityIdsByAddress.get(fromAddress);
  return entityId === undefined ? '' : (maps.entityById.get(entityId)?.id ?? '');
}
