import type { RelationalMetadataStore } from '@onlydoge/platform';

type ActiveNetwork = Awaited<ReturnType<RelationalMetadataStore['listActiveNetworks']>>[number];

export function parsePositiveInteger(value: string, label: string): number {
  const parsed = Number(value);
  if (!isPositiveInteger(parsed)) {
    throw new Error(`invalid ${label}: ${value}`);
  }
  return parsed;
}

export function parseNonNegativeInteger(value: string, label: string): number {
  const parsed = Number(value);
  if (!isNonNegativeInteger(parsed)) {
    throw new Error(`invalid ${label}: ${value}`);
  }
  return parsed;
}

export async function resolveDogecoinNetworkId(
  metadata: RelationalMetadataStore,
  value: string | undefined,
): Promise<number> {
  if (value) {
    return parsePositiveInteger(value, 'networkId');
  }

  const networks = await metadata.listActiveNetworks();
  return requireSingleDogecoinNetworkId(networks.filter(isDogecoinNetwork));
}

function isDogecoinNetwork(network: ActiveNetwork): boolean {
  return network.architecture === 'dogecoin';
}

function requireSingleDogecoinNetworkId(networks: ActiveNetwork[]): number {
  assertSingleDogecoinNetwork(networks);
  return requireDogecoinNetworkId(networks[0]);
}

function requireDogecoinNetworkId(network: ActiveNetwork | undefined): number {
  if (network === undefined) {
    throw new Error('missing Dogecoin network id');
  }

  return network.networkId;
}

function isPositiveInteger(value: number): boolean {
  return [Number.isSafeInteger(value), value > 0].every(Boolean);
}

function isNonNegativeInteger(value: number): boolean {
  return [Number.isSafeInteger(value), value >= 0].every(Boolean);
}

function assertSingleDogecoinNetwork(networks: ActiveNetwork[]): void {
  if (networks.length !== 1) {
    throw new Error(`expected exactly one Dogecoin network, found ${networks.length}`);
  }
}
