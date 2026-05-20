import type { RelationalMetadataStore } from '@onlydoge/platform';

export function parsePositiveInteger(value: string, label: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`invalid ${label}: ${value}`);
  }
  return parsed;
}

export function parseNonNegativeInteger(value: string, label: string): number {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
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

  const networks = (await metadata.listActiveNetworks()).filter(
    (network) => network.architecture === 'dogecoin',
  );
  if (networks.length !== 1) {
    throw new Error(`expected exactly one Dogecoin network, found ${networks.length}`);
  }
  const networkId = networks[0]?.networkId;
  if (networkId === undefined) {
    throw new Error('missing Dogecoin network id');
  }
  return networkId;
}
