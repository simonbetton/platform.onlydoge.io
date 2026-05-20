import { HDKey } from '@scure/bip32';
import { describe, expect, it, vi } from 'vitest';

import {
  ExplorerQueryService,
  type ExplorerWarehouse,
} from '../../packages/modules/explorer-query/src/application/explorer-query-service';
import type { WarehouseAddressSummary } from '../../packages/modules/explorer-query/src/application/explorer-response-builders';
import {
  deriveDogecoinP2pkhAddress,
  scanHdWalletBalance,
} from '../../packages/modules/explorer-query/src/application/hd-wallet-balance';

describe('HD wallet balance scanning', () => {
  it('scans receive and change chains until the minimum unused gap is reached', async () => {
    const account = testAccountKey();
    const summaries = new Map<string, WarehouseAddressSummary>([
      [
        deriveDogecoinP2pkhAddress(account, 0, 5),
        {
          balance: '1000',
          receivedBase: '1500',
          sentBase: '500',
          txCount: 2,
          utxoCount: 1,
        },
      ],
      [
        deriveDogecoinP2pkhAddress(account, 1, 2),
        {
          balance: '200',
          receivedBase: '200',
          sentBase: '0',
          txCount: 1,
          utxoCount: 1,
        },
      ],
    ]);

    const result = await scanHdWalletBalance(
      {
        xpub: account.publicExtendedKey,
        gapLimit: 3,
      },
      async (address) => summaries.get(address) ?? null,
    );

    expect(result.gapLimit).toBe(20);
    expect(result.balanceBase).toBe('1200');
    expect(result.utxoCount).toBe(2);
    expect(result.complete).toBe(true);

    const [receive, change] = result.chains;
    expect(receive).toMatchObject({
      chain: 0,
      role: 'receive',
      lastUsedIndex: 5,
      nextUnusedIndex: 6,
      scannedAddressCount: 26,
      usedAddressCount: 1,
    });
    expect(change).toMatchObject({
      chain: 1,
      role: 'change',
      lastUsedIndex: 2,
      nextUnusedIndex: 3,
      scannedAddressCount: 23,
      usedAddressCount: 1,
    });
    expect(receive?.usedAddresses[0]?.path).toBe('m/0/5');
    expect(change?.usedAddresses[0]?.path).toBe('m/1/2');
  });

  it('rejects private extended keys', async () => {
    const account = testAccountKey();

    await expect(
      scanHdWalletBalance({ xpub: account.privateExtendedKey }, async () => null),
    ).rejects.toThrow('private extended keys are rejected');
  });

  it('rejects unused gap limits above the production balance cap', async () => {
    const account = testAccountKey();

    await expect(
      scanHdWalletBalance(
        {
          xpub: account.publicExtendedKey,
          chains: [0],
          gapLimit: 201,
        },
        async () => null,
      ),
    ).rejects.toThrow('maximum 200');
  });

  it('caches matching service requests for one minute', async () => {
    const account = testAccountKey();
    const readAddressSummary = vi.fn(async () => null);
    const service = createExplorerService(readAddressSummary);

    const first = await service.getHdWalletBalance({
      xpub: account.publicExtendedKey,
      chains: [0],
    });
    const second = await service.getHdWalletBalance({
      xpub: account.publicExtendedKey,
      chains: [0],
    });

    expect(first.cache.hit).toBe(false);
    expect(second.cache.hit).toBe(true);
    expect(second.cache.ttlSeconds).toBeGreaterThan(0);
    expect(readAddressSummary).toHaveBeenCalledTimes(20);
  });
});

function testAccountKey(): HDKey {
  return HDKey.fromMasterSeed(Uint8Array.from({ length: 32 }, (_, index) => index + 1)).derive(
    "m/44'/3'/0'",
  );
}

function createExplorerService(
  readAddressSummary: (address: string) => Promise<WarehouseAddressSummary | null>,
): ExplorerQueryService {
  const network = {
    architecture: 'dogecoin' as const,
    blockTime: 60,
    chainId: 0,
    id: 'net_dogecoin',
    name: 'Dogecoin Mainnet',
    networkId: 1,
    rpcEndpoint: 'https://doge.example/rpc',
    rps: 1,
  };
  const warehouse = {
    getAddressSummary: (_networkId: number, address: string) => readAddressSummary(address),
    getAppliedBlockByHash: async () => null,
    getBalancesByAddresses: async () => [],
    getDistinctLinksByAddresses: async () => [],
    getTokensByAddresses: async () => [],
    getTransactionRef: async () => null,
    getUtxoOutputs: async () => new Map(),
    listAddressTransactions: async () => [],
    listAddressUtxos: async () => [],
    listAppliedBlocks: async () => [],
  } satisfies ExplorerWarehouse;

  return new ExplorerQueryService(
    {
      listActiveNetworks: async () => [network],
    },
    {
      getEntityById: async () => null,
      listActiveNetworks: async () => [{ name: network.name, networkId: network.networkId }],
      listAddressesByEntityIds: async () => [],
      listAddressesByValues: async () => [],
      listEntitiesByIds: async () => [],
      listNetworksByInternalIds: async () => [],
      listTagsByEntityIds: async () => [],
    },
    warehouse,
    {
      getPart: async () => null,
    },
    {
      canReadDogecoinHistory: async () => true,
      getJsonValue: async () => null,
    },
  );
}
