type DogecoinFixture = {
  blocksByHeight: Record<number, DogecoinFixtureBlock>;
  intermediaryAddress: string;
  latestBlockHeight: number;
  sourceAddress: string;
  targetAddress: string;
};

type DogecoinFixtureBlock = {
  hash: string;
  height: number;
  time: number;
  tx: DogecoinFixtureTransaction[];
};

type DogecoinFixtureTransaction = {
  txid: string;
  vin: DogecoinFixtureVin[];
  vout: DogecoinFixtureVout[];
};

type DogecoinFixtureVin = {
  coinbase?: string;
  txid?: string;
  vout?: number;
};

type DogecoinFixtureVout = {
  n: number;
  scriptPubKey: {
    addresses: string[];
    hex: string;
    type: string;
  };
  value: string;
};

export const dogecoinFixture = {
  latestBlockHeight: 2,
  sourceAddress: 'DMinerSource1111111111111111111111111',
  intermediaryAddress: 'DSeedRelay1111111111111111111111111',
  targetAddress: 'DTargetSink1111111111111111111111111',
  blocksByHeight: {
    0: {
      hash: 'doge-block-0',
      height: 0,
      time: 1_700_000_000,
      tx: [
        {
          txid: 'doge-tx-0',
          vin: [{ coinbase: 'coinbase' }],
          vout: [
            {
              n: 0,
              value: '100.00000000',
              scriptPubKey: {
                type: 'pubkeyhash',
                hex: '76a914000000000000000000000000000000000000000088ac',
                addresses: ['DMinerSource1111111111111111111111111'],
              },
            },
          ],
        },
      ],
    },
    1: {
      hash: 'doge-block-1',
      height: 1,
      time: 1_700_000_060,
      tx: [
        {
          txid: 'doge-tx-1',
          vin: [{ txid: 'doge-tx-0', vout: 0 }],
          vout: [
            {
              n: 0,
              value: '40.00000000',
              scriptPubKey: {
                type: 'pubkeyhash',
                hex: '76a914111111111111111111111111111111111111111188ac',
                addresses: ['DSeedRelay1111111111111111111111111'],
              },
            },
            {
              n: 1,
              value: '59.00000000',
              scriptPubKey: {
                type: 'pubkeyhash',
                hex: '76a914222222222222222222222222222222222222222288ac',
                addresses: ['DMinerSource1111111111111111111111111'],
              },
            },
          ],
        },
      ],
    },
    2: {
      hash: 'doge-block-2',
      height: 2,
      time: 1_700_000_120,
      tx: [
        {
          txid: 'doge-tx-2',
          vin: [{ txid: 'doge-tx-1', vout: 0 }],
          vout: [
            {
              n: 0,
              value: '25.00000000',
              scriptPubKey: {
                type: 'pubkeyhash',
                hex: '76a914333333333333333333333333333333333333333388ac',
                addresses: ['DTargetSink1111111111111111111111111'],
              },
            },
            {
              n: 1,
              value: '14.00000000',
              scriptPubKey: {
                type: 'pubkeyhash',
                hex: '76a914444444444444444444444444444444444444444488ac',
                addresses: ['DSeedRelay1111111111111111111111111'],
              },
            },
          ],
        },
      ],
    },
  },
} satisfies DogecoinFixture;

export const dogecoinHashesByHeight = new Map(
  Object.values(dogecoinFixture.blocksByHeight).map((block) => [block.height, block.hash]),
);

export const dogecoinBlocksByHash = new Map(
  Object.values(dogecoinFixture.blocksByHeight).map((block) => [block.hash, block]),
);
