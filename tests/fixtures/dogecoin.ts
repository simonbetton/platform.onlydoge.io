type DogecoinFixture = {
  blocksByHeight: Record<number, DogecoinFixtureBlock>;
  intermediaryAddress: string;
  latestBlockHeight: number;
  mempoolEntries: Record<string, DogecoinFixtureMempoolEntry>;
  mempoolInfo: DogecoinFixtureMempoolInfo;
  sourceAddress: string;
  targetAddress: string;
};

type DogecoinFixtureBlock = {
  hash: string;
  height: number;
  previousblockhash: string | null;
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
    type: string;
  };
  value: string;
};

type DogecoinFixtureMempoolInfo = {
  bytes: number;
  maxmempool: number;
  mempoolminfee: string;
  minrelaytxfee: string;
  size: number;
  usage: number;
};

type DogecoinFixtureMempoolEntry = {
  ancestorcount: number;
  ancestorfees: string;
  ancestorsize: number;
  depends: string[];
  descendantcount: number;
  descendantfees: string;
  descendantsize: number;
  fee: string;
  height: number;
  modifiedfee: string;
  size: number;
  time: number;
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
      previousblockhash: null,
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
      previousblockhash: 'doge-block-0',
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
                addresses: ['DSeedRelay1111111111111111111111111'],
              },
            },
            {
              n: 1,
              value: '59.00000000',
              scriptPubKey: {
                type: 'pubkeyhash',
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
      previousblockhash: 'doge-block-1',
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
                addresses: ['DTargetSink1111111111111111111111111'],
              },
            },
            {
              n: 1,
              value: '14.00000000',
              scriptPubKey: {
                type: 'pubkeyhash',
                addresses: ['DSeedRelay1111111111111111111111111'],
              },
            },
          ],
        },
      ],
    },
  },
  mempoolInfo: {
    size: 3,
    bytes: 750,
    usage: 1200,
    maxmempool: 300_000_000,
    mempoolminfee: '0.00002000',
    minrelaytxfee: '0.00001000',
  },
  mempoolEntries: {
    'doge-mempool-a': {
      size: 250,
      fee: '0.00100000',
      modifiedfee: '0.00150000',
      time: 1_700_000_260,
      height: 3,
      descendantcount: 2,
      descendantsize: 500,
      descendantfees: '0.00250000',
      ancestorcount: 1,
      ancestorsize: 250,
      ancestorfees: '0.00100000',
      depends: ['doge-parent-a'],
    },
    'doge-mempool-b': {
      size: 100,
      fee: '0.00010000',
      modifiedfee: '0.00010000',
      time: 1_700_000_320,
      height: 3,
      descendantcount: 1,
      descendantsize: 100,
      descendantfees: '0.00010000',
      ancestorcount: 1,
      ancestorsize: 100,
      ancestorfees: '0.00010000',
      depends: [],
    },
    'doge-mempool-c': {
      size: 400,
      fee: '0.00400000',
      modifiedfee: '0.00400000',
      time: 1_700_000_320,
      height: 3,
      descendantcount: 1,
      descendantsize: 400,
      descendantfees: '0.00400000',
      ancestorcount: 2,
      ancestorsize: 650,
      ancestorfees: '0.00500000',
      depends: ['doge-mempool-a'],
    },
  },
} satisfies DogecoinFixture;

export const dogecoinHashesByHeight = new Map(
  Object.values(dogecoinFixture.blocksByHeight).map((block) => [block.height, block.hash]),
);

export const dogecoinBlocksByHash = new Map(
  Object.values(dogecoinFixture.blocksByHeight).map((block) => [block.hash, block]),
);
