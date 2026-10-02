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

import { encodeChain, type EncodedBlock, testAddress } from './dogecoin-encoder';

const sourceAddress = testAddress('miner-source');
const intermediaryAddress = testAddress('seed-relay');
const targetAddress = testAddress('target-sink');

/**
 * Real, decodable Dogecoin blocks. Symbolic ids (`doge-tx-2`) resolve to the
 * actual txids via `dogecoinTxid()`; block hashes via `dogecoinBlockHash()`.
 */
const encodedChain = encodeChain([
  {
    time: 1_700_000_000,
    tx: [
      {
        id: 'doge-tx-0',
        vin: [{ coinbase: true }],
        vout: [{ address: sourceAddress, value: '100.00000000' }],
      },
    ],
  },
  {
    time: 1_700_000_060,
    tx: [
      {
        id: 'doge-tx-1',
        vin: [{ id: 'doge-tx-0', vout: 0 }],
        vout: [
          { address: intermediaryAddress, value: '40.00000000' },
          { address: sourceAddress, value: '59.00000000' },
        ],
      },
    ],
  },
  {
    time: 1_700_000_120,
    tx: [
      {
        id: 'doge-tx-2',
        vin: [{ id: 'doge-tx-1', vout: 0 }],
        vout: [
          { address: targetAddress, value: '25.00000000' },
          { address: intermediaryAddress, value: '14.00000000' },
        ],
      },
    ],
  },
]);

const txidsById = new Map(
  encodedChain.flatMap((block) => block.tx.map((tx) => [tx.id, tx.txid] as const)),
);

export function dogecoinTxid(id: string): string {
  const txid = txidsById.get(id);
  if (!txid) {
    throw new Error(`unknown fixture transaction id: ${id}`);
  }
  return txid;
}

export function dogecoinBlockHash(height: number): string {
  return requireEncodedBlock(height).hash;
}

function requireEncodedBlock(height: number): EncodedBlock {
  const block = encodedChain[height];
  if (!block) {
    throw new Error(`unknown fixture block height: ${height}`);
  }
  return block;
}

function toFixtureBlock(block: EncodedBlock): DogecoinFixtureBlock {
  return {
    hash: block.hash,
    height: block.height,
    previousblockhash: block.previousblockhash,
    time: block.time,
    tx: block.tx.map((tx) => ({
      txid: tx.txid,
      vin: tx.vin,
      vout: tx.vout.map((output) => ({
        n: output.n,
        value: output.value,
        scriptPubKey: { type: 'pubkeyhash', addresses: [output.address] },
      })),
    })),
  };
}

export const dogecoinFixture = {
  latestBlockHeight: encodedChain.length - 1,
  sourceAddress,
  intermediaryAddress,
  targetAddress,
  blocksByHeight: {
    0: toFixtureBlock(requireEncodedBlock(0)),
    1: toFixtureBlock(requireEncodedBlock(1)),
    2: toFixtureBlock(requireEncodedBlock(2)),
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

export const dogecoinRawBlocksByHash = new Map(
  encodedChain.map((block) => [block.hash, block.hex]),
);
