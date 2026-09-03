import { createHash } from 'node:crypto';

import { dogecoinMainnetAddressPrefixes, encodeBase58Check } from '@onlydoge/platform';

/**
 * Test-only Dogecoin wire encoder. Builds real, decodable raw blocks from a
 * symbolic chain description so fetch-level RPC mocks can serve
 * `getblock(hash, false)` exactly like Dogecoin Core does.
 */

export interface ChainSpecOutput {
  address: string;
  value: string;
}

export interface ChainSpecTransaction {
  id: string;
  vin: Array<{ coinbase: true } | { id: string; vout: number }>;
  vout: ChainSpecOutput[];
}

export interface ChainSpecBlock {
  time: number;
  tx: ChainSpecTransaction[];
}

export interface EncodedTransaction {
  id: string;
  txid: string;
  vin: Array<{ coinbase: string } | { txid: string; vout: number }>;
  vout: Array<{ address: string; n: number; value: string }>;
}

export interface EncodedBlock {
  hash: string;
  height: number;
  hex: string;
  previousblockhash: string | null;
  time: number;
  tx: EncodedTransaction[];
}

export function testAddress(label: string): string {
  const hash160 = ripemd160(sha256(Buffer.from(label, 'utf8')));
  return encodeBase58Check(dogecoinMainnetAddressPrefixes.pubkeyHash, hash160);
}

export function encodeChain(blocks: ChainSpecBlock[]): EncodedBlock[] {
  const txidsById = new Map<string, string>();
  const encoded: EncodedBlock[] = [];
  let previousHash: string | null = null;

  for (const [height, spec] of blocks.entries()) {
    const block = encodeBlock(spec, height, previousHash, txidsById);
    encoded.push(block);
    previousHash = block.hash;
  }

  return encoded;
}

function encodeBlock(
  spec: ChainSpecBlock,
  height: number,
  previousHash: string | null,
  txidsById: Map<string, string>,
): EncodedBlock {
  const transactions = spec.tx.map((tx) => encodeTransaction(tx, txidsById));
  const header = Buffer.concat([
    uint32LE(1),
    hashBytes(previousHash ?? '0'.repeat(64)),
    hashBytes(merkleRoot(transactions.map((tx) => tx.txid))),
    uint32LE(spec.time),
    uint32LE(0x1e0ffff0),
    uint32LE(height + 1),
  ]);
  const body = Buffer.concat([
    header,
    compactSize(transactions.length),
    ...transactions.map((tx) => tx.raw),
  ]);

  return {
    hash: displayHash(sha256d(header)),
    height,
    hex: body.toString('hex'),
    previousblockhash: previousHash,
    time: spec.time,
    tx: transactions.map(({ raw: _raw, ...tx }) => tx),
  };
}

function encodeTransaction(
  spec: ChainSpecTransaction,
  txidsById: Map<string, string>,
): EncodedTransaction & { raw: Buffer } {
  const inputs = spec.vin.map((input) => encodeInput(input, txidsById));
  const outputs = spec.vout.map((output) => encodeOutput(output));
  const raw = Buffer.concat([
    uint32LE(1),
    compactSize(inputs.length),
    ...inputs.map((input) => input.raw),
    compactSize(outputs.length),
    ...outputs.map((output) => output.raw),
    uint32LE(0),
  ]);
  const txid = displayHash(sha256d(raw));
  txidsById.set(spec.id, txid);

  return {
    id: spec.id,
    raw,
    txid,
    vin: inputs.map((input) => input.json),
    vout: outputs.map((output, index) => ({ ...output.json, n: index })),
  };
}

function encodeInput(
  input: ChainSpecTransaction['vin'][number],
  txidsById: Map<string, string>,
): { json: EncodedTransaction['vin'][number]; raw: Buffer } {
  if ('coinbase' in input) {
    const script = Buffer.from('03000000', 'hex');
    return {
      json: { coinbase: script.toString('hex') },
      raw: Buffer.concat([
        Buffer.alloc(32, 0),
        uint32LE(0xffffffff),
        compactSize(script.length),
        script,
        uint32LE(0xffffffff),
      ]),
    };
  }

  const txid = txidsById.get(input.id);
  if (!txid) {
    throw new Error(`unknown fixture transaction id: ${input.id}`);
  }
  const scriptSig = Buffer.from('00', 'hex');
  return {
    json: { txid, vout: input.vout },
    raw: Buffer.concat([
      hashBytes(txid),
      uint32LE(input.vout),
      compactSize(scriptSig.length),
      scriptSig,
      uint32LE(0xffffffff),
    ]),
  };
}

function encodeOutput(output: ChainSpecOutput): {
  json: Omit<EncodedTransaction['vout'][number], 'n'>;
  raw: Buffer;
} {
  const script = p2pkhScript(output.address);
  return {
    json: { address: output.address, value: output.value },
    raw: Buffer.concat([int64LE(parseKoinu(output.value)), compactSize(script.length), script]),
  };
}

function p2pkhScript(address: string): Buffer {
  const payload = decodeBase58(address);
  const hash160 = payload.subarray(1, 21);
  return Buffer.concat([
    Buffer.from([0x76, 0xa9, 0x14]),
    hash160,
    Buffer.from([0x88, 0xac]),
  ]);
}

const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';

function decodeBase58(value: string): Buffer {
  let number = 0n;
  for (const char of value) {
    const digit = BASE58_ALPHABET.indexOf(char);
    if (digit < 0) {
      throw new Error(`invalid base58 character: ${char}`);
    }
    number = number * 58n + BigInt(digit);
  }
  const bytes: number[] = [];
  while (number > 0n) {
    bytes.unshift(Number(number % 256n));
    number /= 256n;
  }
  for (const char of value) {
    if (char !== '1') {
      break;
    }
    bytes.unshift(0);
  }
  return Buffer.from(bytes);
}

function merkleRoot(txids: string[]): string {
  let layer = txids.map((txid) => hashBytes(txid));
  while (layer.length > 1) {
    const next: Buffer[] = [];
    for (let index = 0; index < layer.length; index += 2) {
      const left = layer[index] as Buffer;
      const right = layer[index + 1] ?? left;
      next.push(sha256d(Buffer.concat([left, right])));
    }
    layer = next;
  }
  return displayHash(layer[0] ?? Buffer.alloc(32, 0));
}

function parseKoinu(value: string): bigint {
  const [whole = '0', fraction = ''] = value.split('.');
  return BigInt(whole) * 100_000_000n + BigInt(fraction.padEnd(8, '0').slice(0, 8));
}

function compactSize(value: number): Buffer {
  if (value < 0xfd) {
    return Buffer.from([value]);
  }
  const buffer = Buffer.alloc(3);
  buffer[0] = 0xfd;
  buffer.writeUInt16LE(value, 1);
  return buffer;
}

function uint32LE(value: number): Buffer {
  const buffer = Buffer.alloc(4);
  buffer.writeUInt32LE(value >>> 0);
  return buffer;
}

function int64LE(value: bigint): Buffer {
  const buffer = Buffer.alloc(8);
  buffer.writeBigInt64LE(value);
  return buffer;
}

/** Display-order hex → internal byte order. */
function hashBytes(displayHex: string): Buffer {
  return Buffer.from(displayHex, 'hex').reverse();
}

function displayHash(internal: Buffer): string {
  return Buffer.from(internal).reverse().toString('hex');
}

function sha256(bytes: Buffer): Buffer {
  return createHash('sha256').update(bytes).digest();
}

function sha256d(bytes: Buffer): Buffer {
  return sha256(sha256(bytes));
}

function ripemd160(bytes: Buffer): Buffer {
  return createHash('ripemd160').update(bytes).digest();
}
