import { createHash } from 'node:crypto';

import { InfrastructureError } from '@onlydoge/shared-kernel';

/**
 * Local decoder for Dogecoin Core `getblock(hash, false)` raw hex.
 *
 * Produces the same JSON shape the indexer previously assembled from
 * `getblock(hash, true)` + `getrawtransaction(txid, true)`, so raw block
 * snapshots stay backwards compatible while sync needs one RPC per block and
 * no txindex lookups. Dogecoin is Bitcoin wire format plus an AuxPoW payload
 * between the header and the transaction list on merge-mined blocks, and it
 * has no segwit, so `hash === txid` for every transaction.
 */

export interface DecodedDogecoinScriptPubKey {
  addresses?: string[];
  hex: string;
  reqSigs?: number;
  type: DogecoinScriptType;
}

export type DogecoinScriptType =
  | 'multisig'
  | 'nonstandard'
  | 'nulldata'
  | 'pubkey'
  | 'pubkeyhash'
  | 'scripthash';

export interface DecodedDogecoinVin {
  coinbase?: string;
  scriptSig?: { hex: string };
  sequence: number;
  txid?: string;
  vout?: number;
}

export interface DecodedDogecoinVout {
  n: number;
  scriptPubKey: DecodedDogecoinScriptPubKey;
  /** Decimal DOGE string with 8 fractional digits, exact for any koinu amount. */
  value: string;
}

export interface DecodedDogecoinTransaction {
  hash: string;
  locktime: number;
  size: number;
  txid: string;
  version: number;
  vin: DecodedDogecoinVin[];
  vout: DecodedDogecoinVout[];
}

export interface DecodedDogecoinBlock {
  auxpow?: { parentBlockHash: string };
  bits: string;
  hash: string;
  height: number;
  merkleroot: string;
  nTx: number;
  nonce: number;
  previousblockhash?: string;
  size: number;
  time: number;
  tx: DecodedDogecoinTransaction[];
  version: number;
}

export interface DogecoinAddressPrefixes {
  pubkeyHash: number;
  scriptHash: number;
}

export const dogecoinMainnetAddressPrefixes: DogecoinAddressPrefixes = {
  pubkeyHash: 0x1e,
  scriptHash: 0x16,
};

const BLOCK_HEADER_BYTES = 80;
const VERSION_AUXPOW_BIT = 1 << 8;
const KOINU_PER_DOGE = 100_000_000n;
const GENESIS_PREVIOUS_HASH = '0'.repeat(64);

export function decodeDogecoinRawBlock(
  hex: string,
  height: number,
  prefixes: DogecoinAddressPrefixes = dogecoinMainnetAddressPrefixes,
): DecodedDogecoinBlock {
  const bytes = hexToBytes(hex);
  const reader = new ByteReader(bytes);
  const header = readBlockHeader(reader);
  if (isAuxPowVersion(header.version)) {
    header.auxpow = readAuxPow(reader);
  }

  const txCount = reader.readCompactSize();
  const tx: DecodedDogecoinTransaction[] = [];
  for (let index = 0; index < txCount; index += 1) {
    const offset = reader.offset;
    try {
      tx.push(readTransaction(reader, prefixes));
    } catch (error) {
      throw invalidPayload(
        `height=${height} tx_index=${index} tx_offset=${offset}: ${errorMessage(error)}`,
      );
    }
  }
  reader.assertConsumed();

  return finalizeBlock(header, tx, height, bytes.length);
}

interface BlockHeaderFields {
  auxpow?: { parentBlockHash: string };
  bits: string;
  hash: string;
  merkleroot: string;
  nonce: number;
  previousblockhash?: string;
  time: number;
  version: number;
}

function readBlockHeader(reader: ByteReader): BlockHeaderFields {
  const headerBytes = reader.peek(BLOCK_HEADER_BYTES);
  const version = reader.readInt32LE();
  const previousHash = reader.readHash();
  const merkleroot = reader.readHash();
  const time = reader.readUint32LE();
  const bits = reader.readUint32LE();
  const nonce = reader.readUint32LE();

  const header: BlockHeaderFields = {
    bits: bits.toString(16).padStart(8, '0'),
    hash: sha256d(headerBytes),
    merkleroot,
    nonce,
    time,
    version,
  };
  if (previousHash !== GENESIS_PREVIOUS_HASH) {
    header.previousblockhash = previousHash;
  }
  return header;
}

function isAuxPowVersion(version: number): boolean {
  return (version & VERSION_AUXPOW_BIT) !== 0;
}

/**
 * CAuxPow = CMerkleTx(parent coinbase) + chain merkle branch + parent header.
 * CMerkleTx = CTransaction + hashBlock + vMerkleBranch + nIndex.
 */
function readAuxPow(reader: ByteReader): { parentBlockHash: string } {
  skipTransaction(reader);
  reader.skip(32);
  skipHashVector(reader);
  reader.skip(4);
  skipHashVector(reader);
  reader.skip(4);
  const parentHeader = reader.read(BLOCK_HEADER_BYTES);
  return { parentBlockHash: sha256d(parentHeader) };
}

function skipHashVector(reader: ByteReader): void {
  const count = reader.readCompactSize();
  reader.skip(count * 32);
}

/**
 * Skips the AuxPoW parent-chain coinbase. Parent chains (Litecoin & co.) are
 * segwit chains, so Dogecoin Core serializes this transaction with the BIP144
 * extended format when the miner's coinbase carries witness data:
 * `version | 0x00 marker | 0x01 flag | vin | vout | witness | locktime`.
 */
function skipTransaction(reader: ByteReader): void {
  reader.skip(4);
  let inputs = reader.readCompactSize();
  let hasWitness = false;
  if (inputs === 0 && reader.peek(1)[0] === 0x01) {
    reader.skip(1);
    hasWitness = true;
    inputs = reader.readCompactSize();
  }
  for (let index = 0; index < inputs; index += 1) {
    reader.skip(36);
    reader.skip(reader.readCompactSize());
    reader.skip(4);
  }
  const outputs = reader.readCompactSize();
  for (let index = 0; index < outputs; index += 1) {
    reader.skip(8);
    reader.skip(reader.readCompactSize());
  }
  if (hasWitness) {
    skipWitnesses(reader, inputs);
  }
  reader.skip(4);
}

function skipWitnesses(reader: ByteReader, inputs: number): void {
  for (let input = 0; input < inputs; input += 1) {
    const items = reader.readCompactSize();
    for (let item = 0; item < items; item += 1) {
      reader.skip(reader.readCompactSize());
    }
  }
}

function readTransaction(
  reader: ByteReader,
  prefixes: DogecoinAddressPrefixes,
): DecodedDogecoinTransaction {
  const start = reader.offset;
  const version = reader.readInt32LE();
  const vin = readInputs(reader);
  const vout = readOutputs(reader, prefixes);
  const locktime = reader.readUint32LE();
  const raw = reader.slice(start, reader.offset);
  const txid = sha256d(raw);

  return { hash: txid, locktime, size: raw.length, txid, version, vin, vout };
}

function readInputs(reader: ByteReader): DecodedDogecoinVin[] {
  const count = reader.readCompactSize();
  const inputs: DecodedDogecoinVin[] = [];
  for (let index = 0; index < count; index += 1) {
    inputs.push(readInput(reader));
  }
  return inputs;
}

function readInput(reader: ByteReader): DecodedDogecoinVin {
  const txid = reader.readHash();
  const vout = reader.readUint32LE();
  const script = reader.read(reader.readCompactSize());
  const sequence = reader.readUint32LE();
  if (isCoinbaseOutpoint(txid, vout)) {
    return { coinbase: bytesToHex(script), sequence };
  }

  return { scriptSig: { hex: bytesToHex(script) }, sequence, txid, vout };
}

function isCoinbaseOutpoint(txid: string, vout: number): boolean {
  return txid === GENESIS_PREVIOUS_HASH && vout === 0xffffffff;
}

function readOutputs(reader: ByteReader, prefixes: DogecoinAddressPrefixes): DecodedDogecoinVout[] {
  const count = reader.readCompactSize();
  const outputs: DecodedDogecoinVout[] = [];
  for (let index = 0; index < count; index += 1) {
    const value = reader.readInt64LE();
    const script = reader.read(reader.readCompactSize());
    outputs.push({
      n: index,
      scriptPubKey: classifyScriptPubKey(script, prefixes),
      value: formatKoinu(value),
    });
  }
  return outputs;
}

function finalizeBlock(
  header: BlockHeaderFields,
  tx: DecodedDogecoinTransaction[],
  height: number,
  size: number,
): DecodedDogecoinBlock {
  const block: DecodedDogecoinBlock = {
    bits: header.bits,
    hash: header.hash,
    height,
    merkleroot: header.merkleroot,
    nTx: tx.length,
    nonce: header.nonce,
    size,
    time: header.time,
    tx,
    version: header.version,
  };
  if (header.previousblockhash) {
    block.previousblockhash = header.previousblockhash;
  }
  if (header.auxpow) {
    block.auxpow = header.auxpow;
  }
  return block;
}

export function formatKoinu(value: bigint): string {
  const negative = value < 0n;
  const magnitude = negative ? -value : value;
  const whole = magnitude / KOINU_PER_DOGE;
  const fraction = (magnitude % KOINU_PER_DOGE).toString().padStart(8, '0');
  return `${negative ? '-' : ''}${whole}.${fraction}`;
}

const OP_0 = 0x00;
const OP_PUSHDATA1 = 0x4c;
const OP_PUSHDATA2 = 0x4d;
const OP_PUSHDATA4 = 0x4e;
const OP_1 = 0x51;
const OP_16 = 0x60;
const OP_RETURN = 0x6a;
const OP_DUP = 0x76;
const OP_EQUAL = 0x87;
const OP_EQUALVERIFY = 0x88;
const OP_HASH160 = 0xa9;
const OP_CHECKSIG = 0xac;
const OP_CHECKMULTISIG = 0xae;
const MAX_SCRIPT_PUBKEY_BYTES = 10_000;

export function classifyScriptPubKey(
  script: Uint8Array,
  prefixes: DogecoinAddressPrefixes,
): DecodedDogecoinScriptPubKey {
  const hex = bytesToHex(script);
  const matched =
    matchPubkeyHash(script, prefixes) ??
    matchScriptHash(script, prefixes) ??
    matchPubkey(script, prefixes) ??
    matchNullData(script) ??
    matchMultisig(script, prefixes);
  if (!matched) {
    return { hex, type: 'nonstandard' };
  }

  return { hex, ...matched };
}

type ScriptMatch = Omit<DecodedDogecoinScriptPubKey, 'hex'>;

function matchPubkeyHash(
  script: Uint8Array,
  prefixes: DogecoinAddressPrefixes,
): ScriptMatch | null {
  const isMatch =
    script.length === 25 &&
    script[0] === OP_DUP &&
    script[1] === OP_HASH160 &&
    script[2] === 20 &&
    script[23] === OP_EQUALVERIFY &&
    script[24] === OP_CHECKSIG;
  if (!isMatch) {
    return null;
  }

  return {
    addresses: [encodeBase58Check(prefixes.pubkeyHash, script.subarray(3, 23))],
    reqSigs: 1,
    type: 'pubkeyhash',
  };
}

function matchScriptHash(
  script: Uint8Array,
  prefixes: DogecoinAddressPrefixes,
): ScriptMatch | null {
  const isMatch =
    script.length === 23 && script[0] === OP_HASH160 && script[1] === 20 && script[22] === OP_EQUAL;
  if (!isMatch) {
    return null;
  }

  return {
    addresses: [encodeBase58Check(prefixes.scriptHash, script.subarray(2, 22))],
    reqSigs: 1,
    type: 'scripthash',
  };
}

function matchPubkey(script: Uint8Array, prefixes: DogecoinAddressPrefixes): ScriptMatch | null {
  const pushLength = script[0];
  if (pushLength === undefined || script.length !== pushLength + 2) {
    return null;
  }
  if (script[script.length - 1] !== OP_CHECKSIG) {
    return null;
  }

  const pubkey = script.subarray(1, 1 + pushLength);
  if (!isPlausiblePubkey(pubkey)) {
    return null;
  }

  return {
    addresses: [encodeBase58Check(prefixes.pubkeyHash, hash160(pubkey))],
    reqSigs: 1,
    type: 'pubkey',
  };
}

function matchNullData(script: Uint8Array): ScriptMatch | null {
  if (script[0] !== OP_RETURN) {
    return null;
  }
  if (!isPushOnly(script.subarray(1))) {
    return null;
  }

  return { type: 'nulldata' };
}

function matchMultisig(script: Uint8Array, prefixes: DogecoinAddressPrefixes): ScriptMatch | null {
  const parsed = parseMultisig(script);
  if (!parsed) {
    return null;
  }

  return {
    addresses: parsed.pubkeys.map((pubkey) =>
      encodeBase58Check(prefixes.pubkeyHash, hash160(pubkey)),
    ),
    reqSigs: parsed.required,
    type: 'multisig',
  };
}

function parseMultisig(script: Uint8Array): { pubkeys: Uint8Array[]; required: number } | null {
  if (script.length < 3 || script[script.length - 1] !== OP_CHECKMULTISIG) {
    return null;
  }
  const required = smallIntegerOpcode(script[0]);
  const total = smallIntegerOpcode(script[script.length - 2]);
  if (required === null || total === null || required < 1 || required > total) {
    return null;
  }

  const pubkeys = readPushes(script.subarray(1, script.length - 2));
  if (!pubkeys || pubkeys.length !== total || !pubkeys.every(isPlausiblePubkey)) {
    return null;
  }

  return { pubkeys, required };
}

function smallIntegerOpcode(opcode: number | undefined): number | null {
  if (opcode === undefined) {
    return null;
  }
  if (opcode === OP_0) {
    return 0;
  }
  if (opcode >= OP_1 && opcode <= OP_16) {
    return opcode - OP_1 + 1;
  }

  return null;
}

function isPlausiblePubkey(pubkey: Uint8Array): boolean {
  const first = pubkey[0];
  if (pubkey.length === 33) {
    return first === 0x02 || first === 0x03;
  }
  if (pubkey.length === 65) {
    return first === 0x04 || first === 0x06 || first === 0x07;
  }

  return false;
}

function isPushOnly(script: Uint8Array): boolean {
  return readPushes(script) !== null;
}

/**
 * Reads a sequence of data pushes (including OP_0 and OP_1..OP_16 as pushes,
 * matching Bitcoin's IsPushOnly). Returns null on any non-push opcode or
 * truncated push.
 */
function readPushes(script: Uint8Array): Uint8Array[] | null {
  const pushes: Uint8Array[] = [];
  let offset = 0;
  while (offset < script.length) {
    const push = readPush(script, offset);
    if (!push) {
      return null;
    }
    pushes.push(push.data);
    offset = push.nextOffset;
  }
  return pushes;
}

function readPush(
  script: Uint8Array,
  offset: number,
): { data: Uint8Array; nextOffset: number } | null {
  const opcode = script[offset];
  if (opcode === undefined || opcode > OP_16) {
    return null;
  }
  if (opcode >= OP_1 || opcode === OP_0) {
    return { data: new Uint8Array(0), nextOffset: offset + 1 };
  }

  const push = pushLength(script, offset, opcode);
  if (!push || push.dataOffset + push.length > script.length) {
    return null;
  }

  return {
    data: script.subarray(push.dataOffset, push.dataOffset + push.length),
    nextOffset: push.dataOffset + push.length,
  };
}

function pushLength(
  script: Uint8Array,
  offset: number,
  opcode: number,
): { dataOffset: number; length: number } | null {
  if (opcode < OP_PUSHDATA1) {
    return { dataOffset: offset + 1, length: opcode };
  }
  if (opcode === OP_PUSHDATA1) {
    return pushLengthFromBytes(script, offset + 1, 1);
  }
  if (opcode === OP_PUSHDATA2) {
    return pushLengthFromBytes(script, offset + 1, 2);
  }
  if (opcode === OP_PUSHDATA4) {
    return pushLengthFromBytes(script, offset + 1, 4);
  }

  return null;
}

function pushLengthFromBytes(
  script: Uint8Array,
  offset: number,
  width: number,
): { dataOffset: number; length: number } | null {
  if (offset + width > script.length) {
    return null;
  }
  let length = 0;
  for (let index = width - 1; index >= 0; index -= 1) {
    length = length * 256 + (script[offset + index] ?? 0);
  }
  if (length > MAX_SCRIPT_PUBKEY_BYTES) {
    return null;
  }

  return { dataOffset: offset + width, length };
}

const BASE58_ALPHABET = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';

export function encodeBase58Check(version: number, payload: Uint8Array): string {
  const body = new Uint8Array(1 + payload.length);
  body[0] = version;
  body.set(payload, 1);
  const checksum = sha256dBytes(body).subarray(0, 4);
  const full = new Uint8Array(body.length + 4);
  full.set(body, 0);
  full.set(checksum, body.length);
  return encodeBase58(full);
}

function encodeBase58(bytes: Uint8Array): string {
  let leadingZeros = 0;
  while (leadingZeros < bytes.length && bytes[leadingZeros] === 0) {
    leadingZeros += 1;
  }

  let value = 0n;
  for (const byte of bytes) {
    value = value * 256n + BigInt(byte);
  }

  let encoded = '';
  while (value > 0n) {
    const remainder = Number(value % 58n);
    value /= 58n;
    encoded = BASE58_ALPHABET[remainder] + encoded;
  }

  return '1'.repeat(leadingZeros) + encoded;
}

function hash160(bytes: Uint8Array): Uint8Array {
  const sha = createHash('sha256').update(bytes).digest();
  return new Uint8Array(createHash('ripemd160').update(sha).digest());
}

function sha256dBytes(bytes: Uint8Array): Uint8Array {
  const first = createHash('sha256').update(bytes).digest();
  return new Uint8Array(createHash('sha256').update(first).digest());
}

/** Double-SHA256 rendered as Bitcoin-style reversed (display order) hex. */
function sha256d(bytes: Uint8Array): string {
  return bytesToHex(sha256dBytes(bytes).reverse());
}

class ByteReader {
  public offset = 0;

  public constructor(private readonly bytes: Uint8Array) {}

  public peek(length: number): Uint8Array {
    this.assertAvailable(length);
    return this.bytes.subarray(this.offset, this.offset + length);
  }

  public read(length: number): Uint8Array {
    const value = this.peek(length);
    this.offset += length;
    return value;
  }

  public skip(length: number): void {
    this.assertAvailable(length);
    this.offset += length;
  }

  public slice(start: number, end: number): Uint8Array {
    return this.bytes.subarray(start, end);
  }

  public readUint32LE(): number {
    const bytes = this.read(4);
    return (
      ((bytes[0] ?? 0) |
        ((bytes[1] ?? 0) << 8) |
        ((bytes[2] ?? 0) << 16) |
        ((bytes[3] ?? 0) << 24)) >>>
      0
    );
  }

  public readInt32LE(): number {
    return this.readUint32LE() | 0;
  }

  public readInt64LE(): bigint {
    const low = BigInt(this.readUint32LE());
    const high = BigInt(this.readUint32LE() | 0);
    return (high << 32n) + low;
  }

  public readCompactSize(): number {
    const first = this.read(1)[0] ?? 0;
    if (first < 0xfd) {
      return first;
    }
    if (first === 0xfd) {
      const bytes = this.read(2);
      return (bytes[0] ?? 0) | ((bytes[1] ?? 0) << 8);
    }
    if (first === 0xfe) {
      return this.readUint32LE();
    }

    throw invalidPayload('compact size exceeds 32 bits');
  }

  /** Reads a 32-byte hash and renders it in display (reversed) order. */
  public readHash(): string {
    return bytesToHex(Uint8Array.from(this.read(32)).reverse());
  }

  public assertConsumed(): void {
    if (this.offset !== this.bytes.length) {
      throw invalidPayload(`trailing ${this.bytes.length - this.offset} bytes`);
    }
  }

  private assertAvailable(length: number): void {
    if (length < 0 || this.offset + length > this.bytes.length) {
      throw invalidPayload(`read past end at offset ${this.offset}`);
    }
  }
}

function invalidPayload(detail: string): InfrastructureError {
  return new InfrastructureError(`invalid dogecoin raw block payload: ${detail}`);
}

function hexToBytes(hex: string): Uint8Array {
  const trimmed = hex.trim();
  if (trimmed.length === 0 || trimmed.length % 2 !== 0 || !/^[0-9a-fA-F]+$/u.test(trimmed)) {
    throw invalidPayload('not a hex string');
  }

  return new Uint8Array(Buffer.from(trimmed, 'hex'));
}

function bytesToHex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('hex');
}

function errorMessage(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  return message.replace(/^invalid dogecoin raw block payload: /u, '');
}
