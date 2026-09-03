import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import {
  classifyScriptPubKey,
  decodeDogecoinRawBlock,
  dogecoinMainnetAddressPrefixes,
  formatKoinu,
} from '@onlydoge/platform';
import { describe, expect, it } from 'vitest';

const fixturesDir = join(import.meta.dirname, '../fixtures/dogecoin-blocks');

/**
 * Mainnet vectors captured from Dogecoin Core 1.14.6 (`getblock hash false`
 * plus verbose `getrawtransaction` for every tx). Covers pre-AuxPoW blocks,
 * the first AuxPoW block (371337), genesis (coinbase absent from txindex), and
 * modern multi-tx blocks with P2PKH, P2SH, P2PK and OP_RETURN outputs.
 */
const vectorHeights = [0, 1, 371337, 371338, 1_000_000, 4_000_000, 6_358_000];

interface ExpectedBlock {
  auxpow?: { parentblock: string };
  bits: string;
  hash: string;
  merkleroot: string;
  nonce: number;
  previousblockhash?: string;
  size: number;
  time: number;
  tx: Array<{
    genesisMissing?: boolean;
    locktime?: number;
    size?: number;
    txid: string;
    version?: number;
    vin?: Array<
      | { coinbase: string; sequence: number }
      | { scriptSig: string; sequence: number; txid: string; vout: number }
    >;
    vout?: Array<{
      n: number;
      scriptPubKey: { addresses?: string[]; hex: string; reqSigs?: number; type: string };
      value: string;
    }>;
  }>;
  version: number;
}

describe('dogecoin raw block decoder', () => {
  for (const height of vectorHeights) {
    it(`decodes mainnet block ${height} identically to Dogecoin Core`, () => {
      const hex = readFileSync(join(fixturesDir, `${height}.hex`), 'utf8');
      const expected: ExpectedBlock = JSON.parse(
        readFileSync(join(fixturesDir, `${height}.expected.json`), 'utf8'),
      );

      const block = decodeDogecoinRawBlock(hex, height);

      expect(block).toMatchObject({
        bits: expected.bits,
        hash: expected.hash,
        height,
        merkleroot: expected.merkleroot,
        nTx: expected.tx.length,
        nonce: expected.nonce,
        size: expected.size,
        time: expected.time,
        version: expected.version,
      });
      expect(block.previousblockhash).toBe(expected.previousblockhash);
      expect(block.auxpow !== undefined).toBe(expected.auxpow !== undefined);

      for (const [index, expectedTx] of expected.tx.entries()) {
        const tx = block.tx[index];
        expect(tx?.txid, `tx ${index} txid`).toBe(expectedTx.txid);
        if (expectedTx.genesisMissing) {
          continue;
        }

        expect(tx).toMatchObject({
          hash: expectedTx.txid,
          locktime: expectedTx.locktime,
          size: expectedTx.size,
          version: expectedTx.version,
        });
        expect(
          tx?.vin.map((input) =>
            input.coinbase !== undefined
              ? { coinbase: input.coinbase, sequence: input.sequence }
              : {
                  scriptSig: input.scriptSig?.hex,
                  sequence: input.sequence,
                  txid: input.txid,
                  vout: input.vout,
                },
          ),
        ).toEqual(expectedTx.vin);
        expect(
          tx?.vout.map((output) => ({
            n: output.n,
            scriptPubKey: stripUndefined(output.scriptPubKey),
            value: output.value,
          })),
        ).toEqual(
          expectedTx.vout?.map((output) => ({
            ...output,
            scriptPubKey: stripUndefined(output.scriptPubKey),
          })),
        );
      }
    });
  }

  it('recovers the merged-mining parent block hash from the AuxPoW payload', () => {
    const hex = readFileSync(join(fixturesDir, '371337.hex'), 'utf8');
    const block = decodeDogecoinRawBlock(hex, 371337);
    expect(block.auxpow?.parentBlockHash).toBe(
      '45df41e40aba5b2a03d08bd1202a1c02ef3954d8aa22ea6c5ae62fd00f290ea9',
    );
  });

  it('classifies bare multisig, nulldata and nonstandard scripts like Core', () => {
    const pubkeyA = `02${'11'.repeat(32)}`;
    const pubkeyB = `03${'22'.repeat(32)}`;
    const multisig = Buffer.from(`5121${pubkeyA}21${pubkeyB}52ae`, 'hex');
    const classified = classifyScriptPubKey(multisig, dogecoinMainnetAddressPrefixes);
    expect(classified.type).toBe('multisig');
    expect(classified.reqSigs).toBe(1);
    expect(classified.addresses).toHaveLength(2);
    expect(classified.addresses?.every((address) => address.startsWith('D'))).toBe(true);

    expect(
      classifyScriptPubKey(Buffer.from('6a04deadbeef', 'hex'), dogecoinMainnetAddressPrefixes),
    ).toEqual({ hex: '6a04deadbeef', type: 'nulldata' });

    expect(classifyScriptPubKey(Buffer.from('51', 'hex'), dogecoinMainnetAddressPrefixes)).toEqual({
      hex: '51',
      type: 'nonstandard',
    });

    // Truncated P2PKH must not be misread as pubkeyhash.
    expect(
      classifyScriptPubKey(Buffer.from('76a914', 'hex'), dogecoinMainnetAddressPrefixes).type,
    ).toBe('nonstandard');
  });

  it('formats koinu amounts beyond 2^53 exactly', () => {
    expect(formatKoinu(0n)).toBe('0.00000000');
    expect(formatKoinu(1n)).toBe('0.00000001');
    expect(formatKoinu(6_250_500_000_001n)).toBe('62505.00000001');
    expect(formatKoinu(123_456_789_012_345_678_901n)).toBe('1234567890123.45678901');
  });

  it('rejects malformed payloads instead of producing partial blocks', () => {
    const hex = readFileSync(join(fixturesDir, '1.hex'), 'utf8').trim();
    expect(() => decodeDogecoinRawBlock('zz', 1)).toThrow('invalid dogecoin raw block payload');
    expect(() => decodeDogecoinRawBlock(hex.slice(0, -8), 1)).toThrow(
      'invalid dogecoin raw block payload',
    );
    expect(() => decodeDogecoinRawBlock(`${hex}00`, 1)).toThrow('trailing 1 bytes');
  });
});

function stripUndefined(value: object): Record<string, unknown> {
  return Object.fromEntries(Object.entries(value).filter(([, entry]) => entry !== undefined));
}
