import { createHash } from 'node:crypto';
import { ValidationError } from '@onlydoge/shared-kernel';
import { createBase58check } from '@scure/base';
import { HDKey } from '@scure/bip32';

import type { WarehouseAddressSummary } from './explorer-response-builders';

export type HdWalletChainRole = 'receive' | 'change';

export interface HdWalletAddressSummary {
  address: string;
  balance: string;
  chain: number;
  index: number;
  path: string;
  receivedBase: string;
  role: HdWalletChainRole;
  sentBase: string;
  txCount: number;
  utxoCount: number;
}

export interface HdWalletChainSummary {
  balanceBase: string;
  chain: number;
  complete: boolean;
  gapLimit: number;
  lastScannedIndex: number | null;
  lastUsedIndex: number | null;
  nextUnusedIndex: number;
  role: HdWalletChainRole;
  scannedAddressCount: number;
  usedAddressCount: number;
  usedAddresses: HdWalletAddressSummary[];
  utxoCount: number;
}

export interface HdWalletScanRequest {
  chains?: number[];
  gapLimit?: number;
  xpub: string;
}

export interface NormalizedHdWalletScanRequest {
  chains: number[];
  gapLimit: number;
  xpub: string;
}

export interface HdWalletScanResult {
  balanceBase: string;
  chains: HdWalletChainSummary[];
  complete: boolean;
  gapLimit: number;
  scannedAddressCount: number;
  usedAddressCount: number;
  utxoCount: number;
  xpubDepth: number;
  xpubFingerprint: number;
}

export type HdWalletAddressSummaryReader = (
  address: string,
) => Promise<WarehouseAddressSummary | null>;

const dogecoinP2pkhVersion = 0x1e;
const defaultGapLimit = 20;
const minimumGapLimit = 20;
const maximumGapLimit = 200;
const maximumNonHardenedChildIndex = 0x80000000;
const scanConcurrency = 8;
const base58check = createBase58check(sha256);

export async function scanHdWalletBalance(
  input: HdWalletScanRequest,
  readAddressSummary: HdWalletAddressSummaryReader,
): Promise<HdWalletScanResult> {
  const key = requireAccountXpub(input.xpub);
  const gapLimit = normalizeIntegerOption(input.gapLimit, {
    defaultValue: defaultGapLimit,
    field: 'gapLimit',
    maximum: maximumGapLimit,
    minimum: minimumGapLimit,
  });
  const chains = normalizeChains(input.chains);
  const chainSummaries = await Promise.all(
    chains.map((chain) =>
      scanHdWalletChain({
        chain,
        gapLimit,
        key,
        readAddressSummary,
      }),
    ),
  );

  return {
    balanceBase: sumBase(chainSummaries.map((chain) => chain.balanceBase)),
    chains: chainSummaries,
    complete: chainSummaries.every((chain) => chain.complete),
    gapLimit,
    scannedAddressCount: chainSummaries.reduce((sum, chain) => sum + chain.scannedAddressCount, 0),
    usedAddressCount: chainSummaries.reduce((sum, chain) => sum + chain.usedAddressCount, 0),
    utxoCount: chainSummaries.reduce((sum, chain) => sum + chain.utxoCount, 0),
    xpubDepth: key.depth,
    xpubFingerprint: key.fingerprint,
  };
}

export function deriveDogecoinP2pkhAddress(
  accountKey: HDKey,
  chain: number,
  index: number,
): string {
  assertChain(chain);
  assertChildIndex(index, 'index');

  const publicKey = accountKey.derive(`m/${chain}/${index}`).publicKey;
  if (!publicKey) {
    throw new ValidationError('invalid parameter for `xpub`: missing public key');
  }

  return dogecoinP2pkhAddress(publicKey);
}

export function hdWalletCacheKey(input: {
  chains: number[];
  gapLimit: number;
  network: string;
  xpub: string;
}): string {
  return createHash('sha256')
    .update(
      JSON.stringify({
        chains: input.chains,
        gapLimit: input.gapLimit,
        network: input.network,
        xpub: input.xpub.trim(),
      }),
    )
    .digest('hex');
}

export function normalizeHdWalletRequest(
  input: HdWalletScanRequest,
): NormalizedHdWalletScanRequest {
  const gapLimit = normalizeIntegerOption(input.gapLimit, {
    defaultValue: defaultGapLimit,
    field: 'gapLimit',
    maximum: maximumGapLimit,
    minimum: minimumGapLimit,
  });

  return {
    chains: normalizeChains(input.chains),
    gapLimit,
    xpub: requireNonEmptyXpub(input.xpub),
  };
}

async function scanHdWalletChain(input: {
  chain: number;
  gapLimit: number;
  key: HDKey;
  readAddressSummary: HdWalletAddressSummaryReader;
}): Promise<HdWalletChainSummary> {
  const role = chainRole(input.chain);
  const usedAddresses: HdWalletAddressSummary[] = [];
  let balanceBase = 0n;
  let consecutiveUnused = 0;
  let index = 0;
  let lastUsedIndex: number | null = null;
  let utxoCount = 0;

  while (index < maximumNonHardenedChildIndex && consecutiveUnused < input.gapLimit) {
    const windowSize = Math.min(input.gapLimit, maximumNonHardenedChildIndex - index);
    const candidates = Array.from({ length: windowSize }, (_, offset) => {
      const candidateIndex = index + offset;
      return {
        address: deriveDogecoinP2pkhAddress(input.key, input.chain, candidateIndex),
        index: candidateIndex,
      };
    });
    const summaries = await mapWithConcurrency(candidates, scanConcurrency, async (candidate) => ({
      ...candidate,
      summary: await input.readAddressSummary(candidate.address),
    }));

    for (const candidate of summaries) {
      index = candidate.index + 1;
      if (!isUsedAddress(candidate.summary)) {
        consecutiveUnused += 1;
        if (consecutiveUnused >= input.gapLimit) {
          break;
        }
        continue;
      }

      consecutiveUnused = 0;
      lastUsedIndex = candidate.index;
      balanceBase += BigInt(candidate.summary.balance);
      utxoCount += candidate.summary.utxoCount;
      usedAddresses.push({
        address: candidate.address,
        balance: candidate.summary.balance,
        chain: input.chain,
        index: candidate.index,
        path: `m/${input.chain}/${candidate.index}`,
        receivedBase: candidate.summary.receivedBase,
        role,
        sentBase: candidate.summary.sentBase,
        txCount: candidate.summary.txCount,
        utxoCount: candidate.summary.utxoCount,
      });
    }
  }

  return {
    balanceBase: balanceBase.toString(),
    chain: input.chain,
    complete: consecutiveUnused >= input.gapLimit,
    gapLimit: input.gapLimit,
    lastScannedIndex: index > 0 ? index - 1 : null,
    lastUsedIndex,
    nextUnusedIndex: lastUsedIndex === null ? 0 : lastUsedIndex + 1,
    role,
    scannedAddressCount: index,
    usedAddressCount: usedAddresses.length,
    usedAddresses,
    utxoCount,
  };
}

async function mapWithConcurrency<TInput, TOutput>(
  values: TInput[],
  concurrency: number,
  mapper: (value: TInput) => Promise<TOutput>,
): Promise<TOutput[]> {
  const results = new Array<TOutput>(values.length);
  let nextIndex = 0;

  await Promise.all(
    Array.from({ length: Math.min(concurrency, values.length) }, async () => {
      while (nextIndex < values.length) {
        const currentIndex = nextIndex;
        nextIndex += 1;
        results[currentIndex] = await mapper(values[currentIndex] as TInput);
      }
    }),
  );

  return results;
}

function requireAccountXpub(input: string): HDKey {
  const xpub = requireNonEmptyXpub(input);
  try {
    const key = HDKey.fromExtendedKey(xpub);
    if (key.privateKey) {
      throw new ValidationError('invalid parameter for `xpub`: private extended keys are rejected');
    }
    if (!key.publicKey || !key.chainCode) {
      throw new ValidationError('invalid parameter for `xpub`: missing public key');
    }
    return key;
  } catch (error) {
    if (error instanceof ValidationError) {
      throw error;
    }
    throw new ValidationError('invalid parameter for `xpub`');
  }
}

function requireNonEmptyXpub(input: string): string {
  const xpub = input?.trim();
  if (!xpub) {
    throw new ValidationError('missing parameter for `xpub`');
  }
  return xpub;
}

function normalizeIntegerOption(
  value: number | undefined,
  options: {
    defaultValue: number;
    field: string;
    maximum: number;
    minimum: number;
  },
): number {
  const resolved = value ?? options.defaultValue;
  if (!Number.isSafeInteger(resolved)) {
    throw new ValidationError(`invalid parameter for \`${options.field}\`: ${resolved}`);
  }
  if (resolved < options.minimum) {
    return options.minimum;
  }
  if (resolved > options.maximum) {
    throw new ValidationError(
      `invalid parameter for \`${options.field}\`: maximum ${options.maximum}`,
    );
  }
  return resolved;
}

function normalizeChains(input: number[] | undefined): number[] {
  if (!input || input.length === 0) {
    return [0, 1];
  }

  const chains = [...new Set(input)];
  for (const chain of chains) {
    assertChain(chain);
  }
  return chains.sort((left, right) => left - right);
}

function assertChain(chain: number): void {
  if (!Number.isInteger(chain) || (chain !== 0 && chain !== 1)) {
    throw new ValidationError(`invalid parameter for \`chains\`: ${chain}`);
  }
}

function assertChildIndex(index: number, field: string): void {
  if (!Number.isInteger(index) || index < 0 || index >= 0x80000000) {
    throw new ValidationError(`invalid parameter for \`${field}\`: ${index}`);
  }
}

function chainRole(chain: number): HdWalletChainRole {
  return chain === 1 ? 'change' : 'receive';
}

function isUsedAddress(
  summary: WarehouseAddressSummary | null,
): summary is WarehouseAddressSummary {
  return (
    summary !== null &&
    (summary.balance !== '0' ||
      summary.receivedBase !== '0' ||
      summary.sentBase !== '0' ||
      summary.txCount > 0 ||
      summary.utxoCount > 0)
  );
}

function sumBase(values: string[]): string {
  return values.reduce((sum, value) => sum + BigInt(value), 0n).toString();
}

function dogecoinP2pkhAddress(publicKey: Uint8Array): string {
  return base58check.encode(concatBytes(Uint8Array.of(dogecoinP2pkhVersion), hash160(publicKey)));
}

function hash160(input: Uint8Array): Uint8Array {
  return ripemd160(sha256(input));
}

function sha256(input: Uint8Array): Uint8Array {
  return createHash('sha256').update(input).digest();
}

function ripemd160(input: Uint8Array): Uint8Array {
  return createHash('ripemd160').update(input).digest();
}

function concatBytes(...parts: Uint8Array[]): Uint8Array {
  const length = parts.reduce((sum, part) => sum + part.length, 0);
  const output = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) {
    output.set(part, offset);
    offset += part.length;
  }
  return output;
}
