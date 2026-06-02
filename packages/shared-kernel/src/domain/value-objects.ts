import { randomBytes, randomUUID, scryptSync } from 'node:crypto';
import { homedir } from 'node:os';
import { resolve } from 'node:path';

import { ValidationError } from './errors';

export type PrimaryId = number;

export type IdPrefix = 'key';

export type ChainFamily = 'dogecoin';

export type Mode = 'both' | 'indexer' | 'http';

const prefixSet = new Set<IdPrefix>(['key']);
const modeSet = new Set<Mode>(['both', 'indexer', 'http']);
const apiSecretLookupNamespace = 'onlydoge-api-token-lookup-v1';

function isIdPrefix(value: string | undefined): value is IdPrefix {
  return value !== undefined ? prefixSet.has(value as IdPrefix) : false;
}

function isChainFamily(value: string): value is ChainFamily {
  return value === 'dogecoin';
}

function isMode(value: string): value is Mode {
  return modeSet.has(value as Mode);
}

export class ExternalId {
  public readonly value: string;
  public readonly prefix: IdPrefix;

  private constructor(value: string, prefix: IdPrefix) {
    this.value = value;
    this.prefix = prefix;
  }

  public static create(prefix: IdPrefix, raw?: string): ExternalId {
    const suffix = raw?.trim() ? raw.trim() : randomUUID().replaceAll('-', '').slice(0, 24);
    const value = `${prefix}_${suffix}`;

    return ExternalId.parse(value, prefix);
  }

  public static parse(value: string, expectedPrefix?: IdPrefix): ExternalId {
    const trimmed = value.trim();
    const [prefix, suffix] = trimmed.split('_');
    const idPrefix = requireIdPrefix(prefix, value);

    assertExpectedPrefix(idPrefix, expectedPrefix, value);
    assertValidIdSuffix(suffix, value);

    return new ExternalId(trimmed, idPrefix);
  }
}

function requireIdPrefix(value: string | undefined, raw: string): IdPrefix {
  if (!isIdPrefix(value)) {
    throw new ValidationError(`invalid id prefix: ${raw}`);
  }

  return value;
}

function assertExpectedPrefix(
  prefix: IdPrefix,
  expectedPrefix: IdPrefix | undefined,
  raw: string,
): void {
  if (!hasExpectedPrefix(prefix, expectedPrefix)) {
    throw new ValidationError(`invalid parameter for \`id\`: ${raw}`);
  }
}

function hasExpectedPrefix(prefix: IdPrefix, expectedPrefix: IdPrefix | undefined): boolean {
  return expectedPrefix === undefined ? true : prefix === expectedPrefix;
}

function assertValidIdSuffix(value: string | undefined, raw: string): void {
  assertIdSuffixPresent(value, raw);
  assertIdSuffixLength(value, raw);
  assertIdSuffixCharacters(value, raw);
}

function assertIdSuffixPresent(value: string | undefined, raw: string): asserts value is string {
  if (!value) {
    throw new ValidationError(`invalid parameter for \`id\`: ${raw}`);
  }
}

function assertIdSuffixLength(value: string, raw: string): void {
  if (value.length > 32) {
    throw new ValidationError(`invalid parameter for \`id\`: ${raw}`);
  }
}

function assertIdSuffixCharacters(value: string, raw: string): void {
  if (!/^[A-Za-z0-9]+$/u.test(value)) {
    throw new ValidationError(`invalid parameter for \`id\`: ${raw}`);
  }
}

export class RpcEndpoint {
  public readonly value: string;

  private constructor(value: string) {
    this.value = value;
  }

  public static parse(value: string): RpcEndpoint {
    const trimmed = value.trim();

    try {
      const url = new URL(trimmed);
      assertRpcProtocol(url, value);
      return new RpcEndpoint(trimmed);
    } catch (_error) {
      throw new ValidationError(`invalid RPC endpoint: ${value}`);
    }
  }
}

function assertRpcProtocol(url: URL, value: string): void {
  if (!['http:', 'https:'].includes(url.protocol)) {
    throw new ValidationError(`invalid RPC endpoint: ${value}`);
  }
}

export class BlockchainAddress {
  public readonly value: string;

  private constructor(value: string) {
    this.value = value;
  }
}

export class BlockHeight {
  public readonly value: number;

  private constructor(value: number) {
    this.value = value;
  }
}

export class BlockTime {
  public readonly value: number;

  private constructor(value: number) {
    this.value = value;
  }

  public static parse(value: number): BlockTime {
    if (!isPositiveInteger(value)) {
      throw new ValidationError(`invalid block time: ${value}`);
    }

    return new BlockTime(value);
  }
}

function isPositiveInteger(value: number): boolean {
  return [Number.isInteger(value), value > 0].every(Boolean);
}

export class ApiSecret {
  public readonly value: string;
  public readonly hash: string;

  private constructor(value: string, hash: string) {
    this.value = value;
    this.hash = hash;
  }

  public static generate(): ApiSecret {
    const token = randomBytes(24).toString('hex');

    return new ApiSecret(`sk_${token}`, hashApiSecretLookupKey(token));
  }

  public static hashFromToken(apiToken: string): string {
    const token = apiToken.split('_').at(-1) ?? apiToken;

    return hashApiSecretLookupKey(token);
  }
}

export function maskRpcEndpointAuth(endpoint: RpcEndpoint | string): string {
  const url = new URL(typeof endpoint === 'string' ? endpoint : endpoint.value);
  maskUrlUsername(url);
  maskUrlPassword(url);

  return url.toString();
}

function maskUrlUsername(url: URL): void {
  if (url.username) {
    url.username = '***';
  }
}

function maskUrlPassword(url: URL): void {
  if (url.password) {
    url.password = '***';
  }
}

export function parseMode(input: string | undefined): Mode {
  const value = modeInputValue(input);
  if (!isMode(value)) {
    throw new ValidationError(`invalid mode: ${input}`);
  }

  return value;
}

function modeInputValue(input: string | undefined): string {
  return input?.trim().toLowerCase() ?? 'both';
}

export function parseChainFamily(input: string): ChainFamily {
  const value = input.trim().toLowerCase();
  if (!isChainFamily(value)) {
    throw new ValidationError(`invalid architecture: ${input}`);
  }

  return value;
}

export function parseNonNegativeInteger(value: string | undefined): number | undefined {
  if (!value) {
    return undefined;
  }

  const parsed = Number(value);
  return nonNegativeIntegerOrUndefined(parsed);
}

function nonNegativeIntegerOrUndefined(value: number): number | undefined {
  if (!isNonNegativeInteger(value)) {
    return undefined;
  }

  return value;
}

function isNonNegativeInteger(value: number): boolean {
  return [Number.isInteger(value), value >= 0].every(Boolean);
}

export function nowIsoString(): string {
  return new Date().toISOString();
}

export function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolvePromise) => {
    setTimeout(resolvePromise, milliseconds);
  });
}

function hashApiSecretLookupKey(value: string): string {
  return scryptSync(value, apiSecretLookupNamespace, 32).toString('hex');
}

export function expandHomePath(value: string): string {
  const homePlaceholder = '${' + 'HOME}';
  if (value.startsWith('~/')) {
    return resolve(homedir(), value.slice(2));
  }

  return value.replaceAll(homePlaceholder, homedir()).replaceAll('$HOME', homedir());
}

export function safeJsonParse<T>(value: string, fallback: T): T {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}
