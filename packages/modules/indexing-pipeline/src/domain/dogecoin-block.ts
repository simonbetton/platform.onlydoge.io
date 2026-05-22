export interface DogecoinVin {
  coinbase?: string;
  txid?: string;
  vout?: number;
}

export interface DogecoinVout {
  n?: number;
  value?: number | string;
  scriptPubKey?: {
    address?: string;
    addresses?: string[];
    type?: string;
  };
}

export interface DogecoinTransaction {
  txid?: string;
  vin?: DogecoinVin[];
  vout?: DogecoinVout[];
}

export interface ParsedDogecoinBlock {
  hash: string;
  height: number;
  time: number;
  tx: DogecoinTransaction[];
}

export function extractDogecoinOutputAddress(output: DogecoinVout): string {
  return firstTrimmedText(outputAddressCandidates(output.scriptPubKey));
}

export function isDogecoinTransaction(value: unknown): value is DogecoinTransaction {
  return isObjectLike(value);
}

function firstTrimmedText(values: Array<string | undefined>): string {
  return values.map(trimOptionalText).find(hasText) ?? '';
}

function outputAddressCandidates(script: DogecoinVout['scriptPubKey']): Array<string | undefined> {
  return [script?.address].concat(outputAddressList(script));
}

function outputAddressList(script: DogecoinVout['scriptPubKey']): string[] {
  if (!script) {
    return [];
  }

  return knownOutputAddressList(script);
}

function knownOutputAddressList(script: NonNullable<DogecoinVout['scriptPubKey']>): string[] {
  return script.addresses ?? [];
}

function trimOptionalText(value: string | undefined): string {
  return value?.trim() ?? '';
}

function hasText(value: string): boolean {
  return value.length > 0;
}

function isObjectLike(value: unknown): value is object {
  return [typeof value === 'object', value !== null].every(Boolean);
}
