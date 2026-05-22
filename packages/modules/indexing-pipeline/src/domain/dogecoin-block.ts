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
  const direct = output.scriptPubKey?.address?.trim();
  if (direct) {
    return direct;
  }

  const first = output.scriptPubKey?.addresses?.find((value) => value.trim());
  return first?.trim() ?? '';
}

export function isDogecoinTransaction(value: unknown): value is DogecoinTransaction {
  return typeof value === 'object' && value !== null;
}
