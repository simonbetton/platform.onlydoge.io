import {
  type DogecoinTransaction,
  type DogecoinVout,
  extractDogecoinOutputAddress,
  fromDecimalUnits,
} from '@onlydoge/indexing-pipeline';

import type { MempoolAppearOutput } from './mempool-watch-types';

export function matchingOutputsForAddress(
  transaction: DogecoinTransaction,
  address: string,
  minValueBase: string | null,
): MempoolAppearOutput[] | null {
  const outputs = collectMatchingOutputs(transaction, address);
  if (outputs.length === 0) {
    return null;
  }

  if (minValueBase === null) {
    return outputs;
  }

  return sumValueBase(outputs) >= BigInt(minValueBase) ? outputs : null;
}

function collectMatchingOutputs(
  transaction: DogecoinTransaction,
  address: string,
): MempoolAppearOutput[] {
  const outputs: MempoolAppearOutput[] = [];
  for (const [index, output] of (transaction.vout ?? []).entries()) {
    const outputAddress = extractDogecoinOutputAddress(output);
    if (outputAddress !== address) {
      continue;
    }

    outputs.push({
      vout: output.n ?? index,
      valueBase: outputValueBase(output),
    });
  }

  return outputs;
}

function outputValueBase(output: DogecoinVout): string {
  if (output.value === undefined || output.value === null) {
    return '0';
  }

  return fromDecimalUnits(output.value, 8);
}

function sumValueBase(outputs: MempoolAppearOutput[]): bigint {
  return outputs.reduce((total, output) => total + BigInt(output.valueBase), 0n);
}
