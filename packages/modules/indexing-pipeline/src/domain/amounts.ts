import { ValidationError } from '@onlydoge/shared-kernel';

type DecimalUnitMatch = RegExpMatchArray & {
  1: string;
  2: string;
  3?: string;
};

export function parseAmountBase(value: string): bigint {
  const trimmed = value.trim();
  if (!/^-?\d+$/u.test(trimmed)) {
    throw new ValidationError(`invalid amount base: ${value}`);
  }

  return BigInt(trimmed);
}

export function formatAmountBase(value: bigint): string {
  return value.toString();
}

export function addAmountBase(left: string, right: string): string {
  return formatAmountBase(parseAmountBase(left) + parseAmountBase(right));
}

export function subtractAmountBase(left: string, right: string): string {
  return formatAmountBase(parseAmountBase(left) - parseAmountBase(right));
}

export function allocateProRataAmount(
  target: bigint,
  inputAddresses: string[],
  inputTotals: Map<string, bigint>,
  totalInput: bigint,
): Array<{ address: string; amount: bigint }> {
  if (!canAllocateProRata(inputAddresses, totalInput)) {
    return [];
  }

  const allocations: Array<{ address: string; amount: bigint }> = [];
  let allocated = 0n;
  for (const [index, address] of inputAddresses.entries()) {
    if (index === inputAddresses.length - 1) {
      allocations.push({
        address,
        amount: target - allocated,
      });
      continue;
    }

    const contribution = amountContribution(inputTotals, address);
    const amount = (target * contribution) / totalInput;
    allocations.push({ address, amount });
    allocated += amount;
  }

  return allocations;
}

function canAllocateProRata(inputAddresses: string[], totalInput: bigint): boolean {
  return totalInput > 0n && inputAddresses.length > 0;
}

function amountContribution(inputTotals: Map<string, bigint>, address: string): bigint {
  return inputTotals.get(address) ?? 0n;
}

export function fromDecimalUnits(value: number | string, decimals: number): string {
  const match = requireDecimalUnitMatch(decimalUnitSource(value, decimals), value);
  const [, sign, whole, fraction = ''] = match;

  assertDecimalFractionPrecision(fraction, decimals, value);
  return formatDecimalUnitParts(sign, whole, fraction, decimals);
}

function decimalUnitSource(value: number | string, decimals: number): string {
  return typeof value === 'number' ? value.toFixed(decimals) : value.trim();
}

function requireDecimalUnitMatch(value: string, raw: number | string): DecimalUnitMatch {
  const match = value.match(/^(-?)(\d+)(?:\.(\d+))?$/u);
  if (!match) {
    throw new ValidationError(`invalid decimal amount: ${raw}`);
  }

  return match as DecimalUnitMatch;
}

function assertDecimalFractionPrecision(
  fraction: string,
  decimals: number,
  raw: number | string,
): void {
  if (fraction.length > decimals && /[^0]/u.test(fraction.slice(decimals))) {
    throw new ValidationError(`invalid decimal amount: ${raw}`);
  }
}

function formatDecimalUnitParts(
  sign: string,
  whole: string,
  fraction: string,
  decimals: number,
): string {
  const paddedFraction = fraction.slice(0, decimals).padEnd(decimals, '0');
  const normalized = `${whole}${paddedFraction}`.replace(/^0+(?=\d)/u, '') || '0';
  return `${sign}${normalized}`;
}

export function fromHexUnits(value: string): string {
  const normalized = value.trim().startsWith('0x') ? value.trim() : `0x${value.trim()}`;
  return formatAmountBase(BigInt(normalized));
}
