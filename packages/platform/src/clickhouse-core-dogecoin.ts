export interface ClickHouseStringRange {
  end: string | null;
  start: string | null;
}

export const clickHouseCoreDogecoinTables = {
  appliedBlocks: 'applied_blocks_v2',
  balances: 'balances_v2',
  coreProcessedBlocks: 'core_processed_blocks_v1',
  coreUtxoCreates: 'core_utxo_creates_v1',
  coreUtxoSpends: 'core_utxo_spends_v1',
  currentUtxos: 'utxo_outputs_current_v2',
  currentUtxosByAddress: 'utxo_outputs_current_by_address_v2',
} as const;

export function buildCoreCurrentStateOutputKeyRanges(): ClickHouseStringRange[] {
  const ranges: ClickHouseStringRange[] = [{ start: null, end: '00' }];

  for (let value = 0; value <= 0xff; value += 1) {
    ranges.push({
      start: value.toString(16).padStart(2, '0'),
      end: value === 0xff ? 'g' : (value + 1).toString(16).padStart(2, '0'),
    });
  }

  ranges.push({ start: 'g', end: null });
  return ranges;
}

export function clickHouseStringRangeClause(column: string, range: ClickHouseStringRange): string {
  const clauses: string[] = [];
  if (range.start !== null) {
    clauses.push(`AND ${column} >= {rangeStart:String}`);
  }
  if (range.end !== null) {
    clauses.push(`AND ${column} < {rangeEnd:String}`);
  }
  return clauses.join('\n');
}

export function clickHouseStringRangeParams(range: ClickHouseStringRange): Record<string, string> {
  return {
    ...(range.start === null ? {} : { rangeStart: range.start }),
    ...(range.end === null ? {} : { rangeEnd: range.end }),
  };
}
