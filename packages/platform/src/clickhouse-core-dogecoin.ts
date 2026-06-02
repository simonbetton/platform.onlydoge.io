export interface ClickHouseStringRange {
  end: string | null;
  start: string | null;
}

export const clickHouseCoreDogecoinTables = {
  appliedBlocks: 'dogecoin_applied_blocks_v1',
  balances: 'dogecoin_balances_current_v1',
  coreProcessedBlocks: 'dogecoin_core_processed_blocks_v1',
  coreUtxoCreates: 'dogecoin_core_utxo_creates_v1',
  coreUtxoSpends: 'dogecoin_core_utxo_spends_v1',
  currentUtxos: 'dogecoin_utxo_outputs_current_v1',
  currentUtxosByAddress: 'dogecoin_utxo_outputs_current_by_address_v1',
} as const;

export function buildCoreCurrentStateOutputKeyRanges(): ClickHouseStringRange[] {
  return [
    { start: null, end: '00' },
    ...Array.from({ length: 0x100 }, (_value, index) => coreCurrentStateOutputRange(index)),
    { start: 'g', end: null },
  ];
}

export function clickHouseStringRangeClause(column: string, range: ClickHouseStringRange): string {
  return [clickHouseRangeStartClause(column, range), clickHouseRangeEndClause(column, range)]
    .filter(hasRangeClause)
    .join('\n');
}

export function clickHouseStringRangeParams(range: ClickHouseStringRange): Record<string, string> {
  const entries: Array<[string, string | null]> = [
    ['rangeStart', range.start],
    ['rangeEnd', range.end],
  ];
  return Object.fromEntries(entries.filter(isClickHouseStringRangeParam));
}

function coreCurrentStateOutputRange(value: number): ClickHouseStringRange {
  return {
    start: value.toString(16).padStart(2, '0'),
    end: coreCurrentStateOutputRangeEnd(value),
  };
}

function coreCurrentStateOutputRangeEnd(value: number): string {
  return value === 0xff ? 'g' : (value + 1).toString(16).padStart(2, '0');
}

function clickHouseRangeStartClause(column: string, range: ClickHouseStringRange): string {
  if (range.start === null) {
    return '';
  }

  return `AND ${column} >= {rangeStart:String}`;
}

function clickHouseRangeEndClause(column: string, range: ClickHouseStringRange): string {
  if (range.end === null) {
    return '';
  }

  return `AND ${column} < {rangeEnd:String}`;
}

function hasRangeClause(value: string): boolean {
  return value.length > 0;
}

function isClickHouseStringRangeParam(entry: [string, string | null]): entry is [string, string] {
  return entry[1] !== null;
}
