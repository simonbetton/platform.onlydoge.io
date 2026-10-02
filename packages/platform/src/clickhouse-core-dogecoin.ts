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

/**
 * Splits the output-key space (lowercase hex txids) into contiguous ranges by
 * the first `prefixLength` hex digits, plus one range below and one above hex.
 * Two digits give 258 ranges, three give 4098.
 */
export function buildCoreCurrentStateOutputKeyRanges(prefixLength = 2): ClickHouseStringRange[] {
  const buckets = 16 ** prefixLength;
  return [
    { start: null, end: '0'.repeat(prefixLength) },
    ...Array.from({ length: buckets }, (_value, index) =>
      coreCurrentStateOutputRange(index, buckets, prefixLength),
    ),
    { start: 'g', end: null },
  ];
}

/**
 * Hex digits to split current-state materialization by, so one range stays
 * around a million created outputs or fewer: each range is one bounded
 * INSERT ... SELECT that has to finish inside the statement timeout.
 */
export function coreCurrentStateRangePrefixLength(createdOutputRows: number): number {
  return createdOutputRows > 256 * 1_000_000 ? 3 : 2;
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

function coreCurrentStateOutputRange(
  value: number,
  buckets: number,
  prefixLength: number,
): ClickHouseStringRange {
  return {
    start: value.toString(16).padStart(prefixLength, '0'),
    end: value === buckets - 1 ? 'g' : (value + 1).toString(16).padStart(prefixLength, '0'),
  };
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
