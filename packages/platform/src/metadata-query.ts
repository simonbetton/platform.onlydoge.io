export type SqlDialect = 'mysql' | 'postgres' | 'sqlite';
export type SqlValue = boolean | number | string | null;

export function compileQuery(kind: SqlDialect, query: string): string {
  if (kind !== 'postgres') {
    return query;
  }

  let index = 0;
  return query.replaceAll('?', () => {
    index += 1;
    return `$${index}`;
  });
}

export function toBoolean(value: unknown): boolean {
  return value === true || value === 1 || value === '1';
}

export function nullableString(value: SqlValue | undefined): string | null {
  return value === null || value === undefined ? null : String(value);
}

export function nullableNumber(value: SqlValue | undefined): number | null {
  return value === null || value === undefined ? null : Number(value);
}

export function currentAddressSummary(
  balance: string,
  utxoCount: number,
): {
  balance: string;
  utxoCount: number;
} | null {
  if (balance === '0' && utxoCount === 0) {
    return null;
  }

  return { balance, utxoCount };
}

export function sqlLimitClause(limit: number | undefined): string {
  return limit === undefined ? '' : 'LIMIT ?';
}

export function sqlOffsetClause(offset: number): string {
  return offset > 0 ? 'OFFSET ?' : '';
}

export function sqlNullableOffsetClause(offset: number | undefined): string {
  return offset === undefined ? '' : 'OFFSET ?';
}

export function sqlPaginationParams(offset: number, limit: number | undefined): SqlValue[] {
  return [...sqlLimitParam(limit), ...sqlPositiveOffsetParam(offset)];
}

export function sqlNullablePaginationParams(
  offset: number | undefined,
  limit: number | undefined,
): SqlValue[] {
  return [...sqlLimitParam(limit), ...sqlNullableOffsetParam(offset)];
}

function sqlLimitParam(limit: number | undefined): SqlValue[] {
  return limit === undefined ? [] : [limit];
}

function sqlPositiveOffsetParam(offset: number): SqlValue[] {
  return offset > 0 ? [offset] : [];
}

function sqlNullableOffsetParam(offset: number | undefined): SqlValue[] {
  return offset === undefined ? [] : [offset];
}
