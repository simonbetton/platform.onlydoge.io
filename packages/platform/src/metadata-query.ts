export type SqlDialect = 'mysql' | 'postgres' | 'sqlite';
export type SqlValue = boolean | number | string | null;

const truthySqlValues = new Set<SqlValue>([true, 1, '1']);

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
  return truthySqlValues.has(value as SqlValue);
}

export function nullableString(value: SqlValue | undefined): string | null {
  return value == null ? null : String(value);
}

export function nullableNumber(value: SqlValue | undefined): number | null {
  return value == null ? null : Number(value);
}

export function currentAddressSummary(
  balance: string,
  utxoCount: number,
): {
  balance: string;
  utxoCount: number;
} | null {
  if (isZeroAddressSummary(balance, utxoCount)) {
    return null;
  }

  return { balance, utxoCount };
}

function isZeroAddressSummary(balance: string, utxoCount: number): boolean {
  return [balance === '0', utxoCount === 0].every(Boolean);
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

export function metadataInfrastructureMessage(error: unknown): string {
  const message = describeMetadataError(error);
  return metadataInfrastructureLabel(
    metadataMessageClassifiers.find((classifier) => classifier.matches(message)),
  );
}

function metadataInfrastructureLabel(
  classifier: (typeof metadataMessageClassifiers)[number] | undefined,
): string {
  if (!classifier) {
    return 'metadata query failed';
  }

  return classifier.label;
}

const metadataMessageClassifiers = [
  { label: 'metadata database unavailable', matches: isMetadataUnavailableMessage },
  { label: 'metadata database request timed out', matches: isMetadataTimeoutMessage },
];

function describeMetadataError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }

  return String(error);
}

function isMetadataUnavailableMessage(message: string): boolean {
  return [
    'ECONNREFUSED',
    'ECONNRESET',
    'ETIMEDOUT',
    'EAI_AGAIN',
    'Connection terminated unexpectedly',
    'the database system is starting up',
    'the database system is shutting down',
  ].some((needle) => message.includes(needle));
}

function isMetadataTimeoutMessage(message: string): boolean {
  return ['timeout expired', 'connect ETIMEDOUT'].some((needle) => message.includes(needle));
}
