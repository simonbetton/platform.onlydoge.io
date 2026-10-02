import { describe, expect, it } from 'vitest';

import {
  compileQuery,
  currentAddressSummary,
  metadataInfrastructureMessage,
  nullableNumber,
  nullableString,
  sqlLimitClause,
  sqlNullableOffsetClause,
  sqlNullablePaginationParams,
  sqlOffsetClause,
  sqlPaginationParams,
  toBoolean,
} from '../../packages/platform/src/metadata-query';

describe('metadata query helpers', () => {
  it('compiles positional parameters for each supported SQL dialect', () => {
    expect(compileQuery('sqlite', 'SELECT ? + ?')).toBe('SELECT ? + ?');
    expect(compileQuery('mysql', 'SELECT ? + ?')).toBe('SELECT ? + ?');
    expect(compileQuery('postgres', 'SELECT ? + ?')).toBe('SELECT $1 + $2');
  });

  it('maps row primitives without leaking database-specific encodings', () => {
    expect(toBoolean(true)).toBe(true);
    expect(toBoolean(1)).toBe(true);
    expect(toBoolean('1')).toBe(true);
    expect(toBoolean(0)).toBe(false);
    expect(nullableString(null)).toBeNull();
    expect(nullableString(7)).toBe('7');
    expect(nullableNumber(undefined)).toBeNull();
    expect(nullableNumber('42')).toBe(42);
  });

  it('builds current address summaries only when state exists', () => {
    expect(currentAddressSummary('0', 0)).toBeNull();
    expect(currentAddressSummary('10', 0)).toEqual({ balance: '10', utxoCount: 0 });
    expect(currentAddressSummary('0', 2)).toEqual({ balance: '0', utxoCount: 2 });
  });

  it('normalizes metadata connection failures', () => {
    expect(metadataInfrastructureMessage(new Error('connect ECONNREFUSED 127.0.0.1:1'))).toBe(
      'metadata database unavailable',
    );
    expect(metadataInfrastructureMessage(new Error('Connection terminated unexpectedly'))).toBe(
      'metadata database unavailable',
    );
    expect(metadataInfrastructureMessage(new Error('timeout expired'))).toBe(
      'metadata database request timed out',
    );
    expect(metadataInfrastructureMessage(new Error('syntax error'))).toBe('metadata query failed');
  });

  it('builds SQL pagination clauses and params in placeholder order', () => {
    expect(sqlLimitClause(undefined)).toBe('');
    expect(sqlLimitClause(25)).toBe('LIMIT ?');
    expect(sqlOffsetClause(0)).toBe('');
    expect(sqlOffsetClause(10)).toBe('OFFSET ?');
    expect(sqlNullableOffsetClause(undefined)).toBe('');
    expect(sqlNullableOffsetClause(0)).toBe('OFFSET ?');
    expect(sqlPaginationParams(10, 25)).toEqual([25, 10]);
    expect(sqlPaginationParams(0, undefined)).toEqual([]);
    expect(sqlNullablePaginationParams(0, 25)).toEqual([25, 0]);
  });
});
