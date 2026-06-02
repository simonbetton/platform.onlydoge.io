import {
  type AnalyticsConfigPort,
  AnalyticsQueryService,
  type AnalyticsWarehousePort,
  analyticsBalancesCurrentTable,
  analyticsTransactionsTable,
  mempoolSamplesTable,
} from '@onlydoge/analytics-query';
import {
  configKeyDogecoinAnalyticsFactsReady,
  configKeyDogecoinAnalyticsFactsTail,
  configKeyIndexerFinalizedTail,
} from '@onlydoge/indexing-pipeline';
import { describe, expect, it } from 'vitest';

const actor = {
  apiKeyId: 1,
  id: 'key_test',
  role: 'member' as const,
};

describe('analytics query service', () => {
  it('returns the curated schema for AI SQL generation', () => {
    const service = createService();

    const schema = service.schema();

    expect(schema.tables[0]?.name).toBe(analyticsTransactionsTable);
    expect(schema.tables.map((table) => table.name)).toContain(analyticsBalancesCurrentTable);
    expect(schema.constraints.maxWindowSeconds).toBe(604800);
    expect(schema.constraints.requiresFinalizedBlocks).toBe(true);
  });

  it('executes validated SQL with server-bound query params', async () => {
    const warehouse = new FakeAnalyticsWarehouse();
    const service = createService({ warehouse });

    const response = await service.query(actor, {
      from: '2026-05-30T00:00:00.000Z',
      to: '2026-05-31T00:00:00.000Z',
      limit: 1,
      sql: averageFeeSql(),
    });

    expect(response.rows).toEqual([{ average_fee_base: '100000000' }]);
    expect(warehouse.lastParams).toEqual({
      fromTime: 1_780_099_200,
      toTime: 1_780_185_600,
      maxFinalizedHeight: 10,
      limit: 1,
    });
    expect(response.query.estimatedRows).toBe(2);
  });

  it('rejects SQL outside the curated analytics surface', async () => {
    const service = createService();

    await expect(
      service.query(actor, {
        from: '2026-05-30T00:00:00.000Z',
        to: '2026-05-31T00:00:00.000Z',
        sql: `
          SELECT count()
          FROM system.tables
          WHERE block_time >= {fromTime:UInt64}
            AND block_time < {toTime:UInt64}
            AND block_height <= {maxFinalizedHeight:UInt64}
        `,
      }),
    ).rejects.toThrow('analytics SQL cannot query table: tables');
  });

  it('requires server-owned time and finality placeholders in concrete predicates', async () => {
    const service = createService();

    await expect(
      service.query(actor, {
        from: '2026-05-30T00:00:00.000Z',
        to: '2026-05-31T00:00:00.000Z',
        sql: `
          SELECT {fromTime:UInt64} AS lower_bound, count() AS transactions
          FROM ${analyticsTransactionsTable}
          WHERE block_time > 0
            AND block_time < {toTime:UInt64}
            AND block_height <= {maxFinalizedHeight:UInt64}
        `,
      }),
    ).rejects.toThrow('analytics SQL must constrain block_time');
  });

  it('allows current-balance analytics with finalized-height bounds', async () => {
    const warehouse = new FakeAnalyticsWarehouse();
    const service = createService({ warehouse });

    await service.query(actor, {
      from: '2026-05-30T00:00:00.000Z',
      to: '2026-05-31T00:00:00.000Z',
      limit: 20,
      sql: `
        SELECT address, balance_i256 AS balance_base
        FROM ${analyticsBalancesCurrentTable}
        WHERE as_of_block_height <= {maxFinalizedHeight:UInt64}
        ORDER BY balance_i256 DESC, address ASC
        LIMIT {limit:UInt64}
      `,
    });

    expect(warehouse.lastParams).toMatchObject({ maxFinalizedHeight: 10, limit: 20 });
  });

  it('requires finalized-height bounds for current-balance analytics', async () => {
    const service = createService();

    await expect(
      service.query(actor, {
        from: '2026-05-30T00:00:00.000Z',
        to: '2026-05-31T00:00:00.000Z',
        sql: `
          SELECT address, balance_i256 AS balance_base
          FROM ${analyticsBalancesCurrentTable}
          ORDER BY balance_i256 DESC
        `,
      }),
    ).rejects.toThrow('analytics SQL must constrain as_of_block_height');
  });

  it('allows bounded mempool sample analytics', async () => {
    const warehouse = new FakeAnalyticsWarehouse();
    const service = createService({ warehouse });

    await service.query(actor, {
      from: '2026-05-30T00:00:00.000Z',
      to: '2026-05-30T01:00:00.000Z',
      sql: `
        SELECT count() AS sampled_transactions
        FROM ${mempoolSamplesTable}
        WHERE sampled_at >= toDateTime({fromTime:UInt64})
          AND sampled_at < toDateTime({toTime:UInt64})
      `,
    });

    expect(warehouse.lastParams).toMatchObject({
      fromTime: 1_780_099_200,
      toTime: 1_780_102_800,
    });
  });

  it('rejects query-level ClickHouse settings', async () => {
    const service = createService();

    await expect(
      service.query(actor, {
        from: '2026-05-30T00:00:00.000Z',
        to: '2026-05-31T00:00:00.000Z',
        sql: `${averageFeeSql()}\nSETTINGS max_threads = 64`,
      }),
    ).rejects.toThrow('analytics SQL cannot use SETTINGS');
  });

  it('rejects windows wider than seven days', async () => {
    const service = createService();

    await expect(
      service.query(actor, {
        from: '2026-05-01T00:00:00.000Z',
        to: '2026-05-31T00:00:00.000Z',
        sql: averageFeeSql(),
      }),
    ).rejects.toThrow('analytics time window cannot exceed 7 days');
  });
});

function createService(options: { warehouse?: AnalyticsWarehousePort } = {}) {
  return new AnalyticsQueryService(
    new FakeAnalyticsConfig(),
    options.warehouse ?? new FakeAnalyticsWarehouse(),
  );
}

class FakeAnalyticsConfig implements AnalyticsConfigPort {
  private readonly values = new Map<string, unknown>([
    [configKeyIndexerFinalizedTail(), 10],
    [configKeyDogecoinAnalyticsFactsReady(), true],
    [configKeyDogecoinAnalyticsFactsTail(), 10],
  ]);

  public async getJsonValue<T>(key: string): Promise<T | null> {
    return (this.values.get(key) as T | undefined) ?? null;
  }

  public async setJsonValue<T>(key: string, value: T): Promise<void> {
    this.values.set(key, value);
  }
}

class FakeAnalyticsWarehouse implements AnalyticsWarehousePort {
  public lastParams: unknown;

  public async backfillAnalyticsTransactionFacts() {
    return { rowsInserted: 1, throughBlockHeight: 10 };
  }

  public async insertAnalyticsTransactionFacts(): Promise<void> {}

  public async preflightAnalyticsQuery(
    input: Parameters<AnalyticsWarehousePort['preflightAnalyticsQuery']>[0],
  ) {
    this.lastParams = input.params;
    return { estimatedRows: 2, estimatedBytes: 200 };
  }

  public async executeAnalyticsQuery() {
    return {
      columns: [{ name: 'average_fee_base', type: 'Nullable(Int256)' }],
      rows: [{ average_fee_base: '100000000' }],
      statistics: { elapsed: 0.001, rowsRead: 2, bytesRead: 200 },
      warnings: [],
    };
  }
}

function averageFeeSql(): string {
  return `
    SELECT avgOrNull(fee_base_i256) AS average_fee_base
    FROM ${analyticsTransactionsTable}
    WHERE block_time >= {fromTime:UInt64}
      AND block_time < {toTime:UInt64}
      AND block_height <= {maxFinalizedHeight:UInt64}
      AND is_coinbase = 0
  `;
}
