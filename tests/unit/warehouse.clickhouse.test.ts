import type { CoreDogecoinBlockApplication } from '@onlydoge/indexing-pipeline';
import {
  ClickHouseWarehouseAdapter,
  CompositeWarehouseAdapter,
  clickHouseMigrations,
} from '@onlydoge/platform';
import { InfrastructureError } from '@onlydoge/shared-kernel';
import { describe, expect, it, vi } from 'vitest';

interface ClickHouseCommandCall {
  clickhouse_settings?: Record<string, unknown>;
  query: string;
  query_params?: Record<string, number | string>;
}

describe('clickhouse warehouse adapter', () => {
  it('applies explorer query budgets without changing indexer reads', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async (_parameters: ClickHouseCommandCall) => ({
      json: async () => [],
    }));
    (adapter as unknown as { client: { query: typeof query } }).client = {
      query,
    };
    const explorer = new CompositeWarehouseAdapter(adapter, adapter);

    await explorer.listAppliedBlocks(0, 50);
    expect(query.mock.calls[0]?.[0].clickhouse_settings).toEqual({
      max_execution_time: 30,
      max_rows_to_read: '10000000',
      max_bytes_to_read: '1073741824',
      max_result_rows: '100000',
      result_overflow_mode: 'throw',
      timeout_before_checking_execution_speed: 0,
    });

    await adapter.listAppliedBlocks(0, 50);
    expect(query.mock.calls[1]?.[0].clickhouse_settings).toBeUndefined();
  });

  it('does not fall back to primary credentials for analytics queries', () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
      user: 'primary',
      password: 'primary-secret',
    });

    expect((adapter as unknown as { analyticsClient: unknown }).analyticsClient).toBeNull();
  });

  it('rejects an incomplete analytics credential pair', () => {
    expect(
      () =>
        new ClickHouseWarehouseAdapter({
          driver: 'clickhouse',
          location: 'http://clickhouse:8123',
          user: 'primary',
          password: 'primary-secret',
          analyticsUser: 'analytics',
        }),
    ).toThrow(/both ONLYDOGE_ANALYTICS_WAREHOUSE_USER and ONLYDOGE_ANALYTICS_WAREHOUSE_PASSWORD/u);
  });

  it('surfaces warehouse connection failures as infrastructure errors', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async () => {
      const error = new Error('connect ECONNREFUSED clickhouse.internal.example.com:8123');
      Object.assign(error, { code: 'ECONNREFUSED' });
      throw error;
    });

    (adapter as unknown as { client: { query: typeof query } }).client = {
      query,
    };

    await expect(adapter.listAppliedBlocks(7)).rejects.toEqual(
      new InfrastructureError('warehouse unavailable', {
        cause: expect.any(Error),
      }),
    );
  });

  it('surfaces warehouse memory-limit failures as infrastructure errors', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async () => {
      const error = new Error('MEMORY_LIMIT_EXCEEDED: User memory limit exceeded');
      Object.assign(error, { code: '241' });
      throw error;
    });

    (adapter as unknown as { client: { query: typeof query } }).client = {
      query,
    };

    await expect(adapter.listAppliedBlocks(7)).rejects.toEqual(
      new InfrastructureError('warehouse query exceeded memory limit', {
        cause: expect.any(Error),
      }),
    );
  });

  it('passes abort signals and bounded execution settings to bootstrap page queries', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
      requestTimeoutMs: 30000,
    });
    const parentController = new AbortController();
    const query = vi.fn(
      async (parameters: {
        abort_signal?: AbortSignal;
        clickhouse_settings?: { max_execution_time?: number };
      }) => {
        expect(parameters.abort_signal).toBeDefined();
        expect(parameters.clickhouse_settings?.max_execution_time).toBe(1);
        return { json: async () => [] };
      },
    );

    (adapter as unknown as { client: { query: typeof query } }).client = {
      query,
    };

    await expect(
      adapter.listCurrentBalancesPage(null, 5000, {
        abortSignal: parentController.signal,
        timeoutMs: 1000,
      }),
    ).resolves.toEqual({
      rows: [],
      nextCursor: null,
    });
    expect(query).toHaveBeenCalledOnce();
  });

  it('surfaces bootstrap page query aborts as request timeouts', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
      requestTimeoutMs: 30000,
    });
    const query = vi.fn(
      async ({ abort_signal }: { abort_signal?: AbortSignal }) =>
        await new Promise<never>((_, reject) => {
          abort_signal?.addEventListener(
            'abort',
            () => reject(new Error('The operation was aborted')),
            { once: true },
          );
        }),
    );

    (adapter as unknown as { client: { query: typeof query } }).client = {
      query,
    };

    await expect(
      adapter.listCurrentBalancesPage(null, 5000, {
        timeoutMs: 10,
      }),
    ).rejects.toEqual(
      new InfrastructureError('warehouse request timed out after 10ms', {
        cause: expect.any(Error),
      }),
    );
  });

  it('chunks oversized output-key queries instead of sending one huge request', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(
      async ({
        query: _query,
        query_params,
      }: {
        query: string;
        query_params: { outputKeys: string[] };
      }) => ({
        json: async () =>
          query_params.outputKeys.map((outputKey) => ({
            ...clickHouseUtxoRow(),
            outputKey,
          })),
      }),
    );

    (adapter as unknown as { client: { query: typeof query } }).client = {
      query,
    };

    const outputKeys = Array.from(
      { length: 400 },
      (_, index) => `doge-${'x'.repeat(64)}-${index.toString().padStart(4, '0')}`,
    );

    const rows = await adapter.getUtxoOutputs(outputKeys);

    expect(rows.size).toBe(outputKeys.length);
    expect(query.mock.calls.length).toBeGreaterThan(1);
    expect(
      query.mock.calls.every(
        ([parameters]) =>
          Array.isArray(parameters.query_params.outputKeys) &&
          parameters.query_params.outputKeys.length < outputKeys.length,
      ),
    ).toBe(true);
    expect(
      query.mock.calls.every(
        ([parameters]) =>
          typeof parameters.query === 'string' &&
          parameters.query.includes('FROM dogecoin_utxo_outputs_current_v1') &&
          parameters.query.includes('LIMIT 1 BY output_key') &&
          !parameters.query.includes('FINAL') &&
          !parameters.query.includes('argMax('),
      ),
    ).toBe(true);
  });

  it('uses exact tuple filters for projection state lookups instead of argMax aggregates', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM dogecoin_applied_blocks_v1')) {
        return { json: async () => [] };
      }

      if (statement.includes('FROM dogecoin_utxo_outputs_current_v1')) {
        return {
          json: async () => [
            clickHouseUtxoRow({
              blockHash: 'prev-hash',
              outputKey: 'prev-txid:1',
              address: 'DInputAddress',
              txid: 'prev-txid',
              valueBase: '10',
              vout: 1,
            }),
          ],
        };
      }

      if (statement.includes('FROM dogecoin_balances_current_v1')) {
        return {
          json: async () => [
            {
              address: 'DInputAddress',
              assetAddress: 'DOGE',
              balance: '10',
              asOfBlockHeight: 1,
              version: 1,
            },
          ],
        };
      }

      return { json: async () => [] };
    });
    const { insert } = installClickHouseClient(adapter, query);

    await adapter.applyProjectionWindow([
      {
        blockHeight: 2,
        blockHash: 'block-hash',
        blockTime: 2,
        utxoCreates: [],
        utxoSpends: [
          {
            outputKey: 'prev-txid:1',
            spentByTxid: 'next-txid',
            spentInBlock: 2,
            spentInputIndex: 0,
          },
        ],
        addressMovements: [
          {
            movementId: 'movement-1',
            blockHeight: 2,
            blockHash: 'block-hash',
            blockTime: 2,
            txid: 'next-txid',
            txIndex: 0,
            entryIndex: 0,
            address: 'DInputAddress',
            assetAddress: 'DOGE',
            direction: 'debit',
            amountBase: '1',
            outputKey: 'prev-txid:1',
            derivationMethod: 'utxo',
          },
          {
            movementId: 'movement-2',
            blockHeight: 2,
            blockHash: 'block-hash',
            blockTime: 2,
            txid: 'next-txid',
            txIndex: 0,
            entryIndex: 1,
            address: 'DOutputAddress',
            assetAddress: 'DOGE',
            direction: 'credit',
            amountBase: '1',
            outputKey: 'next-txid:0',
            derivationMethod: 'utxo',
          },
        ],
      },
    ]);

    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(
      statements.find((statement) => statement.includes('FROM dogecoin_utxo_outputs_current_v1')),
    ).toContain('LIMIT 1 BY output_key');
    expect(
      statements.find((statement) => statement.includes('FROM dogecoin_balances_current_v1')),
    ).toContain(
      "(address, asset_address) IN (('DInputAddress', 'DOGE'), ('DOutputAddress', 'DOGE'))",
    );
    expect(statements.some((statement) => statement.includes('FROM direct_links_v2'))).toBe(false);
    expect(statements.some((statement) => statement.includes('argMax('))).toBe(false);
    const insertedTables = insert.mock.calls.map(
      (call) => (call as Array<{ table: string }>).at(0)?.table ?? '<missing-table>',
    );

    expect(insertedTables).toContain('dogecoin_utxo_outputs_current_v1');
  });

  it('lists address UTXOs from the address-ordered current state table', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM dogecoin_utxo_outputs_current_by_address_v1')) {
        return {
          json: async () => [
            clickHouseUtxoRow({
              blockHeight: 123,
              blockHash: 'prev-hash',
              blockTime: 456,
              txid: 'prev-txid',
              txIndex: 4,
              vout: 1,
              outputKey: 'prev-txid:1',
              address: 'DInputAddress',
              scriptType: 'pubkeyhash',
              valueBase: '10',
              isCoinbase: false,
              isSpendable: true,
              spentByTxid: null,
              spentInBlock: null,
              spentInputIndex: null,
            }),
          ],
        };
      }

      return { json: async () => [] };
    });

    (adapter as unknown as { client: { query: typeof query } }).client = {
      query,
    };

    const rows = await adapter.listAddressUtxos('DInputAddress', 0, 50);
    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      outputKey: 'prev-txid:1',
      address: 'DInputAddress',
    });
    expect(
      statements.some(
        (statement) =>
          statement.includes('FROM dogecoin_utxo_outputs_current_by_address_v1') &&
          statement.includes('WHERE address = {address:String}') &&
          statement.includes('LIMIT 1 BY output_key'),
      ),
    ).toBe(true);
    expect(statements.some((statement) => statement.includes('address_outputs AS'))).toBe(false);
  });

  it('does not query the legacy versioned UTXO table when current-state rows are missing', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM dogecoin_utxo_outputs_current_v1')) {
        return { json: async () => [] };
      }

      if (
        statement.includes('FROM dogecoin_core_utxo_creates_v1') &&
        statement.includes('FROM dogecoin_core_utxo_spends_v1')
      ) {
        return {
          json: async () => [
            clickHouseUtxoRow({
              blockHeight: 123,
              blockHash: 'prev-hash',
              blockTime: 456,
              txid: 'prev-txid',
              txIndex: 0,
              vout: 1,
              outputKey: 'prev-txid:1',
              address: 'DInputAddress',
              valueBase: '10',
              spentByTxid: 'next-txid',
              spentInBlock: 124,
            }),
          ],
        };
      }

      return { json: async () => [] };
    });
    (adapter as unknown as { client: { query: typeof query } }).client = {
      query,
    };

    const rows = await adapter.getUtxoOutputs(['prev-txid:1']);

    expect(rows.get('prev-txid:1')).toMatchObject({
      outputKey: 'prev-txid:1',
      spentByTxid: 'next-txid',
      spentInBlock: 124,
    });
    expect(
      query.mock.calls.some(
        ([parameters]) =>
          parameters.query.includes('FROM dogecoin_utxo_outputs_current_v1') &&
          parameters.query.includes('LIMIT 1 BY output_key') &&
          !parameters.query.includes('FINAL'),
      ),
    ).toBe(true);
    expect(
      query.mock.calls.some(([parameters]) => parameters.query.includes('FROM utxo_outputs_v2')),
    ).toBe(false);
  });

  it('falls back to core Dogecoin create and spend tables for historical UTXO lookups', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (
        statement.includes('FROM dogecoin_utxo_outputs_current_v1') ||
        statement.includes('FROM utxo_outputs_v2')
      ) {
        return { json: async () => [] };
      }

      if (
        statement.includes('FROM dogecoin_core_utxo_creates_v1') &&
        statement.includes('FROM dogecoin_core_utxo_spends_v1')
      ) {
        return {
          json: async () => [
            clickHouseUtxoRow({
              blockHeight: 123,
              blockHash: 'prev-hash',
              blockTime: 456,
              txid: 'prev-txid',
              txIndex: 0,
              vout: 1,
              outputKey: 'prev-txid:1',
              address: 'DInputAddress',
              valueBase: '10',
              spentByTxid: 'next-txid',
              spentInBlock: 124,
            }),
          ],
        };
      }

      return { json: async () => [] };
    });

    (adapter as unknown as { client: { query: typeof query } }).client = {
      query,
    };

    const rows = await adapter.getUtxoOutputs(['prev-txid:1']);
    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(rows.get('prev-txid:1')).toMatchObject({
      outputKey: 'prev-txid:1',
      spentByTxid: 'next-txid',
      spentInBlock: 124,
    });
    expect(
      statements.some(
        (statement) =>
          statement.includes('FROM dogecoin_core_utxo_creates_v1') &&
          statement.includes('FROM dogecoin_core_utxo_spends_v1') &&
          statement.includes('LIMIT 1 BY output_key') &&
          statement.includes('LIMIT 1 BY spent_output_key'),
      ),
    ).toBe(true);
  });

  it('resolves created UTXO outputs from core creates without spend joins', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM dogecoin_core_utxo_creates_v1')) {
        return {
          json: async () => [
            clickHouseUtxoRow({
              blockHeight: 123,
              blockHash: 'prev-hash',
              blockTime: 456,
              txid: 'prev-txid',
              txIndex: 0,
              vout: 1,
              outputKey: 'prev-txid:1',
              address: 'DInputAddress',
              valueBase: '10',
              spentByTxid: null,
              spentInBlock: null,
              spentInputIndex: null,
            }),
          ],
        };
      }

      return { json: async () => [] };
    });

    (adapter as unknown as { client: { query: typeof query } }).client = {
      query,
    };

    const rows = await adapter.getCreatedUtxoOutputs(['prev-txid:1']);
    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(rows.get('prev-txid:1')).toMatchObject({
      outputKey: 'prev-txid:1',
      address: 'DInputAddress',
      valueBase: '10',
    });
    expect(statements).toHaveLength(1);
    expect(statements[0]).toContain('FROM dogecoin_core_utxo_creates_v1');
    expect(statements[0]).toContain('LIMIT 1 BY output_key');
    expect(statements[0]).not.toContain('FROM dogecoin_core_utxo_spends_v1');
    expect(statements[0]).not.toContain('FROM dogecoin_utxo_outputs_current_v1');
    expect(statements[0]).not.toContain('spent_by_txid');
  });

  it('uses indexed transaction refs before core output-key lookup', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM dogecoin_transaction_refs_v1')) {
        return {
          json: async () => [
            {
              blockHeight: 123,
              blockHash: 'block-hash',
              blockTime: 456,
              txIndex: 7,
            },
          ],
        };
      }

      if (statement.includes('FROM dogecoin_core_utxo_creates_v1')) {
        throw new Error('core output-key lookup should not run when indexed refs hit');
      }

      return { json: async () => [] };
    });

    (adapter as unknown as { client: { query: typeof query } }).client = {
      query,
    };

    const ref = await adapter.getTransactionRef('doge-txid');
    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(ref).toEqual({
      blockHeight: 123,
      blockHash: 'block-hash',
      blockTime: 456,
      txIndex: 7,
    });
    expect(statements[0]).toContain('FROM dogecoin_transaction_refs_v1');
  });

  it('falls back to core and current UTXO state when indexed refs miss', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM dogecoin_transaction_refs_v1')) {
        return { json: async () => [] };
      }

      if (statement.includes('FROM dogecoin_core_utxo_creates_v1')) {
        return { json: async () => [] };
      }

      if (statement.includes('FROM dogecoin_utxo_outputs_current_v1')) {
        return {
          json: async () => [
            {
              blockHeight: 321,
              blockHash: 'current-block-hash',
              blockTime: 654,
              txIndex: 9,
            },
          ],
        };
      }

      return { json: async () => [] };
    });

    (adapter as unknown as { client: { query: typeof query } }).client = {
      query,
    };

    const ref = await adapter.getTransactionRef('doge-txid');
    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(ref).toEqual({
      blockHeight: 321,
      blockHash: 'current-block-hash',
      blockTime: 654,
      txIndex: 9,
    });
    expect(statements[0]).toContain('FROM dogecoin_transaction_refs_v1');
    expect(statements[1]).toContain('FROM dogecoin_core_utxo_creates_v1');
    expect(statements[2]).toContain('FROM dogecoin_utxo_outputs_current_v1');
  });

  it('resets old_parts_lifetime on every table the schema created with it', () => {
    const migrations = clickHouseMigrations();
    const reset = migrations.find((migration) => migration.name === 'old_parts_lifetime_default');
    const statements = (reset?.source ?? '')
      .split(';')
      .map((statement) => statement.trim())
      .filter(Boolean);

    // RESET SETTING only rewrites table metadata; anything else here could
    // rewrite hundreds of gigabytes on a synced warehouse.
    expect(statements.length).toBeGreaterThan(0);
    for (const statement of statements) {
      expect(statement).toMatch(/^ALTER TABLE \w+ RESET SETTING old_parts_lifetime$/u);
    }

    // Earlier migrations' CREATE statements are checksummed and cannot lose
    // the override, so the reset must cover exactly the tables they gave it.
    const createdWithZeroLifetime = migrations
      .filter((migration) => migration.version < (reset?.version ?? 0))
      .flatMap((migration) => migration.source.split(';'))
      .filter((statement) => statement.includes('old_parts_lifetime = 0'))
      .map((statement) => /CREATE TABLE IF NOT EXISTS (\w+)/u.exec(statement)?.[1])
      .sort();
    expect(createdWithZeroLifetime).toHaveLength(5);
    expect(statements.map((statement) => /ALTER TABLE (\w+)/u.exec(statement)?.[1]).sort()).toEqual(
      createdWithZeroLifetime,
    );
  });

  it('defines ordered checksummed schema and read-model migrations', () => {
    const migrations = clickHouseMigrations();

    expect(migrations.map(({ name, version }) => ({ name, version }))).toEqual([
      { name: 'canonical_schema', version: 1 },
      { name: 'address_read_models_backfill', version: 2 },
      { name: 'transaction_refs_table', version: 3 },
      { name: 'zstd_column_codecs', version: 4 },

      { name: 'old_parts_lifetime_default', version: 5 },
    ]);
    expect(migrations.every((migration) => migration.checksum.length === 64)).toBe(true);
    expect(migrations[0]?.source).toContain(
      'CREATE MATERIALIZED VIEW IF NOT EXISTS dogecoin_utxo_outputs_current_by_address_v1_mv',
    );
    expect(migrations[0]?.source).toContain(
      'CREATE MATERIALIZED VIEW IF NOT EXISTS dogecoin_address_movements_by_address_v1_mv',
    );
    expect(migrations[0]?.source).toContain(
      'ADD INDEX IF NOT EXISTS core_utxo_creates_address_idx',
    );
    expect(migrations[1]?.source).toContain(
      'LEFT ANTI JOIN dogecoin_utxo_outputs_current_by_address_v1',
    );
    expect(migrations[1]?.source).toContain(
      'LEFT ANTI JOIN dogecoin_address_movements_by_address_v1',
    );
  });

  it('switches hash-heavy core columns to ZSTD without rewriting existing parts', () => {
    const codecs = clickHouseMigrations().find((migration) => migration.version === 4);
    const statements = (codecs?.source ?? '')
      .split(';')
      .map((statement) => statement.trim())
      .filter(Boolean);

    // Codec-only MODIFY COLUMN is a metadata change; any other ALTER here
    // could rewrite hundreds of gigabytes on a synced warehouse.
    expect(statements.length).toBeGreaterThan(0);
    for (const statement of statements) {
      const [alter, ...modifications] = statement.split('\n').map((line) => line.trim());
      expect(alter).toMatch(/^ALTER TABLE \w+$/u);
      for (const modification of modifications) {
        expect(modification).toMatch(/^MODIFY COLUMN \w+ CODEC\(ZSTD\(1\)\),?$/u);
      }
    }
    expect(codecs?.source).toContain('ALTER TABLE dogecoin_address_movements_by_address_v1');
    expect(codecs?.source).toContain('MODIFY COLUMN movement_id CODEC(ZSTD(1))');
    expect(codecs?.source).toContain('ALTER TABLE dogecoin_core_utxo_spends_v1');
    expect(codecs?.source).toContain('MODIFY COLUMN spent_by_txid CODEC(ZSTD(1))');
  });

  it('uses address-oriented movement reads with a precomputed integer amount column', async () => {
    const { adapter, query } = addressSummaryAdapter({
      addressMovementRows: [addressSummaryMovementRow('11', '7', 2)],
    });

    const { statements, summary } = await readTestAddressSummary(adapter, query);

    expectStandardAddressSummary(summary);
    expect(
      statements.some(
        (statement) =>
          statement.includes('FROM dogecoin_address_movements_by_address_v1') &&
          statement.includes('sumIf(amount_base_i256'),
      ),
    ).toBe(true);
  });

  it('prefers address movement read models over core Dogecoin address scans', async () => {
    const { adapter, query } = addressSummaryAdapter({
      addressMovementRows: [addressSummaryMovementRow('999', '500', 1)],
      coreMovementRows: [addressSummaryMovementRow('11', '7', 2)],
      coreSpendableRows: [{ balance: '4', utxoCount: 1 }],
    });

    const { statements, summary } = await readTestAddressSummary(adapter, query);

    expect(summary).toMatchObject({
      balance: '4',
      receivedBase: '999',
      sentBase: '500',
      txCount: 1,
      utxoCount: 1,
    });
    expect(statements.some((statement) => statement.includes('WITH address_outputs'))).toBe(false);
  });

  it('uses address movement read models for address transaction history', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM dogecoin_address_movements_by_address_v1')) {
        return {
          json: async () => [
            {
              blockHeight: 12,
              blockHash: 'block-hash',
              blockTime: 123,
              txid: 'doge-tx',
              txIndex: 2,
              receivedBase: '11',
              sentBase: '0',
              isCoinbase: 0,
              inputCount: 1,
              outputCount: 1,
              totalInputBase: '0',
              totalOutputBase: '11',
              feeBase: null,
            },
          ],
        };
      }

      return { json: async () => [] };
    });

    (adapter as unknown as { client: { query: typeof query } }).client = {
      query,
    };

    const rows = await adapter.listAddressTransactions('DInputAddress', 0, 5);
    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      txid: 'doge-tx',
      txIndex: 2,
    });
    expect(
      statements.some((statement) =>
        statement.includes('ORDER BY movements.block_height DESC, movements.tx_index DESC'),
      ),
    ).toBe(true);
    expect(statements.some((statement) => statement.includes('analytics_transactions_v1'))).toBe(
      true,
    );
    // Facts must be scoped to the paginated movement page, never aggregated over
    // the whole analytics table (that was a full scan + hash build per request).
    const history = statements.find((statement) => statement.includes('analytics_transactions_v1'));
    expect(history).toContain('WITH page AS (');
    expect(history).toContain('(block_time, txid) IN (SELECT block_time, txid FROM page)');
    expect(history?.indexOf('LIMIT {limit:UInt64}')).toBeLessThan(
      history?.indexOf('analytics_transactions_v1') ?? -1,
    );
  });

  it('appends core Dogecoin create, spend, and processed-block rows by window', async () => {
    const { adapter, insert } = installEmptyClickHouseClient();
    const applications = [
      coreApplication({
        blockHeight: 1,
        blockHash: 'block-1',
        creates: ['coinbase-tx:0'],
      }),
      coreApplication({
        blockHeight: 2,
        blockHash: 'block-2',
        spends: ['coinbase-tx:0'],
        creates: ['spend-tx:0'],
      }),
    ];

    await expect(adapter.applyCoreDogecoinWindow(applications)).resolves.toEqual({
      applied: true,
      processTail: 2,
    });

    const insertedTables = insert.mock.calls.map(
      (call) => (call as Array<{ table: string }>).at(0)?.table ?? '<missing-table>',
    );
    expect(insertedTables).toEqual([
      'dogecoin_core_utxo_creates_v1',
      'dogecoin_core_utxo_spends_v1',
      'dogecoin_address_movements_v1',
      'analytics_transactions_v1',
      'dogecoin_core_processed_blocks_v1',
    ]);
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'dogecoin_core_utxo_spends_v1',
        values: [
          expect.objectContaining({
            spent_output_key: 'coinbase-tx:0',
            spent_by_txid: 'tx-2',
            spent_in_block: 2,
          }),
        ],
      }),
    );
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'dogecoin_address_movements_v1',
        values: expect.arrayContaining([
          expect.objectContaining({
            movement_id: 'core-credit:coinbase-tx:0',
            address: 'DAddress0',
            direction: 'credit',
            amount_base: '100000000',
          }),
          expect.objectContaining({
            movement_id: 'core-debit:coinbase-tx:0:tx-2:0',
            address: 'DAddress0',
            direction: 'debit',
            amount_base: '100000000',
          }),
        ]),
      }),
    );
  });

  it('reads processed blocks for a contiguous window with one range query', async () => {
    const { adapter, query } = installEmptyClickHouseClient();
    const applications = Array.from({ length: 1_000 }, (_value, index) =>
      coreApplication({ blockHeight: index + 1, blockHash: `block-${index + 1}` }),
    );

    await adapter.applyCoreDogecoinWindow(applications, { validatePrevouts: false });

    const processedBlockQueries = queryCalls(query).filter(
      (parameters) =>
        parameters.query.includes('FROM dogecoin_core_processed_blocks_v1') &&
        !parameters.query.includes('block_height = {blockHeight:UInt64}'),
    );
    expect(processedBlockQueries).toHaveLength(1);
    expect(processedBlockQueries[0]?.query).toContain(
      'block_height >= {startHeight:UInt64} AND block_height <= {endHeight:UInt64}',
    );
    expect(processedBlockQueries[0]?.query_params).toEqual({ startHeight: 1, endHeight: 1_000 });
  });

  it('does not look up prevouts while current state is not maintained', async () => {
    const { adapter, insert, query } = installEmptyClickHouseClient();

    await adapter.applyCoreDogecoinWindow(
      [
        coreApplication({
          blockHeight: 2,
          blockHash: 'block-2',
          spends: ['prev-tx:0'],
          creates: ['new-tx:0'],
        }),
      ],
      { updateCurrentState: false, validatePrevouts: false },
    );

    const statements = queryCalls(query).map((parameters) => parameters.query);
    // During backfill the current-state table is empty by construction, so
    // querying it for every spent output would only add round trips.
    expect(
      statements.some((statement) => statement.includes('dogecoin_utxo_outputs_current_v1')),
    ).toBe(false);
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'dogecoin_address_movements_v1',
        values: [expect.objectContaining({ movement_id: 'core-credit:new-tx:0' })],
      }),
    );
  });

  it('looks up external prevouts once for movements, facts, and current state', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: ClickHouseCommandCall) => {
      if (statement.includes('FROM dogecoin_utxo_outputs_current_v1')) {
        return jsonRows([
          clickHouseUtxoRow({ outputKey: 'prev-tx:0', txid: 'prev-tx', valueBase: '150000000' }),
        ]);
      }
      if (statement.includes('FROM dogecoin_balances_current_v1')) {
        return jsonRows([
          {
            address: 'DTestAddress',
            assetAddress: '',
            balance: '150000000',
            asOfBlockHeight: 1,
            version: 1,
          },
        ]);
      }
      return jsonRows([]);
    });
    const { insert } = installClickHouseClient(adapter, query);

    await adapter.applyCoreDogecoinWindow(
      [
        coreApplication({
          blockHeight: 2,
          blockHash: 'block-2',
          spends: ['prev-tx:0'],
          creates: ['new-tx:0'],
        }),
      ],
      { updateCurrentState: true, validatePrevouts: false },
    );

    const prevoutLookups = query.mock.calls
      .map(([parameters]) => parameters)
      .filter((parameters) => parameters.query.includes('FROM dogecoin_utxo_outputs_current_v1'));
    expect(prevoutLookups).toHaveLength(1);
    expect(prevoutLookups[0]?.query_params).toEqual({ outputKeys: ['prev-tx:0'] });
    // The one lookup feeds both the debit movement and the current-state row.
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'dogecoin_address_movements_v1',
        values: expect.arrayContaining([
          expect.objectContaining({
            movement_id: 'core-debit:prev-tx:0:tx-2:0',
            address: 'DTestAddress',
            amount_base: '150000000',
          }),
        ]),
      }),
    );
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'dogecoin_utxo_outputs_current_v1',
        values: expect.arrayContaining([
          expect.objectContaining({ output_key: 'prev-tx:0', spent_by_txid: 'tx-2' }),
        ]),
      }),
    );
  });

  it('writes core window inserts synchronously under a recognizable query id', async () => {
    const { adapter, insert } = installEmptyClickHouseClient();

    await adapter.applyCoreDogecoinWindow(
      [coreApplication({ blockHeight: 1, blockHash: 'block-1', creates: ['coinbase-tx:0'] })],
      { validatePrevouts: false },
    );

    const calls = insert.mock.calls.map(
      ([parameters]) =>
        parameters as unknown as {
          clickhouse_settings?: Record<string, unknown>;
          query_id?: string;
          table: string;
        },
    );
    expect(calls.map((call) => call.table)).toContain('dogecoin_core_processed_blocks_v1');
    for (const call of calls) {
      expect(call.clickhouse_settings).toEqual({ async_insert: 0 });
      expect(call.query_id).toMatch(/^onlydoge-core-window-[0-9a-f-]{36}$/u);
    }
    expect(new Set(calls.map((call) => call.query_id)).size).toBe(calls.length);
  });

  it('waits for abandoned core window inserts before deleting the tail', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const events: string[] = [];
    let running = 2;
    const query = vi.fn(async (parameters: ClickHouseCommandCall) => {
      if (parameters.query.includes('FROM system.processes')) {
        events.push(`processes:${running}`);
        const rows = [{ running }];
        running = Math.max(0, running - 1);
        return jsonRows(rows);
      }
      return jsonRows([{ core_tail_height: 7 }]);
    });
    const { command } = installClickHouseClient(adapter, query);
    command.mockImplementation(async (parameters: ClickHouseCommandCall) => {
      events.push(parameters.query.split(' WHERE ')[0] ?? parameters.query);
    });

    await adapter.recoverCoreDogecoinWindow(7);

    expect(events.slice(0, 4)).toEqual([
      'processes:2',
      'processes:1',
      'processes:0',
      'DELETE FROM dogecoin_core_utxo_creates_v1',
    ]);
    expect(
      query.mock.calls.find(([parameters]) =>
        parameters.query.includes('FROM system.processes'),
      )?.[0].query_params,
    ).toEqual({ queryIdPrefix: 'onlydoge-core-window-' });
  });

  it('lets every fact insert finish before surfacing a failed one', async () => {
    const { adapter, insert } = installEmptyClickHouseClient();
    const finished: string[] = [];
    insert.mockImplementation(async (parameters: { table: string; values: unknown[] }) => {
      if (parameters.table === 'dogecoin_core_utxo_creates_v1') {
        throw new Error('creates insert failed');
      }
      await new Promise((resolve) => setTimeout(resolve, 5));
      finished.push(parameters.table);
    });

    await expect(
      adapter.applyCoreDogecoinWindow(
        [
          coreApplication({ blockHeight: 1, blockHash: 'block-1', creates: ['coinbase-tx:0'] }),
          coreApplication({
            blockHeight: 2,
            blockHash: 'block-2',
            spends: ['coinbase-tx:0'],
            creates: ['spend-tx:0'],
          }),
        ],
        { validatePrevouts: false },
      ),
    ).rejects.toThrow('warehouse query failed');

    // Recovery deletes the window's rows next; none of its inserts may still
    // be in flight when that starts.
    expect(finished.sort()).toEqual([
      'analytics_transactions_v1',
      'dogecoin_address_movements_v1',
      'dogecoin_core_utxo_spends_v1',
    ]);
    expect(finished).not.toContain('dogecoin_core_processed_blocks_v1');
  });

  it('applies core Dogecoin windows to current read state when requested', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM dogecoin_core_processed_blocks_v1')) {
        return { json: async () => [] };
      }
      if (statement.includes('FROM dogecoin_utxo_outputs_current_v1')) {
        return {
          json: async () => [
            clickHouseUtxoRow({
              outputKey: 'prev-tx:0',
              txid: 'prev-tx',
              address: 'DPrevAddress',
              valueBase: '100000000',
            }),
          ],
        };
      }
      if (statement.includes('FROM dogecoin_balances_current_v1')) {
        return {
          json: async () => [
            {
              address: 'DPrevAddress',
              assetAddress: '',
              balance: '100000000',
              asOfBlockHeight: 1,
              version: 1,
            },
          ],
        };
      }
      return { json: async () => [] };
    });
    const { insert } = installClickHouseClient(adapter, query);

    await expect(
      adapter.applyCoreDogecoinWindow(
        [
          coreApplication({
            blockHeight: 2,
            blockHash: 'block-2',
            spends: ['prev-tx:0'],
            creates: ['new-tx:0'],
          }),
        ],
        { updateCurrentState: true, validatePrevouts: false },
      ),
    ).resolves.toEqual({
      applied: true,
      processTail: 2,
    });

    const insertedTables = insert.mock.calls.map(
      (call) => (call as Array<{ table: string }>).at(0)?.table ?? '<missing-table>',
    );
    expect(insertedTables).toEqual([
      'dogecoin_core_utxo_creates_v1',
      'dogecoin_core_utxo_spends_v1',
      'dogecoin_address_movements_v1',
      'analytics_transactions_v1',
      'dogecoin_utxo_outputs_current_v1',
      'dogecoin_balances_current_v1',
      'analytics_balances_current_v1',
      'dogecoin_applied_blocks_v1',
      'dogecoin_core_processed_blocks_v1',
    ]);
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'dogecoin_utxo_outputs_current_v1',
        values: expect.arrayContaining([
          expect.objectContaining({
            output_key: 'prev-tx:0',
            spent_by_txid: 'tx-2',
            version: 5,
          }),
          expect.objectContaining({
            output_key: 'new-tx:0',
            spent_by_txid: null,
            version: 4,
          }),
        ]),
      }),
    );
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'dogecoin_balances_current_v1',
        values: expect.arrayContaining([
          expect.objectContaining({
            address: 'DPrevAddress',
            balance: '0',
            version: 5,
          }),
          expect.objectContaining({
            address: 'DAddress0',
            balance: '100000000',
            version: 5,
          }),
        ]),
      }),
    );
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'dogecoin_address_movements_v1',
        values: expect.arrayContaining([
          expect.objectContaining({
            movement_id: 'core-debit:prev-tx:0:tx-2:0',
            address: 'DPrevAddress',
            direction: 'debit',
          }),
          expect.objectContaining({
            movement_id: 'core-credit:new-tx:0',
            address: 'DAddress0',
            direction: 'credit',
          }),
        ]),
      }),
    );
  });

  it('recovers current Dogecoin state from core facts when a pending prevout is missing', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    let recoveredCurrentState = false;
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('AS core_tail_height')) {
        return { json: async () => [{ core_tail_height: 2 }] };
      }
      if (statement.includes('FROM dogecoin_core_processed_blocks_v1')) {
        return { json: async () => [] };
      }
      if (statement.includes('FROM dogecoin_utxo_outputs_current_v1')) {
        return {
          json: async () =>
            recoveredCurrentState
              ? [
                  clickHouseUtxoRow({
                    outputKey: 'prev-tx:0',
                    txid: 'prev-tx',
                    address: 'DPrevAddress',
                    valueBase: '100000000',
                  }),
                ]
              : [],
        };
      }
      if (
        statement.includes('FROM dogecoin_core_utxo_creates_v1') &&
        statement.includes('LEFT JOIN') &&
        statement.includes('block_height <= {asOfBlockHeight:UInt64}')
      ) {
        return {
          json: async () => [
            clickHouseUtxoRow({
              blockHeight: 1,
              blockHash: 'block-1',
              blockTime: 1,
              outputKey: 'prev-tx:0',
              txid: 'prev-tx',
              address: 'DPrevAddress',
              valueBase: '100000000',
            }),
          ],
        };
      }
      if (statement.includes('FROM dogecoin_balances_current_v1')) {
        return {
          json: async () => [
            {
              address: 'DPrevAddress',
              assetAddress: '',
              balance: '100000000',
              asOfBlockHeight: 1,
              version: 1,
            },
          ],
        };
      }
      return { json: async () => [] };
    });
    const { command, insert } = installClickHouseClient(adapter, query);
    insert.mockImplementation(async (parameters: { table: string; values: unknown[] }) => {
      if (
        parameters.table === 'dogecoin_utxo_outputs_current_v1' &&
        parameters.values.some(
          (value) =>
            (value as { output_key?: string; spent_by_txid?: string | null }).output_key ===
              'prev-tx:0' && (value as { spent_by_txid?: string | null }).spent_by_txid === null,
        )
      ) {
        recoveredCurrentState = true;
      }
    });

    await expect(
      adapter.applyCoreDogecoinWindow(
        [
          coreApplication({
            blockHeight: 2,
            blockHash: 'block-2',
            spends: ['prev-tx:0'],
            creates: ['new-tx:0'],
          }),
        ],
        {
          updateCurrentState: true,
          validatePrevouts: false,
          statementTimeoutMs: 30000,
        },
      ),
    ).resolves.toEqual({
      applied: true,
      processTail: 2,
    });

    expect(command).toHaveBeenCalledWith(
      expect.objectContaining({
        query:
          'DELETE FROM dogecoin_core_utxo_creates_v1 WHERE block_height >= {fromBlockHeight:UInt64}',
        query_params: { fromBlockHeight: 2 },
      }),
    );
    expect(command).not.toHaveBeenCalledWith(
      expect.objectContaining({
        query: expect.stringContaining('INSERT INTO dogecoin_utxo_outputs_current_v1'),
      }),
    );
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'dogecoin_utxo_outputs_current_v1',
        values: [
          expect.objectContaining({
            output_key: 'prev-tx:0',
            spent_by_txid: null,
            version: 2,
          }),
        ],
      }),
    );
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'dogecoin_address_movements_v1',
        values: expect.arrayContaining([
          expect.objectContaining({
            movement_id: 'core-debit:prev-tx:0:tx-2:0',
            address: 'DPrevAddress',
            direction: 'debit',
          }),
        ]),
      }),
    );
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'dogecoin_core_processed_blocks_v1',
        values: [expect.objectContaining({ block_hash: 'block-2', block_height: 2 })],
      }),
    );
  });

  it('rejects missing external prevouts in core Dogecoin windows', async () => {
    const { adapter } = installEmptyClickHouseClient();

    await expect(
      adapter.applyCoreDogecoinWindow([
        coreApplication({
          blockHeight: 2,
          blockHash: 'block-2',
          spends: ['missing-tx:0'],
        }),
      ]),
    ).rejects.toThrow('missing core dogecoin prevout: missing-tx:0');
  });

  it('rejects duplicate spends before appending core Dogecoin windows', async () => {
    const { adapter, insert } = installEmptyClickHouseClient();

    await expect(
      adapter.applyCoreDogecoinWindow([
        coreApplication({
          blockHeight: 2,
          blockHash: 'block-2',
          spends: ['prev-tx:0', 'prev-tx:0'],
        }),
      ]),
    ).rejects.toThrow('duplicate dogecoin spend in core window: prev-tx:0');
    expect(insert).not.toHaveBeenCalled();
  });

  it('rejects non-contiguous block hashes inside core Dogecoin windows', async () => {
    const { adapter, insert } = installEmptyClickHouseClient();

    await expect(
      adapter.applyCoreDogecoinWindow([
        coreApplication({
          blockHeight: 2,
          blockHash: 'canonical-block-2',
          previousBlockHash: 'canonical-block-1',
        }),
        coreApplication({
          blockHeight: 3,
          blockHash: 'canonical-block-3',
          previousBlockHash: 'orphan-block-2',
        }),
      ]),
    ).rejects.toThrow(
      'non-contiguous core dogecoin chain previous_height=2 previous_hash=canonical-block-2 next_height=3 next_previous=orphan-block-2',
    );
    expect(insert).not.toHaveBeenCalled();
  });

  it('rejects core Dogecoin windows whose previous processed block was orphaned', async () => {
    const { adapter, insert } = installCoreProcessedBlocksClient({
      blockHashByHeight: { 1: 'orphan-block-1' },
    });

    await expect(
      adapter.applyCoreDogecoinWindow([
        coreApplication({
          blockHeight: 2,
          blockHash: 'canonical-block-2',
          previousBlockHash: 'canonical-block-1',
        }),
      ]),
    ).rejects.toThrow(
      'non-contiguous core dogecoin chain previous_height=1 previous_hash=orphan-block-1 next_height=2 next_previous=canonical-block-1',
    );
    expect(insert).not.toHaveBeenCalled();
  });

  it('rewinds and replays the core Dogecoin tail on a block hash mismatch', async () => {
    const { adapter, command, insert } = installCoreProcessedBlocksClient({
      blockHashByHeight: { 1: 'block-1' },
      latestRows: [{ blockHeight: 2, blockHash: 'old-block-2' }],
    });

    await expect(
      adapter.applyCoreDogecoinWindow(
        [
          coreApplication({
            blockHeight: 2,
            blockHash: 'new-block-2',
            previousBlockHash: 'block-1',
            creates: ['new-tx:0'],
          }),
        ],
        {
          updateCurrentState: true,
          validatePrevouts: false,
          statementTimeoutMs: 30000,
        },
      ),
    ).resolves.toEqual({
      applied: true,
      processTail: 2,
    });

    const commandStatements = command.mock.calls.map(([parameters]) => parameters.query);
    expect(commandStatements).toEqual(
      expect.arrayContaining([
        'DELETE FROM dogecoin_core_utxo_creates_v1 WHERE block_height >= {fromBlockHeight:UInt64}',
        'DELETE FROM dogecoin_core_utxo_spends_v1 WHERE spent_in_block >= {fromBlockHeight:UInt64}',
        'DELETE FROM dogecoin_core_processed_blocks_v1 WHERE block_height >= {fromBlockHeight:UInt64}',
        'DELETE FROM dogecoin_address_movements_v1 WHERE block_height >= {fromBlockHeight:UInt64}',
        'DELETE FROM dogecoin_address_movements_by_address_v1 WHERE block_height >= {fromBlockHeight:UInt64}',
        'DELETE FROM analytics_transactions_v1 WHERE block_height >= {fromBlockHeight:UInt64}',
      ]),
    );
    expect(
      command.mock.calls
        .map(([parameters]) => parameters)
        .filter((parameters) => parameters.query.includes('fromBlockHeight'))
        .every(
          (parameters) =>
            parameters.query_params?.fromBlockHeight === 2 &&
            parameters.clickhouse_settings?.lightweight_deletes_sync === '2',
        ),
    ).toBe(true);
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'dogecoin_core_processed_blocks_v1',
        values: [
          expect.objectContaining({
            block_hash: 'new-block-2',
            block_height: 2,
          }),
        ],
      }),
    );
  });

  it('skips core window recovery deletes when the tail has no rows', async () => {
    const { adapter, command } = installEmptyClickHouseClient();

    await adapter.recoverCoreDogecoinWindow(5009500);

    expect(command).not.toHaveBeenCalled();
  });

  it('skips core window recovery deletes when ClickHouse JSON has an empty data payload', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async () => ({
      json: async () => ({ data: [], rows: 0 }),
    }));
    const { command } = installClickHouseClient(adapter, query);

    await adapter.recoverCoreDogecoinWindow(5009500);

    expect(command).not.toHaveBeenCalled();
  });

  it('recovers a pending core window with lightweight deletes', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async () => jsonRows([{ core_tail_height: 5009500 }]));
    const { command } = installClickHouseClient(adapter, query);

    await adapter.recoverCoreDogecoinWindow(5009500);

    expect(command.mock.calls.map(([parameters]) => parameters.query)).toEqual([
      'DELETE FROM dogecoin_core_utxo_creates_v1 WHERE block_height >= {fromBlockHeight:UInt64}',
      'DELETE FROM dogecoin_core_utxo_spends_v1 WHERE spent_in_block >= {fromBlockHeight:UInt64}',
      'DELETE FROM dogecoin_core_processed_blocks_v1 WHERE block_height >= {fromBlockHeight:UInt64}',
      'DELETE FROM dogecoin_address_movements_v1 WHERE block_height >= {fromBlockHeight:UInt64}',
      'DELETE FROM dogecoin_address_movements_by_address_v1 WHERE block_height >= {fromBlockHeight:UInt64}',
      'DELETE FROM dogecoin_applied_blocks_v1 WHERE block_height >= {fromBlockHeight:UInt64}',
      'DELETE FROM analytics_transactions_v1 WHERE block_height >= {fromBlockHeight:UInt64}',
      'DELETE FROM dogecoin_transaction_refs_v1 WHERE block_height >= {fromBlockHeight:UInt64}',
    ]);
    expect(
      command.mock.calls.every(
        ([parameters]) =>
          parameters.query_params?.fromBlockHeight === 5009500 &&
          parameters.clickhouse_settings?.lightweight_deletes_sync === '2' &&
          parameters.clickhouse_settings?.mutations_sync === undefined,
      ),
    ).toBe(true);
  });

  it('materializes core Dogecoin current state in bounded output-key ranges', async () => {
    const { adapter, command } = installEmptyClickHouseClient();

    await adapter.materializeCoreDogecoinCurrentState(25, {
      statementTimeoutMs: 30000,
    });

    const commands = command.mock.calls.map(([parameters]) => parameters);
    const statements = commands.map((parameters) => parameters.query);
    const currentStateInserts = commands.filter((parameters) =>
      parameters.query.includes('INSERT INTO dogecoin_utxo_outputs_current_v1'),
    );
    const rangeParams = currentStateInserts
      .map((parameters) => parameters.query_params)
      .filter((params) => params?.rangeStart === '00' && params.rangeEnd === '01');
    const balanceInsert = commands.find((parameters) =>
      parameters.query.includes('INSERT INTO dogecoin_balances_current_v1'),
    );
    const analyticsBalanceInsert = commands.find((parameters) =>
      parameters.query.includes('INSERT INTO analytics_balances_current_v1'),
    );

    expect(statements.slice(0, 5)).toEqual([
      'ALTER TABLE dogecoin_utxo_outputs_current_v1 DELETE WHERE 1 = 1',
      'ALTER TABLE dogecoin_utxo_outputs_current_by_address_v1 DELETE WHERE 1 = 1',
      'ALTER TABLE dogecoin_balances_current_v1 DELETE WHERE 1 = 1',
      'ALTER TABLE dogecoin_applied_blocks_v1 DELETE WHERE 1 = 1',
      'ALTER TABLE analytics_balances_current_v1 DELETE WHERE 1 = 1',
    ]);
    expect(
      commands
        .slice(0, 4)
        .every((parameters) => parameters.clickhouse_settings?.mutations_sync === '2'),
    ).toBe(true);
    expect(currentStateInserts).toHaveLength(258);
    expect(rangeParams).toHaveLength(1);
    expect(currentStateInserts[0]?.query).toContain('output_key < {rangeEnd:String}');
    expect(
      currentStateInserts[0]?.query.match(/version <= \{asOfBlockHeight:UInt64\}/gu),
    ).toHaveLength(2);
    expect(currentStateInserts.at(-1)?.query).toContain('output_key >= {rangeStart:String}');
    expect(balanceInsert?.query).toContain('FROM dogecoin_utxo_outputs_current_by_address_v1');
    expect(analyticsBalanceInsert?.query).toContain(
      'FROM dogecoin_utxo_outputs_current_by_address_v1',
    );
    expect(analyticsBalanceInsert?.query).toContain('GROUP BY address');
    expect(balanceInsert?.clickhouse_settings).toMatchObject({
      max_execution_time: 30,
      optimize_aggregation_in_order: 1,
    });
  });

  it('resumes current-state materialization after the ranges an earlier attempt finished', async () => {
    const { adapter, command } = installEmptyClickHouseClient();
    const completed: Array<{ completedRanges: number; rangeCount: number }> = [];

    await adapter.materializeCoreDogecoinCurrentState(25, {
      statementTimeoutMs: 30000,
      materialization: {
        resumeFrom: { completedRanges: 100, rangeCount: 258 },
        onRangeCompleted: (progress) => {
          completed.push(progress);
        },
      },
    });

    const commands = command.mock.calls.map(([parameters]) => parameters);
    const statements = commands.map((parameters) => parameters.query);
    const currentStateInserts = commands.filter((parameters) =>
      parameters.query.includes('INSERT INTO dogecoin_utxo_outputs_current_v1'),
    );
    const lastCurrentStateInsert = statements.findLastIndex((statement) =>
      statement.includes('INSERT INTO dogecoin_utxo_outputs_current_v1'),
    );
    const firstBalanceInsert = statements.findIndex((statement) =>
      statement.includes('INSERT INTO dogecoin_balances_current_v1'),
    );

    // The current UTXOs of the finished ranges are kept.
    expect(statements).not.toContain(
      'ALTER TABLE dogecoin_utxo_outputs_current_v1 DELETE WHERE 1 = 1',
    );
    expect(statements).not.toContain(
      'ALTER TABLE dogecoin_utxo_outputs_current_by_address_v1 DELETE WHERE 1 = 1',
    );
    expect(currentStateInserts).toHaveLength(158);
    // Range 0 is everything below "00", so range 100 starts at hex 99.
    expect(currentStateInserts[0]?.query_params).toMatchObject({
      rangeStart: '63',
      rangeEnd: '64',
    });
    expect(completed).toHaveLength(158);
    expect(completed[0]).toEqual({ completedRanges: 101, rangeCount: 258 });
    expect(completed.at(-1)).toEqual({ completedRanges: 258, rangeCount: 258 });
    // Balances and applied blocks are derived from the whole UTXO set, so
    // whatever the failed attempt left in them is cleared before the rebuild.
    expect(statements.slice(lastCurrentStateInsert + 1, firstBalanceInsert)).toEqual([
      'ALTER TABLE dogecoin_balances_current_v1 DELETE WHERE 1 = 1',
      'ALTER TABLE analytics_balances_current_v1 DELETE WHERE 1 = 1',
      'ALTER TABLE dogecoin_applied_blocks_v1 DELETE WHERE 1 = 1',
    ]);
  });

  it('restarts materialization when the checkpoint used another range split', async () => {
    const { adapter, command } = installEmptyClickHouseClient();

    await adapter.materializeCoreDogecoinCurrentState(25, {
      statementTimeoutMs: 30000,
      materialization: { resumeFrom: { completedRanges: 100, rangeCount: 4098 } },
    });

    const statements = command.mock.calls.map(([parameters]) => parameters.query);
    expect(statements[0]).toBe('ALTER TABLE dogecoin_utxo_outputs_current_v1 DELETE WHERE 1 = 1');
    expect(
      statements.filter((statement) =>
        statement.includes('INSERT INTO dogecoin_utxo_outputs_current_v1'),
      ),
    ).toHaveLength(258);
  });

  it('splits current-state materialization finer on large warehouses', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async (parameters: { query: string }) =>
      jsonRows(parameters.query.includes('FROM system.parts') ? [{ rows: '300000000' }] : []),
    );
    const { command } = installClickHouseClient(adapter, query);

    await adapter.materializeCoreDogecoinCurrentState(25, { statementTimeoutMs: 30000 });

    const rangeParams = command.mock.calls
      .map(([parameters]) => parameters)
      .filter((parameters) =>
        parameters.query.includes('INSERT INTO dogecoin_utxo_outputs_current_v1'),
      )
      .map((parameters) => parameters.query_params);
    expect(queryCalls(query)[0]?.query_params).toEqual({ table: 'dogecoin_core_utxo_creates_v1' });
    expect(rangeParams).toHaveLength(4098);
    expect(rangeParams[0]).toMatchObject({ rangeEnd: '000' });
    expect(rangeParams[1]).toMatchObject({ rangeStart: '000', rangeEnd: '001' });
    expect(rangeParams.at(-2)).toMatchObject({ rangeStart: 'fff', rangeEnd: 'g' });
    expect(rangeParams.at(-1)).toMatchObject({ rangeStart: 'g' });
  });

  it('aggregates balances per address range over deduplicated current UTXOs', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async (parameters: { query: string }) =>
      jsonRows(
        parameters.query.includes('mergeTreeIndex(')
          ? // Unsorted, with a repeat and the empty address a non-standard output has.
            [{ boundary: 'DM' }, { boundary: '' }, { boundary: 'D7' }, { boundary: 'DM' }]
          : [],
      ),
    );
    const { command } = installClickHouseClient(adapter, query);
    const onActivity = vi.fn();

    await adapter.materializeCoreDogecoinCurrentState(25, {
      statementTimeoutMs: 30000,
      materialization: { onActivity },
    });

    const balanceInserts = command.mock.calls
      .map(([parameters]) => parameters)
      .filter((parameters) =>
        parameters.query.includes('INSERT INTO dogecoin_balances_current_v1'),
      );
    const analyticsBalanceInserts = command.mock.calls
      .map(([parameters]) => parameters)
      .filter((parameters) =>
        parameters.query.includes('INSERT INTO analytics_balances_current_v1'),
      );
    const indexQuery = queryCalls(query).find((call) => call.query.includes('mergeTreeIndex('));

    expect(indexQuery?.query).toContain(
      "mergeTreeIndex(currentDatabase(), 'dogecoin_utxo_outputs_current_by_address_v1')",
    );
    expect(indexQuery?.query_params).toEqual({ rowsPerRange: 500_000 });
    expect(balanceInserts.map((parameters) => parameters.query_params)).toEqual([
      { asOfBlockHeight: 25, rangeEnd: 'D7' },
      { asOfBlockHeight: 25, rangeStart: 'D7', rangeEnd: 'DM' },
      { asOfBlockHeight: 25, rangeStart: 'DM' },
    ]);
    expect(analyticsBalanceInserts).toHaveLength(3);
    for (const insert of [...balanceInserts, ...analyticsBalanceInserts]) {
      // FINAL: a replayed range may have written the same output twice.
      expect(insert.query).toContain('FROM dogecoin_utxo_outputs_current_by_address_v1 FINAL');
    }
    expect(balanceInserts[1]?.query).toContain('address >= {rangeStart:String}');
    expect(balanceInserts[1]?.query).toContain('address < {rangeEnd:String}');
    // One activity signal per address range and one per applied-block range.
    expect(onActivity).toHaveBeenCalledTimes(4);
  });

  it('copies applied blocks in bounded height ranges', async () => {
    const { adapter, command } = installEmptyClickHouseClient();

    await adapter.materializeCoreDogecoinCurrentState(1_200_000, { statementTimeoutMs: 30000 });

    const appliedBlockInserts = command.mock.calls
      .map(([parameters]) => parameters)
      .filter((parameters) => parameters.query.includes('INSERT INTO dogecoin_applied_blocks_v1'));
    expect(appliedBlockInserts.map((parameters) => parameters.query_params)).toEqual([
      { startHeight: 0, endHeight: 499_999 },
      { startHeight: 500_000, endHeight: 999_999 },
      { startHeight: 1_000_000, endHeight: 1_200_000 },
    ]);
    expect(appliedBlockInserts[0]?.query).toContain('FROM dogecoin_core_processed_blocks_v1');
  });

  it('runs materialization statements on the long-timeout client', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const windowCommand = vi.fn(async (_parameters: ClickHouseCommandCall) => undefined);
    const materializationCommand = vi.fn(async (_parameters: ClickHouseCommandCall) => undefined);
    Object.assign(adapter as unknown as Record<string, unknown>, {
      client: { command: windowCommand, query: vi.fn(async () => jsonRows([])) },
      materializationClient: { command: materializationCommand },
    });

    await adapter.materializeCoreDogecoinCurrentState(25, { statementTimeoutMs: 30000 });

    expect(windowCommand).not.toHaveBeenCalled();
    expect(materializationCommand.mock.calls.length).toBeGreaterThan(258);
  });

  it('reads the processed tail the warehouse actually holds', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    let rows: Array<Record<string, unknown>> = [];
    const query = vi.fn(async (_parameters: { query: string }) => jsonRows(rows));
    installClickHouseClient(adapter, query);

    await expect(adapter.getCoreProcessedTail()).resolves.toBeNull();

    rows = [{ tail: '3449099' }];
    await expect(adapter.getCoreProcessedTail()).resolves.toBe(3_449_099);
    // Height 0 is a processed block, not an empty warehouse.
    rows = [{ tail: 0 }];
    await expect(adapter.getCoreProcessedTail()).resolves.toBe(0);
    expect(queryCalls(query)[0]?.query).toContain('FROM dogecoin_core_processed_blocks_v1');
    expect(queryCalls(query)[0]?.query).toContain('ORDER BY block_height DESC');
  });
});

function clickHouseUtxoRow(overrides: Record<string, unknown> = {}) {
  return {
    blockHeight: 1,
    blockHash: 'hash',
    blockTime: 1,
    txid: 'txid',
    txIndex: 0,
    vout: 0,
    outputKey: 'txid:0',
    address: 'DTestAddress',
    scriptType: 'pubkeyhash',
    valueBase: '1',
    isCoinbase: false,
    isSpendable: true,
    spentByTxid: null,
    spentInBlock: null,
    spentInputIndex: null,
    ...overrides,
  };
}

function coreApplication(input: {
  blockHash: string;
  blockHeight: number;
  creates?: string[];
  previousBlockHash?: string | null;
  spends?: string[];
}): CoreDogecoinBlockApplication {
  return {
    blockHeight: input.blockHeight,
    blockHash: input.blockHash,
    blockTime: input.blockHeight,
    previousBlockHash:
      input.previousBlockHash ?? (input.blockHeight > 0 ? `block-${input.blockHeight - 1}` : null),
    rawStorageKey: 'block',
    txCount: 1,
    utxoCreates: (input.creates ?? []).map((outputKey, index) => ({
      blockHeight: input.blockHeight,
      blockHash: input.blockHash,
      blockTime: input.blockHeight,
      txid: outputKey.split(':')[0] ?? outputKey,
      txIndex: 0,
      vout: index,
      outputKey,
      address: `DAddress${index}`,
      scriptType: 'pubkeyhash',
      valueBase: '100000000',
      isCoinbase: input.blockHeight === 1,
      isSpendable: true,
      spentByTxid: null,
      spentInBlock: null,
      spentInputIndex: null,
    })),
    utxoSpends: (input.spends ?? []).map((outputKey, index) => ({
      outputKey,
      spentByTxid: `tx-${input.blockHeight}`,
      spentInBlock: input.blockHeight,
      spentInputIndex: index,
    })),
  };
}

function addressSummaryAdapter(input: {
  addressMovementRows: Array<{
    receivedBase: string;
    sentBase: string;
    txCount: number;
  }>;
  coreMovementRows?: Array<{
    receivedBase: string;
    sentBase: string;
    txCount: number;
  }>;
  coreSpendableRows?: Array<{ balance: string; utxoCount: number }>;
}) {
  const adapter = new ClickHouseWarehouseAdapter({
    driver: 'clickhouse',
    location: 'http://clickhouse:8123',
  });
  const query = vi.fn(async ({ query: statement }: { query: string }) => {
    if (statement.includes('FROM dogecoin_address_movements_by_address_v1')) {
      return jsonRows(input.addressMovementRows);
    }

    if (statement.includes('address_outputs AS') && !statement.includes('UNION ALL')) {
      return jsonRows(input.coreSpendableRows ?? [{ balance: '0', utxoCount: 0 }]);
    }

    if (statement.includes('address_outputs AS')) {
      return jsonRows(input.coreMovementRows ?? []);
    }

    if (
      statement.includes('FROM dogecoin_utxo_outputs_current_by_address_v1') &&
      statement.includes('sum(toInt256(value_base))')
    ) {
      return jsonRows([{ balance: '4' }]);
    }

    if (statement.includes('FROM dogecoin_utxo_outputs_current_by_address_v1')) {
      return jsonRows([{ utxoCount: 1 }]);
    }

    return jsonRows([]);
  });

  (adapter as unknown as { client: { query: typeof query } }).client = {
    query,
  };
  return { adapter, query };
}

function addressSummaryMovementRow(receivedBase: string, sentBase: string, txCount: number) {
  return { receivedBase, sentBase, txCount };
}

async function readTestAddressSummary(
  adapter: ClickHouseWarehouseAdapter,
  query: { mock: { calls: Array<[{ query: string }]> } },
) {
  const summary = await adapter.getAddressSummary('DInputAddress');
  return {
    summary,
    statements: query.mock.calls.map(([parameters]) => parameters.query),
  };
}

function expectStandardAddressSummary(summary: unknown): void {
  expect(summary).toMatchObject({
    balance: '4',
    receivedBase: '11',
    sentBase: '7',
    txCount: 2,
    utxoCount: 1,
  });
}

function jsonRows<T>(rows: T[]) {
  return { json: async () => rows };
}

function queryCalls(query: { mock: { calls: unknown[][] } }): ClickHouseCommandCall[] {
  return query.mock.calls.map(([parameters]) => parameters as ClickHouseCommandCall);
}

function installCoreProcessedBlocksClient(input: {
  blockHashByHeight: Record<number, string>;
  latestRows?: Array<{ blockHash: string; blockHeight: number }>;
}) {
  const adapter = new ClickHouseWarehouseAdapter({
    driver: 'clickhouse',
    location: 'http://clickhouse:8123',
  });
  const query = vi.fn(async (parameters: { query: string }) => {
    const params = (parameters as { query_params?: Record<string, unknown> }).query_params;
    if (parameters.query.includes('AS core_tail_height')) {
      return jsonRows([{ core_tail_height: 2 }]);
    }
    if (parameters.query.includes('FROM dogecoin_core_processed_blocks_v1')) {
      if (parameters.query.includes('block_height = {blockHeight:UInt64}')) {
        const blockHeight = Number(params?.blockHeight);
        const blockHash = input.blockHashByHeight[blockHeight];
        return jsonRows(
          blockHash === undefined ? [] : [{ blockHeight: params?.blockHeight, blockHash }],
        );
      }
      return jsonRows(input.latestRows ?? []);
    }
    return jsonRows([]);
  });
  const { command, insert } = installClickHouseClient(adapter, query);
  return { adapter, command, insert, query };
}

function installClickHouseClient(
  adapter: ClickHouseWarehouseAdapter,
  query: (parameters: { query: string }) => Promise<unknown>,
) {
  const insert = vi.fn(async (_parameters: { table: string; values: unknown[] }) => undefined);
  const command = vi.fn(async (_parameters: ClickHouseCommandCall) => undefined);
  const client = { command, query, insert };
  // Materialization statements run on a second client with a long request
  // timeout; route both through the same mock.
  Object.assign(adapter as unknown as Record<string, unknown>, {
    client,
    materializationClient: client,
  });
  return { command, insert };
}

function installEmptyClickHouseClient() {
  const adapter = new ClickHouseWarehouseAdapter({
    driver: 'clickhouse',
    location: 'http://clickhouse:8123',
  });
  const query = vi.fn(async () => ({ json: async () => [] }));
  const { command, insert } = installClickHouseClient(adapter, query);
  return { adapter, command, insert, query };
}
