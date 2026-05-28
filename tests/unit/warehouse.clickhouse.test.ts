import type { CoreDogecoinBlockApplication } from '@onlydoge/indexing-pipeline';
import { ClickHouseWarehouseAdapter } from '@onlydoge/platform';
import { InfrastructureError } from '@onlydoge/shared-kernel';
import { describe, expect, it, vi } from 'vitest';

interface ClickHouseCommandCall {
  clickhouse_settings?: Record<string, unknown>;
  query: string;
  query_params?: Record<string, number | string>;
}

describe('clickhouse warehouse adapter', () => {
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

    (adapter as unknown as { client: { query: typeof query } }).client = { query };

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

    (adapter as unknown as { client: { query: typeof query } }).client = { query };

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

    (adapter as unknown as { client: { query: typeof query } }).client = { query };

    await expect(
      adapter.listCurrentBalancesPage(7, null, 5000, {
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

    (adapter as unknown as { client: { query: typeof query } }).client = { query };

    await expect(
      adapter.listCurrentBalancesPage(7, null, 5000, {
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

    (adapter as unknown as { client: { query: typeof query } }).client = { query };

    const outputKeys = Array.from(
      { length: 400 },
      (_, index) => `doge-${'x'.repeat(64)}-${index.toString().padStart(4, '0')}`,
    );

    const rows = await adapter.getUtxoOutputs(7, outputKeys);

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
          parameters.query.includes('FROM utxo_outputs_current_v2') &&
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
      if (statement.includes('FROM applied_blocks_v2')) {
        return { json: async () => [] };
      }

      if (statement.includes('FROM utxo_outputs_current_v2')) {
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

      if (statement.includes('FROM balances_v2')) {
        return {
          json: async () => [
            {
              networkId: 7,
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
        networkId: 7,
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
            networkId: 7,
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
            networkId: 7,
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
        transfers: [],
        directLinkDeltas: [
          {
            networkId: 7,
            fromAddress: 'DInputAddress',
            toAddress: 'DOutputAddress',
            assetAddress: 'DOGE',
            transferCount: 1,
            totalAmountBase: '1',
            firstSeenBlockHeight: 2,
            lastSeenBlockHeight: 2,
          },
        ],
      },
    ]);

    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(
      statements.find((statement) => statement.includes('FROM utxo_outputs_current_v2')),
    ).toContain('LIMIT 1 BY output_key');
    expect(statements.find((statement) => statement.includes('FROM balances_v2'))).toContain(
      "(address, asset_address) IN (('DInputAddress', 'DOGE'), ('DOutputAddress', 'DOGE'))",
    );
    expect(statements.find((statement) => statement.includes('FROM direct_links_v2'))).toContain(
      "(from_address, to_address, asset_address) IN (('DInputAddress', 'DOutputAddress', 'DOGE'))",
    );
    expect(statements.some((statement) => statement.includes('argMax('))).toBe(false);
    const insertedTables = insert.mock.calls.map(
      (call) => (call as Array<{ table: string }>).at(0)?.table ?? '<missing-table>',
    );

    expect(insertedTables).toContain('utxo_outputs_current_v2');
  });

  it('lists address UTXOs from a deduplicated key page before loading full rows', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM utxo_outputs_current_by_address_v2')) {
        return {
          json: async () => [
            {
              outputKey: 'prev-txid:1',
              blockHeight: 123,
              txIndex: 4,
              vout: 1,
            },
          ],
        };
      }

      if (
        statement.includes('FROM utxo_outputs_current_v2') &&
        statement.includes('AND output_key IN')
      ) {
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

    (adapter as unknown as { client: { query: typeof query } }).client = { query };

    const rows = await adapter.listAddressUtxos(7, 'DInputAddress', 0, 50);
    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      outputKey: 'prev-txid:1',
      address: 'DInputAddress',
    });
    expect(
      statements.some(
        (statement) =>
          statement.includes('LIMIT 1 BY output_key') &&
          statement.includes('FROM utxo_outputs_current_by_address_v2') &&
          !statement.includes('FINAL'),
      ),
    ).toBe(true);
  });

  it('falls back to the versioned UTXO table when the current-state table misses rows', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM utxo_outputs_current_v2')) {
        return { json: async () => [] };
      }

      if (statement.includes('FROM utxo_outputs_v2')) {
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
    const { insert } = installClickHouseClient(adapter, query);

    const rows = await adapter.getUtxoOutputs(7, ['prev-txid:1']);

    expect(rows.get('prev-txid:1')).toMatchObject({
      outputKey: 'prev-txid:1',
      spentByTxid: 'next-txid',
      spentInBlock: 124,
    });
    expect(
      query.mock.calls.some(
        ([parameters]) =>
          parameters.query.includes('FROM utxo_outputs_current_v2') &&
          parameters.query.includes('LIMIT 1 BY output_key') &&
          !parameters.query.includes('FINAL'),
      ),
    ).toBe(true);
    expect(
      query.mock.calls.some(([parameters]) => parameters.query.includes('FROM utxo_outputs_v2')),
    ).toBe(true);
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'utxo_outputs_current_v2',
        values: [
          expect.objectContaining({
            output_key: 'prev-txid:1',
            version: 124,
          }),
        ],
      }),
    );
  });

  it('falls back to core Dogecoin create and spend tables for historical UTXO lookups', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (
        statement.includes('FROM utxo_outputs_current_v2') ||
        statement.includes('FROM utxo_outputs_v2')
      ) {
        return { json: async () => [] };
      }

      if (
        statement.includes('FROM core_utxo_creates_v1') &&
        statement.includes('FROM core_utxo_spends_v1')
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

    (adapter as unknown as { client: { query: typeof query } }).client = { query };

    const rows = await adapter.getUtxoOutputs(7, ['prev-txid:1']);
    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(rows.get('prev-txid:1')).toMatchObject({
      outputKey: 'prev-txid:1',
      spentByTxid: 'next-txid',
      spentInBlock: 124,
    });
    expect(
      statements.some(
        (statement) =>
          statement.includes('FROM core_utxo_creates_v1') &&
          statement.includes('FROM core_utxo_spends_v1') &&
          statement.includes('LIMIT 1 BY output_key') &&
          statement.includes('LIMIT 1 BY spent_output_key'),
      ),
    ).toBe(true);
  });

  it('falls back to core Dogecoin outputs for transaction refs', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM utxo_outputs_v2')) {
        return { json: async () => [] };
      }

      if (statement.includes('FROM core_utxo_creates_v1')) {
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

      return { json: async () => [] };
    });

    (adapter as unknown as { client: { query: typeof query } }).client = { query };

    const ref = await adapter.getTransactionRef(7, 'doge-txid');
    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(ref).toEqual({
      blockHeight: 123,
      blockHash: 'block-hash',
      blockTime: 456,
      txIndex: 7,
    });
    expect(
      statements.some(
        (statement) =>
          statement.includes('FROM core_utxo_creates_v1') &&
          statement.includes('output_key >= {prefix:String}') &&
          statement.includes('output_key < {prefixEnd:String}'),
      ),
    ).toBe(true);
  });

  it('boots clickhouse with address-oriented read models and backfills them once', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const command = vi.fn(async () => undefined);
    const query = vi.fn(async () => ({ json: async () => [] }));

    (
      adapter as unknown as {
        client: {
          command: typeof command;
          query: typeof query;
        };
      }
    ).client = {
      command,
      query,
    };

    await adapter.boot();

    const statements = command.mock.calls.map(
      (call) => (call as Array<{ query: string }>).at(0)?.query ?? '',
    );

    expect(
      statements.some((statement) =>
        statement.includes('CREATE TABLE IF NOT EXISTS utxo_outputs_current_by_address_v2'),
      ),
    ).toBe(true);
    expect(
      statements.some((statement) =>
        statement.includes(
          'CREATE MATERIALIZED VIEW IF NOT EXISTS utxo_outputs_current_by_address_v2_mv',
        ),
      ),
    ).toBe(true);
    expect(
      statements.some((statement) =>
        statement.includes('CREATE TABLE IF NOT EXISTS address_movements_by_address_v2'),
      ),
    ).toBe(true);
    expect(
      statements.some((statement) =>
        statement.includes(
          'CREATE MATERIALIZED VIEW IF NOT EXISTS address_movements_by_address_v2_mv',
        ),
      ),
    ).toBe(true);
    expect(
      statements.some((statement) =>
        statement.includes('INSERT INTO utxo_outputs_current_by_address_v2'),
      ),
    ).toBe(true);
    expect(
      statements.some((statement) =>
        statement.includes('INSERT INTO address_movements_by_address_v2'),
      ),
    ).toBe(true);
    expect(
      statements.some((statement) =>
        statement.includes('ADD INDEX IF NOT EXISTS core_utxo_creates_address_idx'),
      ),
    ).toBe(true);
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
          statement.includes('FROM address_movements_by_address_v2') &&
          statement.includes('sumIf(amount_base_i256'),
      ),
    ).toBe(true);
  });

  it('falls back to core Dogecoin history when address movement tables are empty', async () => {
    const { adapter, query } = addressSummaryAdapter({
      addressMovementRows: [addressSummaryMovementRow('0', '0', 0)],
      coreMovementRows: [addressSummaryMovementRow('11', '7', 2)],
    });

    const { statements, summary } = await readTestAddressSummary(adapter, query);

    expectStandardAddressSummary(summary);
    expect(
      statements.some(
        (statement) =>
          statement.includes('WITH address_outputs') &&
          statement.includes('FROM core_utxo_creates_v1') &&
          statement.includes('FROM core_utxo_spends_v1'),
      ),
    ).toBe(true);
  });

  it('uses aggregate ordering for core-backed address transaction history', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM address_movements_by_address_v2')) {
        return { json: async () => [] };
      }

      if (statement.includes('address_outputs AS')) {
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
            },
          ],
        };
      }

      return { json: async () => [] };
    });

    (adapter as unknown as { client: { query: typeof query } }).client = { query };

    const rows = await adapter.listAddressTransactions(7, 'DInputAddress', 0, 5);
    const statements = query.mock.calls.map(([parameters]) => parameters.query);

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      txid: 'doge-tx',
      txIndex: 2,
    });
    expect(
      statements.some((statement) =>
        statement.includes('ORDER BY block_height DESC, max(tx_index) DESC, txid DESC'),
      ),
    ).toBe(true);
    expect(
      statements.some(
        (statement) =>
          statement.includes('address_spends AS') &&
          statement.includes('block_height IN (SELECT spent_in_block FROM address_spends)'),
      ),
    ).toBe(true);
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
      'core_utxo_creates_v1',
      'core_utxo_spends_v1',
      'address_movements_v2',
      'core_processed_blocks_v1',
    ]);
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'core_utxo_spends_v1',
        values: [
          expect.objectContaining({
            network_id: 7,
            spent_output_key: 'coinbase-tx:0',
            spent_by_txid: 'tx-2',
            spent_in_block: 2,
          }),
        ],
      }),
    );
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'address_movements_v2',
        values: expect.arrayContaining([
          expect.objectContaining({
            movement_id: 'core-credit:7:coinbase-tx:0',
            address: 'DAddress0',
            direction: 'credit',
            amount_base: '100000000',
          }),
          expect.objectContaining({
            movement_id: 'core-debit:7:coinbase-tx:0:tx-2:0',
            address: 'DAddress0',
            direction: 'debit',
            amount_base: '100000000',
          }),
        ]),
      }),
    );
  });

  it('applies core Dogecoin windows to current read state when requested', async () => {
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(async ({ query: statement }: { query: string }) => {
      if (statement.includes('FROM core_processed_blocks_v1')) {
        return { json: async () => [] };
      }
      if (statement.includes('FROM utxo_outputs_current_v2')) {
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
      if (statement.includes('FROM balances_v2')) {
        return {
          json: async () => [
            {
              networkId: 7,
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
      'core_utxo_creates_v1',
      'core_utxo_spends_v1',
      'address_movements_v2',
      'utxo_outputs_current_v2',
      'balances_v2',
      'applied_blocks_v2',
      'core_processed_blocks_v1',
    ]);
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'utxo_outputs_current_v2',
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
        table: 'balances_v2',
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
        table: 'address_movements_v2',
        values: expect.arrayContaining([
          expect.objectContaining({
            movement_id: 'core-debit:7:prev-tx:0:tx-2:0',
            address: 'DPrevAddress',
            direction: 'debit',
          }),
          expect.objectContaining({
            movement_id: 'core-credit:7:new-tx:0',
            address: 'DAddress0',
            direction: 'credit',
          }),
        ]),
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
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(
      async ({
        query: statement,
        query_params: params,
      }: {
        query: string;
        query_params?: Record<string, unknown>;
      }) => {
        if (statement.includes('FROM core_processed_blocks_v1')) {
          if (statement.includes('block_height = {blockHeight:UInt64}')) {
            return {
              json: async () => [
                {
                  blockHeight: params?.blockHeight,
                  blockHash: 'orphan-block-1',
                },
              ],
            };
          }
          return { json: async () => [] };
        }
        return { json: async () => [] };
      },
    );
    const { insert } = installClickHouseClient(adapter, query);

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
    const adapter = new ClickHouseWarehouseAdapter({
      driver: 'clickhouse',
      location: 'http://clickhouse:8123',
    });
    const query = vi.fn(
      async ({
        query: statement,
        query_params: params,
      }: {
        query: string;
        query_params?: Record<string, unknown>;
      }) => {
        if (statement.includes('FROM core_processed_blocks_v1')) {
          if (statement.includes('block_height = {blockHeight:UInt64}')) {
            return {
              json: async () => [
                {
                  blockHeight: params?.blockHeight,
                  blockHash: 'block-1',
                },
              ],
            };
          }
          return {
            json: async () => [
              {
                blockHeight: 2,
                blockHash: 'old-block-2',
              },
            ],
          };
        }
        return { json: async () => [] };
      },
    );
    const { command, insert } = installClickHouseClient(adapter, query);

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
        { updateCurrentState: true, validatePrevouts: false, statementTimeoutMs: 30000 },
      ),
    ).resolves.toEqual({
      applied: true,
      processTail: 2,
    });

    const commandStatements = command.mock.calls.map(([parameters]) => parameters.query);
    expect(commandStatements).toEqual(
      expect.arrayContaining([
        'ALTER TABLE core_utxo_creates_v1 DELETE WHERE network_id = {networkId:UInt64} AND block_height >= {fromBlockHeight:UInt64}',
        'ALTER TABLE core_utxo_spends_v1 DELETE WHERE network_id = {networkId:UInt64} AND spent_in_block >= {fromBlockHeight:UInt64}',
        'ALTER TABLE core_processed_blocks_v1 DELETE WHERE network_id = {networkId:UInt64} AND block_height >= {fromBlockHeight:UInt64}',
        'ALTER TABLE address_movements_v2 DELETE WHERE network_id = {networkId:UInt64} AND block_height >= {fromBlockHeight:UInt64}',
        'ALTER TABLE address_movements_by_address_v2 DELETE WHERE network_id = {networkId:UInt64} AND block_height >= {fromBlockHeight:UInt64}',
      ]),
    );
    expect(
      command.mock.calls
        .map(([parameters]) => parameters)
        .filter((parameters) => parameters.query.includes('fromBlockHeight'))
        .every(
          (parameters) =>
            parameters.query_params?.fromBlockHeight === 2 &&
            parameters.clickhouse_settings?.mutations_sync === '2',
        ),
    ).toBe(true);
    expect(insert).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'core_processed_blocks_v1',
        values: [expect.objectContaining({ block_hash: 'new-block-2', block_height: 2 })],
      }),
    );
  });

  it('materializes core Dogecoin current state per network and in bounded output-key ranges', async () => {
    const { adapter, command } = installEmptyClickHouseClient();

    await adapter.materializeCoreDogecoinCurrentState(7, 25, { statementTimeoutMs: 30000 });

    const commands = command.mock.calls.map(([parameters]) => parameters);
    const statements = commands.map((parameters) => parameters.query);
    const currentStateInserts = commands.filter((parameters) =>
      parameters.query.includes('INSERT INTO utxo_outputs_current_v2'),
    );
    const rangeParams = currentStateInserts
      .map((parameters) => parameters.query_params)
      .filter((params) => params?.rangeStart === '00' && params.rangeEnd === '01');
    const balanceInsert = commands.find((parameters) =>
      parameters.query.includes('INSERT INTO balances_v2'),
    );

    expect(statements.slice(0, 4)).toEqual([
      'ALTER TABLE utxo_outputs_current_v2 DELETE WHERE network_id = {networkId:UInt64}',
      'ALTER TABLE utxo_outputs_current_by_address_v2 DELETE WHERE network_id = {networkId:UInt64}',
      'ALTER TABLE balances_v2 DELETE WHERE network_id = {networkId:UInt64}',
      'ALTER TABLE applied_blocks_v2 DELETE WHERE network_id = {networkId:UInt64}',
    ]);
    expect(
      commands.slice(0, 4).every((parameters) => parameters.query_params?.networkId === 7),
    ).toBe(true);
    expect(
      commands
        .slice(0, 4)
        .every((parameters) => parameters.clickhouse_settings?.mutations_sync === '2'),
    ).toBe(true);
    expect(currentStateInserts).toHaveLength(258);
    expect(rangeParams).toHaveLength(1);
    expect(currentStateInserts[0]?.query).toContain('output_key < {rangeEnd:String}');
    expect(
      currentStateInserts[0]?.query.match(/AND version <= \{asOfBlockHeight:UInt64\}/gu),
    ).toHaveLength(2);
    expect(currentStateInserts.at(-1)?.query).toContain('output_key >= {rangeStart:String}');
    expect(balanceInsert?.query).toContain('FROM utxo_outputs_current_by_address_v2');
    expect(balanceInsert?.clickhouse_settings).toMatchObject({
      max_execution_time: 30,
      optimize_aggregation_in_order: 1,
    });
  });
});

function clickHouseUtxoRow(overrides: Record<string, unknown> = {}) {
  return {
    networkId: 7,
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
    networkId: 7,
    blockHeight: input.blockHeight,
    blockHash: input.blockHash,
    blockTime: input.blockHeight,
    previousBlockHash:
      input.previousBlockHash ?? (input.blockHeight > 0 ? `block-${input.blockHeight - 1}` : null),
    rawStorageKey: 'block',
    txCount: 1,
    utxoCreates: (input.creates ?? []).map((outputKey, index) => ({
      networkId: 7,
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
  addressMovementRows: Array<{ receivedBase: string; sentBase: string; txCount: number }>;
  coreMovementRows?: Array<{ receivedBase: string; sentBase: string; txCount: number }>;
}) {
  const adapter = new ClickHouseWarehouseAdapter({
    driver: 'clickhouse',
    location: 'http://clickhouse:8123',
  });
  const query = vi.fn(async ({ query: statement }: { query: string }) => {
    if (statement.includes('FROM address_movements_by_address_v2')) {
      return jsonRows(input.addressMovementRows);
    }

    if (statement.includes('WITH address_outputs')) {
      return jsonRows(input.coreMovementRows ?? []);
    }

    if (statement.includes('FROM balances_v2')) {
      return jsonRows([{ balance: '4' }]);
    }

    if (statement.includes('FROM utxo_outputs_current_by_address_v2')) {
      return jsonRows([{ utxoCount: 1 }]);
    }

    return jsonRows([]);
  });

  (adapter as unknown as { client: { query: typeof query } }).client = { query };
  return { adapter, query };
}

function addressSummaryMovementRow(receivedBase: string, sentBase: string, txCount: number) {
  return { receivedBase, sentBase, txCount };
}

async function readTestAddressSummary(
  adapter: ClickHouseWarehouseAdapter,
  query: { mock: { calls: Array<[{ query: string }]> } },
) {
  const summary = await adapter.getAddressSummary(7, 'DInputAddress');
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

function installClickHouseClient(
  adapter: ClickHouseWarehouseAdapter,
  query: (parameters: { query: string }) => Promise<unknown>,
) {
  const insert = vi.fn(async () => undefined);
  const command = vi.fn(async (_parameters: ClickHouseCommandCall) => undefined);
  (
    adapter as unknown as {
      client: { command: typeof command; insert: typeof insert; query: typeof query };
    }
  ).client = {
    command,
    query,
    insert,
  };
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
