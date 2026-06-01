import { createHash } from 'node:crypto';
import { type AuthenticatedApiKey, InMemoryApiKeyRateLimiter } from '@onlydoge/access-control';
import {
  configKeyDogecoinAnalyticsFactsReady,
  configKeyDogecoinAnalyticsFactsTail,
  configKeyIndexerFinalizedTail,
} from '@onlydoge/indexing-pipeline';
import {
  NotFoundError,
  OnlyDogeError,
  type PrimaryId,
  TooEarlyError,
  ValidationError,
} from '@onlydoge/shared-kernel';
import { Parser } from 'node-sql-parser';

import type {
  AnalyticsConfigPort,
  AnalyticsNetworkPort,
  AnalyticsWarehousePort,
} from '../contracts/ports';
import {
  type AnalyticsQueryInput,
  type AnalyticsQueryLimits,
  type AnalyticsQueryParams,
  type AnalyticsQueryResponse,
  type AnalyticsSchemaResponse,
  analyticsQueryDefaultLimit,
  analyticsQueryMaxBytesToRead,
  analyticsQueryMaxConcurrentRequests,
  analyticsQueryMaxExecutionSeconds,
  analyticsQueryMaxResultRows,
  analyticsQueryMaxRowsToRead,
  analyticsQueryMaxSqlCharacters,
  analyticsQueryMaxWindowSeconds,
  analyticsQueryRateLimitMaxRequests,
  analyticsQueryRateLimitWindowMs,
  analyticsTransactionsTable,
} from '../domain/query-models';

type AnalyticsNetwork = Awaited<ReturnType<AnalyticsNetworkPort['listActiveNetworks']>>[number];

const parser = new Parser();
const placeholderPatterns = [
  { pattern: /\{networkId:UInt64\}/gu, replacement: '1' },
  { pattern: /\{fromTime:UInt64\}/gu, replacement: '1' },
  { pattern: /\{toTime:UInt64\}/gu, replacement: '2' },
  { pattern: /\{maxFinalizedHeight:UInt64\}/gu, replacement: '100' },
  { pattern: /\{limit:UInt64\}/gu, replacement: '100' },
];
const requiredPlaceholders = [
  '{networkId:UInt64}',
  '{fromTime:UInt64}',
  '{toTime:UInt64}',
  '{maxFinalizedHeight:UInt64}',
] as const;
const requiredColumns = ['network_id', 'block_time', 'block_height'];
const requiredPredicatePatterns = [
  {
    column: 'network_id',
    pattern: /(?:\b[a-zA-Z_][\w]*\.)?\bnetwork_id\b\s*=\s*\{networkId:UInt64\}/iu,
  },
  {
    column: 'block_time',
    pattern: /(?:\b[a-zA-Z_][\w]*\.)?\bblock_time\b\s*>=\s*\{fromTime:UInt64\}/iu,
  },
  {
    column: 'block_time',
    pattern: /(?:\b[a-zA-Z_][\w]*\.)?\bblock_time\b\s*<\s*\{toTime:UInt64\}/iu,
  },
  {
    column: 'block_height',
    pattern: /(?:\b[a-zA-Z_][\w]*\.)?\bblock_height\b\s*<=\s*\{maxFinalizedHeight:UInt64\}/iu,
  },
] as const;
const disallowedSqlPatterns = [
  { label: 'FINAL', pattern: /\bFINAL\b/iu },
  { label: 'FORMAT', pattern: /\bFORMAT\b/iu },
  { label: 'INTO OUTFILE', pattern: /\bINTO\s+OUTFILE\b/iu },
  { label: 'SETTINGS', pattern: /\bSETTINGS\b/iu },
] as const;

export class AnalyticsRateLimitError extends OnlyDogeError {
  public constructor(message = 'analytics rate limit exceeded') {
    super(message, 429);
  }
}

export class AnalyticsQueryService {
  private readonly activeQueries = new Map<string, number>();
  private readonly limiter = new InMemoryApiKeyRateLimiter({
    maxRequests: analyticsQueryRateLimitMaxRequests,
    windowMs: analyticsQueryRateLimitWindowMs,
  });

  public constructor(
    private readonly networks: AnalyticsNetworkPort,
    private readonly configs: AnalyticsConfigPort,
    private readonly warehouse: AnalyticsWarehousePort,
  ) {}

  public schema(): AnalyticsSchemaResponse {
    return analyticsSchemaResponse();
  }

  public async query(
    actor: AuthenticatedApiKey,
    input: AnalyticsQueryInput,
  ): Promise<AnalyticsQueryResponse> {
    this.consumeRateLimit(actor);
    const subject = analyticsBudgetSubject(actor);
    this.acquireConcurrency(subject);

    try {
      return await this.executeQuery(input);
    } finally {
      this.releaseConcurrency(subject);
    }
  }

  // fallow-ignore-next-line unused-class-member
  public async backfill(input: { network?: string; throughBlockHeight?: number }): Promise<{
    network: string;
    rowsInserted: number | null;
    throughBlockHeight: number;
  }> {
    const network = await this.resolveNetwork(input.network);
    const throughBlockHeight =
      input.throughBlockHeight ?? (await this.requireFinalizedTail(network.networkId));
    if (throughBlockHeight < 0) {
      throw new TooEarlyError('dogecoin analytics facts are not ready');
    }

    const result = await this.warehouse.backfillAnalyticsTransactionFacts({
      networkId: network.networkId,
      throughBlockHeight,
    });
    await this.configs.setJsonValue(
      configKeyDogecoinAnalyticsFactsTail(network.networkId),
      result.throughBlockHeight,
    );
    await this.configs.setJsonValue(configKeyDogecoinAnalyticsFactsReady(network.networkId), true);

    return {
      network: network.id,
      rowsInserted: result.rowsInserted,
      throughBlockHeight: result.throughBlockHeight,
    };
  }

  private async executeQuery(input: AnalyticsQueryInput): Promise<AnalyticsQueryResponse> {
    const network = await this.resolveNetwork(input.network);
    const timeWindow = parseTimeWindow(input);
    const finalizedTail = await this.requireFinalizedTail(network.networkId);
    await this.assertAnalyticsReady(network.networkId, finalizedTail);

    const sql = validateAnalyticsSql(input.sql);
    const limits = analyticsQueryLimits();
    const params = analyticsQueryParams(network.networkId, timeWindow, finalizedTail, input.limit);
    const estimate = await this.warehouse.preflightAnalyticsQuery({ sql, params, limits });
    assertEstimateWithinLimits(estimate.estimatedRows, limits.maxRowsToRead, 'rows');
    assertEstimateWithinLimits(estimate.estimatedBytes, limits.maxBytesToRead, 'bytes');

    const result = await this.warehouse.executeAnalyticsQuery({ sql, params, limits });

    return {
      query: {
        hash: queryHash(sql),
        network: network.id,
        from: timeWindow.fromIso,
        to: timeWindow.toIso,
        finalizedBlockHeight: finalizedTail,
        estimatedRows: estimate.estimatedRows,
        estimatedBytes: estimate.estimatedBytes,
      },
      rows: result.rows,
      columns: result.columns,
      statistics: result.statistics,
      limits: {
        ...limits,
        maxSqlCharacters: analyticsQueryMaxSqlCharacters,
        maxWindowSeconds: analyticsQueryMaxWindowSeconds,
      },
      warnings: result.warnings,
    };
  }

  private async resolveNetwork(networkId: string | undefined): Promise<AnalyticsNetwork> {
    const activeNetworks = (await this.networks.listActiveNetworks()).filter(
      (network) => network.architecture === 'dogecoin',
    );
    if (networkId) {
      const network = activeNetworks.find((candidate) => candidate.id === networkId);
      if (!network) {
        throw new NotFoundError('network not found');
      }
      return network;
    }

    if (activeNetworks.length === 1) {
      return activeNetworks[0] as AnalyticsNetwork;
    }

    throw new ValidationError(
      'invalid parameter for `network`: required when multiple networks exist',
    );
  }

  private async requireFinalizedTail(networkId: PrimaryId): Promise<number> {
    const finalizedTail = await this.configs.getJsonValue<number>(
      configKeyIndexerFinalizedTail(networkId),
    );
    if (typeof finalizedTail !== 'number') {
      throw new TooEarlyError('dogecoin finalized chain is not ready');
    }

    return finalizedTail;
  }

  private async assertAnalyticsReady(networkId: PrimaryId, finalizedTail: number): Promise<void> {
    const [ready, factsTail] = await Promise.all([
      this.configs.getJsonValue<boolean>(configKeyDogecoinAnalyticsFactsReady(networkId)),
      this.configs.getJsonValue<number>(configKeyDogecoinAnalyticsFactsTail(networkId)),
    ]);
    if (ready !== true || typeof factsTail !== 'number' || factsTail < finalizedTail) {
      throw new TooEarlyError('dogecoin analytics facts are not ready');
    }
  }

  private consumeRateLimit(actor: AuthenticatedApiKey): void {
    const result = this.limiter.consume(analyticsBudgetSubject(actor));
    if (!result.allowed) {
      throw new AnalyticsRateLimitError();
    }
  }

  private acquireConcurrency(subject: string): void {
    const active = this.activeQueries.get(subject) ?? 0;
    if (active >= analyticsQueryMaxConcurrentRequests) {
      throw new AnalyticsRateLimitError('analytics concurrency limit exceeded');
    }

    this.activeQueries.set(subject, active + 1);
  }

  private releaseConcurrency(subject: string): void {
    const active = this.activeQueries.get(subject) ?? 0;
    if (active <= 1) {
      this.activeQueries.delete(subject);
      return;
    }

    this.activeQueries.set(subject, active - 1);
  }
}

interface AnalyticsTimeWindow {
  fromIso: string;
  fromTime: number;
  toIso: string;
  toTime: number;
}

function parseTimeWindow(input: Pick<AnalyticsQueryInput, 'from' | 'to'>): AnalyticsTimeWindow {
  const fromTime = parseTimestampSeconds(input.from, 'from');
  const toTime = parseTimestampSeconds(input.to, 'to');
  if (toTime <= fromTime) {
    throw new ValidationError('invalid analytics time window');
  }
  if (toTime - fromTime > analyticsQueryMaxWindowSeconds) {
    throw new ValidationError('analytics time window cannot exceed 7 days');
  }

  return {
    fromTime,
    toTime,
    fromIso: new Date(fromTime * 1000).toISOString(),
    toIso: new Date(toTime * 1000).toISOString(),
  };
}

function parseTimestampSeconds(value: string, field: string): number {
  const numeric = Number(value);
  if (Number.isInteger(numeric) && numeric >= 0) {
    return numeric;
  }

  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) {
    throw new ValidationError(`invalid parameter for \`${field}\`: ${value}`);
  }

  return Math.floor(parsed / 1000);
}

function validateAnalyticsSql(input: string): string {
  const sql = normalizeSql(input);
  assertRequiredPlaceholders(sql);
  const parsedSql = sqlWithParserPlaceholders(sql);
  const ast = parser.astify(parsedSql, { database: 'postgresql' });
  if (Array.isArray(ast)) {
    throw new ValidationError('analytics SQL must contain one SELECT statement');
  }

  assertSelectAst(ast);
  assertNoSelectStar(ast);
  assertAllowedTables(parsedSql, ast);
  assertRequiredColumns(parsedSql, sql);
  return sql;
}

function normalizeSql(input: string): string {
  const trimmed = input.trim().replace(/;+\s*$/u, '');
  if (!trimmed) {
    throw new ValidationError('analytics SQL is required');
  }
  if (trimmed.length > analyticsQueryMaxSqlCharacters) {
    throw new ValidationError('analytics SQL exceeds maximum length');
  }
  if (trimmed.includes(';')) {
    throw new ValidationError('analytics SQL must contain one statement');
  }
  assertNoDisallowedSqlClauses(trimmed);

  return trimmed;
}

function assertNoDisallowedSqlClauses(sql: string): void {
  const searchable = stripSqlCommentsAndStrings(sql);
  const disallowed = disallowedSqlPatterns.find(({ pattern }) => pattern.test(searchable));
  if (disallowed) {
    throw new ValidationError(`analytics SQL cannot use ${disallowed.label}`);
  }
}

function assertRequiredPlaceholders(sql: string): void {
  for (const placeholder of requiredPlaceholders) {
    if (!sql.includes(placeholder)) {
      throw new ValidationError(`analytics SQL must include ${placeholder}`);
    }
  }
}

function sqlWithParserPlaceholders(sql: string): string {
  return placeholderPatterns.reduce(
    (current, replacement) => current.replace(replacement.pattern, replacement.replacement),
    sql,
  );
}

function assertSelectAst(ast: unknown): void {
  if (!isRecord(ast) || ast.type !== 'select') {
    throw new ValidationError('analytics SQL must be a SELECT');
  }

  for (const statement of collectSelectStatements(ast)) {
    if (!isRecord(statement) || statement.type !== 'select') {
      throw new ValidationError('analytics SQL must only use SELECT statements');
    }
  }
}

function collectSelectStatements(ast: unknown): unknown[] {
  const statements = [ast];
  if (!isRecord(ast) || !Array.isArray(ast.with)) {
    return statements;
  }

  for (const item of ast.with) {
    if (isRecord(item) && 'stmt' in item) {
      statements.push(Reflect.get(item, 'stmt'));
    }
  }
  return statements;
}

function assertNoSelectStar(ast: unknown): void {
  if (containsSelectStar(ast)) {
    throw new ValidationError('analytics SQL cannot use SELECT *');
  }
}

function containsSelectStar(value: unknown): boolean {
  if (!isRecord(value)) {
    return Array.isArray(value) && value.some(containsSelectStar);
  }
  if (value.type === 'column_ref' && value.column === '*') {
    return true;
  }
  return Object.values(value).some(containsSelectStar);
}

function assertAllowedTables(sql: string, ast: unknown): void {
  const cteNames = collectCteNames(ast);
  const tables = parser.tableList(sql, { database: 'postgresql' }).map(tableListName);
  const invalid = tables.find(
    (table) => table !== analyticsTransactionsTable && !cteNames.has(table),
  );
  if (invalid) {
    throw new ValidationError(`analytics SQL cannot query table: ${invalid}`);
  }
}

function collectCteNames(ast: unknown): Set<string> {
  if (!isRecord(ast) || !Array.isArray(ast.with)) {
    return new Set();
  }

  return new Set(
    ast.with
      .map((item) => (isRecord(item) && isRecord(item.name) ? stringValue(item.name.value) : null))
      .filter((value): value is string => Boolean(value)),
  );
}

function tableListName(value: string): string {
  return value.split('::').at(-1) ?? value;
}

function assertRequiredColumns(parsedSql: string, originalSql: string): void {
  const columns = new Set(
    parser.columnList(parsedSql, { database: 'postgresql' }).map(columnListName),
  );
  for (const column of requiredColumns) {
    if (!columns.has(column)) {
      throw new ValidationError(`analytics SQL must constrain ${column}`);
    }
  }
  assertRequiredPredicates(originalSql);
}

function assertRequiredPredicates(sql: string): void {
  const searchable = stripSqlCommentsAndStrings(sql);
  for (const { column, pattern } of requiredPredicatePatterns) {
    if (!pattern.test(searchable)) {
      throw new ValidationError(`analytics SQL must constrain ${column}`);
    }
  }
}

function stripSqlCommentsAndStrings(sql: string): string {
  return sql
    .replace(/\/\*[\s\S]*?\*\//gu, ' ')
    .replace(/--[^\n\r]*/gu, ' ')
    .replace(/'(?:''|[^'])*'/gu, "''");
}

function columnListName(value: string): string {
  return value.split('::').at(-1) ?? value;
}

function analyticsQueryParams(
  networkId: PrimaryId,
  timeWindow: AnalyticsTimeWindow,
  finalizedTail: number,
  limit: number | undefined,
): AnalyticsQueryParams {
  return {
    networkId,
    fromTime: timeWindow.fromTime,
    toTime: timeWindow.toTime,
    maxFinalizedHeight: finalizedTail,
    limit: boundedLimit(limit),
  };
}

function boundedLimit(limit: number | undefined): number {
  if (limit === undefined) {
    return analyticsQueryDefaultLimit;
  }
  if (!Number.isInteger(limit) || limit < 1 || limit > analyticsQueryMaxResultRows) {
    throw new ValidationError(`invalid parameter for \`limit\`: ${limit}`);
  }

  return limit;
}

function analyticsQueryLimits(): AnalyticsQueryLimits {
  return {
    maxBytesToRead: analyticsQueryMaxBytesToRead,
    maxExecutionSeconds: analyticsQueryMaxExecutionSeconds,
    maxResultRows: analyticsQueryMaxResultRows,
    maxRowsToRead: analyticsQueryMaxRowsToRead,
  };
}

function assertEstimateWithinLimits(
  estimated: number | null,
  limit: number,
  label: 'bytes' | 'rows',
): void {
  if (estimated !== null && estimated > limit) {
    throw new ValidationError(`analytics query estimated ${label} exceed limit`);
  }
}

function analyticsBudgetSubject(actor: AuthenticatedApiKey): string {
  return `analytics:${actor.apiKeyId}`;
}

function queryHash(sql: string): string {
  return createHash('sha256').update(sql).digest('hex');
}

function analyticsSchemaResponse(): AnalyticsSchemaResponse {
  return {
    tables: [
      {
        name: analyticsTransactionsTable,
        description:
          'Finalized Dogecoin transaction facts for AI analytics. Values ending in `_base_i256` are integer base units, where 100000000 base units equals 1 DOGE.',
        columns: [
          { name: 'network_id', type: 'UInt64', description: 'Internal network identifier.' },
          { name: 'block_height', type: 'UInt64', description: 'Confirmed block height.' },
          { name: 'block_hash', type: 'String', description: 'Confirmed block hash.' },
          { name: 'block_time', type: 'UInt64', description: 'Block timestamp in Unix seconds.' },
          { name: 'txid', type: 'String', description: 'Dogecoin transaction id.' },
          { name: 'tx_index', type: 'UInt64', description: 'Transaction index within the block.' },
          { name: 'is_coinbase', type: 'UInt8', description: '1 for coinbase transactions.' },
          { name: 'input_count', type: 'UInt64', description: 'Transaction input count.' },
          { name: 'output_count', type: 'UInt64', description: 'Transaction output count.' },
          {
            name: 'total_input_base_i256',
            type: 'Int256',
            description: 'Resolved total input value in base units. Coinbase rows are 0.',
          },
          {
            name: 'gross_output_base_i256',
            type: 'Int256',
            description: 'Gross output value in base units. This includes change outputs.',
          },
          {
            name: 'fee_base_i256',
            type: 'Nullable(Int256)',
            description: 'Resolved fee in base units. Coinbase or unresolved-fee rows are NULL.',
          },
        ],
      },
    ],
    constraints: {
      maxWindowSeconds: analyticsQueryMaxWindowSeconds,
      maxSqlCharacters: analyticsQueryMaxSqlCharacters,
      requiresFinalizedBlocks: true,
      placeholders: [
        {
          name: 'networkId',
          sql: '{networkId:UInt64}',
          description: 'Bound by OnlyDoge from the request network parameter.',
        },
        {
          name: 'fromTime',
          sql: '{fromTime:UInt64}',
          description: 'Inclusive Unix-second lower bound from the request `from` parameter.',
        },
        {
          name: 'toTime',
          sql: '{toTime:UInt64}',
          description: 'Exclusive Unix-second upper bound from the request `to` parameter.',
        },
        {
          name: 'maxFinalizedHeight',
          sql: '{maxFinalizedHeight:UInt64}',
          description:
            'Finalized chain tail; include `block_height <= {maxFinalizedHeight:UInt64}`.',
        },
      ],
      unsupportedQuestions: [
        'mempool to first confirmation latency',
        'unconfirmed mempool analytics',
        'change-adjusted economic transfer value',
      ],
    },
    examples: [
      {
        description: 'Average confirmed transaction fee in the requested window.',
        sql: `SELECT avgOrNull(fee_base_i256) AS average_fee_base
FROM ${analyticsTransactionsTable}
WHERE network_id = {networkId:UInt64}
  AND block_time >= {fromTime:UInt64}
  AND block_time < {toTime:UInt64}
  AND block_height <= {maxFinalizedHeight:UInt64}
  AND is_coinbase = 0`,
      },
      {
        description: 'Highest confirmed transaction fee in the requested window.',
        sql: `SELECT txid, block_height, block_time, fee_base_i256 AS fee_base
FROM ${analyticsTransactionsTable}
WHERE network_id = {networkId:UInt64}
  AND block_time >= {fromTime:UInt64}
  AND block_time < {toTime:UInt64}
  AND block_height <= {maxFinalizedHeight:UInt64}
  AND is_coinbase = 0
  AND fee_base_i256 IS NOT NULL
ORDER BY fee_base_i256 DESC, block_height DESC, tx_index DESC
LIMIT {limit:UInt64}`,
      },
      {
        description:
          'Biggest confirmed transactions by gross output value. Gross output includes change.',
        sql: `SELECT txid, block_height, block_time, gross_output_base_i256 AS gross_output_base
FROM ${analyticsTransactionsTable}
WHERE network_id = {networkId:UInt64}
  AND block_time >= {fromTime:UInt64}
  AND block_time < {toTime:UInt64}
  AND block_height <= {maxFinalizedHeight:UInt64}
  AND is_coinbase = 0
ORDER BY gross_output_base_i256 DESC, block_height DESC, tx_index DESC
LIMIT {limit:UInt64}`,
      },
    ],
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function stringValue(value: unknown): string | null {
  return typeof value === 'string' ? value : null;
}
