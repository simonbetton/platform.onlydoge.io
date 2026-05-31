import type { PrimaryId } from '@onlydoge/shared-kernel';

export const analyticsTransactionsTable = 'analytics_transactions_v1';
export const analyticsQueryMaxWindowSeconds = 7 * 24 * 60 * 60;
export const analyticsQueryDefaultLimit = 100;
export const analyticsQueryMaxResultRows = 1_000;
export const analyticsQueryMaxRowsToRead = 50_000_000;
export const analyticsQueryMaxBytesToRead = 5_000_000_000;
export const analyticsQueryMaxExecutionSeconds = 5;
export const analyticsQueryRateLimitMaxRequests = 30;
export const analyticsQueryRateLimitWindowMs = 60_000;
export const analyticsQueryMaxConcurrentRequests = 2;
export const analyticsQueryMaxSqlCharacters = 16_384;

export interface AnalyticsTransactionFact {
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  feeBase: string | null;
  grossOutputBase: string;
  inputCount: number;
  isCoinbase: boolean;
  networkId: PrimaryId;
  outputCount: number;
  totalInputBase: string;
  txIndex: number;
  txid: string;
  version: number;
}

export interface AnalyticsQueryInput {
  from: string;
  limit?: number;
  network?: string;
  sql: string;
  to: string;
}

export interface AnalyticsQueryParams {
  fromTime: number;
  limit: number;
  maxFinalizedHeight: number;
  networkId: PrimaryId;
  toTime: number;
}

export interface AnalyticsQueryLimits {
  maxBytesToRead: number;
  maxExecutionSeconds: number;
  maxResultRows: number;
  maxRowsToRead: number;
}

export interface AnalyticsQueryEstimate {
  estimatedBytes: number | null;
  estimatedRows: number | null;
}

export interface AnalyticsQueryColumn {
  name: string;
  type: string;
}

export interface AnalyticsQueryExecutionResult {
  columns: AnalyticsQueryColumn[];
  rows: Array<Record<string, unknown>>;
  statistics: {
    elapsed: number | null;
    rowsRead: number | null;
    bytesRead: number | null;
  };
  warnings: string[];
}

export interface AnalyticsQueryResponse {
  columns: AnalyticsQueryColumn[];
  limits: AnalyticsQueryLimits & {
    maxWindowSeconds: number;
    maxSqlCharacters: number;
  };
  query: {
    estimatedBytes: number | null;
    estimatedRows: number | null;
    finalizedBlockHeight: number;
    from: string;
    hash: string;
    network: string;
    to: string;
  };
  rows: Array<Record<string, unknown>>;
  statistics: AnalyticsQueryExecutionResult['statistics'];
  warnings: string[];
}

export interface AnalyticsSchemaResponse {
  constraints: {
    maxWindowSeconds: number;
    maxSqlCharacters: number;
    placeholders: Array<{
      description: string;
      name: string;
      sql: string;
    }>;
    requiresFinalizedBlocks: boolean;
    unsupportedQuestions: string[];
  };
  examples: Array<{
    description: string;
    sql: string;
  }>;
  tables: Array<{
    columns: Array<{
      description: string;
      name: string;
      type: string;
    }>;
    description: string;
    name: string;
  }>;
}
