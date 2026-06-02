import type {
  AnalyticsQueryEstimate,
  AnalyticsQueryExecutionResult,
  AnalyticsQueryLimits,
  AnalyticsQueryParams,
  AnalyticsTransactionFact,
} from '../domain/query-models';

export interface AnalyticsConfigPort {
  getJsonValue<T>(key: string): Promise<T | null>;
  setJsonValue<T>(key: string, value: T): Promise<void>;
}

export interface AnalyticsWarehousePort {
  backfillAnalyticsTransactionFacts(input: { throughBlockHeight: number }): Promise<{
    rowsInserted: number | null;
    throughBlockHeight: number;
  }>;
  executeAnalyticsQuery(input: {
    limits: AnalyticsQueryLimits;
    params: AnalyticsQueryParams;
    sql: string;
  }): Promise<AnalyticsQueryExecutionResult>;
  insertAnalyticsTransactionFacts(rows: AnalyticsTransactionFact[]): Promise<void>;
  preflightAnalyticsQuery(input: {
    limits: AnalyticsQueryLimits;
    params: AnalyticsQueryParams;
    sql: string;
  }): Promise<AnalyticsQueryEstimate>;
}
