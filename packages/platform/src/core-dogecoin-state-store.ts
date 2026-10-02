import {
  type CoreBlockRecord,
  type CoreDogecoinApplyContext,
  type CoreDogecoinApplyResult,
  type CoreDogecoinBlockApplication,
  type CoreDogecoinStateStorePort,
  type CoreIndexerStage,
  type CoreIndexerState,
  type CoreStateMaterializationProgress,
  configKeyDogecoinCurrentStateMaterialization,
  configKeyDogecoinCurrentStateReady,
  configKeyDogecoinHistoryReady,
  type ProjectionUtxoOutput,
} from '@onlydoge/indexing-pipeline';

import type { RelationalMetadataStore } from './metadata-store';

export interface ClickHouseCoreDogecoinStore {
  applyCoreDogecoinWindow(
    input: CoreDogecoinBlockApplication[],
    context?: CoreDogecoinApplyContext,
  ): Promise<CoreDogecoinApplyResult>;
  getCoreProcessedTail?(): Promise<number | null>;
  getUtxoOutputs(outputKeys: string[]): Promise<Map<string, ProjectionUtxoOutput>>;
  materializeCoreDogecoinCurrentState(
    asOfBlockHeight: number,
    context?: CoreDogecoinApplyContext,
  ): Promise<void>;
  recoverCoreDogecoinWindow(
    fromBlockHeight: number,
    context?: CoreDogecoinApplyContext,
  ): Promise<void>;
  upsertTransactionRefs(
    refs: Array<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      source: 'raw_sync' | 'core_process';
      txIndex: number;
      txid: string;
      version: number;
    }>,
  ): Promise<void>;
}

export function isClickHouseCoreDogecoinStore(
  value: unknown,
): value is ClickHouseCoreDogecoinStore {
  return (
    isRecord(value) &&
    clickHouseCoreDogecoinStoreMethods.every((method) => hasMethod(value, method))
  );
}

const clickHouseCoreDogecoinStoreMethods = [
  'applyCoreDogecoinWindow',
  'getUtxoOutputs',
  'materializeCoreDogecoinCurrentState',
  'recoverCoreDogecoinWindow',
  'upsertTransactionRefs',
] as const;

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object';
}

function hasMethod(value: Record<string, unknown>, method: string): boolean {
  return typeof value[method] === 'function';
}

export class ClickHouseCoreDogecoinStateStore implements CoreDogecoinStateStorePort {
  public constructor(
    private readonly metadata: RelationalMetadataStore,
    private readonly clickhouse: ClickHouseCoreDogecoinStore,
  ) {}

  public applyCoreDogecoinBlock(
    input: CoreDogecoinBlockApplication,
    context?: CoreDogecoinApplyContext,
  ) {
    return this.applyCoreDogecoinWindow([input], context);
  }

  public applyCoreDogecoinWindow(
    input: CoreDogecoinBlockApplication[],
    context?: CoreDogecoinApplyContext,
  ) {
    return this.clickhouse.applyCoreDogecoinWindow(input, context);
  }

  public getCoreIndexerState() {
    return this.metadata.getCoreIndexerState();
  }

  public async getCoreProcessedTail(): Promise<number | null | undefined> {
    return this.clickhouse.getCoreProcessedTail?.();
  }

  public getCoreUtxoOutputs(outputKeys: string[]) {
    return this.clickhouse.getUtxoOutputs(outputKeys);
  }

  /**
   * Materialization reads every create and spend once, which is hours on a
   * large warehouse. Its range progress is checkpointed in metadata so a
   * failed or interrupted attempt continues from the last finished range
   * instead of clearing the current-state tables and starting again.
   */
  public async materializeCoreDogecoinCurrentState(
    asOfBlockHeight: number,
    context?: CoreDogecoinApplyContext,
  ) {
    const resumeFrom = await this.materializationResumePoint(asOfBlockHeight);
    await this.clickhouse.materializeCoreDogecoinCurrentState(asOfBlockHeight, {
      ...context,
      materialization: {
        ...context?.materialization,
        ...(resumeFrom ? { resumeFrom } : {}),
        onRangeCompleted: async (progress) => {
          await this.metadata.setJsonValues([
            [configKeyDogecoinCurrentStateMaterialization(), { asOfBlockHeight, ...progress }],
          ]);
          await context?.materialization?.onRangeCompleted?.(progress);
        },
      },
    });
    await Promise.all([
      this.metadata.setJsonValue(configKeyDogecoinCurrentStateReady(), true),
      this.metadata.setJsonValue(configKeyDogecoinHistoryReady(), false),
    ]);
    await this.metadata.deleteByPrefix(configKeyDogecoinCurrentStateMaterialization());
  }

  private async materializationResumePoint(
    asOfBlockHeight: number,
  ): Promise<CoreStateMaterializationProgress | null> {
    const checkpoint = await this.metadata.getJsonValue<
      Partial<CoreStateMaterializationProgress> & { asOfBlockHeight?: number }
    >(configKeyDogecoinCurrentStateMaterialization());
    if (
      checkpoint?.asOfBlockHeight !== asOfBlockHeight ||
      !Number.isInteger(checkpoint.completedRanges) ||
      !Number.isInteger(checkpoint.rangeCount)
    ) {
      return null;
    }

    return {
      completedRanges: Number(checkpoint.completedRanges),
      rangeCount: Number(checkpoint.rangeCount),
    };
  }

  public recoverCoreDogecoinWindow(fromBlockHeight: number, context?: CoreDogecoinApplyContext) {
    return this.clickhouse.recoverCoreDogecoinWindow(fromBlockHeight, context);
  }

  public upsertTransactionRefs(
    refs: Array<{
      blockHash: string;
      blockHeight: number;
      blockTime: number;
      source: 'raw_sync' | 'core_process';
      txIndex: number;
      txid: string;
      version: number;
    }>,
  ) {
    return this.clickhouse.upsertTransactionRefs(refs);
  }

  public setCoreIndexerError(error: string | null) {
    return this.metadata.setCoreIndexerError(error);
  }

  public setCoreIndexerStage(stage: CoreIndexerStage) {
    return this.metadata.setCoreIndexerStage(stage);
  }

  public upsertCoreBlock(record: CoreBlockRecord) {
    return this.metadata.upsertCoreBlock(record);
  }

  public upsertCoreBlocks(records: CoreBlockRecord[]) {
    return this.metadata.upsertCoreBlocks(records);
  }

  public upsertCoreIndexerState(input: {
    lastError?: string | null;
    onlineTip?: number;
    processTail?: number;
    stage?: CoreIndexerStage;
    syncTail?: number;
  }): Promise<CoreIndexerState> {
    return this.metadata.upsertCoreIndexerState(input);
  }
}
