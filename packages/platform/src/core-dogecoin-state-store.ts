import {
  type CoreBlockRecord,
  type CoreDogecoinApplyContext,
  type CoreDogecoinApplyResult,
  type CoreDogecoinBlockApplication,
  type CoreDogecoinStateStorePort,
  type CoreIndexerStage,
  type CoreIndexerState,
  configKeyDogecoinCurrentStateReady,
  configKeyDogecoinHistoryReady,
  type ProjectionUtxoOutput,
} from '@onlydoge/indexing-pipeline';
import type { PrimaryId } from '@onlydoge/shared-kernel';

import type { RelationalMetadataStore } from './metadata-store';

export interface ClickHouseCoreDogecoinStore {
  applyCoreDogecoinWindow(
    input: CoreDogecoinBlockApplication[],
    context?: CoreDogecoinApplyContext,
  ): Promise<CoreDogecoinApplyResult>;
  getUtxoOutputs(
    networkId: PrimaryId,
    outputKeys: string[],
  ): Promise<Map<string, ProjectionUtxoOutput>>;
  materializeCoreDogecoinCurrentState(
    networkId: PrimaryId,
    asOfBlockHeight: number,
    context?: CoreDogecoinApplyContext,
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

  public getCoreIndexerState(networkId: PrimaryId) {
    return this.metadata.getCoreIndexerState(networkId);
  }

  public getCoreUtxoOutputs(networkId: PrimaryId, outputKeys: string[]) {
    return this.clickhouse.getUtxoOutputs(networkId, outputKeys);
  }

  public async materializeCoreDogecoinCurrentState(
    networkId: PrimaryId,
    asOfBlockHeight: number,
    context?: CoreDogecoinApplyContext,
  ) {
    await this.clickhouse.materializeCoreDogecoinCurrentState(networkId, asOfBlockHeight, context);
    await Promise.all([
      this.metadata.setJsonValue(configKeyDogecoinCurrentStateReady(networkId), true),
      this.metadata.setJsonValue(configKeyDogecoinHistoryReady(networkId), false),
    ]);
  }

  public setCoreIndexerError(networkId: PrimaryId, error: string | null) {
    return this.metadata.setCoreIndexerError(networkId, error);
  }

  public setCoreIndexerStage(networkId: PrimaryId, stage: CoreIndexerStage) {
    return this.metadata.setCoreIndexerStage(networkId, stage);
  }

  public upsertCoreBlock(record: CoreBlockRecord) {
    return this.metadata.upsertCoreBlock(record);
  }

  public upsertCoreIndexerState(input: {
    lastError?: string | null;
    networkId: PrimaryId;
    onlineTip?: number;
    processTail?: number;
    stage?: CoreIndexerStage;
    syncTail?: number;
  }): Promise<CoreIndexerState> {
    return this.metadata.upsertCoreIndexerState(input);
  }
}
