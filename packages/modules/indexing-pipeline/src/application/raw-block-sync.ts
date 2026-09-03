import { noopServiceLogger, type ServiceLogger } from '@onlydoge/shared-kernel';

import type { BlockchainRpcPort, RawBlockStoragePort } from '../contracts/ports';
import type { CoreBlockRecord } from '../domain/projection-models';
import { deriveTransactionRefsFromBlock, type TransactionRef } from '../domain/transaction-ref';

/**
 * Raw block sync: pulls block snapshots from Dogecoin Core in RPC batches,
 * persists them to raw storage + metadata, and checkpoints the highest
 * contiguous synced height after every round so a crash or a slow node never
 * discards completed work.
 *
 * Concurrency adapts (AIMD): every failed batch halves the number of parallel
 * RPC batches, every `rampUpAfterSuccesses` clean batches add one back. This
 * keeps the indexer under whatever the node can actually serve instead of
 * saturating its RPC work queue.
 */

export interface RawBlockSyncSettings {
  /** Blocks requested per JSON-RPC batch. */
  syncBatchSize: number;
  /** Maximum parallel RPC batches. */
  syncConcurrency: number;
  /** Attempts per batch before the window fails. */
  syncRetryAttempts: number;
  /** Base delay for exponential backoff between batch attempts. */
  syncRetryBaseDelayMs: number;
  coreRawStorageTimeoutMs: number;
}

export interface RawBlockSyncDogecoin {
  architecture: 'dogecoin';
  id: string;
  rpcEndpoint: string;
  rps: number;
}

export interface RawBlockSyncSink {
  upsertCoreBlock(record: CoreBlockRecord): Promise<void>;
  upsertTransactionRefs(refs: TransactionRef[]): Promise<void>;
}

export interface RawBlockSyncHooks {
  /** Invoked before each RPC round; throw to abort (e.g. lost leadership). */
  assertActive?: () => void;
  /** Invoked whenever the highest contiguous synced height advances. */
  onCheckpoint?: (frontier: number) => Promise<void>;
  /** Invoked after every batch attempt, success or failure. */
  onActivity?: () => void;
}

export interface RawBlockSyncResult {
  blocks: number;
  elapsedMs: number;
  failedAttempts: number;
  frontier: number;
  /** Summed wall time of RPC fetches across batches (overlaps under concurrency). */
  rpcMs: number;
  /** Summed wall time of storage writes across batches (overlaps under concurrency). */
  storeMs: number;
}

interface BatchOutcome {
  failedAttempts: number;
  rpcMs: number;
  storeMs: number;
}

export interface ParsedRawBlockSnapshot {
  hash: string;
  height: number;
  previousHash: string | null;
  time: number;
  txids: string[];
}

export type RawBlockSnapshotParser = (snapshot: Record<string, unknown>) => ParsedRawBlockSnapshot;

export const rawBlockPart = 'block';

export class AdaptiveConcurrencyController {
  private current: number;
  private successStreak = 0;

  public constructor(
    private readonly max: number,
    private readonly rampUpAfterSuccesses = 4,
    initial = max,
  ) {
    this.current = clampConcurrency(initial, max);
  }

  public get value(): number {
    return this.current;
  }

  public recordSuccess(): void {
    this.successStreak += 1;
    if (this.successStreak >= this.rampUpAfterSuccesses) {
      this.successStreak = 0;
      this.current = clampConcurrency(this.current + 1, this.max);
    }
  }

  public recordFailure(): void {
    this.successStreak = 0;
    this.current = clampConcurrency(Math.floor(this.current / 2), this.max);
  }
}

function clampConcurrency(value: number, max: number): number {
  return Math.max(1, Math.min(max, value));
}

export class RawBlockSyncer {
  private readonly concurrency: AdaptiveConcurrencyController;
  private readonly logger: ServiceLogger;
  private readonly sleep: (ms: number) => Promise<void>;

  public constructor(
    private readonly rpc: Pick<BlockchainRpcPort, 'getBlockSnapshots'>,
    private readonly rawBlocks: Pick<RawBlockStoragePort, 'putPart'>,
    private readonly sink: RawBlockSyncSink,
    private readonly parseSnapshot: RawBlockSnapshotParser,
    private readonly settings: RawBlockSyncSettings,
    options: { logger?: ServiceLogger; sleep?: (ms: number) => Promise<void> } = {},
  ) {
    this.concurrency = new AdaptiveConcurrencyController(settings.syncConcurrency);
    this.logger = options.logger ?? noopServiceLogger();
    this.sleep = options.sleep ?? defaultSleep;
  }

  public get currentConcurrency(): number {
    return this.concurrency.value;
  }

  public async sync(
    dogecoin: RawBlockSyncDogecoin,
    heights: number[],
    hooks: RawBlockSyncHooks = {},
  ): Promise<RawBlockSyncResult> {
    const startedAt = Date.now();
    const sorted = [...heights].sort((left, right) => left - right);
    const tracker = new ContiguousFrontier((sorted[0] ?? 0) - 1);
    const batches = chunk(sorted, this.settings.syncBatchSize);
    const totals: BatchOutcome = { failedAttempts: 0, rpcMs: 0, storeMs: 0 };

    let cursor = 0;
    while (cursor < batches.length) {
      hooks.assertActive?.();
      const round = batches.slice(cursor, cursor + this.concurrency.value);
      cursor += round.length;
      const outcomes = await Promise.all(
        round.map((batch) => this.syncBatchWithRetry(dogecoin, batch, hooks)),
      );
      for (const outcome of outcomes) {
        totals.failedAttempts += outcome.failedAttempts;
        totals.rpcMs += outcome.rpcMs;
        totals.storeMs += outcome.storeMs;
      }
      for (const batch of round) {
        tracker.markCompleted(batch);
      }
      await this.checkpointIfAdvanced(tracker, hooks);
    }

    return {
      blocks: sorted.length,
      elapsedMs: Date.now() - startedAt,
      frontier: tracker.frontier,
      ...totals,
    };
  }

  private async checkpointIfAdvanced(
    tracker: ContiguousFrontier,
    hooks: RawBlockSyncHooks,
  ): Promise<void> {
    if (!tracker.consumeAdvance()) {
      return;
    }

    await hooks.onCheckpoint?.(tracker.frontier);
  }

  private async syncBatchWithRetry(
    dogecoin: RawBlockSyncDogecoin,
    batch: number[],
    hooks: RawBlockSyncHooks,
  ): Promise<BatchOutcome> {
    let failedAttempts = 0;
    for (let attempt = 1; ; attempt += 1) {
      try {
        const timings = await this.syncBatch(dogecoin, batch);
        hooks.onActivity?.();
        this.concurrency.recordSuccess();
        return { failedAttempts, ...timings };
      } catch (error) {
        hooks.onActivity?.();
        failedAttempts += 1;
        this.concurrency.recordFailure();
        await this.handleBatchFailure(dogecoin, batch, attempt, error);
      }
    }
  }

  private async handleBatchFailure(
    dogecoin: RawBlockSyncDogecoin,
    batch: number[],
    attempt: number,
    error: unknown,
  ): Promise<void> {
    const delayMs = backoffDelayMs(this.settings.syncRetryBaseDelayMs, attempt);
    const bindings = {
      attempt,
      blockEnd: batch.at(-1),
      blockStart: batch[0],
      chain: dogecoin.id,
      component: 'core-indexer',
      concurrency: this.concurrency.value,
      err: error instanceof Error ? error : new Error(String(error)),
      phase: 'raw-block-sync',
    };
    if (attempt >= this.settings.syncRetryAttempts) {
      this.logger.error(bindings, 'raw block batch failed; giving up on window');
      throw error;
    }

    this.logger.warn({ ...bindings, retryInMs: delayMs }, 'raw block batch failed; retrying');
    await this.sleep(delayMs);
  }

  private async syncBatch(
    dogecoin: RawBlockSyncDogecoin,
    batch: number[],
  ): Promise<{ rpcMs: number; storeMs: number }> {
    const rpcStartedAt = Date.now();
    const snapshots = await this.rpc.getBlockSnapshots(dogecoin, batch);
    const rpcMs = Date.now() - rpcStartedAt;
    if (snapshots.length !== batch.length) {
      throw new Error(
        `raw block batch size mismatch requested=${batch.length} received=${snapshots.length}`,
      );
    }

    // Storage writes are latency-bound (object PUT + metadata upsert per
    // block), so run them for the whole batch in parallel.
    const refsPerBlock = await Promise.all(
      snapshots.map((snapshot, index) => this.storeSnapshot(batch[index] ?? -1, snapshot)),
    );
    const refs = refsPerBlock.flat();
    if (refs.length > 0) {
      await this.sink.upsertTransactionRefs(refs);
    }

    return { rpcMs, storeMs: Date.now() - rpcStartedAt - rpcMs };
  }

  private async storeSnapshot(
    height: number,
    snapshot: Record<string, unknown>,
  ): Promise<TransactionRef[]> {
    const block = this.parseSnapshot(snapshot);
    if (block.height !== height) {
      throw new Error(`raw block height mismatch requested=${height} decoded=${block.height}`);
    }

    await this.rawBlocks.putPart(height, rawBlockPart, snapshot, {
      timeoutMs: this.settings.coreRawStorageTimeoutMs,
    });
    await this.sink.upsertCoreBlock({
      blockHeight: block.height,
      blockHash: block.hash,
      previousBlockHash: block.previousHash,
      blockTime: block.time,
      txCount: block.txids.length,
      rawStorageKey: rawBlockPart,
      fetchedAt: new Date().toISOString(),
      processedAt: null,
    });

    return deriveTransactionRefsFromBlock({
      blockHash: block.hash,
      blockHeight: block.height,
      blockTime: block.time,
      source: 'raw_sync',
      transactions: block.txids.map((txid) => ({ txid })),
    });
  }
}

/** Tracks the highest height H such that every height in (base, H] completed. */
export class ContiguousFrontier {
  private readonly completed = new Set<number>();
  private advanced = false;

  public constructor(private current: number) {}

  public get frontier(): number {
    return this.current;
  }

  public markCompleted(heights: number[]): void {
    for (const height of heights) {
      this.completed.add(height);
    }
    while (this.completed.has(this.current + 1)) {
      this.current += 1;
      this.completed.delete(this.current);
      this.advanced = true;
    }
  }

  public consumeAdvance(): boolean {
    const advanced = this.advanced;
    this.advanced = false;
    return advanced;
  }
}

export function chunk<T>(values: T[], size: number): T[][] {
  const chunkSize = Math.max(1, size);
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += chunkSize) {
    chunks.push(values.slice(index, index + chunkSize));
  }
  return chunks;
}

function backoffDelayMs(baseDelayMs: number, attempt: number): number {
  const exponential = Math.min(30_000, baseDelayMs * 2 ** (attempt - 1));
  return Math.round(exponential * (0.5 + Math.random() * 0.5));
}

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
