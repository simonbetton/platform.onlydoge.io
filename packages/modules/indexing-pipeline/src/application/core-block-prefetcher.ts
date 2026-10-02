/**
 * Ordered read-ahead for core block processing.
 *
 * Backfill processing consumes block snapshots strictly in height order, while
 * every source (Dogecoin Core RPC, raw block storage) is latency-bound per
 * block. The prefetcher keeps a bounded number of batch fetches in flight ahead
 * of the consumer's cursor, so the source stays busy while the previous window
 * is being built and applied instead of idling between windows.
 *
 * Read-ahead is bounded by weight (rows for the indexer) rather than block
 * count: Dogecoin blocks range from one transaction to several thousand, and a
 * fixed block budget would either starve sparse ranges or exhaust memory on
 * dense ones. In-flight batches are charged at the running average weight.
 */

export type CoreBlockSnapshot = Record<string, unknown>;

export type CoreBlockBatchFetcher = (heights: number[]) => Promise<CoreBlockSnapshot[]>;

export interface CoreBlockPrefetcherOptions {
  /** Heights requested per fetch call. */
  batchSize: number;
  /** Fetch calls in flight at once. */
  concurrency: number;
  /** Upper bound on buffered blocks, loaded or in flight. */
  maxBufferedBlocks: number;
  /** Upper bound on buffered weight: loaded batches plus an estimate for in-flight ones. */
  maxBufferedWeight: number;
  /** Weight of one loaded snapshot. Must not throw. */
  weigh: (snapshot: CoreBlockSnapshot) => number;
}

interface PrefetchBatch {
  done: Promise<void>;
  end: number;
  error: unknown;
  generation: number;
  snapshots: CoreBlockSnapshot[] | null;
  start: number;
  weight: number;
}

const averageWeightSmoothing = 0.3;

export class CoreBlockPrefetcher {
  private averageWeight = 0;
  private batches: PrefetchBatch[] = [];
  private generation = 0;
  private inFlight = 0;
  private limit = -1;
  private nextStart = 0;

  public constructor(
    private readonly fetchBatch: CoreBlockBatchFetcher,
    private readonly options: CoreBlockPrefetcherOptions,
  ) {}

  /** Blocks currently loaded or in flight at or above the consumer's cursor. */
  public get bufferedBlocks(): number {
    return this.batches.reduce((sum, batch) => sum + (batch.end - batch.start + 1), 0);
  }

  /**
   * Resolves the snapshot for `height` and keeps reading ahead up to
   * `limitHeight`. Calls must move forward; any other cursor drops the
   * read-ahead and starts over at `height`.
   */
  public async get(height: number, limitHeight: number): Promise<CoreBlockSnapshot> {
    this.limit = limitHeight;
    this.alignTo(height);
    this.pump(height);

    const batch = this.batchFor(height);
    if (!batch) {
      throw new Error(`core block prefetch has no batch for height=${height}`);
    }

    await batch.done;
    return this.snapshotFrom(batch, height);
  }

  /** Drops all read-ahead. In-flight fetches settle into the void. */
  public reset(): void {
    this.generation += 1;
    this.batches = [];
    this.inFlight = 0;
  }

  private snapshotFrom(batch: PrefetchBatch, height: number): CoreBlockSnapshot {
    if (batch.generation !== this.generation) {
      throw new Error(`core block prefetch was reset while height=${height} was pending`);
    }
    if (!batch.snapshots) {
      const error = batch.error;
      this.reset();
      throw error instanceof Error ? error : new Error(String(error));
    }

    const snapshot = batch.snapshots[height - batch.start];
    if (!snapshot) {
      this.reset();
      throw new Error(`core block prefetch returned no snapshot for height=${height}`);
    }

    return snapshot;
  }

  private alignTo(height: number): void {
    if (!this.covers(height)) {
      this.reset();
      this.nextStart = height;
      return;
    }

    this.batches = this.batches.filter((batch) => batch.end >= height);
  }

  private covers(height: number): boolean {
    const first = this.batches[0];
    if (!first) {
      return false;
    }

    return height >= first.start && height <= this.nextStart;
  }

  private batchFor(height: number): PrefetchBatch | undefined {
    return this.batches.find((batch) => height >= batch.start && height <= batch.end);
  }

  private pump(cursor: number): void {
    while (this.shouldSchedule(cursor)) {
      this.schedule();
    }
  }

  private shouldSchedule(cursor: number): boolean {
    if (this.nextStart > this.limit) {
      return false;
    }
    // The batch holding the cursor is always scheduled, whatever the budget:
    // the consumer is blocked on it.
    if (this.nextStart <= cursor) {
      return true;
    }

    return this.inFlight < this.options.concurrency && this.hasBudget();
  }

  private hasBudget(): boolean {
    return (
      this.bufferedBlocks < this.options.maxBufferedBlocks &&
      this.bufferedWeight() < this.options.maxBufferedWeight
    );
  }

  private bufferedWeight(): number {
    return this.batches.reduce((sum, batch) => sum + this.batchWeight(batch), 0);
  }

  private batchWeight(batch: PrefetchBatch): number {
    if (batch.snapshots) {
      return batch.weight;
    }

    return (batch.end - batch.start + 1) * this.averageWeight;
  }

  private schedule(): void {
    const start = this.nextStart;
    const end = Math.min(this.limit, start + Math.max(1, this.options.batchSize) - 1);
    const heights = Array.from({ length: end - start + 1 }, (_value, index) => start + index);
    const generation = this.generation;
    const batch: PrefetchBatch = {
      done: Promise.resolve(),
      end,
      error: null,
      generation,
      snapshots: null,
      start,
      weight: 0,
    };

    this.nextStart = end + 1;
    this.inFlight += 1;
    // `done` never rejects: a failed fetch is surfaced by `get`, and read-ahead
    // nobody consumes must not become an unhandled rejection.
    batch.done = this.fetchBatch(heights).then(
      (snapshots) => this.settle(batch, generation, heights.length, snapshots, null),
      (error) => this.settle(batch, generation, heights.length, null, error),
    );
    this.batches.push(batch);
  }

  private settle(
    batch: PrefetchBatch,
    generation: number,
    expected: number,
    snapshots: CoreBlockSnapshot[] | null,
    error: unknown,
  ): void {
    if (generation !== this.generation) {
      return;
    }

    this.inFlight -= 1;
    if (!snapshots) {
      batch.error = error;
      return;
    }
    if (snapshots.length !== expected) {
      batch.error = new Error(
        `core block prefetch size mismatch requested=${expected} received=${snapshots.length}`,
      );
      return;
    }

    batch.snapshots = snapshots;
    batch.weight = snapshots.reduce((sum, snapshot) => sum + this.options.weigh(snapshot), 0);
    this.observeWeight(batch.weight / expected);
    this.pump(this.batches[0]?.start ?? batch.start);
  }

  private observeWeight(sample: number): void {
    this.averageWeight =
      this.averageWeight === 0
        ? sample
        : this.averageWeight + averageWeightSmoothing * (sample - this.averageWeight);
  }
}
