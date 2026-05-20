#!/usr/bin/env bun

import { performance } from 'node:perf_hooks';

import {
  buildFastCoreDogecoinBlockApplications,
  type CoreDogecoinBlockApplication,
  mapWithConcurrency,
  range,
} from '@onlydoge/indexing-pipeline';
import {
  ClickHouseWarehouseAdapter,
  createRawBlockStorage,
  loadSettings,
  RelationalMetadataStore,
} from '@onlydoge/platform';
import { Command } from 'commander';
import {
  parseNonNegativeInteger,
  parsePositiveInteger,
  resolveDogecoinNetworkId,
} from './dogecoin-script-utils';

const gateBlocksPerHour = 50_000;
const rawBlockPart = 'block';

const program = new Command()
  .name('benchmark-clickhouse-core-backfill')
  .description(
    'Benchmark the fast ClickHouse-only Dogecoin current-state backfill on stored raw snapshots.',
  )
  .option('--execute', 'write into isolated benchmark ClickHouse tables; otherwise load/parse only')
  .option('--networkId <id>', 'internal Dogecoin network id')
  .option('--start <height>', 'explicit benchmark range start height')
  .option('--end <height>', 'explicit benchmark range end height')
  .option('--blocks <count>', 'blocks per large-block sample range', '100')
  .option('--ranges <count>', 'number of largest-block sample ranges', '3')
  .option('--concurrency <count>', 'raw snapshot load concurrency', '8')
  .option('--prefix <name>', 'ClickHouse benchmark table prefix', 'core_backfill_benchmark')
  .option('--keep', 'keep isolated benchmark tables after an execute run')
  .parse();

const options = program.opts<{
  blocks: string;
  concurrency: string;
  end?: string;
  execute?: boolean;
  keep?: boolean;
  networkId?: string;
  prefix: string;
  ranges: string;
  start?: string;
}>();

type BenchmarkRange = {
  end: number;
  start: number;
  txCount?: number;
};

type BenchmarkResult = {
  blocks: number;
  blocksPerHour: number;
  clickHouseInsertMs: number;
  creates: number;
  end: number;
  finalMaterializationMs: number;
  parseMs: number;
  rawLoadMs: number;
  rowsInserted: number;
  rowsPlanned: number;
  spends: number;
  start: number;
  txCount?: number;
};

async function main() {
  const settings = loadSettings({ mode: 'indexer' });
  if (settings.warehouse.driver !== 'clickhouse') {
    throw new Error('ClickHouse warehouse is required for this benchmark');
  }

  const metadata = await RelationalMetadataStore.connect(settings.database);
  const networkId = await resolveDogecoinNetworkId(metadata, options.networkId);
  const blocks = parsePositiveInteger(options.blocks, 'blocks');
  const ranges = parsePositiveInteger(options.ranges, 'ranges');
  const concurrency = parsePositiveInteger(options.concurrency, 'concurrency');
  const benchmarkRanges = await resolveBenchmarkRanges(metadata, networkId, { blocks, ranges });
  if (benchmarkRanges.length === 0) {
    throw new Error(`no benchmark ranges found for network=${networkId}`);
  }
  const rawBlocks = createRawBlockStorage(settings.storage);
  const warehouse = new ClickHouseWarehouseAdapter(settings.warehouse);

  const results: BenchmarkResult[] = [];
  for (const [index, benchmarkRange] of benchmarkRanges.entries()) {
    const heights = range(benchmarkRange.start, benchmarkRange.end);
    const rawLoadStarted = performance.now();
    const snapshots = await mapWithConcurrency(heights, concurrency, async (height) => {
      const snapshot = await rawBlocks.getPart<Record<string, unknown>>(
        networkId,
        height,
        rawBlockPart,
        {
          timeoutMs: settings.indexer.coreRawStorageTimeoutMs,
        },
      );
      if (!snapshot) {
        throw new Error(
          `missing raw dogecoin block snapshot network=${networkId} height=${height}`,
        );
      }
      return snapshot;
    });
    const rawLoadMs = performance.now() - rawLoadStarted;

    const parseStarted = performance.now();
    const applications = buildFastCoreDogecoinBlockApplications(networkId, snapshots);
    const parseMs = performance.now() - parseStarted;
    const creates = countCreates(applications);
    const spends = countSpends(applications);
    const rowsPlanned = creates + spends + applications.length;

    let clickHouseInsertMs = 0;
    let finalMaterializationMs = 0;
    let rowsInserted = 0;
    const prefix = `${options.prefix}_${index}`;

    if (options.execute) {
      try {
        await warehouse.resetCoreDogecoinBenchmarkStorage(prefix);
        const insertStarted = performance.now();
        const insertResult = await warehouse.insertCoreDogecoinBenchmarkWindow(
          applications,
          prefix,
        );
        clickHouseInsertMs = performance.now() - insertStarted;
        rowsInserted = insertResult.rowsInserted;

        const materializeStarted = performance.now();
        await warehouse.materializeCoreDogecoinBenchmarkCurrentState(
          prefix,
          networkId,
          applications.at(-1)?.blockHeight ?? benchmarkRange.end,
        );
        finalMaterializationMs = performance.now() - materializeStarted;
      } finally {
        if (!options.keep) {
          await warehouse.dropCoreDogecoinBenchmarkStorage(prefix);
        }
      }
    }

    const processingMs = rawLoadMs + parseMs + clickHouseInsertMs;
    results.push({
      start: benchmarkRange.start,
      end: benchmarkRange.end,
      txCount: benchmarkRange.txCount,
      blocks: applications.length,
      creates,
      spends,
      rowsPlanned,
      rowsInserted,
      rawLoadMs: roundMs(rawLoadMs),
      parseMs: roundMs(parseMs),
      clickHouseInsertMs: roundMs(clickHouseInsertMs),
      finalMaterializationMs: roundMs(finalMaterializationMs),
      blocksPerHour: Math.round((applications.length * 3_600_000) / Math.max(processingMs, 1)),
    });
  }

  const worstBlocksPerHour = Math.min(...results.map((result) => result.blocksPerHour));
  const payload = {
    mode: options.execute ? 'execute' : 'dry-run',
    networkId,
    gateBlocksPerHour,
    worstBlocksPerHour,
    passedGate: options.execute ? worstBlocksPerHour >= gateBlocksPerHour : null,
    results,
  };
  console.log(JSON.stringify(payload, null, 2));

  if (options.execute && worstBlocksPerHour < gateBlocksPerHour) {
    process.exitCode = 2;
  }
}

async function resolveBenchmarkRanges(
  metadata: RelationalMetadataStore,
  networkId: number,
  input: {
    blocks: number;
    ranges: number;
  },
): Promise<BenchmarkRange[]> {
  if (options.start !== undefined || options.end !== undefined) {
    if (options.start === undefined || options.end === undefined) {
      throw new Error('both --start and --end are required for an explicit benchmark range');
    }
    const start = parseNonNegativeInteger(options.start, 'start');
    const end = parseNonNegativeInteger(options.end, 'end');
    if (end < start) {
      throw new Error(`invalid range: end ${end} is before start ${start}`);
    }
    return [{ start, end }];
  }

  return metadata.listCoreBackfillBenchmarkRanges(networkId, input);
}

function countCreates(applications: CoreDogecoinBlockApplication[]): number {
  return applications.reduce((sum, application) => sum + application.utxoCreates.length, 0);
}

function countSpends(applications: CoreDogecoinBlockApplication[]): number {
  return applications.reduce((sum, application) => sum + application.utxoSpends.length, 0);
}

function roundMs(value: number): number {
  return Math.round(value * 100) / 100;
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
