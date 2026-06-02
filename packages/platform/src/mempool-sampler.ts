import { fromDecimalUnits } from '@onlydoge/indexing-pipeline';

import type { DogecoinSettings } from './settings';
import type { MempoolSampleRow, MempoolSampleWarehousePort } from './warehouse';

export interface MempoolDogecoinConfigPort {
  getDogecoinConfig(): Promise<{
    architecture: 'dogecoin';
    rpcEndpoint: string;
    rps: number;
  }>;
}

export interface MempoolRpcPort {
  getMempoolSnapshot(dogecoin: {
    architecture: 'dogecoin';
    rpcEndpoint: string;
    rps: number;
  }): Promise<{
    entries: Record<string, Record<string, unknown>>;
    fetchedAt: string;
    info: Record<string, unknown>;
  }>;
}

export class DogecoinMempoolSamplerService {
  public constructor(
    private readonly configs: MempoolDogecoinConfigPort,
    private readonly rpc: MempoolRpcPort,
    private readonly warehouse: MempoolSampleWarehousePort,
    private readonly settings: Pick<DogecoinSettings, 'mempoolSampleIntervalMs'>,
  ) {}

  public async runOnce(): Promise<boolean> {
    const dogecoin = await this.configs.getDogecoinConfig();
    const snapshot = await this.rpc.getMempoolSnapshot(dogecoin);
    await this.warehouse.insertMempoolSamples(mempoolSampleRows(snapshot));
    return true;
  }

  public async start(signal?: AbortSignal): Promise<void> {
    while (signal?.aborted !== true) {
      await this.runSamplerIteration();
      await sleep(this.settings.mempoolSampleIntervalMs, signal);
    }
  }

  private async runSamplerIteration(): Promise<void> {
    try {
      await this.runOnce();
    } catch (error) {
      console.error(`[onlydoge] mempool sampler failed error=${formatError(error)}`);
    }
  }
}

export function mempoolSampleRows(input: {
  entries: Record<string, Record<string, unknown>>;
  fetchedAt: string;
}): MempoolSampleRow[] {
  const sampledAt = clickHouseDateTime(input.fetchedAt);
  return Object.entries(input.entries).map(([txid, entry]) =>
    mempoolSampleRow(txid, entry, sampledAt),
  );
}

function mempoolSampleRow(
  txid: string,
  entry: Record<string, unknown>,
  sampledAt: string,
): MempoolSampleRow {
  const sizeBytes = readInteger(entry, ['vsize', 'size']);
  const feeBase = readFeeBase(entry);
  return {
    sampledAt,
    txid,
    entryTime: readInteger(entry, ['time']),
    height: readInteger(entry, ['height']),
    sizeBytes,
    feeBase,
    feeRateBasePerKilobyte: feeRateBasePerKilobyte(feeBase, sizeBytes),
    rawJson: JSON.stringify(entry),
  };
}

function readInteger(record: Record<string, unknown>, keys: string[]): number | null {
  for (const key of keys) {
    const value = record[key];
    if (typeof value === 'number' && Number.isInteger(value) && value >= 0) {
      return value;
    }
  }

  return null;
}

function readFeeBase(entry: Record<string, unknown>): string | null {
  return decimalDogecoinToBase(readFeeValue(entry.fee) ?? readNestedFeeValue(entry.fees));
}

function readNestedFeeValue(value: unknown): number | string | null {
  if (!isRecord(value)) {
    return null;
  }

  return readFeeValue(value.base);
}

function readFeeValue(value: unknown): number | string | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === 'string' && value.trim().length > 0) {
    return value;
  }

  return null;
}

function decimalDogecoinToBase(value: number | string | null): string | null {
  if (value === null) {
    return null;
  }

  try {
    return fromDecimalUnits(value, 8);
  } catch {
    return null;
  }
}

function feeRateBasePerKilobyte(feeBase: string | null, sizeBytes: number | null): string | null {
  if (feeBase === null || sizeBytes === null || sizeBytes === 0) {
    return null;
  }

  return ((BigInt(feeBase) * 1000n) / BigInt(sizeBytes)).toString();
}

function clickHouseDateTime(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return clickHouseDateTime(new Date().toISOString());
  }

  return parsed.toISOString().slice(0, 19).replace('T', ' ');
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted === true) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    const timeout = setTimeout(resolve, ms);
    timeout.unref?.();
    signal?.addEventListener(
      'abort',
      () => {
        clearTimeout(timeout);
        resolve();
      },
      { once: true },
    );
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function formatError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
