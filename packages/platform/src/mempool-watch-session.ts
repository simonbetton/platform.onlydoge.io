import { randomUUID } from 'node:crypto';

import { TooEarlyError, ValidationError } from '@onlydoge/shared-kernel';
import type { MempoolWatchBus } from './mempool-watch-bus';
import {
  type ActiveMempoolWatch,
  MEMPOOL_WATCH_DETECTOR_STATUS_KEY,
  MEMPOOL_WATCH_HEARTBEAT_MS,
  MEMPOOL_WATCH_SESSION_MS,
  type MempoolAppearEvent,
  type MempoolWatchDetectorStatus,
} from './mempool-watch-types';

export interface MempoolWatchSessionRegistryPort {
  createActiveMempoolWatch(input: {
    address: string;
    apiKeyId: string;
    expiresAt: string;
    id: string;
    minValueBase: string | null;
  }): Promise<ActiveMempoolWatch>;
  deleteActiveMempoolWatch(id: string): Promise<void>;
  getJsonValue<T>(key: string): Promise<T | null>;
}

export type MempoolWatchSseEvent =
  | {
      event: 'mempool.watch.appeared';
      data: Omit<MempoolAppearEvent, 'apiKeyId' | 'watchId'>;
    }
  | {
      event: 'mempool.watch.timeout';
      data: { address: string; expiresAt: string };
    }
  | {
      event: 'comment';
      data: string;
    };

export class MempoolWatchSessionService {
  public constructor(
    private readonly registry: MempoolWatchSessionRegistryPort,
    private readonly bus: MempoolWatchBus,
  ) {}

  public async *openSession(input: {
    address: string;
    apiKeyId: string;
    minValueBase?: string | null;
    signal?: AbortSignal;
  }): AsyncGenerator<MempoolWatchSseEvent> {
    const address = normalizeWatchAddress(input.address);
    const minValueBase = normalizeMinValueBase(input.minValueBase);
    const createdAtMs = Date.now();
    const expiresAt = new Date(createdAtMs + MEMPOOL_WATCH_SESSION_MS).toISOString();
    let settled = false;
    let appear: MempoolAppearEvent | null = null;
    let watch: ActiveMempoolWatch | null = null;
    const unsubscribe = this.bus.subscribeAppears((event) => {
      if (event.watchId !== watch?.id || settled) {
        return;
      }
      appear = event;
    });

    try {
      await this.assertDetectorAvailable();
      watch = await this.registry.createActiveMempoolWatch({
        id: `mw_${randomUUID().replaceAll('-', '')}`,
        apiKeyId: input.apiKeyId,
        address,
        minValueBase,
        expiresAt,
      });
      await this.bus.publishWatchChanged();

      yield { event: 'comment', data: 'connected' };

      let nextHeartbeatAt = Date.now() + MEMPOOL_WATCH_HEARTBEAT_MS;
      while (!settled && !input.signal?.aborted) {
        if (appear) {
          settled = true;
          yield {
            event: 'mempool.watch.appeared',
            data: publicAppearPayload(appear),
          };
          return;
        }

        if (Date.now() >= createdAtMs + MEMPOOL_WATCH_SESSION_MS) {
          settled = true;
          yield {
            event: 'mempool.watch.timeout',
            data: { address, expiresAt },
          };
          return;
        }

        if (Date.now() >= nextHeartbeatAt) {
          yield { event: 'comment', data: 'heartbeat' };
          nextHeartbeatAt = Date.now() + MEMPOOL_WATCH_HEARTBEAT_MS;
        }

        await sleep(100, input.signal);
      }

      if (!settled) {
        yield {
          event: 'mempool.watch.timeout',
          data: { address, expiresAt },
        };
      }
    } finally {
      unsubscribe();
      settled = true;
      if (watch) {
        await this.registry.deleteActiveMempoolWatch(watch.id);
        await this.bus.publishWatchChanged();
      }
    }
  }

  private async assertDetectorAvailable(): Promise<void> {
    const status = await this.registry.getJsonValue<MempoolWatchDetectorStatus>(
      MEMPOOL_WATCH_DETECTOR_STATUS_KEY,
    );
    if (status?.degraded === true && Date.parse(status.expiresAt) > Date.now()) {
      throw new TooEarlyError('mempool watch detector is temporarily unavailable');
    }
  }
}

function publicAppearPayload(
  event: MempoolAppearEvent,
): Omit<MempoolAppearEvent, 'apiKeyId' | 'watchId'> {
  return {
    address: event.address,
    detectedAt: event.detectedAt,
    outputs: event.outputs,
    source: event.source,
    txid: event.txid,
  };
}

function normalizeWatchAddress(address: string): string {
  const trimmed = address.trim();
  if (trimmed.length === 0) {
    throw new ValidationError('address is required');
  }
  return trimmed;
}

function normalizeMinValueBase(value: string | null | undefined): string | null {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  const trimmed = value.trim();
  if (!/^\d+$/u.test(trimmed)) {
    throw new ValidationError('minValueBase must be a non-negative integer string');
  }

  return trimmed.replace(/^0+(?=\d)/u, '') || '0';
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (signal?.aborted) {
    return Promise.resolve();
  }

  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal?.addEventListener(
      'abort',
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}
