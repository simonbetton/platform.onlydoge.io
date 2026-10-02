import { EventEmitter } from 'node:events';
import { Client, type Notification } from 'pg';

import { createLogger } from './logger';
import {
  MEMPOOL_APPEAR_CHANNEL,
  MEMPOOL_WATCH_CHANGED_CHANNEL,
  type MempoolAppearEvent,
} from './mempool-watch-types';
import type { DatabaseSettings } from './settings';

export interface MempoolWatchBus {
  publishAppear(event: MempoolAppearEvent): Promise<void>;
  publishWatchChanged(): Promise<void>;
  start(signal?: AbortSignal): Promise<void>;
  stop(): Promise<void>;
  subscribeAppears(handler: (event: MempoolAppearEvent) => void): () => void;
  subscribeWatchChanged(handler: () => void): () => void;
}

export class InProcessMempoolWatchBus implements MempoolWatchBus {
  private readonly emitter = new EventEmitter();

  public async publishAppear(event: MempoolAppearEvent): Promise<void> {
    this.emitter.emit('appear', event);
  }

  public async publishWatchChanged(): Promise<void> {
    this.emitter.emit('watch_changed');
  }

  public async start(): Promise<void> {}

  public async stop(): Promise<void> {
    this.emitter.removeAllListeners();
  }

  public subscribeAppears(handler: (event: MempoolAppearEvent) => void): () => void {
    this.emitter.on('appear', handler);
    return () => {
      this.emitter.off('appear', handler);
    };
  }

  public subscribeWatchChanged(handler: () => void): () => void {
    this.emitter.on('watch_changed', handler);
    return () => {
      this.emitter.off('watch_changed', handler);
    };
  }
}

export class PostgresMempoolWatchBus implements MempoolWatchBus {
  private readonly local = new InProcessMempoolWatchBus();
  private listener: Client | null = null;
  private notifier: Client | null = null;

  public constructor(private readonly database: DatabaseSettings) {}

  public async publishAppear(event: MempoolAppearEvent): Promise<void> {
    await this.local.publishAppear(event);
    await this.notify(MEMPOOL_APPEAR_CHANNEL, JSON.stringify(event));
  }

  public async publishWatchChanged(): Promise<void> {
    await this.local.publishWatchChanged();
    await this.notify(MEMPOOL_WATCH_CHANGED_CHANNEL, '1');
  }

  public async start(signal?: AbortSignal): Promise<void> {
    if (this.listener) {
      return;
    }

    const listener = new Client({
      connectionString: this.database.location,
      connectionTimeoutMillis: 5_000,
      ...sslOptions(this.database),
    });
    const notifier = new Client({
      connectionString: this.database.location,
      connectionTimeoutMillis: 5_000,
      ...sslOptions(this.database),
    });
    attachClientErrorHandler(listener, () => {
      void this.stop();
    });
    attachClientErrorHandler(notifier, () => {
      void this.stop();
    });
    try {
      await listener.connect();
      await notifier.connect();
      await listener.query(`LISTEN ${MEMPOOL_APPEAR_CHANNEL}`);
      await listener.query(`LISTEN ${MEMPOOL_WATCH_CHANGED_CHANNEL}`);
    } catch (error) {
      await Promise.allSettled([listener.end(), notifier.end()]);
      throw error;
    }

    this.listener = listener;
    this.notifier = notifier;
    this.listener.on('notification', (message) => this.handleNotification(message));
    signal?.addEventListener(
      'abort',
      () => {
        void this.stop();
      },
      { once: true },
    );
  }

  public async stop(): Promise<void> {
    const listener = this.listener;
    const notifier = this.notifier;
    this.listener = null;
    this.notifier = null;
    await this.local.stop();
    await Promise.allSettled([listener?.end(), notifier?.end()]);
  }

  public subscribeAppears(handler: (event: MempoolAppearEvent) => void): () => void {
    return this.local.subscribeAppears(handler);
  }

  public subscribeWatchChanged(handler: () => void): () => void {
    return this.local.subscribeWatchChanged(handler);
  }

  private handleNotification(message: Notification): void {
    if (message.channel === MEMPOOL_WATCH_CHANGED_CHANNEL) {
      void this.local.publishWatchChanged();
      return;
    }

    if (message.channel !== MEMPOOL_APPEAR_CHANNEL || !message.payload) {
      return;
    }

    try {
      const event = JSON.parse(message.payload) as MempoolAppearEvent;
      void this.local.publishAppear(event);
    } catch {
      // Ignore malformed payloads from the bus.
    }
  }

  private async notify(channel: string, payload: string): Promise<void> {
    await this.start();
    if (!this.notifier) {
      throw new Error('mempool watch bus is not started');
    }

    await this.notifier.query('SELECT pg_notify($1, $2)', [channel, payload]);
  }
}

export function createMempoolWatchBus(input: {
  database: DatabaseSettings;
  shareInProcess: boolean;
}): MempoolWatchBus {
  if (input.shareInProcess || input.database.driver !== 'postgres') {
    return new InProcessMempoolWatchBus();
  }

  return new PostgresMempoolWatchBus(input.database);
}

function sslOptions(database: DatabaseSettings): { ssl?: DatabaseSettings['ssl'] } {
  return database.ssl ? { ssl: database.ssl } : {};
}

function attachClientErrorHandler(client: Client, onError: () => void): void {
  client.on('error', (error) => {
    createLogger({ component: 'mempool-watch-bus', service: 'onlydoge' }).error(
      { err: error },
      'mempool watch bus client error',
    );
    onError();
  });
}
