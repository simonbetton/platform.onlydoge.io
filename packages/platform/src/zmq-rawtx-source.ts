import { spawn } from 'node:child_process';
import { dirname, join } from 'node:path';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';

import { noopServiceLogger, type ServiceLogger } from '@onlydoge/shared-kernel';

export interface MempoolRawTxSource {
  start(handler: (rawTxHex: string) => void | Promise<void>, signal?: AbortSignal): Promise<void>;
}

export class NoopMempoolRawTxSource implements MempoolRawTxSource {
  public async start(): Promise<void> {}
}

/**
 * Out-of-process ZMQ subscriber. Native zeromq crashes Bun in-process, so we
 * spawn the Node bridge script when `node` is available.
 */
export class BridgedZmqRawTxSource implements MempoolRawTxSource {
  private readonly logger: ServiceLogger;

  public constructor(
    private readonly endpoint: string,
    logger: ServiceLogger = noopServiceLogger(),
    private readonly bridgeCommand = defaultBridgeCommand(),
  ) {
    this.logger = logger;
  }

  public async start(
    handler: (rawTxHex: string) => void | Promise<void>,
    signal?: AbortSignal,
  ): Promise<void> {
    if (!this.endpoint) {
      return;
    }

    const child = spawn(this.bridgeCommand.command, [...this.bridgeCommand.args, this.endpoint], {
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    const fail = (error: Error) => {
      this.logger.error({ component: 'zmq-rawtx', err: error }, 'zmq rawtx bridge failed');
    };

    child.on('error', (error) => fail(error));
    child.stderr?.on('data', (chunk: Buffer) => {
      this.logger.error(
        { bridgeOutput: chunk.toString('utf8').trim(), component: 'zmq-rawtx' },
        'zmq rawtx bridge stderr',
      );
    });

    const lines = createInterface({ input: child.stdout });
    const onAbort = () => {
      child.kill('SIGTERM');
      lines.close();
    };
    signal?.addEventListener('abort', onAbort, { once: true });

    try {
      for await (const line of lines) {
        if (signal?.aborted) {
          break;
        }
        const hex = line.trim();
        if (hex.length === 0) {
          continue;
        }
        await handler(hex);
      }
    } finally {
      signal?.removeEventListener('abort', onAbort);
      if (!child.killed) {
        child.kill('SIGTERM');
      }
    }
  }
}

function defaultBridgeCommand(): { args: string[]; command: string } {
  const bridgePath = join(
    dirname(fileURLToPath(import.meta.url)),
    '../../../scripts/zmq-rawtx-bridge/index.mjs',
  );
  return {
    command: process.env.ONLYDOGE_ZMQ_BRIDGE_BIN ?? 'node',
    args: [bridgePath],
  };
}
