import type { BlockchainRpcPort } from '@onlydoge/indexing-pipeline';

import type { NetworkRpcGateway } from '@onlydoge/network-catalog';
import { type ChainFamily, InfrastructureError } from '@onlydoge/shared-kernel';

export class HttpBlockchainRpcGateway implements NetworkRpcGateway, BlockchainRpcPort {
  private readonly rateLimitQueues = new Map<string, Promise<void>>();
  private readonly rateLimitState = new Map<string, number>();

  public constructor(
    private readonly timeoutMs = 10_000,
    private readonly mempoolTimeoutMs = 2_000,
  ) {}

  public async assertHealthy(architecture: ChainFamily, rpcEndpoint: string): Promise<void> {
    await this.getBlockHeight({ architecture, rpcEndpoint, rps: Number.MAX_SAFE_INTEGER });
  }

  public async getBlockHeight(network: {
    architecture: ChainFamily;
    rpcEndpoint: string;
    rps: number;
  }): Promise<number> {
    return this.callDogecoin<number>(network.rpcEndpoint, network.rps, 'getblockcount', []);
  }

  public async getBlockSnapshot(
    network: {
      architecture: ChainFamily;
      rpcEndpoint: string;
      rps: number;
    },
    blockHeight: number,
  ): Promise<Record<string, unknown>> {
    const hash = await this.callDogecoin<string>(network.rpcEndpoint, network.rps, 'getblockhash', [
      blockHeight,
    ]);
    const block = await this.callDogecoin<Record<string, unknown>>(
      network.rpcEndpoint,
      network.rps,
      'getblock',
      [hash, 2],
    );

    return { block };
  }

  public async getMempoolSnapshot(network: {
    architecture: ChainFamily;
    rpcEndpoint: string;
    rps: number;
  }): Promise<{
    entries: Record<string, Record<string, unknown>>;
    fetchedAt: string;
    info: Record<string, unknown>;
  }> {
    const [info, rawEntries] = await Promise.all([
      this.callDogecoin<Record<string, unknown>>(
        network.rpcEndpoint,
        network.rps,
        'getmempoolinfo',
        [],
        this.mempoolTimeoutMs,
      ),
      this.callDogecoin<Record<string, unknown>>(
        network.rpcEndpoint,
        network.rps,
        'getrawmempool',
        [true],
        this.mempoolTimeoutMs,
      ),
    ]);

    return {
      fetchedAt: new Date().toISOString(),
      info: requireRecord(info, 'getmempoolinfo'),
      entries: toMempoolEntries(rawEntries),
    };
  }

  private async callDogecoin<T>(
    rpcEndpoint: string,
    rps: number,
    method: string,
    params: unknown[],
    timeoutMs = this.timeoutMs,
  ): Promise<T> {
    return this.callJsonRpc(rpcEndpoint, rps, method, params, timeoutMs);
  }

  private async callJsonRpc<T>(
    rpcEndpoint: string,
    rps: number,
    method: string,
    params: unknown[],
    timeoutMs: number,
  ): Promise<T> {
    try {
      const request = this.toRpcRequest(rpcEndpoint);
      await this.waitForRateLimit(request.url, rps);
      const response = await fetch(request.url, {
        method: 'POST',
        headers: request.headers,
        signal: AbortSignal.timeout(timeoutMs),
        body: JSON.stringify({
          jsonrpc: '1.0',
          id: 'onlydoge',
          method,
          params,
        }),
      });
      const payload: {
        error?: unknown;
        result?: T;
      } | null = await response.json().catch(() => null);

      return readRpcResult(response, payload, rpcEndpoint);
    } catch (error) {
      throw this.toInfrastructureError(rpcEndpoint, error);
    }
  }

  private toInfrastructureError(rpcEndpoint: string, error: unknown): InfrastructureError {
    if (error instanceof InfrastructureError) {
      return error;
    }

    return new InfrastructureError(`could not connect to \`${rpcEndpoint}\``, {
      ...infrastructureErrorCause(error),
    });
  }

  private async waitForRateLimit(rpcEndpoint: string, rps: number): Promise<void> {
    if (!shouldRateLimit(rps)) {
      return;
    }

    const releaseCurrent = this.createRateLimitRelease(rpcEndpoint, 1000 / rps);

    this.rateLimitQueues.set(rpcEndpoint, releaseCurrent);
    await releaseCurrent;
    this.clearRateLimitQueue(rpcEndpoint, releaseCurrent);
  }

  private createRateLimitRelease(rpcEndpoint: string, intervalMs: number): Promise<void> {
    const previous = this.rateLimitQueues.get(rpcEndpoint) ?? Promise.resolve();
    return previous.then(async () => {
      const now = Date.now();
      const scheduledAt = this.reserveRateLimitSlot(rpcEndpoint, now, intervalMs);
      await sleepUntilScheduled(scheduledAt, now);
    });
  }

  private reserveRateLimitSlot(rpcEndpoint: string, now: number, intervalMs: number): number {
    const nextAvailableAt = this.rateLimitState.get(rpcEndpoint) ?? now;
    const scheduledAt = Math.max(now, nextAvailableAt);
    this.rateLimitState.set(rpcEndpoint, scheduledAt + intervalMs);
    return scheduledAt;
  }

  private clearRateLimitQueue(rpcEndpoint: string, releaseCurrent: Promise<void>): void {
    if (this.rateLimitQueues.get(rpcEndpoint) === releaseCurrent) {
      this.rateLimitQueues.delete(rpcEndpoint);
    }
  }

  private toRpcRequest(rpcEndpoint: string): {
    headers: Record<string, string>;
    url: string;
  } {
    const url = new URL(rpcEndpoint);
    const headers: Record<string, string> = {
      'content-type': 'application/json',
    };

    if (hasRpcCredentials(url)) {
      const username = decodeURIComponent(url.username);
      const password = decodeURIComponent(url.password);
      headers.authorization = `Basic ${Buffer.from(`${username}:${password}`).toString('base64')}`;
      url.username = '';
      url.password = '';
    }

    return {
      url: url.toString(),
      headers,
    };
  }
}

function infrastructureErrorCause(error: unknown): { cause?: Error } {
  if (!(error instanceof Error)) {
    return {};
  }

  return { cause: error };
}

function hasRpcCredentials(url: URL): boolean {
  return [url.username, url.password].some(hasText);
}

function hasText(value: string): boolean {
  return value.length > 0;
}

function readRpcResult<T>(
  response: Response,
  payload: { error?: unknown; result?: T } | null,
  rpcEndpoint: string,
): T {
  assertValidRpcResult(response, payload, rpcEndpoint);
  return payload.result;
}

function assertValidRpcResult<T>(
  response: Response,
  payload: { error?: unknown; result?: T } | null,
  rpcEndpoint: string,
): asserts payload is { result: T } {
  if (isInvalidRpcResult(response, payload)) {
    throw new InfrastructureError(`could not connect to \`${rpcEndpoint}\``);
  }
}

function isInvalidRpcResult<T>(
  response: Response,
  payload: { error?: unknown; result?: T } | null,
): payload is null {
  if (!response.ok) {
    return true;
  }

  return isInvalidRpcPayload(payload);
}

function isInvalidRpcPayload<T>(payload: { error?: unknown; result?: T } | null): payload is null {
  if (!payload) {
    return true;
  }

  return hasInvalidRpcPayloadBody(payload);
}

function hasInvalidRpcPayloadBody<T>(payload: { error?: unknown; result?: T }): boolean {
  return [Boolean(payload.error), payload.result === undefined].some(Boolean);
}

function shouldRateLimit(rps: number): boolean {
  return Number.isFinite(rps) && rps > 0;
}

async function sleepUntilScheduled(scheduledAt: number, now: number): Promise<void> {
  const delayMs = scheduledAt - now;
  if (delayMs > 0) {
    await new Promise((resolve) => setTimeout(resolve, delayMs));
  }
}

function requireRecord(value: unknown, method: string): Record<string, unknown> {
  if (!isPlainRecord(value)) {
    throw new InfrastructureError(`invalid dogecoin rpc response for ${method}`);
  }

  return Object.fromEntries(Object.entries(value));
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return [Boolean(value), typeof value === 'object', !Array.isArray(value)].every(Boolean);
}

function toMempoolEntries(value: unknown): Record<string, Record<string, unknown>> {
  const entries = requireRecord(value, 'getrawmempool');

  return Object.fromEntries(
    Object.entries(entries).map(([txid, entry]) => [txid, mempoolEntryRecord(entry)]),
  );
}

function mempoolEntryRecord(entry: unknown): Record<string, unknown> {
  if (!isPlainRecord(entry)) {
    return {};
  }

  return Object.fromEntries(Object.entries(entry));
}
