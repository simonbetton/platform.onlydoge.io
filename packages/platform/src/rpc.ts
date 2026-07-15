import type { BlockchainRpcPort } from '@onlydoge/indexing-pipeline';

import {
  type ChainFamily,
  InfrastructureError,
  maskRpcEndpointAuth,
} from '@onlydoge/shared-kernel';

const RPC_RETRY_ATTEMPTS = 4;
const RPC_RETRY_BASE_DELAY_MS = 100;
const WORK_QUEUE_EXCEEDED = 'work queue depth exceeded';

export class HttpBlockchainRpcGateway implements BlockchainRpcPort {
  private readonly rateLimitQueues = new Map<string, Promise<void>>();
  private readonly rateLimitState = new Map<string, number>();

  public constructor(
    private readonly timeoutMs = 10_000,
    private readonly mempoolTimeoutMs = 2_000,
  ) {}

  public async assertHealthy(architecture: ChainFamily, rpcEndpoint: string): Promise<void> {
    await this.getBlockHeight({ architecture, rpcEndpoint, rps: Number.MAX_SAFE_INTEGER });
  }

  public async getBlockHeight(dogecoin: {
    architecture: ChainFamily;
    rpcEndpoint: string;
    rps: number;
  }): Promise<number> {
    return this.callDogecoin<number>(dogecoin.rpcEndpoint, dogecoin.rps, 'getblockcount', []);
  }

  public async getBlockSnapshot(
    dogecoin: {
      architecture: ChainFamily;
      rpcEndpoint: string;
      rps: number;
    },
    blockHeight: number,
  ): Promise<Record<string, unknown>> {
    const hash = await this.callDogecoin<string>(
      dogecoin.rpcEndpoint,
      dogecoin.rps,
      'getblockhash',
      [blockHeight],
    );
    const block = await this.loadDogecoinBlock(dogecoin, hash);

    return { block };
  }

  private async loadDogecoinBlock(
    dogecoin: {
      rpcEndpoint: string;
      rps: number;
    },
    hash: string,
  ): Promise<Record<string, unknown>> {
    // Dogecoin Core only accepts boolean getblock verbosity. Integer verbosity 2
    // (Bitcoin Core) fails, so request verbose JSON and hydrate txids in one batch.
    const block = await this.callDogecoin<Record<string, unknown>>(
      dogecoin.rpcEndpoint,
      dogecoin.rps,
      'getblock',
      [hash, true],
    );

    if (needsTransactionHydration(block.tx)) {
      block.tx = await this.hydrateDogecoinTransactions(dogecoin, block.tx);
    }

    return block;
  }

  private async hydrateDogecoinTransactions(
    dogecoin: {
      rpcEndpoint: string;
      rps: number;
    },
    txids: string[],
  ): Promise<Record<string, unknown>[]> {
    const transactions: Record<string, unknown>[] = [];

    for (const chunk of chunkArray(txids, rawTransactionBatchSize)) {
      const decoded = await this.callDogecoinBatch<Record<string, unknown>>(
        dogecoin.rpcEndpoint,
        dogecoin.rps,
        chunk.map((txid) => ({
          method: 'getrawtransaction',
          params: [txid, true],
        })),
      );
      transactions.push(...decoded);
    }

    return transactions;
  }

  public async getMempoolSnapshot(dogecoin: {
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
        dogecoin.rpcEndpoint,
        dogecoin.rps,
        'getmempoolinfo',
        [],
        this.mempoolTimeoutMs,
      ),
      this.callDogecoin<Record<string, unknown>>(
        dogecoin.rpcEndpoint,
        dogecoin.rps,
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

  public async getRawTransaction(
    dogecoin: {
      architecture: ChainFamily;
      rpcEndpoint: string;
      rps: number;
    },
    txid: string,
  ): Promise<Record<string, unknown>> {
    const decoded = await this.callDogecoin<Record<string, unknown>>(
      dogecoin.rpcEndpoint,
      dogecoin.rps,
      'getrawtransaction',
      [txid, true],
      this.mempoolTimeoutMs,
    );
    return requireRecord(decoded, 'getrawtransaction');
  }

  public async decodeRawTransaction(
    dogecoin: {
      architecture: ChainFamily;
      rpcEndpoint: string;
      rps: number;
    },
    rawTxHex: string,
  ): Promise<Record<string, unknown>> {
    const decoded = await this.callDogecoin<Record<string, unknown>>(
      dogecoin.rpcEndpoint,
      dogecoin.rps,
      'decoderawtransaction',
      [rawTxHex],
      this.mempoolTimeoutMs,
    );
    return requireRecord(decoded, 'decoderawtransaction');
  }

  public async getRawTransactions(
    dogecoin: {
      architecture: ChainFamily;
      rpcEndpoint: string;
      rps: number;
    },
    txids: string[],
  ): Promise<Record<string, unknown>[]> {
    return this.hydrateDogecoinTransactions(dogecoin, txids);
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

  private async callDogecoinBatch<T>(
    rpcEndpoint: string,
    rps: number,
    calls: Array<{ method: string; params: unknown[] }>,
    timeoutMs = this.timeoutMs,
  ): Promise<T[]> {
    if (calls.length === 0) {
      return [];
    }

    return this.callJsonRpcBatch(rpcEndpoint, rps, calls, timeoutMs);
  }

  private async callJsonRpc<T>(
    rpcEndpoint: string,
    rps: number,
    method: string,
    params: unknown[],
    timeoutMs: number,
  ): Promise<T> {
    return this.withRpcRetry(rpcEndpoint, async () => {
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
      const body = await readResponseBody(response);
      assertNotWorkQueueExceeded(body, rpcEndpoint);
      const payload = parseRpcJsonBody<{ error?: unknown; result?: T }>(body);

      return readRpcResult(response, payload, rpcEndpoint);
    });
  }

  private async callJsonRpcBatch<T>(
    rpcEndpoint: string,
    rps: number,
    calls: Array<{ method: string; params: unknown[] }>,
    timeoutMs: number,
  ): Promise<T[]> {
    return this.withRpcRetry(rpcEndpoint, async () => {
      const request = this.toRpcRequest(rpcEndpoint);
      await this.waitForRateLimit(request.url, rps);
      const response = await fetch(request.url, {
        method: 'POST',
        headers: request.headers,
        signal: AbortSignal.timeout(timeoutMs),
        body: JSON.stringify(
          calls.map((call, index) => ({
            jsonrpc: '1.0',
            id: index,
            method: call.method,
            params: call.params,
          })),
        ),
      });
      const body = await readResponseBody(response);
      assertNotWorkQueueExceeded(body, rpcEndpoint);
      const payload = parseRpcJsonBody<Array<{ error?: unknown; id?: unknown; result?: T }>>(body);

      return readRpcBatchResults(response, payload, rpcEndpoint, calls.length);
    });
  }

  private async withRpcRetry<T>(rpcEndpoint: string, operation: () => Promise<T>): Promise<T> {
    let attempt = 0;
    let lastError: unknown;

    while (attempt < RPC_RETRY_ATTEMPTS) {
      try {
        return await operation();
      } catch (error) {
        lastError = error;
        if (!isRetryableRpcError(error) || attempt >= RPC_RETRY_ATTEMPTS - 1) {
          throw this.toInfrastructureError(rpcEndpoint, error);
        }

        await sleep(RPC_RETRY_BASE_DELAY_MS * 2 ** attempt);
        attempt += 1;
      }
    }

    throw this.toInfrastructureError(rpcEndpoint, lastError);
  }

  private toInfrastructureError(rpcEndpoint: string, error: unknown): InfrastructureError {
    if (error instanceof InfrastructureError) {
      return error;
    }

    return new InfrastructureError(rpcConnectionErrorMessage(rpcEndpoint), {
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

function rpcConnectionErrorMessage(rpcEndpoint: string): string {
  return `could not connect to \`${displayRpcEndpoint(rpcEndpoint)}\``;
}

function rpcWorkQueueErrorMessage(rpcEndpoint: string): string {
  return `dogecoin rpc work queue exceeded at \`${displayRpcEndpoint(rpcEndpoint)}\``;
}

function displayRpcEndpoint(rpcEndpoint: string): string {
  try {
    return maskRpcEndpointAuth(rpcEndpoint);
  } catch {
    return 'RPC endpoint';
  }
}

function hasRpcCredentials(url: URL): boolean {
  return [url.username, url.password].some(hasText);
}

function hasText(value: string): boolean {
  return value.length > 0;
}

async function readResponseBody(response: Response): Promise<string> {
  return response.text();
}

function parseRpcJsonBody<T>(body: string): T | null {
  const trimmed = body.trim();
  if (trimmed.length === 0) {
    return null;
  }

  try {
    return JSON.parse(trimmed) as T;
  } catch {
    return null;
  }
}

function assertNotWorkQueueExceeded(body: string, rpcEndpoint: string): void {
  if (!isWorkQueueExceededBody(body)) {
    return;
  }

  throw new InfrastructureError(rpcWorkQueueErrorMessage(rpcEndpoint));
}

function isWorkQueueExceededBody(body: string): boolean {
  return body.trim().toLowerCase() === WORK_QUEUE_EXCEEDED;
}

function isRetryableRpcError(error: unknown): boolean {
  return error instanceof InfrastructureError && isWorkQueueExceededMessage(error.message);
}

function isWorkQueueExceededMessage(message: string): boolean {
  return message.includes('work queue exceeded');
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
    throw new InfrastructureError(rpcConnectionErrorMessage(rpcEndpoint));
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

function needsTransactionHydration(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((entry) => typeof entry === 'string');
}

const rawTransactionBatchSize = 128;

function chunkArray<T>(values: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < values.length; index += size) {
    chunks.push(values.slice(index, index + size));
  }
  return chunks;
}

function readRpcBatchResults<T>(
  response: Response,
  payload: Array<{ error?: unknown; id?: unknown; result?: T }> | null,
  rpcEndpoint: string,
  expectedCount: number,
): T[] {
  if (!response.ok || !Array.isArray(payload) || payload.length !== expectedCount) {
    throw new InfrastructureError(rpcConnectionErrorMessage(rpcEndpoint));
  }

  const results = new Array<T | undefined>(expectedCount);
  for (const entry of payload) {
    if (isInvalidRpcPayload(entry) || !isBatchResultIndex(entry.id, expectedCount)) {
      throw new InfrastructureError(rpcConnectionErrorMessage(rpcEndpoint));
    }

    results[entry.id] = entry.result as T;
  }

  if (results.some((entry) => entry === undefined)) {
    throw new InfrastructureError(rpcConnectionErrorMessage(rpcEndpoint));
  }

  return results as T[];
}

function isBatchResultIndex(value: unknown, expectedCount: number): value is number {
  return Number.isInteger(value) && Number(value) >= 0 && Number(value) < expectedCount;
}

async function sleepUntilScheduled(scheduledAt: number, now: number): Promise<void> {
  const delayMs = scheduledAt - now;
  if (delayMs > 0) {
    await sleep(delayMs);
  }
}

function sleep(delayMs: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, delayMs));
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
