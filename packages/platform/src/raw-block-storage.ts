import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { gunzipSync, gzipSync } from 'node:zlib';

import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';

import type {
  RawBlockStoragePort,
  RawBlockStorageRequestContext,
} from '@onlydoge/indexing-pipeline';

import type { StorageSettings } from './settings';

export class FileRawBlockStorageAdapter implements RawBlockStoragePort {
  public constructor(private readonly basePath: string) {}

  public async getPart<T extends Record<string, unknown>>(
    blockHeight: number,
    part: string,
    context?: RawBlockStorageRequestContext,
  ): Promise<T | null> {
    assertNotAborted(context);
    const filePath = join(this.basePath, String(blockHeight), `${part}.json.gz`);
    try {
      const payload = await readFile(filePath);
      assertNotAborted(context);
      return decodeJsonGzip<T>(payload);
    } catch {
      assertNotAborted(context);
      return null;
    }
  }

  public async putPart(
    blockHeight: number,
    part: string,
    payload: Record<string, unknown>,
    context?: RawBlockStorageRequestContext,
  ): Promise<void> {
    assertNotAborted(context);
    const filePath = join(this.basePath, String(blockHeight), `${part}.json.gz`);
    await mkdir(dirname(filePath), { recursive: true });
    await writeFile(filePath, gzipSync(Buffer.from(JSON.stringify(payload))));
    assertNotAborted(context);
  }
}

export class S3RawBlockStorageAdapter implements RawBlockStoragePort {
  private readonly bucket: string;
  private readonly client: S3Client;
  private readonly prefix: string;

  public constructor(settings: StorageSettings) {
    const location = parseS3Location(settings.location);
    this.bucket = location.bucket;
    this.prefix = location.prefix;
    this.client = createS3Client(settings, location.url);
  }

  public async getPart<T extends Record<string, unknown>>(
    blockHeight: number,
    part: string,
    context?: RawBlockStorageRequestContext,
  ): Promise<T | null> {
    const key = this.storageKey(blockHeight, part);
    const request = createRequestAbortSignal(context);
    try {
      const body = await this.getObjectBody(key, request);
      assertNotAborted(context);
      return decodeJsonGzip<T>(body);
    } catch {
      throwIfRequestAborted(request, `raw block storage get timed out key=${key}`);
      return null;
    } finally {
      request.cleanup();
    }
  }

  public async putPart(
    blockHeight: number,
    part: string,
    payload: Record<string, unknown>,
    context?: RawBlockStorageRequestContext,
  ): Promise<void> {
    const key = this.storageKey(blockHeight, part);
    const request = createRequestAbortSignal(context);
    try {
      await this.putObjectBody(key, payload, request);
    } catch (error) {
      throwIfRequestAborted(request, `raw block storage put timed out key=${key}`);
      throw error;
    } finally {
      request.cleanup();
    }
  }

  private async getObjectBody(key: string, request: RawBlockStorageAbortRequest): Promise<Buffer> {
    const response = await this.client.send(
      new GetObjectCommand({
        Bucket: this.bucket,
        Key: key,
      }),
      requestOptions(request),
    );
    return toBuffer(response.Body);
  }

  private async putObjectBody(
    key: string,
    payload: Record<string, unknown>,
    request: RawBlockStorageAbortRequest,
  ): Promise<void> {
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: gzipSync(Buffer.from(JSON.stringify(payload))),
        ContentType: 'application/json',
        ContentEncoding: 'gzip',
      }),
      requestOptions(request),
    );
  }

  private storageKey(blockHeight: number, part: string): string {
    return [this.prefix, blockHeight, `${part}.json.gz`].filter(Boolean).join('/');
  }
}

export function createRawBlockStorage(settings: StorageSettings): RawBlockStoragePort {
  return settings.driver === 'file'
    ? new FileRawBlockStorageAdapter(settings.location)
    : new S3RawBlockStorageAdapter(settings);
}

async function toBuffer(body: unknown): Promise<Buffer> {
  if (!body) {
    return Buffer.alloc(0);
  }

  return requireKnownBodyBuffer(await knownBodyToBuffer(body));
}

async function knownBodyToBuffer(body: unknown): Promise<Buffer | null> {
  const direct = bufferFromDirectBody(body);
  if (direct) return direct;

  return knownStreamingBodyToBuffer(body);
}

async function knownStreamingBodyToBuffer(body: unknown): Promise<Buffer | null> {
  if (hasTransformToByteArray(body)) {
    return Buffer.from(await body.transformToByteArray());
  }

  return asyncIterableBodyToBuffer(body);
}

async function asyncIterableBodyToBuffer(body: unknown): Promise<Buffer | null> {
  if (isAsyncIterable(body)) {
    return bufferFromAsyncIterable(body);
  }

  return null;
}

function requireKnownBodyBuffer(buffer: Buffer | null): Buffer {
  if (buffer) {
    return buffer;
  }

  throw new Error('Unsupported S3 response body');
}

function parseS3Location(location: string): {
  bucket: string;
  prefix: string;
  url: URL;
} {
  const url = new URL(location);
  const pathParts = url.pathname.replace(/^\/+/u, '').split('/');
  return {
    bucket: pathParts[0] ?? 'onlydoge',
    prefix: pathParts.slice(1).join('/'),
    url,
  };
}

function createS3Client(settings: StorageSettings, url: URL): S3Client {
  const credentials = s3Credentials(settings);
  return new S3Client({
    endpoint: `${url.protocol}//${url.host}`,
    region: 'auto',
    ...(credentials ? { credentials } : {}),
    forcePathStyle: true,
  });
}

function s3Credentials(settings: StorageSettings):
  | {
      accessKeyId: string;
      secretAccessKey: string;
    }
  | undefined {
  if (!hasS3Credentials(settings)) {
    return undefined;
  }

  return {
    accessKeyId: settings.accessKeyId,
    secretAccessKey: settings.secretAccessKey,
  };
}

function hasS3Credentials(settings: StorageSettings): settings is StorageSettings & {
  accessKeyId: string;
  secretAccessKey: string;
} {
  return [settings.accessKeyId, settings.secretAccessKey].every(Boolean);
}

function decodeJsonGzip<T extends Record<string, unknown>>(payload: Uint8Array): T {
  return JSON.parse(gunzipSync(payload).toString('utf8'));
}

interface RawBlockStorageAbortRequest {
  cleanup(): void;
  signal?: AbortSignal;
}

function createRequestAbortSignal(
  context?: RawBlockStorageRequestContext,
): RawBlockStorageAbortRequest {
  if (!needsAbortRequest(context)) {
    return { cleanup() {} };
  }

  return createKnownRequestAbortSignal(context);
}

function needsAbortRequest(
  context: RawBlockStorageRequestContext | undefined,
): context is RawBlockStorageRequestContext {
  return context !== undefined && hasAbortRequestOptions(context);
}

function hasAbortRequestOptions(context: RawBlockStorageRequestContext): boolean {
  return [context.abortSignal, context.timeoutMs].some(Boolean);
}

function createKnownRequestAbortSignal(
  context: RawBlockStorageRequestContext,
): RawBlockStorageAbortRequest {
  const controller = new AbortController();
  const abortFromParent = () => controller.abort(context.abortSignal?.reason);
  const timeout = startAbortTimeout(controller, context.timeoutMs);
  attachParentAbortSignal(context.abortSignal, controller, abortFromParent);

  return {
    signal: controller.signal,
    cleanup() {
      if (timeout) {
        clearTimeout(timeout);
      }
      context.abortSignal?.removeEventListener('abort', abortFromParent);
    },
  };
}

function startAbortTimeout(
  controller: AbortController,
  timeoutMs?: number,
): ReturnType<typeof setTimeout> | null {
  if (!timeoutMs) {
    return null;
  }

  return setTimeout(
    () => controller.abort(new Error('raw block storage request timed out')),
    timeoutMs,
  );
}

function attachParentAbortSignal(
  abortSignal: AbortSignal | undefined,
  controller: AbortController,
  abortFromParent: () => void,
): void {
  if (!abortSignal) {
    return;
  }

  attachKnownParentAbortSignal(abortSignal, controller, abortFromParent);
}

function attachKnownParentAbortSignal(
  abortSignal: AbortSignal,
  controller: AbortController,
  abortFromParent: () => void,
): void {
  if (abortSignal.aborted) {
    controller.abort(abortSignal.reason);
    return;
  }

  abortSignal.addEventListener('abort', abortFromParent, { once: true });
}

function isAbortedSignal(abortSignal: AbortSignal | undefined): abortSignal is AbortSignal {
  return abortSignal?.aborted === true;
}

function requestOptions(
  request: RawBlockStorageAbortRequest,
): { abortSignal: AbortSignal } | undefined {
  return request.signal ? { abortSignal: request.signal } : undefined;
}

function throwIfRequestAborted(request: RawBlockStorageAbortRequest, message: string): void {
  if (isAbortedSignal(request.signal)) {
    throw new Error(message);
  }
}

function assertNotAborted(context?: RawBlockStorageRequestContext): void {
  if (isAbortedSignal(contextAbortSignal(context))) {
    throw new Error('raw block storage request aborted');
  }
}

function contextAbortSignal(
  context: RawBlockStorageRequestContext | undefined,
): AbortSignal | undefined {
  return context?.abortSignal;
}

function bufferFromDirectBody(body: unknown): Buffer | null {
  if (isDirectBody(body)) {
    return Buffer.from(body);
  }

  return null;
}

function isDirectBody(body: unknown): body is Uint8Array | string {
  return [body instanceof Uint8Array, typeof body === 'string'].some(Boolean);
}

function hasTransformToByteArray(
  body: unknown,
): body is { transformToByteArray(): Promise<Uint8Array> } {
  if (!isObjectValue(body)) {
    return false;
  }

  return hasTransformToByteArrayMethod(body);
}

function hasTransformToByteArrayMethod(body: object): body is {
  transformToByteArray(): Promise<Uint8Array>;
} {
  return [
    'transformToByteArray' in body,
    typeof Reflect.get(body, 'transformToByteArray') === 'function',
  ].every(Boolean);
}

async function bufferFromAsyncIterable(body: AsyncIterable<unknown>): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of body) {
    chunks.push(bufferFromChunk(chunk));
  }

  return Buffer.concat(chunks);
}

function bufferFromChunk(chunk: unknown): Buffer {
  return typeof chunk === 'string' ? Buffer.from(chunk) : Buffer.from(chunk as Uint8Array);
}

function isAsyncIterable(value: unknown): value is AsyncIterable<Buffer | Uint8Array | string> {
  if (!isObjectValue(value)) {
    return false;
  }

  return Symbol.asyncIterator in value;
}

function isObjectValue(value: unknown): value is object {
  return [value !== null, typeof value === 'object'].every(Boolean);
}
