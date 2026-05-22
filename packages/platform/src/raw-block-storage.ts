import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { gunzipSync, gzipSync } from 'node:zlib';

import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';

import type {
  RawBlockStoragePort,
  RawBlockStorageRequestContext,
} from '@onlydoge/indexing-pipeline';
import type { PrimaryId } from '@onlydoge/shared-kernel';

import type { StorageSettings } from './settings';

export class FileRawBlockStorageAdapter implements RawBlockStoragePort {
  public constructor(private readonly basePath: string) {}

  public async getPart<T extends Record<string, unknown>>(
    networkId: PrimaryId,
    blockHeight: number,
    part: string,
    context?: RawBlockStorageRequestContext,
  ): Promise<T | null> {
    assertNotAborted(context);
    const filePath = join(this.basePath, String(networkId), String(blockHeight), `${part}.json.gz`);
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
    networkId: PrimaryId,
    blockHeight: number,
    part: string,
    payload: Record<string, unknown>,
    context?: RawBlockStorageRequestContext,
  ): Promise<void> {
    assertNotAborted(context);
    const filePath = join(this.basePath, String(networkId), String(blockHeight), `${part}.json.gz`);
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
    networkId: PrimaryId,
    blockHeight: number,
    part: string,
    context?: RawBlockStorageRequestContext,
  ): Promise<T | null> {
    const key = [this.prefix, networkId, blockHeight, `${part}.json.gz`].filter(Boolean).join('/');
    const request = createRequestAbortSignal(context);
    try {
      const response = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: key,
        }),
        request.signal ? { abortSignal: request.signal } : undefined,
      );
      const body = await toBuffer(response.Body);
      assertNotAborted(context);
      return decodeJsonGzip<T>(body);
    } catch {
      if (request.signal?.aborted) {
        throw new Error(`raw block storage get timed out key=${key}`);
      }
      return null;
    } finally {
      request.cleanup();
    }
  }

  public async putPart(
    networkId: PrimaryId,
    blockHeight: number,
    part: string,
    payload: Record<string, unknown>,
    context?: RawBlockStorageRequestContext,
  ): Promise<void> {
    const key = [this.prefix, networkId, blockHeight, `${part}.json.gz`].filter(Boolean).join('/');
    const request = createRequestAbortSignal(context);
    try {
      await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: key,
          Body: gzipSync(Buffer.from(JSON.stringify(payload))),
          ContentType: 'application/json',
          ContentEncoding: 'gzip',
        }),
        request.signal ? { abortSignal: request.signal } : undefined,
      );
    } catch (error) {
      if (request.signal?.aborted) {
        throw new Error(`raw block storage put timed out key=${key}`);
      }
      throw error;
    } finally {
      request.cleanup();
    }
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

  const buffer = await knownBodyToBuffer(body);
  if (buffer) {
    return buffer;
  }

  throw new Error('Unsupported S3 response body');
}

async function knownBodyToBuffer(body: unknown): Promise<Buffer | null> {
  const direct = bufferFromDirectBody(body);
  if (direct) {
    return direct;
  }

  if (hasTransformToByteArray(body)) {
    return Buffer.from(await body.transformToByteArray());
  }

  if (isAsyncIterable(body)) {
    return bufferFromAsyncIterable(body);
  }

  return null;
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
  if (!settings.accessKeyId || !settings.secretAccessKey) {
    return undefined;
  }

  return {
    accessKeyId: settings.accessKeyId,
    secretAccessKey: settings.secretAccessKey,
  };
}

function decodeJsonGzip<T extends Record<string, unknown>>(payload: Uint8Array): T {
  return JSON.parse(gunzipSync(payload).toString('utf8'));
}

function createRequestAbortSignal(context?: RawBlockStorageRequestContext): {
  cleanup(): void;
  signal?: AbortSignal;
} {
  if (!context?.abortSignal && !context?.timeoutMs) {
    return { cleanup() {} };
  }

  const controller = new AbortController();
  const abortFromParent = () => controller.abort(context.abortSignal?.reason);
  const timeout = context.timeoutMs
    ? setTimeout(
        () => controller.abort(new Error('raw block storage request timed out')),
        context.timeoutMs,
      )
    : null;

  if (context.abortSignal?.aborted) {
    controller.abort(context.abortSignal.reason);
  } else {
    context.abortSignal?.addEventListener('abort', abortFromParent, { once: true });
  }

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

function assertNotAborted(context?: RawBlockStorageRequestContext): void {
  if (context?.abortSignal?.aborted) {
    throw new Error('raw block storage request aborted');
  }
}

function bufferFromDirectBody(body: unknown): Buffer | null {
  if (body instanceof Uint8Array || typeof body === 'string') {
    return Buffer.from(body);
  }

  return null;
}

function hasTransformToByteArray(
  body: unknown,
): body is { transformToByteArray(): Promise<Uint8Array> } {
  if (body === null || typeof body !== 'object') {
    return false;
  }

  return 'transformToByteArray' in body && typeof body.transformToByteArray === 'function';
}

async function bufferFromAsyncIterable(body: AsyncIterable<unknown>): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of body) {
    chunks.push(typeof chunk === 'string' ? Buffer.from(chunk) : Buffer.from(chunk as Uint8Array));
  }

  return Buffer.concat(chunks);
}

function isAsyncIterable(value: unknown): value is AsyncIterable<Buffer | Uint8Array | string> {
  if (value === null || typeof value !== 'object') {
    return false;
  }

  return Symbol.asyncIterator in value;
}
