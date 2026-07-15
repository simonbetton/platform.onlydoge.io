import { chmod, mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { gzipSync } from 'node:zlib';

import { FileRawBlockStorageAdapter, S3RawBlockStorageAdapter } from '@onlydoge/platform';
import { InfrastructureError } from '@onlydoge/shared-kernel';
import { describe, expect, it, vi } from 'vitest';

describe('file raw block storage adapter', () => {
  it('returns null for a missing file', async () => {
    await withTemporaryDirectory(async (basePath) => {
      const adapter = new FileRawBlockStorageAdapter(basePath);

      await expect(adapter.getPart(100, 'block')).resolves.toBeNull();
    });
  });

  it('surfaces file permission failures with their cause', async () => {
    await withTemporaryDirectory(async (basePath) => {
      const filePath = await writeRawPart(basePath, gzipJson({ height: 100 }));
      await chmod(filePath, 0o000);
      const adapter = new FileRawBlockStorageAdapter(basePath);

      try {
        await expect(adapter.getPart(100, 'block')).rejects.toEqual(
          new InfrastructureError(`raw block storage file read failed path=${filePath}`, {
            cause: expect.objectContaining({ code: 'EACCES' }),
          }),
        );
      } finally {
        await chmod(filePath, 0o600);
      }
    });
  });

  it('surfaces corrupt gzip data with its cause', async () => {
    await withTemporaryDirectory(async (basePath) => {
      const filePath = await writeRawPart(basePath, Buffer.from('not gzip data'));
      const adapter = new FileRawBlockStorageAdapter(basePath);

      await expect(adapter.getPart(100, 'block')).rejects.toEqual(
        new InfrastructureError(`raw block storage file read failed path=${filePath}`, {
          cause: expect.any(Error),
        }),
      );
    });
  });

  it('surfaces malformed JSON with its cause', async () => {
    await withTemporaryDirectory(async (basePath) => {
      const filePath = await writeRawPart(basePath, gzipSync(Buffer.from('{invalid')));
      const adapter = new FileRawBlockStorageAdapter(basePath);

      await expect(adapter.getPart(100, 'block')).rejects.toEqual(
        new InfrastructureError(`raw block storage file read failed path=${filePath}`, {
          cause: expect.any(SyntaxError),
        }),
      );
    });
  });
});

describe('S3 raw block storage adapter', () => {
  it.each([
    serviceError('NoSuchKey', 500),
    serviceError('NotFound', 500),
    serviceError('S3ServiceException', 404),
  ])('returns null for a typed not-found response', async (error) => {
    const adapter = s3Adapter(vi.fn().mockRejectedValue(error));

    await expect(adapter.getPart(100, 'block')).resolves.toBeNull();
  });

  it.each([403, 500])('surfaces S3 HTTP %i failures with their cause', async (statusCode) => {
    const error = serviceError('S3ServiceException', statusCode);
    const adapter = s3Adapter(vi.fn().mockRejectedValue(error));

    await expect(adapter.getPart(100, 'block')).rejects.toEqual(
      new InfrastructureError('raw block storage S3 get failed key=raw/100/block.json.gz', {
        cause: error,
      }),
    );
  });

  it('surfaces corrupt S3 gzip data with its cause', async () => {
    const adapter = s3Adapter(vi.fn().mockResolvedValue({ Body: Buffer.from('not gzip data') }));

    await expect(adapter.getPart(100, 'block')).rejects.toEqual(
      new InfrastructureError('raw block storage S3 get failed key=raw/100/block.json.gz', {
        cause: expect.any(Error),
      }),
    );
  });

  it('preserves aborted request behavior ahead of not-found classification', async () => {
    const controller = new AbortController();
    controller.abort();
    const adapter = s3Adapter(vi.fn().mockRejectedValue(serviceError('NoSuchKey', 404)));

    await expect(adapter.getPart(100, 'block', { abortSignal: controller.signal })).rejects.toThrow(
      'raw block storage get timed out key=raw/100/block.json.gz',
    );
  });
});

function s3Adapter(send: (...parameters: never[]) => unknown): S3RawBlockStorageAdapter {
  const adapter = new S3RawBlockStorageAdapter({
    driver: 's3',
    location: 'http://localhost:9000/test-bucket/raw',
  });
  (adapter as unknown as { client: { send: typeof send } }).client = { send };
  return adapter;
}

function serviceError(name: string, httpStatusCode: number): Error {
  const error = new Error(`${name} response`);
  error.name = name;
  return Object.assign(error, { $metadata: { httpStatusCode } });
}

async function withTemporaryDirectory(run: (basePath: string) => Promise<void>): Promise<void> {
  const basePath = await mkdtemp(join(tmpdir(), 'onlydoge-raw-storage-'));
  try {
    await run(basePath);
  } finally {
    await rm(basePath, { force: true, recursive: true });
  }
}

async function writeRawPart(basePath: string, payload: Uint8Array): Promise<string> {
  const directory = join(basePath, '100');
  const filePath = join(directory, 'block.json.gz');
  await mkdir(directory, { recursive: true });
  await writeFile(filePath, payload);
  return filePath;
}

function gzipJson(payload: Record<string, unknown>): Buffer {
  return gzipSync(Buffer.from(JSON.stringify(payload)));
}
