import { CreateBucketCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { S3RawBlockStorageAdapter } from '@onlydoge/platform';
import { InfrastructureError } from '@onlydoge/shared-kernel';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { DockerService } from './docker-service';
import { adapterCredentials, minioUrl, startMinio } from './services';

const adapterTimeoutMs = 180_000;
const bucket = 'onlydoge-adapter';
let minio: DockerService | null = null;
let client: S3Client | null = null;
let adapter: S3RawBlockStorageAdapter | null = null;

describe.skipIf(process.env.ONLYDOGE_RUN_ADAPTER_TESTS !== '1')(
  'MinIO S3 raw block storage adapter',
  () => {
    beforeAll(async () => {
      minio = await startMinio();
      client = createClient(adapterCredentials.password);
      await client.send(new CreateBucketCommand({ Bucket: bucket }));
      adapter = new S3RawBlockStorageAdapter(storageSettings(adapterCredentials.password));
    }, adapterTimeoutMs);

    afterAll(async () => {
      client?.destroy();
      await minio?.stop();
    }, 30_000);

    it('round-trips compressed JSON and returns exact not-found semantics', async () => {
      const storage = requireValue(adapter);
      const payload = { hash: 'adapter-block', height: 42 };

      await storage.putPart(42, 'block', payload);

      await expect(storage.getPart(42, 'block')).resolves.toEqual(payload);
      await expect(storage.getPart(404, 'block')).resolves.toBeNull();
    });

    it('classifies corrupt objects and service authentication failures', async () => {
      await requireValue(client).send(
        new PutObjectCommand({
          Body: Buffer.from('not-gzip'),
          Bucket: bucket,
          Key: 'raw/43/block.json.gz',
        }),
      );

      await expect(requireValue(adapter).getPart(43, 'block')).rejects.toEqual(
        new InfrastructureError('raw block storage S3 get failed key=raw/43/block.json.gz', {
          cause: expect.any(Error),
        }),
      );

      const unauthorized = new S3RawBlockStorageAdapter(storageSettings('incorrect-password'));
      await expect(unauthorized.getPart(42, 'block')).rejects.toEqual(
        new InfrastructureError('raw block storage S3 get failed key=raw/42/block.json.gz', {
          cause: expect.objectContaining({
            $metadata: expect.objectContaining({ httpStatusCode: 403 }),
          }),
        }),
      );
    });
  },
);

function createClient(secretAccessKey: string): S3Client {
  return new S3Client({
    credentials: {
      accessKeyId: adapterCredentials.user,
      secretAccessKey,
    },
    endpoint: minioUrl(requireValue(minio)),
    forcePathStyle: true,
    region: 'us-east-1',
  });
}

function storageSettings(secretAccessKey: string) {
  return {
    accessKeyId: adapterCredentials.user,
    driver: 's3' as const,
    location: minioUrl(requireValue(minio), `${bucket}/raw`),
    secretAccessKey,
  };
}

function requireValue<T>(value: T | null): T {
  if (value === null) {
    throw new Error('MinIO adapter was not initialized');
  }
  return value;
}
