import { readFile } from 'node:fs/promises';
import { describe, expect, it } from 'vitest';

const root = new URL('../../', import.meta.url);
const localCompose = new URL('docker-compose.local.yml', root);
const productionCompose = new URL('docker-compose.prod.yml', root);
const clickHouseSmoke = new URL('tests/integration/clickhouse-analytics-smoke.test.ts', root);
const dependabot = new URL('.github/dependabot.yml', root);

const images = {
  clickhouse:
    'clickhouse/clickhouse-server:26.6.1.1193@sha256:1d1f6508eba2dccce2cee9913907c5f7766327debc57a6b1991f2c9e3176c163',
  minio:
    'minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e',
  minioClient:
    'minio/mc:RELEASE.2025-08-13T08-35-41Z@sha256:a7fe349ef4bd8521fb8497f55c6042871b2ae640607cf99d9bede5e9bdf11727',
} as const;

function serviceImage(compose: string, service: string): string {
  const match = compose.match(
    new RegExp(`^  ${service}:\\n(?: {4}.*\\n)*? {4}image: ([^\\n]+)$`, 'mu'),
  );
  expect(match, `missing image for ${service}`).not.toBeNull();
  return match?.[1] ?? '';
}

describe('service image pins', () => {
  it('uses identical immutable infrastructure images in local and production Compose', async () => {
    for (const composePath of [localCompose, productionCompose]) {
      const compose = await readFile(composePath, 'utf8');

      expect(serviceImage(compose, 'clickhouse')).toBe(images.clickhouse);
      expect(serviceImage(compose, 'minio')).toBe(images.minio);
      expect(serviceImage(compose, 'minio-create-bucket')).toBe(images.minioClient);
      expect(compose).not.toMatch(/(?:clickhouse\/clickhouse-server|minio\/(?:minio|mc)):latest/u);
    }
  });

  it('keeps the ClickHouse smoke default aligned while allowing an environment override', async () => {
    const smoke = await readFile(clickHouseSmoke, 'utf8');

    expect(smoke).toContain('process.env.ONLYDOGE_CLICKHOUSE_SMOKE_IMAGE ??');
    expect(smoke).toContain(`'${images.clickhouse}'`);
  });

  it('keeps Dependabot Docker Compose coverage enabled', async () => {
    const config = await readFile(dependabot, 'utf8');

    expect(config).toMatch(/package-ecosystem: docker-compose[\s\S]*?directory: \//u);
  });
});
