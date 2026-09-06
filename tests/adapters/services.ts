import { resolve } from 'node:path';

import mysql from 'mysql2/promise';
import { Client } from 'pg';

import { type DockerService, startDockerService, waitForHttp } from './docker-service';

const realFetch = globalThis.fetch.bind(globalThis);

export const adapterImages = {
  clickhouse:
    'clickhouse/clickhouse-server:26.6.1.1193@sha256:1d1f6508eba2dccce2cee9913907c5f7766327debc57a6b1991f2c9e3176c163',
  minio:
    'minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e',
  mysql: 'mysql:8.4.5',
  postgres: 'postgres:17.5-alpine',
} as const;

export const adapterCredentials = {
  database: 'onlydoge',
  password: 'onlydoge_adapter_password',
  user: 'onlydoge',
} as const;

export function startPostgres(): Promise<DockerService> {
  return startDockerService({
    environment: {
      POSTGRES_DB: adapterCredentials.database,
      POSTGRES_PASSWORD: adapterCredentials.password,
      POSTGRES_USER: adapterCredentials.user,
    },
    image: adapterImages.postgres,
    name: 'postgres',
    ports: [5432],
    readiness: async (service) => {
      const client = new Client({ connectionString: postgresUrl(service) });
      try {
        await client.connect();
        await client.query('SELECT 1');
        return true;
      } finally {
        await client.end().catch(() => undefined);
      }
    },
  });
}

export function startMysql(): Promise<DockerService> {
  return startDockerService({
    environment: {
      MYSQL_DATABASE: adapterCredentials.database,
      MYSQL_PASSWORD: adapterCredentials.password,
      MYSQL_ROOT_PASSWORD: 'onlydoge_adapter_root_password',
      MYSQL_USER: adapterCredentials.user,
    },
    image: adapterImages.mysql,
    name: 'mysql',
    ports: [3306],
    readiness: async (service) => {
      const connection = await mysql.createConnection(mysqlUrl(service));
      try {
        await connection.query('SELECT 1');
        return true;
      } finally {
        await connection.end();
      }
    },
  });
}

export function startMinio(): Promise<DockerService> {
  return startDockerService({
    command: ['server', '/data'],
    environment: {
      MINIO_ROOT_PASSWORD: adapterCredentials.password,
      MINIO_ROOT_USER: adapterCredentials.user,
    },
    image: adapterImages.minio,
    name: 'minio',
    ports: [9000],
    readiness: (service) =>
      waitForHttp(`http://127.0.0.1:${service.hostPort(9000)}/minio/health/ready`),
  });
}

export function startClickHouse(image: string = adapterImages.clickhouse): Promise<DockerService> {
  const root = process.cwd();
  return startDockerService({
    environment: {
      CLICKHOUSE_ANALYTICS_PASSWORD: 'onlydoge_analytics',
      CLICKHOUSE_DB: adapterCredentials.database,
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: '1',
      CLICKHOUSE_PASSWORD: 'onlydoge',
      CLICKHOUSE_USER: adapterCredentials.user,
    },
    image,
    name: 'clickhouse',
    ports: [8123],
    readiness: (service) => clickHouseQuery(service, 'SELECT 1').then((response) => response.ok),
    volumes: [
      `${resolve(root, 'docker/clickhouse/config.d/onlydoge-memory.xml')}:/etc/clickhouse-server/config.d/onlydoge-memory.xml:ro`,
      `${resolve(root, 'docker/clickhouse/config.d/onlydoge-log-retention.xml')}:/etc/clickhouse-server/config.d/onlydoge-log-retention.xml:ro`,
      `${resolve(root, 'docker/clickhouse/users.d/onlydoge-memory.xml')}:/etc/clickhouse-server/users.d/onlydoge-memory.xml:ro`,
      `${resolve(root, 'docker/clickhouse/users.d/onlydoge-analytics.xml')}:/etc/clickhouse-server/users.d/onlydoge-analytics.xml:ro`,
    ],
  });
}

export function postgresUrl(service: DockerService): string {
  return `postgres://${adapterCredentials.user}:${adapterCredentials.password}@127.0.0.1:${service.hostPort(5432)}/${adapterCredentials.database}`;
}

export function mysqlUrl(service: DockerService): string {
  return `mysql://${adapterCredentials.user}:${adapterCredentials.password}@127.0.0.1:${service.hostPort(3306)}/${adapterCredentials.database}`;
}

export function minioUrl(service: DockerService, path = ''): string {
  return `http://127.0.0.1:${service.hostPort(9000)}/${path}`;
}

export function clickHouseUrl(service: DockerService): string {
  return `http://127.0.0.1:${service.hostPort(8123)}`;
}

export function clickHouseQuery(
  service: DockerService,
  query: string,
  user: string = adapterCredentials.user,
  password = 'onlydoge',
): Promise<Response> {
  const url = new URL(clickHouseUrl(service));
  url.searchParams.set('query', query);
  return realFetch(url, {
    headers: {
      'X-ClickHouse-Key': password,
      'X-ClickHouse-User': user,
    },
  });
}
