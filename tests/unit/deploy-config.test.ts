import { describe, expect, it } from 'vitest';

import {
  createDeployConfig,
  findLegacyOnceContainers,
  formatLegacyOnceContainerError,
  loadDeployEnvFile,
  validateProductionE2eEnv,
} from '../../scripts/deploy-config';

describe('deploy config', () => {
  it('reports a helpful error when the managed env file is missing', async () => {
    await expect(loadDeployEnvFile('/tmp/onlydoge-missing-.env.managed')).rejects.toThrow(
      /cp \.env\.managed\.example \.env\.managed/u,
    );
  });

  it('accepts canonical managed deploy env values', () => {
    const config = createDeployConfig(deployConfigInput(canonicalEnv()));

    expect(config).toMatchObject({
      envFile: '.env.managed',
      host: 'platform.onlydoge.io',
      image: 'ghcr.io/simonbetton/onlydoge-indexer:latest',
      remoteDir: '/opt/onlydoge',
      sshJump: 'root@203.0.113.10',
      sshTarget: 'root@10.0.0.10',
    });
    expect(config.envValues.ONLYDOGE_PUBLIC_HOST).toBe('platform.onlydoge.io');
    expect(config.envValues.ONLYDOGE_SSH_TARGET).toBeUndefined();
    expect(config.envValues.ONLYDOGE_DATABASE).toBe('postgres://onlydoge:secret@db/onlydoge');
  });

  it('uses an explicit deploy host in the generated runtime env', () => {
    const config = createDeployConfig({
      ...deployConfigInput(canonicalEnv()),
      host: 'api.example.com',
    });

    expect(config.host).toBe('api.example.com');
    expect(config.envValues.ONLYDOGE_PUBLIC_HOST).toBe('api.example.com');
  });

  it('rejects missing deploy target settings', () => {
    const missingHost = canonicalEnv();
    delete missingHost.ONLYDOGE_PUBLIC_HOST;

    expect(() => createDeployConfig(deployConfigInput(missingHost))).toThrow(
      /ONLYDOGE_PUBLIC_HOST/u,
    );

    const missingSshTarget = canonicalEnv();
    delete missingSshTarget.ONLYDOGE_SSH_TARGET;

    expect(() => createDeployConfig(deployConfigInput(missingSshTarget))).toThrow(
      /ONLYDOGE_SSH_TARGET/u,
    );
  });

  it('rejects missing runtime secrets', () => {
    const env = canonicalEnv();
    delete env.ONLYDOGE_DATABASE;

    expect(() => createDeployConfig(deployConfigInput(env))).toThrow(
      /Missing required deploy env vars: ONLYDOGE_DATABASE/u,
    );
  });

  it('ignores non-canonical deploy env keys', () => {
    const env = {
      ...canonicalEnv(),
      EXTERNAL_DEPLOY_HOST: 'platform.onlydoge.io',
      EXTERNAL_SSH_TARGET: 'root@10.0.0.10',
    };
    const config = createDeployConfig(deployConfigInput(env));

    expect(config.envValues.EXTERNAL_DEPLOY_HOST).toBeUndefined();
    expect(config.envValues.EXTERNAL_SSH_TARGET).toBeUndefined();
  });

  it('rejects legacy mounted database CA paths', () => {
    const env = {
      ...canonicalEnv(),
      ONLYDOGE_DATABASE:
        'postgres://onlydoge:secret@db/onlydoge?sslmode=verify-full&sslrootcert=/storage/do-ca.pem',
      ONLYDOGE_DATABASE_SSLROOTCERT_PEM:
        '-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----',
    };

    expect(() => createDeployConfig(deployConfigInput(env))).toThrow(
      /must not reference sslrootcert=\/storage\/do-ca\.pem.*ONLYDOGE_DATABASE_SSLROOTCERT_PEM/u,
    );
  });

  it('rejects database sslrootcert paths without env CA material', () => {
    const env = {
      ...canonicalEnv(),
      ONLYDOGE_DATABASE:
        'postgres://onlydoge:secret@db/onlydoge?sslmode=verify-full&sslrootcert=/etc/ssl/do-ca.pem',
    };

    expect(() => createDeployConfig(deployConfigInput(env))).toThrow(
      /contains sslrootcert.*ONLYDOGE_DATABASE_SSLROOTCERT_PEM.*ONLYDOGE_DATABASE_SSLROOTCERT_BASE64/u,
    );
  });

  it('surfaces legacy once-managed containers before deploy', () => {
    expect(
      findLegacyOnceContainers([
        'onlydoge-caddy-1',
        'once-proxy',
        'once-app-onlydoge-indexer-abc123',
      ]),
    ).toEqual(['once-proxy', 'once-app-onlydoge-indexer-abc123']);

    expect(formatLegacyOnceContainerError(['once-proxy'])).toMatch(
      /once-proxy[\s\S]*docker stop once-proxy/u,
    );
  });

  it('requires a production E2E admin API token unless E2E is skipped', () => {
    expect(() => validateProductionE2eEnv({}, { skipE2e: false })).toThrow(/PROD_ADMIN_API_TOKEN/u);
    expect(() => validateProductionE2eEnv({}, { skipE2e: true })).not.toThrow();
    expect(() =>
      validateProductionE2eEnv({ PROD_ADMIN_API_TOKEN: 'sk_test' }, { skipE2e: false }),
    ).not.toThrow();
  });
});

function deployConfigInput(fileValues: Record<string, string>) {
  return {
    envFile: '.env.managed',
    fileValues,
    host: undefined,
    image: undefined,
    remoteDir: undefined,
    sshJump: undefined,
    sshTarget: undefined,
  };
}

function canonicalEnv(): Record<string, string> {
  return {
    ONLYDOGE_CORE_BLOCK_TIMEOUT_MS: '120000',
    ONLYDOGE_DATABASE: 'postgres://onlydoge:secret@db/onlydoge',
    ONLYDOGE_DOGECOIN_RPC_ENDPOINT: 'http://rpc-user:rpc-password@dogecoin-rpc:22555/',
    ONLYDOGE_IMAGE: 'ghcr.io/simonbetton/onlydoge-indexer:latest',
    ONLYDOGE_PUBLIC_HOST: 'platform.onlydoge.io',
    ONLYDOGE_REMOTE_DIR: '/opt/onlydoge',
    ONLYDOGE_S3_ACCESS_KEY_ID: 'spaces-key',
    ONLYDOGE_S3_SECRET_ACCESS_KEY: 'spaces-secret',
    ONLYDOGE_SSH_JUMP: 'root@203.0.113.10',
    ONLYDOGE_SSH_TARGET: 'root@10.0.0.10',
    ONLYDOGE_STORAGE: 'https://sfo3.digitaloceanspaces.com/bucket/storage',
    ONLYDOGE_WAREHOUSE: 'http://clickhouse:8123?database=onlydoge',
    ONLYDOGE_WAREHOUSE_PASSWORD: 'clickhouse-secret',
    ONLYDOGE_WAREHOUSE_USER: 'onlydoge',
  };
}
