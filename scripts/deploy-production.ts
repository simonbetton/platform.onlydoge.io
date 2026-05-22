#!/usr/bin/env bun

import { parseArgs } from 'node:util';
import {
  createDeployConfig,
  DEFAULT_ENV_FILE,
  loadDeployEnvFile,
  resolveImage,
  runCommand,
  validateProductionE2eEnv,
} from './deploy-config';

const { values } = parseArgs({
  args: Bun.argv.slice(2),
  options: {
    dryRun: {
      type: 'boolean',
      default: false,
    },
    envFile: {
      type: 'string',
    },
    skipE2e: {
      type: 'boolean',
      default: false,
    },
  },
  strict: true,
  allowPositionals: false,
});

async function main(): Promise<void> {
  const config = await loadProductionDeployConfig();
  const skipE2e = Boolean(values.skipE2e);

  validateProductionE2eEnv(process.env, { skipE2e });
  validateDeployProductionImage(config.image);

  if (values.dryRun) {
    printProductionDeployPlan(config, { skipE2e });
    return;
  }

  await runStep('quality checks', () => runCommand('bun', ['run', 'ci']));
  await runStep('build and push production image', () =>
    runCommand('bun', ['run', 'image:push'], {
      env: {
        ...process.env,
        ONLYDOGE_IMAGE: config.image,
      },
    }),
  );
  const resolvedImage = await runStep('resolve pushed image digest', () =>
    resolveImage(config.image),
  );
  await runStep('deploy production image', () =>
    runCommand('bun', [
      'run',
      'deploy:docker',
      '--',
      '--envFile',
      config.envFile,
      '--image',
      resolvedImage,
    ]),
  );

  await runProductionE2eIfNeeded(skipE2e, config, resolvedImage);

  console.log(`production deploy completed: ${resolvedImage}`);
}

async function loadProductionDeployConfig(): Promise<ReturnType<typeof createDeployConfig>> {
  const envFile = values.envFile ?? DEFAULT_ENV_FILE;
  return createDeployConfig({
    envFile,
    fileValues: await loadDeployEnvFile(envFile),
    host: undefined,
    image: undefined,
    remoteDir: undefined,
    sshJump: undefined,
    sshTarget: undefined,
  });
}

async function runStep<T>(label: string, work: () => Promise<T>): Promise<T> {
  console.log(`[deploy:production] ${label}`);
  return work();
}

async function runProductionE2eIfNeeded(
  skipE2e: boolean,
  config: ReturnType<typeof createDeployConfig>,
  resolvedImage: string,
): Promise<void> {
  if (skipE2e) {
    return;
  }

  await runStep('production E2E', () =>
    runCommand('bun', ['run', 'e2e:production'], {
      env: productionE2eEnv(config, resolvedImage),
    }),
  );
}

function validateDeployProductionImage(image: string): void {
  if (image.includes('@sha256:')) {
    throw new Error(
      'deploy:production builds and pushes an image tag. Use deploy:docker with --image for digest rollback deploys.',
    );
  }
}

function productionE2eEnv(
  config: ReturnType<typeof createDeployConfig>,
  resolvedImage: string,
): Record<string, string | undefined> {
  return {
    ...process.env,
    EXPECTED_IMAGE_DIGEST: imageDigest(resolvedImage),
    ONLYDOGE_RUN_PRODUCTION_E2E: '1',
    PROD_BASE_URL: optionalEnvOrDefault(process.env.PROD_BASE_URL, `https://${config.host}`),
    PROD_SSH_JUMP: optionalEnvOrDefault(process.env.PROD_SSH_JUMP, config.sshJump),
    PROD_SSH_TARGET: optionalEnvOrDefault(process.env.PROD_SSH_TARGET, config.sshTarget),
  };
}

function optionalEnvOrDefault(value: string | undefined, fallback: string): string {
  return nonEmptyText(value) ?? fallback;
}

function nonEmptyText(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}

function imageDigest(image: string): string {
  return image.match(/sha256:[a-fA-F0-9]{64}/u)?.[0].toLowerCase() ?? image;
}

function printProductionDeployPlan(
  config: ReturnType<typeof createDeployConfig>,
  options: { skipE2e: boolean },
): void {
  console.log(`env file: ${config.envFile}`);
  console.log(`host: ${config.host}`);
  console.log(`remote dir: ${config.remoteDir}`);
  console.log(`image tag: ${config.image}`);
  console.log('ssh target: configured');
  console.log(`ssh jump: ${configuredLabel(config.sshJump)}`);
  console.log(`production E2E: ${productionE2eLabel(options)}`);
  console.log(`steps: ${productionDeploySteps(options).join(', ')}`);
}

function configuredLabel(value: string): string {
  return value ? 'configured' : '(none)';
}

function productionE2eLabel(options: { skipE2e: boolean }): string {
  return options.skipE2e ? 'skipped' : 'required';
}

function productionDeploySteps(options: { skipE2e: boolean }): string[] {
  return ['ci', 'image push', 'digest deploy', 'health checks', 'stats summary'].concat(
    optionalProductionE2eStep(options),
  );
}

function optionalProductionE2eStep(options: { skipE2e: boolean }): string[] {
  return options.skipE2e ? [] : ['production E2E'];
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
