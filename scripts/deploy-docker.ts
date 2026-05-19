#!/usr/bin/env bun

import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { parseArgs } from 'node:util';
import { parse as parseDotenv } from 'dotenv';

const DEFAULT_IMAGE = 'ghcr.io/simonbetton/onlydoge-indexer:latest';
const DEFAULT_ENV_FILE = '.env.managed';
const DEFAULT_FALLBACK_ENV_FILE = '.env.once';
const DEFAULT_HOST = 'api.example.com';
const DEFAULT_REMOTE_DIR = '/opt/onlydoge';
const DEFAULT_SSH_JUMP = 'root@203.0.113.10';
const DEFAULT_SSH_TARGET = 'root@10.0.0.10';

const FORWARDED_ENV_KEYS = new Set(['SECRET_KEY_BASE', 'VAPID_PRIVATE_KEY', 'VAPID_PUBLIC_KEY']);

const REQUIRED_ENV_KEYS = [
  'SECRET_KEY_BASE',
  'VAPID_PUBLIC_KEY',
  'VAPID_PRIVATE_KEY',
  'ONLYDOGE_DATABASE',
  'ONLYDOGE_STORAGE',
  'ONLYDOGE_S3_ACCESS_KEY_ID',
  'ONLYDOGE_S3_SECRET_ACCESS_KEY',
  'ONLYDOGE_WAREHOUSE',
  'ONLYDOGE_WAREHOUSE_USER',
  'ONLYDOGE_WAREHOUSE_PASSWORD',
  'ONLYDOGE_INDEXER_LEASE_HEARTBEAT_INTERVAL_MS',
  'ONLYDOGE_INDEXER_BOOTSTRAP_TIMEOUT_MS',
  'ONLYDOGE_INDEXER_NETWORK_CONCURRENCY',
  'ONLYDOGE_INDEXER_SYNC_BACKLOG_HIGH_WATERMARK',
  'ONLYDOGE_INDEXER_SYNC_BACKLOG_LOW_WATERMARK',
  'ONLYDOGE_INDEXER_SYNC_WINDOW',
  'ONLYDOGE_INDEXER_SYNC_WINDOW_MIN',
  'ONLYDOGE_INDEXER_SYNC_WINDOW_MAX',
  'ONLYDOGE_INDEXER_SYNC_CONCURRENCY',
  'ONLYDOGE_INDEXER_SYNC_TARGET_MS',
  'ONLYDOGE_INDEXER_SYNC_TIMEOUT_MS',
  'ONLYDOGE_INDEXER_FACT_WINDOW',
  'ONLYDOGE_INDEXER_FACT_TIMEOUT_MS',
  'ONLYDOGE_CORE_BLOCK_TIMEOUT_MS',
  'ONLYDOGE_CORE_DB_STATEMENT_TIMEOUT_MS',
  'ONLYDOGE_CORE_SYNC_COMPLETE_DISTANCE',
  'ONLYDOGE_CORE_PROCESS_LOAD_CONCURRENCY',
  'ONLYDOGE_CORE_PROCESS_WINDOW',
  'ONLYDOGE_CORE_PROGRESS_WATCHDOG_MS',
  'ONLYDOGE_CORE_RAW_STORAGE_TIMEOUT_MS',
  'ONLYDOGE_CORE_ONLINE_TIP_DISTANCE',
  'ONLYDOGE_INDEXER_PROJECT_WINDOW',
  'ONLYDOGE_INDEXER_PROJECT_WINDOW_MIN',
  'ONLYDOGE_INDEXER_PROJECT_WINDOW_MAX',
  'ONLYDOGE_INDEXER_PROJECT_TARGET_MS',
  'ONLYDOGE_INDEXER_PROJECT_TIMEOUT_MS',
  'ONLYDOGE_INDEXER_RELINK_BACKLOG_THRESHOLD',
  'ONLYDOGE_INDEXER_RELINK_TIP_DISTANCE',
  'ONLYDOGE_INDEXER_RELINK_BATCH_SIZE',
  'ONLYDOGE_INDEXER_RELINK_CONCURRENCY',
  'ONLYDOGE_INDEXER_RELINK_FRONTIER_BATCH',
  'ONLYDOGE_INDEXER_RELINK_TIMEOUT_MS',
] as const;

interface DeployPlan {
  envFile: string;
  envValues: Record<string, string>;
  host: string;
  remoteDir: string;
  resolvedImage: string;
  sshJump: string;
  sshTarget: string;
  stopOnce: boolean;
}

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
    host: {
      type: 'string',
    },
    image: {
      type: 'string',
      default: DEFAULT_IMAGE,
    },
    remoteDir: {
      type: 'string',
    },
    sshJump: {
      type: 'string',
    },
    sshTarget: {
      type: 'string',
    },
    stopOnce: {
      type: 'boolean',
      default: true,
    },
    importRunningEnv: {
      type: 'boolean',
      default: true,
    },
  },
  strict: true,
  allowPositionals: false,
});

async function main() {
  const plan = await createDeployPlan();
  if (values.dryRun) {
    printDeployPlan(plan);
    return;
  }

  await runDeployPlan(plan);
}

async function createDeployPlan(): Promise<DeployPlan> {
  const envFile =
    values.envFile ??
    ((await fileExists(DEFAULT_ENV_FILE)) ? DEFAULT_ENV_FILE : DEFAULT_FALLBACK_ENV_FILE);
  const fileValues = await loadEnvFile(envFile);
  const host = resolveDeployValue(values.host, fileValues.ONLYDOGE_PUBLIC_HOST, DEFAULT_HOST);
  const sshJump = resolveDeployValue(values.sshJump, fileValues.ONCE_SSH_JUMP, DEFAULT_SSH_JUMP);
  const sshTarget = resolveDeployValue(
    values.sshTarget,
    fileValues.ONCE_SSH_TARGET,
    DEFAULT_SSH_TARGET,
  );
  const remoteDir = resolveDeployValue(
    values.remoteDir,
    fileValues.ONLYDOGE_REMOTE_DIR,
    DEFAULT_REMOTE_DIR,
  );
  const resolvedImage = await resolveImage(
    resolveDeployValue(values.image, fileValues.ONLYDOGE_IMAGE, DEFAULT_IMAGE),
  );
  const envValues = {
    ...collectForwardedEnv(fileValues),
    ONLYDOGE_IMAGE: resolvedImage,
  };
  validateRequiredEnv(envValues);

  return {
    envFile,
    envValues,
    host,
    remoteDir,
    resolvedImage,
    sshJump,
    sshTarget,
    stopOnce: values.stopOnce ?? true,
  };
}

async function runDeployPlan(plan: DeployPlan): Promise<void> {
  const tempRoot = await mkdtemp(join(tmpdir(), 'onlydoge-deploy-'));
  try {
    const envValues = values.importRunningEnv
      ? {
          ...(await readRunningOnlyDogeEnv(plan).catch(() => ({}))),
          ...plan.envValues,
        }
      : plan.envValues;
    if (values.importRunningEnv && !envValues.ONLYDOGE_DATABASE_SSLROOTCERT_PEM) {
      const ca = await readRunningDatabaseCa(plan).catch(() => '');
      if (ca.trim()) {
        envValues.ONLYDOGE_DATABASE_SSLROOTCERT_PEM = ca;
      }
    }
    const envPath = join(tempRoot, '.env');
    await writeFile(envPath, formatEnvFile(envValues));
    await runSsh(plan, `mkdir -p ${shellEscape(`${plan.remoteDir}/docker/caddy`)}`);
    await runScp(
      plan,
      'docker-compose.managed.yml',
      `${plan.remoteDir}/docker-compose.managed.yml`,
    );
    await runScp(plan, 'docker/caddy/Caddyfile', `${plan.remoteDir}/docker/caddy/Caddyfile`);
    await runScp(plan, envPath, `${plan.remoteDir}/.env`);
    await runSsh(plan, buildRemoteDeployCommand(plan));
    await verifyHealth(plan.host);
    await waitForIndexerHealth(plan);
    console.log(`deployed ${plan.resolvedImage} to ${plan.host}`);
  } finally {
    await rm(tempRoot, { recursive: true, force: true });
  }
}

async function readRunningDatabaseCa(plan: DeployPlan): Promise<string> {
  return runSsh(
    plan,
    `${runningIndexerContainerCommand()}; if [ -n "$name" ]; then docker exec "$name" sh -lc 'if [ -f /storage/do-ca.pem ]; then cat /storage/do-ca.pem; fi'; fi`,
  );
}

async function readRunningOnlyDogeEnv(plan: DeployPlan): Promise<Record<string, string>> {
  const output = await runSsh(
    plan,
    `${runningIndexerContainerCommand()}; if [ -n "$name" ]; then docker exec "$name" env -0; fi`,
  );
  const env: Record<string, string> = {};
  for (const line of output.split('\0')) {
    const separator = line.indexOf('=');
    if (separator <= 0) {
      continue;
    }
    const key = line.slice(0, separator);
    if (!key.startsWith('ONLYDOGE_') && !FORWARDED_ENV_KEYS.has(key)) {
      continue;
    }
    env[key] = line.slice(separator + 1);
  }
  return env;
}

function runningIndexerContainerCommand(): string {
  return "name=$(docker ps --format '{{.Names}}' | grep -E '^(onlydoge-onlydoge-indexer-1|onlydoge-onlydoge-api-1|once-app-onlydoge-indexer|once-app-onlydoge-api)' | head -n1)";
}

function buildRemoteDeployCommand(plan: DeployPlan): string {
  const compose = 'docker compose --env-file .env -f docker-compose.managed.yml';
  const stopOnce = plan.stopOnce
    ? "docker ps --format '{{.Names}}' | grep -E '^(once-app-onlydoge-indexer|once-proxy)' | xargs -r docker stop"
    : 'true';

  return [
    'set -eu',
    `cd ${shellEscape(plan.remoteDir)}`,
    stopOnce,
    `${compose} pull`,
    `${compose} up -d --remove-orphans`,
    `${compose} ps`,
  ].join('; ');
}

async function waitForIndexerHealth(plan: DeployPlan): Promise<void> {
  const startedAt = Date.now();
  const timeoutMs = 180_000;
  while (Date.now() - startedAt < timeoutMs) {
    try {
      await runSsh(
        plan,
        `cd ${shellEscape(plan.remoteDir)}; docker compose --env-file .env -f docker-compose.managed.yml exec -T onlydoge-indexer bun run scripts/indexer-health.ts`,
      );
      return;
    } catch {}

    await Bun.sleep(5_000);
  }

  throw new Error('Timed out waiting for onlydoge-indexer health');
}

async function verifyHealth(host: string): Promise<void> {
  await waitForStatus(`https://${host}/up`, 200, 120_000);
  await waitForStatus(`https://${host}/v1/heartbeat`, 204, 120_000);
}

async function waitForStatus(url: string, expectedStatus: number, timeoutMs: number) {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(url, {
        signal: AbortSignal.timeout(5_000),
      });
      if (response.status === expectedStatus) {
        return;
      }
    } catch {}

    await Bun.sleep(2_000);
  }

  throw new Error(`Timed out waiting for ${url} to return ${expectedStatus}`);
}

async function loadEnvFile(path: string): Promise<Record<string, string>> {
  const raw = await readFile(path, 'utf8');
  return parseDotenv(raw);
}

function collectForwardedEnv(values: Record<string, string>): Record<string, string> {
  const forwarded: Record<string, string> = {};
  for (const [key, value] of Object.entries(values)) {
    if (key.startsWith('ONLYDOGE_') || FORWARDED_ENV_KEYS.has(key)) {
      forwarded[key] = value;
    }
  }
  return forwarded;
}

function validateRequiredEnv(env: Record<string, string>) {
  const missing = REQUIRED_ENV_KEYS.filter((key) => !env[key]);
  if (missing.length > 0) {
    throw new Error(`Missing required deploy env vars: ${missing.join(', ')}`);
  }
}

function formatEnvFile(env: Record<string, string>): string {
  return `${Object.keys(env)
    .sort()
    .map((key) => `${key}=${quoteEnvValue(env[key] ?? '')}`)
    .join('\n')}\n`;
}

function quoteEnvValue(value: string): string {
  if (/^[A-Za-z0-9_./:@?=&%+,~-]*$/u.test(value)) {
    return value;
  }

  return JSON.stringify(value);
}

function resolveDeployValue(
  explicit: string | undefined,
  envValue: string | undefined,
  fallback: string,
): string {
  return explicit ?? envValue ?? fallback;
}

async function resolveImage(reference: string): Promise<string> {
  if (reference.includes('@sha256:')) {
    return reference;
  }

  const output = await runCommand('docker', ['buildx', 'imagetools', 'inspect', reference]);
  const digest = output.match(/^Digest:\s+(sha256:[a-f0-9]+)$/mu)?.[1];
  if (!digest) {
    throw new Error(`Could not resolve image digest for ${reference}`);
  }

  return `${reference}@${digest}`.replace(/:[^/@]+@/u, '@');
}

async function fileExists(path: string): Promise<boolean> {
  try {
    await readFile(path);
    return true;
  } catch {
    return false;
  }
}

async function runSsh(plan: DeployPlan, command: string): Promise<string> {
  return runCommand('ssh', [
    '-o',
    'BatchMode=yes',
    '-J',
    plan.sshJump,
    plan.sshTarget,
    `sh -lc ${shellEscape(command)}`,
  ]);
}

async function runScp(plan: DeployPlan, source: string, target: string): Promise<void> {
  await runCommand('scp', [
    '-o',
    'BatchMode=yes',
    '-o',
    'StrictHostKeyChecking=accept-new',
    '-J',
    plan.sshJump,
    source,
    `${plan.sshTarget}:${target}`,
  ]);
}

async function runCommand(command: string, args: string[]): Promise<string> {
  const proc = Bun.spawn([command, ...args], {
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });

  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);

  if (exitCode !== 0) {
    throw new Error(`${command} failed with exit ${exitCode}\n${stderr || stdout}`);
  }

  return stdout;
}

function printDeployPlan(plan: DeployPlan): void {
  console.log(`env file: ${plan.envFile}`);
  console.log(`host: ${plan.host}`);
  console.log(`remote dir: ${plan.remoteDir}`);
  console.log(`ssh jump: ${plan.sshJump}`);
  console.log(`ssh target: ${plan.sshTarget}`);
  console.log(`image: ${plan.resolvedImage}`);
  console.log(`stop once: ${plan.stopOnce}`);
  console.log(`env keys: ${Object.keys(plan.envValues).sort().join(', ')}`);
}

function shellEscape(value: string): string {
  return `'${value.replace(/'/gu, `'\\''`)}'`;
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
