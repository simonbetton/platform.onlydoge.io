#!/usr/bin/env bun

import { readFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { parse as parseDotenv } from 'dotenv';

const DEFAULT_IMAGE = 'ghcr.io/simonbetton/onlydoge-indexer:latest';
const DEFAULT_ENV_FILE = '.env.once';
const DEFAULT_HOST = 'api.example.com';
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
  forwardedEnv: Record<string, string>;
  host: string;
  remoteArgs: string[];
  resolvedImage: string;
  sshJump: string;
  sshTarget: string;
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
      default: DEFAULT_ENV_FILE,
    },
    host: {
      type: 'string',
    },
    image: {
      type: 'string',
      default: DEFAULT_IMAGE,
    },
    sshJump: {
      type: 'string',
    },
    sshTarget: {
      type: 'string',
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
  const fileValues = await loadEnvFile(values.envFile);
  const host = resolveDeployValue(values.host, fileValues.ONCE_APP_HOST, DEFAULT_HOST);
  const sshJump = resolveDeployValue(values.sshJump, fileValues.ONCE_SSH_JUMP, DEFAULT_SSH_JUMP);
  const sshTarget = resolveDeployValue(
    values.sshTarget,
    fileValues.ONCE_SSH_TARGET,
    DEFAULT_SSH_TARGET,
  );
  const resolvedImage = await resolveImage(
    resolveDeployValue(values.image, undefined, DEFAULT_IMAGE),
  );
  const forwardedEnv = collectForwardedEnv(fileValues);
  validateRequiredEnv(forwardedEnv);

  return {
    envFile: values.envFile,
    forwardedEnv,
    host,
    remoteArgs: buildSshRemoteArgs(sshJump, sshTarget, host, resolvedImage, forwardedEnv),
    resolvedImage,
    sshJump,
    sshTarget,
  };
}

function resolveDeployValue(
  explicit: string | undefined,
  envValue: string | undefined,
  fallback: string,
): string {
  return explicit ?? envValue ?? fallback;
}

function buildSshRemoteArgs(
  sshJump: string,
  sshTarget: string,
  host: string,
  resolvedImage: string,
  forwardedEnv: Record<string, string>,
): string[] {
  return [
    '-o',
    'BatchMode=yes',
    '-J',
    sshJump,
    sshTarget,
    `sh -lc ${shellEscape(buildRemoteCommand(host, resolvedImage, forwardedEnv))}`,
  ];
}

function printDeployPlan(plan: DeployPlan): void {
  console.log(`env file: ${plan.envFile}`);
  console.log(`host: ${plan.host}`);
  console.log(`ssh jump: ${plan.sshJump}`);
  console.log(`ssh target: ${plan.sshTarget}`);
  console.log(`image: ${plan.resolvedImage}`);
  console.log(`env keys: ${Object.keys(plan.forwardedEnv).sort().join(', ')}`);
}

async function runDeployPlan(plan: DeployPlan): Promise<void> {
  await runCommand('ssh', plan.remoteArgs);
  await verifyHealth(plan.host);
  console.log(`deployed ${plan.resolvedImage} to ${plan.host}`);
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

function validateRequiredEnv(values: Record<string, string>) {
  const missing = REQUIRED_ENV_KEYS.filter((key) => !values[key]);
  if (missing.length > 0) {
    throw new Error(`Missing required deploy env vars: ${missing.join(', ')}`);
  }
}

function buildEnvArgs(values: Record<string, string>): string[] {
  const args: string[] = [];
  for (const key of Object.keys(values).sort()) {
    args.push('--env', `${key}=${values[key]}`);
  }

  return args;
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

function buildRemoteCommand(host: string, image: string, env: Record<string, string>): string {
  const envArgs = buildEnvArgs(env).map(shellEscape).join(' ');
  const quotedHost = shellEscape(host);
  const quotedImage = shellEscape(image);

  return `set -eu; if once list | grep -F ${quotedHost} >/dev/null 2>&1; then exec once update ${quotedHost} --image ${quotedImage} ${envArgs}; else exec once deploy ${quotedImage} --host ${quotedHost} ${envArgs}; fi`;
}

async function verifyHealth(host: string): Promise<void> {
  await waitForStatus(`https://${host}/up`, 200, 60_000);
  await waitForStatus(`https://${host}/v1/heartbeat`, 204, 60_000);
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

async function runCommand(command: string, args: string[], stdin?: string): Promise<string> {
  const proc = Bun.spawn([command, ...args], {
    stdin: stdin ? 'pipe' : 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });

  if (stdin) {
    proc.stdin.write(stdin);
    proc.stdin.end();
  }

  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);

  if (exitCode !== 0) {
    throw new Error([stdout, stderr].filter(Boolean).join('\n').trim());
  }

  return stdout.trim();
}

function shellEscape(value: string): string {
  return `'${value.replaceAll("'", `'\\''`)}'`;
}

await main();
