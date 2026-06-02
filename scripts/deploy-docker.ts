#!/usr/bin/env bun

import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { parseArgs } from 'node:util';
import {
  createDeployConfig,
  DEFAULT_ENV_FILE,
  findLegacyOnceContainers,
  formatLegacyOnceContainerError,
  isForwardedRuntimeEnvKey,
  loadDeployEnvFile,
  resolveImage,
  runCommand,
} from './deploy-config';

interface DeployPlan {
  envFile: string;
  envValues: Record<string, string>;
  host: string;
  remoteDir: string;
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
    },
    host: {
      type: 'string',
    },
    image: {
      type: 'string',
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
  const envFile = values.envFile ?? DEFAULT_ENV_FILE;
  const fileValues = await loadDeployEnvFile(envFile);
  const config = createDeployConfig({
    envFile,
    fileValues,
    host: values.host,
    image: values.image,
    remoteDir: values.remoteDir,
    sshJump: values.sshJump,
    sshTarget: values.sshTarget,
  });
  const resolvedImage = await resolveImage(config.image);
  const envValues = {
    ...config.envValues,
    ONLYDOGE_IMAGE: resolvedImage,
  };

  return {
    envFile,
    envValues,
    host: config.host,
    remoteDir: config.remoteDir,
    resolvedImage,
    sshJump: config.sshJump,
    sshTarget: config.sshTarget,
  };
}

async function runDeployPlan(plan: DeployPlan): Promise<void> {
  await runRemotePreflight(plan);

  const tempRoot = await mkdtemp(join(tmpdir(), 'onlydoge-deploy-'));
  try {
    const envValues = values.importRunningEnv
      ? {
          ...(await readRunningOnlyDogeEnv(plan).catch(() => ({}))),
          ...plan.envValues,
        }
      : plan.envValues;
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
    await reportIndexerStats(plan);
    console.log(`deployed ${plan.resolvedImage} to ${plan.host}`);
  } finally {
    await rm(tempRoot, { recursive: true, force: true });
  }
}

async function runRemotePreflight(plan: DeployPlan): Promise<void> {
  const output = await runSsh(plan, buildRemotePreflightCommand(plan));
  printNonEmptyOutput(output);

  const legacyContainers = findLegacyOnceContainers(readPreflightContainerNames(output));
  assertNoLegacyOnceContainers(legacyContainers);
}

async function readRunningOnlyDogeEnv(plan: DeployPlan): Promise<Record<string, string>> {
  const output = await runSsh(
    plan,
    `${runningIndexerContainerCommand()}; if [ -n "$name" ]; then docker exec "$name" env -0; fi`,
  );
  return Object.fromEntries(output.split('\0').map(readRuntimeEnvEntry).filter(isRuntimeEnvEntry));
}

function printNonEmptyOutput(output: string): void {
  const trimmed = output.trim();
  if (trimmed) {
    console.log(trimmed);
  }
}

function assertNoLegacyOnceContainers(legacyContainers: string[]): void {
  if (legacyContainers.length > 0) {
    throw new Error(formatLegacyOnceContainerError(legacyContainers));
  }
}

function readRuntimeEnvEntry(line: string): [string, string] | null {
  const separator = line.indexOf('=');
  if (separator <= 0) {
    return null;
  }

  return runtimeEnvEntry(line, separator);
}

function runtimeEnvEntry(line: string, separator: number): [string, string] | null {
  const key = line.slice(0, separator);
  if (!isForwardedRuntimeEnvKey(key)) {
    return null;
  }

  return [key, line.slice(separator + 1)];
}

function isRuntimeEnvEntry(entry: [string, string] | null): entry is [string, string] {
  return entry !== null;
}

function buildRemotePreflightCommand(plan: DeployPlan): string {
  return [
    'set -eu',
    `remote_dir=${shellEscape(plan.remoteDir)}`,
    'if [ -d "$remote_dir" ]; then echo "[deploy:preflight] remote_dir=present $remote_dir"; else echo "[deploy:preflight] remote_dir=missing $remote_dir"; fi',
    'if [ -f "$remote_dir/docker-compose.managed.yml" ]; then echo "[deploy:preflight] managed_compose=present"; else echo "[deploy:preflight] managed_compose=missing"; fi',
    `docker ps --format ${shellEscape('container={{.Names}}')}`,
  ].join('\n');
}

function readPreflightContainerNames(output: string): string[] {
  return output
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.startsWith('container='))
    .map((line) => line.slice('container='.length))
    .filter(Boolean);
}

function runningIndexerContainerCommand(): string {
  return "name=$(docker ps --format '{{.Names}}' | grep -E '^(onlydoge-onlydoge-indexer-1|onlydoge-onlydoge-api-1)' | head -n1)";
}

function buildRemoteDeployCommand(plan: DeployPlan): string {
  const compose = 'docker compose --env-file .env -f docker-compose.managed.yml';

  return [
    'set -eu',
    `cd ${shellEscape(plan.remoteDir)}`,
    `${compose} pull`,
    `${compose} up -d --remove-orphans`,
    `${compose} ps`,
  ].join('; ');
}

async function waitForIndexerHealth(plan: DeployPlan): Promise<void> {
  await waitUntil(
    Date.now(),
    180_000,
    () => checkIndexerHealth(plan),
    5_000,
    'Timed out waiting for onlydoge-indexer health',
  );
}

async function checkIndexerHealth(plan: DeployPlan): Promise<void> {
  await runSsh(
    plan,
    `cd ${shellEscape(plan.remoteDir)}; docker compose --env-file .env -f docker-compose.managed.yml exec -T onlydoge-indexer bun run scripts/indexer-health.ts`,
  );
}

async function reportIndexerStats(plan: DeployPlan): Promise<void> {
  const output = await runSsh(plan, buildIndexerStatsCommand(plan));
  if (output.trim()) {
    console.log(output.trim());
  }
}

function buildIndexerStatsCommand(plan: DeployPlan): string {
  const compose = 'docker compose --env-file .env -f docker-compose.managed.yml';
  const script = `
const { createRuntime } = await import("@onlydoge/platform");
const runtime = await createRuntime({ mode: "http" });
const state = await runtime.metadata.getCoreIndexerState();
const blockHeight = await runtime.metadata.getJsonValue("block_height");
const factTail = await runtime.metadata.getJsonValue("dogecoin_analytics_facts_tail");
const onlineTip = typeof state?.onlineTip === "number" ? state.onlineTip : blockHeight;
const processTail = state?.processTail;
const freshness =
  typeof onlineTip === "number" && typeof processTail === "number"
    ? onlineTip - processTail
    : "unknown";
const lastError = state?.lastError == null ? "null" : JSON.stringify(String(state.lastError));
console.log(
  [
    "[deploy:stats]",
    "Dogecoin",
    "stage=" + String(state?.stage ?? "unknown"),
    "blockHeight=" + numberOrUnknown(blockHeight),
    "processTail=" + numberOrUnknown(processTail),
    "factTail=" + numberOrUnknown(factTail),
    "freshnessBlocks=" + freshness,
    "lastError=" + lastError,
  ].join(" "),
);
process.exit(0);

function numberOrUnknown(value) {
  return typeof value === "number" ? value : "unknown";
}
`;

  return `cd ${shellEscape(plan.remoteDir)}; ${compose} exec -T onlydoge-api bun -e ${shellEscape(
    script,
  )}`;
}

async function verifyHealth(host: string): Promise<void> {
  await waitForStatus(`https://${host}/up`, 200, 120_000);
  await waitForStatus(`https://${host}/v1/heartbeat`, 204, 120_000);
  await waitForStatus(`https://${host}/openapi/json`, 200, 120_000);
}

async function waitForStatus(url: string, expectedStatus: number, timeoutMs: number) {
  await waitUntil(
    Date.now(),
    timeoutMs,
    () => checkHttpStatus(url, expectedStatus),
    2_000,
    `Timed out waiting for ${url} to return ${expectedStatus}`,
  );
}

async function checkHttpStatus(url: string, expectedStatus: number): Promise<void> {
  const response = await fetch(url, {
    signal: AbortSignal.timeout(5_000),
  });
  if (response.status !== expectedStatus) {
    throw new Error(`unexpected status ${response.status}`);
  }
}

async function waitUntil(
  startedAt: number,
  timeoutMs: number,
  check: () => Promise<void>,
  sleepMs: number,
  timeoutMessage: string,
): Promise<void> {
  assertWaitNotTimedOut(startedAt, timeoutMs, timeoutMessage);
  if (await didCheckPass(check)) {
    return;
  }

  await Bun.sleep(sleepMs);
  await waitUntil(startedAt, timeoutMs, check, sleepMs, timeoutMessage);
}

function assertWaitNotTimedOut(startedAt: number, timeoutMs: number, message: string): void {
  if (Date.now() - startedAt >= timeoutMs) {
    throw new Error(message);
  }
}

async function didCheckPass(check: () => Promise<void>): Promise<boolean> {
  try {
    await check();
    return true;
  } catch {
    return false;
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

async function runSsh(plan: DeployPlan, command: string): Promise<string> {
  return runCommand('ssh', [
    '-o',
    'BatchMode=yes',
    '-o',
    'StrictHostKeyChecking=accept-new',
    ...sshJumpArgs(plan.sshJump),
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
    ...sshJumpArgs(plan.sshJump),
    source,
    `${plan.sshTarget}:${target}`,
  ]);
}

function sshJumpArgs(sshJump: string): string[] {
  return sshJump.trim() ? ['-J', sshJump] : [];
}

function printDeployPlan(plan: DeployPlan): void {
  console.log(`env file: ${plan.envFile}`);
  console.log(`host: ${plan.host}`);
  console.log(`remote dir: ${plan.remoteDir}`);
  console.log(`ssh jump: ${plan.sshJump || '(none)'}`);
  console.log(`ssh target: ${plan.sshTarget}`);
  console.log(`image: ${plan.resolvedImage}`);
  console.log(`env keys: ${Object.keys(plan.envValues).sort().join(', ')}`);
  console.log('checks: remote preflight, public health, indexer health, stats summary');
}

function shellEscape(value: string): string {
  return `'${value.replace(/'/gu, `'\\''`)}'`;
}

void main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
