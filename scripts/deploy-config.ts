import { readFile } from 'node:fs/promises';
import { parse as parseDotenv } from 'dotenv';

export const DEFAULT_IMAGE = 'ghcr.io/simonbetton/onlydoge-indexer:latest';
export const DEFAULT_ENV_FILE = '.env.managed';
export const DEFAULT_REMOTE_DIR = '/opt/onlydoge';

const DEPLOY_ONLY_ENV_KEYS = new Set([
  'ONLYDOGE_PUBLIC_HOST',
  'ONLYDOGE_REMOTE_DIR',
  'ONLYDOGE_SSH_JUMP',
  'ONLYDOGE_SSH_TARGET',
]);
const REQUIRED_RUNTIME_ENV_KEYS = [
  'ONLYDOGE_DATABASE',
  'ONLYDOGE_STORAGE',
  'ONLYDOGE_S3_ACCESS_KEY_ID',
  'ONLYDOGE_S3_SECRET_ACCESS_KEY',
  'ONLYDOGE_WAREHOUSE',
  'ONLYDOGE_WAREHOUSE_USER',
  'ONLYDOGE_WAREHOUSE_PASSWORD',
] as const;

export interface DeployConfigInput {
  envFile: string;
  fileValues: Record<string, string>;
  host: string | undefined;
  image: string | undefined;
  remoteDir: string | undefined;
  sshJump: string | undefined;
  sshTarget: string | undefined;
}

export interface DeployConfig {
  envFile: string;
  envValues: Record<string, string>;
  host: string;
  image: string;
  remoteDir: string;
  sshJump: string;
  sshTarget: string;
}

export async function loadDeployEnvFile(path: string): Promise<Record<string, string>> {
  try {
    return parseDotenv(await readFile(path, 'utf8'));
  } catch (error) {
    throwDeployEnvFileError(path, error);
  }
}

export function createDeployConfig(input: DeployConfigInput): DeployConfig {
  const envValues = collectForwardedEnv(input.fileValues);
  validateRequiredRuntimeEnv(envValues);
  validateManagedDatabaseCa(envValues, input.envFile);
  const host = requireDeployValue(
    'ONLYDOGE_PUBLIC_HOST',
    input.host,
    input.fileValues.ONLYDOGE_PUBLIC_HOST,
    input.envFile,
  );

  return {
    envFile: input.envFile,
    envValues: {
      ...envValues,
      ONLYDOGE_PUBLIC_HOST: host,
    },
    host,
    image: resolveDeployValue(input.image, input.fileValues.ONLYDOGE_IMAGE, DEFAULT_IMAGE),
    remoteDir: resolveDeployValue(
      input.remoteDir,
      input.fileValues.ONLYDOGE_REMOTE_DIR,
      DEFAULT_REMOTE_DIR,
    ),
    sshJump: resolveDeployValue(input.sshJump, input.fileValues.ONLYDOGE_SSH_JUMP, ''),
    sshTarget: requireDeployValue(
      'ONLYDOGE_SSH_TARGET',
      input.sshTarget,
      input.fileValues.ONLYDOGE_SSH_TARGET,
      input.envFile,
    ),
  };
}

export function validateProductionE2eEnv(
  env: Record<string, string | undefined>,
  options: { skipE2e: boolean },
): void {
  if (options.skipE2e) {
    return;
  }

  requireProductionE2eToken(env.PROD_ADMIN_API_TOKEN);
}

export function isForwardedRuntimeEnvKey(key: string): boolean {
  return key.startsWith('ONLYDOGE_');
}

export function findLegacyOnceContainers(containerNames: string[]): string[] {
  return containerNames.filter((name) => name.startsWith('once-'));
}

export function formatLegacyOnceContainerError(containerNames: string[]): string {
  const names = containerNames.join(', ');
  const stopCommands = containerNames.map((name) => `docker stop ${name}`).join('\n');

  return [
    `Legacy once-managed containers are still running: ${names}.`,
    'They can hold ports 80/443 or compete for the indexer lease.',
    'Stop or remove them before running the managed deploy, for example:',
    stopCommands,
  ].join('\n');
}

export async function resolveImage(reference: string): Promise<string> {
  if (reference.includes('@sha256:')) {
    return reference;
  }

  const output = await runCommand('docker', ['buildx', 'imagetools', 'inspect', reference]);
  return resolvedImageDigestReference(reference, requireImageDigest(reference, output));
}

export async function runCommand(
  command: string,
  args: string[],
  options: { env?: Record<string, string | undefined> } = {},
): Promise<string> {
  const proc = Bun.spawn([command, ...args], {
    ...spawnEnvOptions(options.env),
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });

  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);

  assertCommandSucceeded(command, exitCode, stdout, stderr);

  return stdout;
}

export function resolveDeployValue(
  explicit: string | undefined,
  envValue: string | undefined,
  fallback: string,
): string {
  return firstNonBlank(explicit, envValue) ?? fallback;
}

function requireDeployValue(
  key: string,
  explicit: string | undefined,
  envValue: string | undefined,
  envFile: string,
): string {
  const value = firstNonBlank(explicit, envValue);
  if (!value) {
    throw new Error(
      `Missing deploy env var ${key}: add it to ${envFile} or pass the matching CLI flag.`,
    );
  }

  return value;
}

function collectForwardedEnv(values: Record<string, string>): Record<string, string> {
  const forwarded: Record<string, string> = {};
  for (const [key, value] of Object.entries(values)) {
    forwardRuntimeEnvEntry(forwarded, key, value);
  }
  return forwarded;
}

function validateRequiredRuntimeEnv(env: Record<string, string>) {
  const missing = REQUIRED_RUNTIME_ENV_KEYS.filter((key) => !env[key]?.trim());
  if (missing.length > 0) {
    throw new Error(`Missing required deploy env vars: ${missing.join(', ')}`);
  }
}

function validateManagedDatabaseCa(env: Record<string, string>, envFile: string): void {
  const sslRootCert = readManagedDatabaseSslRootCert(env.ONLYDOGE_DATABASE);
  if (!sslRootCert) {
    return;
  }

  rejectManagedStorageCaPath(sslRootCert, envFile);
  requireManagedDatabaseCaEnv(env, sslRootCert, envFile);
}

function readManagedDatabaseSslRootCert(database: string | undefined): string | null {
  const location = database?.trim();
  if (!location) {
    return null;
  }

  return readPostgresSslRootCertParam(location);
}

function rejectManagedStorageCaPath(sslRootCert: string, envFile: string): void {
  if (sslRootCert === '/storage/do-ca.pem') {
    throw new Error(
      `ONLYDOGE_DATABASE in ${envFile} must not reference sslrootcert=/storage/do-ca.pem. Managed production does not mount /storage; put the CA in ONLYDOGE_DATABASE_SSLROOTCERT_PEM or ONLYDOGE_DATABASE_SSLROOTCERT_BASE64 and remove sslrootcert from ONLYDOGE_DATABASE.`,
    );
  }
}

function requireManagedDatabaseCaEnv(
  env: Record<string, string>,
  sslRootCert: string,
  envFile: string,
): void {
  if (!hasDatabaseCaEnv(env)) {
    throw new Error(
      `ONLYDOGE_DATABASE in ${envFile} contains sslrootcert=${sslRootCert}. Managed deploys pass database CA material through ONLYDOGE_DATABASE_SSLROOTCERT_PEM or ONLYDOGE_DATABASE_SSLROOTCERT_BASE64; set one of those env vars and remove file-path CA assumptions.`,
    );
  }
}

function isPostgresUrl(location: string): boolean {
  return location.startsWith('postgres://') || location.startsWith('postgresql://');
}

function readSslRootCertParam(location: string): string | null {
  try {
    return trimNullable(new URL(location).searchParams.get('sslrootcert'));
  } catch {
    return null;
  }
}

function hasDatabaseCaEnv(env: Record<string, string>): boolean {
  return Boolean(
    env.ONLYDOGE_DATABASE_SSLROOTCERT_PEM?.trim() ||
      env.ONLYDOGE_DATABASE_SSLROOTCERT_BASE64?.trim(),
  );
}

function firstNonBlank(...values: Array<string | undefined>): string | null {
  const [value] = values.map(trimOptional).filter(isPresent);
  if (value) {
    return value;
  }

  return null;
}

function isFileNotFound(error: unknown): boolean {
  return Reflect.get(Object(error), 'code') === 'ENOENT';
}

function throwDeployEnvFileError(path: string, error: unknown): never {
  if (isFileNotFound(error)) {
    throw new Error(
      `Missing production deploy env file: ${path}. Create it with: cp .env.managed.example .env.managed. Keep the real file private.`,
    );
  }

  throw error;
}

function requireProductionE2eToken(token: string | undefined): void {
  if (!token?.trim()) {
    throw new Error('Missing PROD_ADMIN_API_TOKEN for production E2E. Set it or pass --skipE2e.');
  }
}

function requireImageDigest(reference: string, output: string): string {
  return requireImageDigestMatch(reference, output.match(/^Digest:\s+(sha256:[a-f0-9]+)$/mu));
}

function requireImageDigestMatch(reference: string, match: RegExpMatchArray | null): string {
  if (!match) {
    throw new Error(`Could not resolve image digest for ${reference}`);
  }

  return requireImageDigestCapture(reference, match[1]);
}

function requireImageDigestCapture(reference: string, digest: string | undefined): string {
  if (!digest) {
    throw new Error(`Could not resolve image digest for ${reference}`);
  }

  return digest;
}

function resolvedImageDigestReference(reference: string, digest: string): string {
  return `${reference}@${digest}`.replace(/:[^/@]+@/u, '@');
}

function spawnEnvOptions(env: Record<string, string | undefined> | undefined): {
  env?: Record<string, string | undefined>;
} {
  if (!env) {
    return {};
  }

  return { env };
}

function assertCommandSucceeded(
  command: string,
  exitCode: number,
  stdout: string,
  stderr: string,
): void {
  if (exitCode === 0) {
    return;
  }

  throw new Error(
    `${command} failed with exit ${exitCode}\n${commandFailureOutput(stdout, stderr)}`,
  );
}

function commandFailureOutput(stdout: string, stderr: string): string {
  if (stderr) {
    return stderr;
  }

  return stdout;
}

function forwardRuntimeEnvEntry(
  forwarded: Record<string, string>,
  key: string,
  value: string,
): void {
  if (shouldForwardRuntimeEnvKey(key)) {
    forwarded[key] = value;
  }
}

function shouldForwardRuntimeEnvKey(key: string): boolean {
  return ![DEPLOY_ONLY_ENV_KEYS.has(key), !isForwardedRuntimeEnvKey(key)].includes(true);
}

function readPostgresSslRootCertParam(location: string): string | null {
  if (!isPostgresUrl(location)) {
    return null;
  }

  return readSslRootCertParam(location);
}

function trimNullable(value: string | null): string | null {
  if (!value) {
    return null;
  }

  return trimOptional(value);
}

function trimOptional(value: string | undefined): string | null {
  if (!value) {
    return null;
  }

  return nullIfEmpty(value.trim());
}

function nullIfEmpty(value: string): string | null {
  if (value === '') {
    return null;
  }

  return value;
}

function isPresent(value: string | null): value is string {
  return value !== null;
}
