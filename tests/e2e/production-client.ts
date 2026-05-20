import { execFileSync } from 'node:child_process';

type Env = Record<string, string | undefined>;

export type ProductionE2EConfig =
  | {
      enabled: false;
    }
  | {
      adminToken: string;
      baseUrl: string;
      enabled: true;
      expectedImageDigest: string;
      ssh?: {
        jump?: string;
        target: string;
      };
    };

export interface ProductionResponse {
  body: unknown;
  headers: Headers;
  status: number;
  text: string;
}

export interface ProductionRequestOptions {
  body?: unknown;
  expectedStatus?: number | number[];
  method?: string;
  token?: string;
}

export class ProductionHttpError extends Error {
  public constructor(
    message: string,
    public readonly status: number,
    public readonly responseBody: unknown,
  ) {
    super(message);
    this.name = 'ProductionHttpError';
  }
}

export class ProductionApiClient {
  public constructor(private readonly config: Extract<ProductionE2EConfig, { enabled: true }>) {}

  public get(path: string, options: Omit<ProductionRequestOptions, 'method'> = {}) {
    return this.request(path, { ...options, method: 'GET' });
  }

  public post(path: string, options: Omit<ProductionRequestOptions, 'method'> = {}) {
    return this.request(path, { ...options, method: 'POST' });
  }

  public delete(path: string, options: Omit<ProductionRequestOptions, 'method'> = {}) {
    return this.request(path, { ...options, method: 'DELETE' });
  }

  public async request(
    path: string,
    options: ProductionRequestOptions = {},
  ): Promise<ProductionResponse> {
    const method = options.method ?? 'GET';
    const url = new URL(path, `${this.config.baseUrl}/`);
    const headers = new Headers({ accept: 'application/json, text/plain;q=0.9' });

    if (options.token) {
      headers.set('x-api-token', options.token);
    }
    if (options.body !== undefined) {
      headers.set('content-type', 'application/json');
    }

    const response = await fetch(url, {
      method,
      headers,
      ...(options.body === undefined ? {} : { body: JSON.stringify(options.body) }),
      signal: AbortSignal.timeout(15_000),
    });
    const text = await response.text();
    const body = parseResponseBody(text);
    const expected = normalizeExpectedStatus(options.expectedStatus);

    if (!expected.includes(response.status)) {
      throw new ProductionHttpError(
        `${method} ${url.pathname}${url.search} returned ${response.status}; expected ${expected.join(
          ' or ',
        )}; body=${redactSecrets(text, [this.config.adminToken, options.token])}`,
        response.status,
        body,
      );
    }

    return {
      body,
      headers: response.headers,
      status: response.status,
      text,
    };
  }
}

export class TeardownStack {
  private readonly steps: Array<{ label: string; run: () => Promise<void> }> = [];

  public add(label: string, run: () => Promise<void>): void {
    this.steps.unshift({ label, run });
  }

  public async run(): Promise<void> {
    const errors: Error[] = [];
    for (const step of this.steps) {
      try {
        await step.run();
      } catch (error) {
        errors.push(
          error instanceof Error
            ? new Error(`${step.label}: ${error.message}`)
            : new Error(step.label),
        );
      }
    }

    if (errors.length > 0) {
      throw new AggregateError(errors, `production E2E teardown failed (${errors.length} step(s))`);
    }
  }
}

export function loadProductionE2EConfig(env: Env = process.env): ProductionE2EConfig {
  if (env.ONLYDOGE_RUN_PRODUCTION_E2E !== '1') {
    return { enabled: false };
  }

  const missing = ['PROD_BASE_URL', 'PROD_ADMIN_API_TOKEN', 'EXPECTED_IMAGE_DIGEST'].filter(
    (key) => !env[key]?.trim(),
  );
  if (missing.length > 0) {
    throw new Error(`Missing required production E2E env vars: ${missing.join(', ')}`);
  }

  const ssh = env.PROD_SSH_TARGET?.trim()
    ? {
        ...(env.PROD_SSH_JUMP?.trim() ? { jump: env.PROD_SSH_JUMP.trim() } : {}),
        target: env.PROD_SSH_TARGET.trim(),
      }
    : undefined;

  return {
    adminToken: requireEnv(env, 'PROD_ADMIN_API_TOKEN'),
    baseUrl: normalizeBaseUrl(requireEnv(env, 'PROD_BASE_URL')),
    enabled: true,
    expectedImageDigest: normalizeDigest(requireEnv(env, 'EXPECTED_IMAGE_DIGEST')),
    ...(ssh ? { ssh } : {}),
  };
}

export function productionQueryPath(
  pathname: string,
  query: Record<string, number | string | undefined>,
): string {
  const url = new URL(pathname, 'http://onlydoge.test');
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined) {
      url.searchParams.set(key, String(value));
    }
  }

  return `${url.pathname}${url.search}`;
}

export function assertRecord(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`expected object for ${label}`);
  }

  return Object.fromEntries(Object.entries(value));
}

export function readRecordField(
  record: Record<string, unknown>,
  field: string,
): Record<string, unknown> {
  return assertRecord(record[field], field);
}

export function readRecordArrayField(
  record: Record<string, unknown>,
  field: string,
): Array<Record<string, unknown>> {
  return assertRecordArray(record[field], field);
}

export function assertRecordArray(value: unknown, label: string): Array<Record<string, unknown>> {
  if (!Array.isArray(value)) {
    throw new TypeError(`expected array for ${label}`);
  }

  return value.map((item, index) => assertRecord(item, `${label}[${index}]`));
}

export function readStringArrayField(record: Record<string, unknown>, field: string): string[] {
  const value = record[field];
  if (!Array.isArray(value) || value.some((item) => typeof item !== 'string')) {
    throw new TypeError(`expected string array for ${field}`);
  }

  return value;
}

export function readStringField(record: Record<string, unknown>, field: string): string {
  const value = record[field];
  if (typeof value !== 'string') {
    throw new TypeError(`expected string for ${field}`);
  }

  return value;
}

export function readOptionalStringField(
  record: Record<string, unknown>,
  field: string,
): string | undefined {
  const value = record[field];
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value !== 'string') {
    throw new TypeError(`expected string for ${field}`);
  }

  return value;
}

export function readNumberField(record: Record<string, unknown>, field: string): number {
  const value = record[field];
  if (typeof value !== 'number') {
    throw new TypeError(`expected number for ${field}`);
  }

  return value;
}

export function readOptionalNumberField(
  record: Record<string, unknown>,
  field: string,
): number | undefined {
  const value = record[field];
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value !== 'number') {
    throw new TypeError(`expected number for ${field}`);
  }

  return value;
}

export async function ignoreNotFound(work: () => Promise<void>): Promise<void> {
  try {
    await work();
  } catch (error) {
    if (error instanceof ProductionHttpError && error.status === 404) {
      return;
    }
    throw error;
  }
}

export async function eventually<T>(
  label: string,
  work: () => Promise<T>,
  options: { intervalMs?: number; timeoutMs: number },
): Promise<T> {
  const startedAt = Date.now();
  let lastError: unknown;

  while (Date.now() - startedAt < options.timeoutMs) {
    try {
      return await work();
    } catch (error) {
      lastError = error;
      await sleep(options.intervalMs ?? 2_000);
    }
  }

  const suffix = lastError instanceof Error ? `: ${lastError.message}` : '';
  throw new Error(`${label} did not pass within ${options.timeoutMs}ms${suffix}`);
}

export function verifyProductionImageDigest(
  config: Extract<ProductionE2EConfig, { enabled: true }>,
): { checked: false; reason: string } | { checked: true; containers: string[] } {
  if (!config.ssh) {
    return {
      checked: false,
      reason: 'PROD_SSH_TARGET not set; host container digest check skipped',
    };
  }

  const output = runSsh(
    config,
    "docker inspect onlydoge-onlydoge-api-1 onlydoge-onlydoge-indexer-1 --format '{{json .}}'",
  );
  const containers = output
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => parseDockerInspect(line));

  if (containers.length !== 2) {
    throw new Error(`expected 2 onlydoge containers, found ${containers.length}`);
  }

  for (const container of containers) {
    assertContainerHealthy(container);
    assertContainerDigest(container, config.expectedImageDigest);
  }

  return {
    checked: true,
    containers: containers.map((container) => container.Name ?? 'unknown'),
  };
}

function normalizeExpectedStatus(value: number | number[] | undefined): number[] {
  if (Array.isArray(value)) {
    return value;
  }

  return [value ?? 200];
}

function parseResponseBody(text: string): unknown {
  const trimmed = text.trim();
  if (!trimmed) {
    return null;
  }

  try {
    return JSON.parse(trimmed);
  } catch {
    return text;
  }
}

function requireEnv(env: Env, key: string): string {
  const value = env[key]?.trim();
  if (!value) {
    throw new Error(`Missing required production E2E env var: ${key}`);
  }

  return value;
}

function normalizeBaseUrl(value: string): string {
  return value.replace(/\/+$/u, '');
}

function normalizeDigest(value: string): string {
  return value.match(/sha256:[a-fA-F0-9]{64}/u)?.[0].toLowerCase() ?? value.trim();
}

function redactSecrets(value: string, tokens: Array<string | undefined>): string {
  return tokens
    .filter((token): token is string => Boolean(token))
    .reduce(
      (current, token) => current.replaceAll(token, '[redacted]'),
      value.replace(/sk_[A-Za-z0-9]+/gu, 'sk_[redacted]'),
    );
}

function runSsh(config: Extract<ProductionE2EConfig, { enabled: true }>, command: string): string {
  if (!config.ssh) {
    throw new Error('missing SSH target');
  }

  const args = [
    '-o',
    'BatchMode=yes',
    '-o',
    'StrictHostKeyChecking=accept-new',
    '-o',
    'ConnectTimeout=10',
    ...(config.ssh.jump ? ['-J', config.ssh.jump] : []),
    config.ssh.target,
    `sh -lc ${shellEscape(command)}`,
  ];

  try {
    return execFileSync('ssh', args, {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      timeout: 30_000,
    });
  } catch (error) {
    throw new Error(
      redactSecrets(error instanceof Error ? error.message : String(error), [config.adminToken]),
    );
  }
}

type DockerInspect = {
  Config?: {
    Image?: string;
  };
  Image?: string;
  Name?: string;
  RepoDigests?: string[];
  State?: {
    Health?: {
      Status?: string;
    };
    Status?: string;
  };
};

function parseDockerInspect(line: string): DockerInspect {
  return assertRecord(JSON.parse(line), 'docker inspect') as DockerInspect;
}

function assertContainerHealthy(container: DockerInspect): void {
  const status = container.State?.Health?.Status ?? container.State?.Status;
  if (status !== 'healthy' && status !== 'running') {
    throw new Error(`${container.Name ?? 'container'} status is ${status ?? 'unknown'}`);
  }
}

function assertContainerDigest(container: DockerInspect, expectedDigest: string): void {
  const references = [
    container.Config?.Image,
    container.Image,
    ...(container.RepoDigests ?? []),
  ].filter((value): value is string => Boolean(value));
  const digests = references.map(normalizeDigest);

  if (!digests.includes(expectedDigest)) {
    throw new Error(
      `${container.Name ?? 'container'} digest mismatch: expected ${expectedDigest}, observed ${
        digests.join(', ') || 'none'
      }`,
    );
  }
}

function shellEscape(value: string): string {
  return `'${value.replace(/'/gu, `'\\''`)}'`;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
