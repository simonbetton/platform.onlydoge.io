#!/usr/bin/env bun

import { Command } from 'commander';

interface CreatedApiKey {
  id: string;
  key: string;
}

interface NetworkResponse {
  architecture: 'dogecoin';
  blockTime: number;
  chainId: number;
  createdAt: string;
  id: string;
  name: string;
  rpcEndpoint: string;
  rps: number;
}

interface StatsResponse {
  networks: Array<{
    blockHeight: number;
    name: string;
    processed: number;
    synced: number;
  }>;
}

interface RunsheetInput {
  apiToken?: string;
  baseUrl: string;
  blockTime: number;
  chainId: number;
  name: string;
  rpcEndpoint: string;
  rps: number;
}

interface RunsheetCredential {
  apiToken: string;
  createdKey: CreatedApiKey | null;
}

function normalizeBaseUrl(value: string): string {
  return value.endsWith('/') ? value : `${value}/`;
}

function buildUrl(baseUrl: string, path: string): URL {
  return new URL(path.replace(/^\/+/u, ''), normalizeBaseUrl(baseUrl));
}

function buildHeaders(apiToken?: string, includeJson = false): Record<string, string> {
  return {
    ...(includeJson ? { 'content-type': 'application/json' } : {}),
    ...(apiToken ? { 'x-api-token': apiToken } : {}),
  };
}

async function readErrorMessage(response: Response): Promise<string> {
  const jsonError = await readJsonError(response);
  if (jsonError) {
    return jsonError;
  }

  return readTextError(response);
}

async function readJsonError(response: Response): Promise<string | null> {
  try {
    const payload = (await response.json()) as { error?: string };
    if (typeof payload.error === 'string' && payload.error) {
      return payload.error;
    }
  } catch {
    return null;
  }

  return null;
}

async function readTextError(response: Response): Promise<string> {
  const text = await response.text().catch(() => '');
  return text || `${response.status} ${response.statusText}`;
}

async function assertHealthy(baseUrl: string): Promise<void> {
  const response = await fetch(buildUrl(baseUrl, '/v1/heartbeat/'));
  if (response.status !== 204) {
    throw new Error(`heartbeat failed: ${await readErrorMessage(response)}`);
  }
}

async function createBootstrapKey(baseUrl: string): Promise<CreatedApiKey> {
  const response = await fetch(buildUrl(baseUrl, '/v1/keys/'), {
    method: 'POST',
    headers: buildHeaders(undefined, true),
    body: JSON.stringify({}),
  });

  if (response.status === 401) {
    throw new Error('API keys already exist. Re-run with --api-token <token>.');
  }

  if (!response.ok) {
    throw new Error(`could not create API key: ${await readErrorMessage(response)}`);
  }

  return (await response.json()) as CreatedApiKey;
}

async function createDogecoinNetwork(
  baseUrl: string,
  apiToken: string,
  input: {
    blockTime: number;
    chainId: number;
    name: string;
    rpcEndpoint: string;
    rps: number;
  },
): Promise<NetworkResponse> {
  const response = await fetch(buildUrl(baseUrl, '/v1/networks/'), {
    method: 'POST',
    headers: buildHeaders(apiToken, true),
    body: JSON.stringify({
      name: input.name,
      architecture: 'dogecoin',
      chainId: input.chainId,
      blockTime: input.blockTime,
      rpcEndpoint: input.rpcEndpoint,
      rps: input.rps,
    }),
  });

  if (!response.ok) {
    throw new Error(`could not create Dogecoin network: ${await readErrorMessage(response)}`);
  }

  return (await response.json()) as NetworkResponse;
}

async function getStats(baseUrl: string, apiToken: string): Promise<StatsResponse> {
  const response = await fetch(buildUrl(baseUrl, '/v1/stats/'), {
    headers: buildHeaders(apiToken),
  });

  if (!response.ok) {
    throw new Error(`could not read stats: ${await readErrorMessage(response)}`);
  }

  return (await response.json()) as StatsResponse;
}

function printCurlRunsheet(input: {
  baseUrl: string;
  blockTime: number;
  chainId: number;
  name: string;
  rpcEndpoint: string;
  rps: number;
}): void {
  const payload = JSON.stringify({
    name: input.name,
    architecture: 'dogecoin',
    chainId: input.chainId,
    blockTime: input.blockTime,
    rpcEndpoint: input.rpcEndpoint,
    rps: input.rps,
  });

  console.log('');
  console.log('Equivalent curl commands:');
  console.log('Set ONLYDOGE_API_TOKEN to the token shown above before running these commands.');
  console.log(
    `curl -X POST '${new URL('/v1/networks/', normalizeBaseUrl(input.baseUrl)).toString()}' \\`,
  );
  console.log('  -H "x-api-token: $ONLYDOGE_API_TOKEN" \\');
  console.log("  -H 'content-type: application/json' \\");
  console.log(`  -d '${payload}'`);
  console.log('');
  console.log(`curl '${new URL('/v1/stats/', normalizeBaseUrl(input.baseUrl)).toString()}' \\`);
  console.log('  -H "x-api-token: $ONLYDOGE_API_TOKEN"');
}

async function main(): Promise<void> {
  const input = readRunsheetInput();
  await assertHealthy(input.baseUrl);
  const credential = await resolveRunsheetCredential(input.baseUrl, input.apiToken);
  const network = await createDogecoinNetwork(input.baseUrl, credential.apiToken, input);
  const stats = await getStats(input.baseUrl, credential.apiToken);
  printRunsheetResult(input, credential, network, stats);
}

function readRunsheetInput(): RunsheetInput {
  const options = parseRunsheetOptions();

  return {
    apiToken: options.apiToken,
    baseUrl: options.baseUrl,
    blockTime: parsePositiveInteger(options.blockTime, '--block-time'),
    chainId: parseNonNegativeInteger(options.chainId, '--chain-id'),
    name: options.name,
    rpcEndpoint: options.rpcEndpoint,
    rps: parsePositiveInteger(options.rps, '--rps'),
  };
}

function parseRunsheetOptions(): {
  apiToken?: string;
  baseUrl: string;
  blockTime: string;
  chainId: string;
  name: string;
  rpcEndpoint: string;
  rps: string;
} {
  const program = new Command();
  program
    .name('dogecoin-runsheet')
    .description(
      'Bootstrap OnlyDoge for Dogecoin indexing by creating a key if needed and adding a Dogecoin network.',
    )
    .requiredOption('--rpc-endpoint <url>', 'Dogecoin JSON-RPC endpoint')
    .option('--base-url <url>', 'OnlyDoge API base URL', 'http://127.0.0.1:2277')
    .option('--api-token <token>', 'Existing OnlyDoge API token')
    .option('--name <name>', 'Network display name', 'Dogecoin Mainnet')
    .option('--chain-id <number>', 'Chain ID', '0')
    .option('--block-time <seconds>', 'Block time in seconds', '60')
    .option('--rps <number>', 'Per-network requests per second cap', '25');

  program.parse();
  return program.opts();
}

function parseNonNegativeInteger(value: string, label: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`invalid ${label}: ${value}`);
  }

  return parsed;
}

function parsePositiveInteger(value: string, label: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error(`invalid ${label}: ${value}`);
  }

  return parsed;
}

async function resolveRunsheetCredential(
  baseUrl: string,
  requestedToken: string | undefined,
): Promise<RunsheetCredential> {
  const apiToken = requestedToken?.trim();
  if (apiToken) {
    return { apiToken, createdKey: null };
  }

  const createdKey = await createBootstrapKey(baseUrl);
  return { apiToken: createdKey.key, createdKey };
}

function printRunsheetResult(
  input: RunsheetInput,
  credential: RunsheetCredential,
  network: NetworkResponse,
  stats: StatsResponse,
): void {
  console.log('Dogecoin indexing runsheet completed.');
  console.log(`Base URL: ${normalizeBaseUrl(input.baseUrl)}`);
  printCredentialSummary(credential);
  console.log(`Registered network: ${network.id} (${network.name})`);
  console.log(`Masked RPC endpoint: ${network.rpcEndpoint}`);
  console.log(
    'Indexing starts automatically when OnlyDoge is running in --mode=both or --mode=indexer.',
  );
  console.log('');
  console.log('Current stats:');
  console.log(JSON.stringify(stats, null, 2));
  printCurlRunsheet(input);
}

function printCredentialSummary(credential: RunsheetCredential): void {
  if (!credential.createdKey) {
    console.log('Used existing API token from --api-token.');
    return;
  }

  console.log(`Created API key: ${credential.createdKey.id}`);
  console.log(`x-api-token: ${credential.createdKey.key}`);
}

void main().catch((error) => {
  console.error(`[dogecoin-runsheet] ${error instanceof Error ? error.message : String(error)}`);
  process.exit(1);
});
