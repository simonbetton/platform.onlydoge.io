import { execFile } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { resolve } from 'node:path';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
const defaultStartupTimeoutMs = 120_000;

export interface DockerService {
  hostPort(containerPort: number): number;
  name: string;
  stop(): Promise<void>;
}

interface DockerServiceOptions {
  command?: string[];
  environment?: Record<string, string>;
  image: string;
  name: string;
  ports: number[];
  readiness: (service: DockerService) => Promise<boolean>;
  startupTimeoutMs?: number;
  volumes?: string[];
}

export async function startDockerService(options: DockerServiceOptions): Promise<DockerService> {
  const name = `onlydoge-adapter-${options.name}-${randomUUID()}`;
  const ports = new Map<number, number>();
  const args = [
    'run',
    '-d',
    '--rm',
    '--name',
    name,
    '--label',
    'io.onlydoge.adapter-test=true',
    ...Object.entries(options.environment ?? {}).flatMap(([key, value]) => [
      '-e',
      `${key}=${value}`,
    ]),
    ...options.ports.flatMap((port) => ['-p', `127.0.0.1::${port}`]),
    ...(options.volumes ?? []).flatMap((volume) => ['-v', volume]),
    options.image,
    ...(options.command ?? []),
  ];

  const service: DockerService = {
    name,
    hostPort(containerPort) {
      const port = ports.get(containerPort);
      if (!port) {
        throw new Error(`container ${name} does not expose port ${containerPort}`);
      }
      return port;
    },
    async stop() {
      await captureDockerLogs(name);
      await docker(['rm', '-f', name]).catch(() => undefined);
    },
  };

  try {
    await docker(args, options.startupTimeoutMs ?? defaultStartupTimeoutMs);
    for (const port of options.ports) {
      ports.set(port, await inspectHostPort(name, port));
    }
    await waitForReadiness(service, options);
    return service;
  } catch (error) {
    await service.stop();
    throw error;
  }
}

export async function docker(
  args: string[],
  timeout = 30_000,
): Promise<{ stderr: string; stdout: string }> {
  return execFileAsync('docker', args, {
    maxBuffer: 10 * 1024 * 1024,
    timeout,
  });
}

export async function waitForHttp(url: string, init?: RequestInit): Promise<boolean> {
  try {
    return (await fetch(url, init)).ok;
  } catch {
    return false;
  }
}

async function inspectHostPort(name: string, containerPort: number): Promise<number> {
  const { stdout } = await docker(['port', name, `${containerPort}/tcp`]);
  const port = Number(stdout.trim().split(':').at(-1));
  if (!Number.isInteger(port) || port <= 0) {
    throw new Error(`invalid host port for ${name}:${containerPort}: ${stdout}`);
  }
  return port;
}

async function waitForReadiness(
  service: DockerService,
  options: DockerServiceOptions,
): Promise<void> {
  const deadline = Date.now() + (options.startupTimeoutMs ?? defaultStartupTimeoutMs);
  let lastError = 'readiness check returned false';

  while (Date.now() < deadline) {
    try {
      if (await options.readiness(service)) {
        return;
      }
    } catch (error) {
      lastError = error instanceof Error ? error.message : String(error);
    }
    await delay(1_000);
  }

  throw new Error(`${options.name} container was not ready: ${lastError}`);
}

async function captureDockerLogs(name: string): Promise<void> {
  try {
    const { stderr, stdout } = await docker(['logs', name]);
    const directory = process.env.ONLYDOGE_ADAPTER_LOG_DIR
      ? resolve(process.cwd(), process.env.ONLYDOGE_ADAPTER_LOG_DIR)
      : resolve(tmpdir(), 'onlydoge-adapter-logs');
    await mkdir(directory, { recursive: true });
    await writeFile(resolve(directory, `${name}.log`), `${stdout}${stderr}`);
  } catch {
    // The container may have exited and removed itself before logs can be collected.
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, ms));
}
