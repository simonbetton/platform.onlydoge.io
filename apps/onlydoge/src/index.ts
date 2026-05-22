import { startApiServer } from '@onlydoge/api';
import { startIndexer } from '@onlydoge/indexer-app';
import { createRuntime } from '@onlydoge/platform';
import { Command } from 'commander';

interface CliOptions {
  database?: string;
  ip?: string;
  mode?: string;
  port?: string;
  storage?: string;
  warehouse?: string;
}

interface BindSettings {
  ip: string;
  port: number;
}

interface CliRuntimeContext {
  abortController: AbortController;
  bind: BindSettings;
  options: CliOptions;
}

function waitForAbort(signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    signal.addEventListener(
      'abort',
      () => {
        resolve();
      },
      { once: true },
    );
  });
}

function createProgram(): Command {
  const program = new Command();

  program
    .option('--mode <mode>', 'Runtime mode: both|http|indexer')
    .option('--database <uri>', 'Metadata database URI')
    .option('--storage <location>', 'Raw block storage location')
    .option('--warehouse <location>', 'Warehouse location')
    .option('--ip <address>', 'HTTP bind address')
    .option('--port <port>', 'HTTP port');

  return program;
}

function applyEnvironmentOverrides(options: CliOptions): void {
  if (options.database) {
    process.env.ONLYDOGE_DATABASE = options.database;
  }
  if (options.storage) {
    process.env.ONLYDOGE_STORAGE = options.storage;
  }
  if (options.warehouse) {
    process.env.ONLYDOGE_WAREHOUSE = options.warehouse;
  }
}

function resolveBindSettings(options: CliOptions): BindSettings {
  return {
    ip: resolveBindIp(options),
    port: resolveBindPort(options),
  };
}

function resolveBindIp(options: CliOptions): string {
  return options.ip ?? process.env.ONLYDOGE_IP ?? '127.0.0.1';
}

function resolveBindPort(options: CliOptions): number {
  return options.port ? Number(options.port) : Number(process.env.ONLYDOGE_PORT ?? 2277);
}

function logRuntimeStartup(runtime: Awaited<ReturnType<typeof createRuntime>>): void {
  console.log(
    `[onlydoge] runtime started mode=${runtime.settings.mode} database=${runtime.settings.database.driver} storage=${runtime.settings.storage.driver} warehouse=${runtime.settings.warehouse.driver}`,
  );
}

function shouldStartDegradedHealthServer(_error: unknown): boolean {
  return process.env.NODE_ENV === 'production';
}

function createCliRuntimeContext(): CliRuntimeContext {
  const program = createProgram();
  program.parse();
  const options = program.opts<CliOptions>();
  applyEnvironmentOverrides(options);
  const bind = resolveBindSettings(options);
  const abortController = new AbortController();

  for (const event of ['SIGINT', 'SIGTERM']) {
    process.on(event, () => {
      abortController.abort();
    });
  }

  return { abortController, bind, options };
}

async function startDegradedHealthServer(
  bind: BindSettings,
  signal: AbortSignal,
  error: unknown,
): Promise<void> {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`[onlydoge] startup failed: ${message}`);
  console.error(
    `[onlydoge] starting degraded health-only server on http://${bind.ip}:${bind.port}`,
  );

  const server = Bun.serve({
    hostname: bind.ip,
    port: bind.port,
    fetch(request) {
      const url = new URL(request.url);
      if (url.pathname === '/up') {
        return new Response('ok', { status: 200 });
      }

      return Response.json(
        {
          error: 'service unavailable',
        },
        { status: 503 },
      );
    },
  });

  await waitForAbort(signal);
  server.stop();
}

async function main(): Promise<void> {
  const { abortController, bind, options } = createCliRuntimeContext();
  const runtime = await createRuntime({
    ...(options.mode ? { mode: options.mode } : {}),
    ...(bind.ip ? { ip: bind.ip } : {}),
    ...(Number.isFinite(bind.port) ? { port: bind.port } : {}),
  });
  logRuntimeStartup(runtime);
  await runRuntime(runtime, abortController.signal);
}

async function runRuntime(
  runtime: Awaited<ReturnType<typeof createRuntime>>,
  signal: AbortSignal,
): Promise<void> {
  const server = await startHttpRuntime(runtime);

  if (runtime.settings.isIndexer) {
    await runIndexerRuntime(runtime, signal, server);
    return;
  }

  await stopHttpRuntimeOnAbort(server, signal);
}

async function startHttpRuntime(runtime: Awaited<ReturnType<typeof createRuntime>>) {
  if (!runtime.settings.isHttp) {
    return null;
  }

  const server = await startApiServer(runtime);
  console.log(`[onlydoge] api listening on http://${runtime.settings.ip}:${runtime.settings.port}`);
  return server;
}

async function runIndexerRuntime(
  runtime: Awaited<ReturnType<typeof createRuntime>>,
  signal: AbortSignal,
  server: Awaited<ReturnType<typeof startApiServer>> | null,
): Promise<void> {
  console.log('[onlydoge] indexer loop started');
  const indexerPromise = startIndexer(runtime, signal);
  if (!runtime.settings.isHttp) {
    await indexerPromise;
    return;
  }

  superviseBackgroundIndexer(indexerPromise, signal);
  await stopHttpRuntimeOnAbort(server, signal);
}

function superviseBackgroundIndexer(indexerPromise: Promise<void>, signal: AbortSignal): void {
  void indexerPromise.then(
    () => {
      if (signal.aborted) {
        return;
      }

      console.error('[onlydoge] indexer loop stopped unexpectedly');
      process.exit(1);
    },
    (error) => {
      if (signal.aborted) {
        return;
      }

      console.error(
        `[onlydoge] indexer loop stopped unexpectedly: ${error instanceof Error ? error.message : String(error)}`,
      );
      process.exit(1);
    },
  );
}

async function stopHttpRuntimeOnAbort(
  server: Awaited<ReturnType<typeof startApiServer>> | null,
  signal: AbortSignal,
): Promise<void> {
  if (!server) {
    return;
  }

  await waitForAbort(signal);
  await server.stop();
}

void main().catch((error) => {
  const { abortController, bind } = createCliRuntimeContext();

  if (!shouldStartDegradedHealthServer(error)) {
    console.error(
      `[onlydoge] startup failed: ${error instanceof Error ? error.message : String(error)}`,
    );
    process.exit(1);
  }

  void startDegradedHealthServer(bind, abortController.signal, error).catch((serverError) => {
    console.error(
      `[onlydoge] degraded health server failed: ${serverError instanceof Error ? serverError.message : String(serverError)}`,
    );
    process.exit(1);
  });
});
