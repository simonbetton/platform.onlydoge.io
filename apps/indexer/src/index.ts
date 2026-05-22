import type { Runtime } from '@onlydoge/platform';

export async function startIndexer(runtime: Runtime, signal?: AbortSignal): Promise<void> {
  await runtime.indexingPipeline.start(signal);
}
