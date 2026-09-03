#!/usr/bin/env bun

import {
  formatIndexerStatusLine,
  loadSettings,
  RelationalMetadataStore,
  readIndexerStatus,
} from '@onlydoge/platform';

/**
 * Prints indexer progress from the metadata database. Pass `--json` for the
 * raw status object, or `--watch [seconds]` to refresh in place.
 */
async function main(argv: string[]): Promise<void> {
  const json = argv.includes('--json');
  const watchIndex = argv.indexOf('--watch');
  const intervalSeconds = watchIndex >= 0 ? Number(argv[watchIndex + 1] ?? 5) || 5 : null;

  const settings = loadSettings({ mode: 'indexer' });
  const metadata = await RelationalMetadataStore.connect(settings.database);

  for (;;) {
    const status = await readIndexerStatus(metadata);
    console.log(json ? JSON.stringify(status) : formatIndexerStatusLine(status));
    if (intervalSeconds === null) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, intervalSeconds * 1_000));
  }
}

void main(process.argv.slice(2)).catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
