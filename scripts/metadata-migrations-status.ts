import { loadSettings, RelationalMetadataStore } from '@onlydoge/platform';

const settings = loadSettings({ mode: 'http' });
const status = await RelationalMetadataStore.migrationStatus(settings.database);

console.log(JSON.stringify(status, null, 2));

if (status.drift.length > 0) {
  process.exitCode = 1;
}
