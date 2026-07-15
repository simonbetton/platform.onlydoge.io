import {
  clickHouseMigrationStatus,
  loadSettings,
  RelationalMetadataStore,
  runClickHouseMigrations,
} from '@onlydoge/platform';

const settings = loadSettings({ mode: 'indexer' });
if (settings.warehouse.driver !== 'clickhouse') {
  throw new Error('ClickHouse migration commands require ONLYDOGE_WAREHOUSE to use ClickHouse');
}

const metadata = await RelationalMetadataStore.connect(settings.database);
try {
  const records =
    process.argv[2] === 'status'
      ? await clickHouseMigrationStatus(settings.warehouse)
      : await runClickHouseMigrations(settings.warehouse, metadata);
  for (const record of records) {
    console.log(
      `${record.version.toString().padStart(4, '0')} ${record.state} ${record.name} ${record.checksum}`,
    );
  }
} finally {
  await metadata.close();
}
