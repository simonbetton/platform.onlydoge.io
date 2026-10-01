import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createClient } from '@libsql/client';
import { RelationalMetadataStore } from '@onlydoge/platform';
import { expect, it } from 'vitest';

it('inspects an empty database without creating a migration ledger', async () => {
  const root = await mkdtemp(join(tmpdir(), 'onlydoge-migration-status-'));
  const settings = { driver: 'sqlite' as const, location: `file:${root}/metadata.sqlite` };
  try {
    const status = await RelationalMetadataStore.migrationStatus(settings);
    expect(status.ledgerExists).toBe(false);
    expect(status.applied).toEqual([]);
    expect(status.pending.length).toBeGreaterThan(0);
    const raw = createClient({ url: settings.location });
    try {
      const tables = await raw.execute("SELECT name FROM sqlite_master WHERE type = 'table'");
      expect(tables.rows).toEqual([]);
    } finally {
      raw.close();
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

it('reports migration checksum drift without attempting to migrate', async () => {
  const root = await mkdtemp(join(tmpdir(), 'onlydoge-migration-status-'));
  const settings = { driver: 'sqlite' as const, location: `file:${root}/metadata.sqlite` };
  try {
    const store = await RelationalMetadataStore.connect(settings);
    await store.close();
    const raw = createClient({ url: settings.location });
    try {
      await raw.execute("UPDATE metadata_migrations SET checksum = 'tampered' WHERE version = 1");
    } finally {
      raw.close();
    }
    const status = await RelationalMetadataStore.migrationStatus(settings);
    expect(status.drift).toContain('checksum/name drift for metadata migration 1');
    await expect(RelationalMetadataStore.connect(settings)).rejects.toThrow(/migration drift/u);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
