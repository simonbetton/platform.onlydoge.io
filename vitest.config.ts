import { resolve } from 'node:path';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: {
    alias: {
      '@onlydoge/shared-kernel': resolve(
        import.meta.dirname,
        'packages/shared-kernel/src/index.ts',
      ),
      '@onlydoge/api': resolve(import.meta.dirname, 'apps/api/src/index.ts'),
      '@onlydoge/indexer-app': resolve(import.meta.dirname, 'apps/indexer/src/index.ts'),
      '@onlydoge/platform': resolve(import.meta.dirname, 'packages/platform/src/index.ts'),
      '@onlydoge/access-control': resolve(
        import.meta.dirname,
        'packages/modules/access-control/src/index.ts',
      ),
      '@onlydoge/network-catalog': resolve(
        import.meta.dirname,
        'packages/modules/network-catalog/src/index.ts',
      ),
      '@onlydoge/entity-labeling': resolve(
        import.meta.dirname,
        'packages/modules/entity-labeling/src/index.ts',
      ),
      '@onlydoge/explorer-query': resolve(
        import.meta.dirname,
        'packages/modules/explorer-query/src/index.ts',
      ),
      '@onlydoge/investigation-query': resolve(
        import.meta.dirname,
        'packages/modules/investigation-query/src/index.ts',
      ),
      '@onlydoge/indexing-pipeline': resolve(
        import.meta.dirname,
        'packages/modules/indexing-pipeline/src/index.ts',
      ),
    },
  },
  test: {
    globals: true,
    include: ['tests/**/*.test.ts'],
    environment: 'node',
    coverage: {
      reporter: ['text', 'html'],
    },
  },
});
