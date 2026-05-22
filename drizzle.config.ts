import { defineConfig } from 'drizzle-kit';

export default defineConfig({
  schema: './packages/platform/src/index.ts',
  out: './drizzle',
  dialect: 'sqlite',
  dbCredentials: {
    url: 'file:./.onlydoge/onlydoge.sqlite.db',
  },
});
