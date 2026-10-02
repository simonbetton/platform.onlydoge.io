import { existsSync } from 'node:fs';

import { buildApiApp } from '@onlydoge/api';
import { createRuntime } from '@onlydoge/platform';

if (existsSync('node_modules/typescript')) {
  throw new Error('Production image must exclude the TypeScript compiler');
}

const runtime = await createRuntime({ mode: 'both', ip: '127.0.0.1', port: 2277 });
const app = buildApiApp(runtime);
const response = await app.handle(new Request('http://localhost/openapi/json'));
if (response.status !== 200) {
  throw new Error(`OpenAPI returned ${response.status}`);
}
const document = (await response.json()) as { paths?: Record<string, unknown> };
if (!document.paths || Object.keys(document.paths).length === 0) {
  throw new Error('OpenAPI document has no routes');
}
console.info('Production API and OpenAPI smoke test passed');
process.exit(0);
