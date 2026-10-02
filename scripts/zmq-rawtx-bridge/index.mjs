#!/usr/bin/env node
/**
 * Out-of-process ZMQ rawtx bridge for OnlyDoge.
 * Bun cannot load native zeromq; run this under Node and pipe hex lines to stdout.
 *
 * Usage: node scripts/zmq-rawtx-bridge/index.mjs tcp://dogecoin:28332
 * Install: npm ci --omit=dev --prefix scripts/zmq-rawtx-bridge
 */
import { createRequire } from 'node:module';

const endpoint = process.argv[2];
if (!endpoint) {
  console.error('usage: node index.mjs <zmq-endpoint>');
  process.exit(2);
}

const require = createRequire(import.meta.url);

let zmq;
try {
  zmq = require('zeromq');
} catch (error) {
  console.error(
    'zeromq is not installed. Run: npm ci --omit=dev --prefix scripts/zmq-rawtx-bridge',
  );
  console.error(error);
  process.exit(1);
}

const sock = new zmq.Subscriber();
sock.connect(endpoint);
sock.subscribe('rawtx');
console.error(`[onlydoge-zmq-bridge] subscribed rawtx at ${endpoint}`);

for await (const frames of sock) {
  const body = frames.length > 1 ? frames[1] : frames[0];
  if (!body) {
    continue;
  }
  process.stdout.write(`${Buffer.from(body).toString('hex')}\n`);
}
