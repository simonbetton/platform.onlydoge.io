import { randomUUID } from 'node:crypto';

import type { ServiceLogger } from '@onlydoge/shared-kernel';
import pino, { type Logger } from 'pino';

export type OnlyDogeLogger = Logger;

export function asServiceLogger(logger: OnlyDogeLogger): ServiceLogger {
  return {
    info: (bindings, message) => logger.info(bindings, message),
    warn: (bindings, message) => logger.warn(bindings, message),
    error: (bindings, message) => logger.error(bindings, message),
  };
}

const redactPaths = [
  'authorization',
  'headers.authorization',
  'headers["x-api-token"]',
  'headers["X-API-TOKEN"]',
  'password',
  'rpcEndpoint',
  'rpc_endpoint',
  'token',
  'apiToken',
  'api_token',
  'secret',
  'secretAccessKey',
  'accessKeyId',
  'err.config.headers.authorization',
  'err.config.headers.Authorization',
];

export function createLogger(
  bindings: Record<string, unknown> = {},
  destination?: { write(chunk: string): void },
): OnlyDogeLogger {
  return pino(
    {
      level: process.env.LOG_LEVEL ?? 'info',
      redact: {
        paths: redactPaths,
        censor: '[REDACTED]',
      },
    },
    destination,
  ).child(bindings);
}

const requestIdPattern = /^[\w.-]{1,128}$/u;

export function resolveRequestId(headerValue: string | null | undefined): string {
  const trimmed = headerValue?.trim();
  if (!trimmed || trimmed.length > 128 || !requestIdPattern.test(trimmed)) {
    return randomUUID();
  }

  return trimmed;
}
