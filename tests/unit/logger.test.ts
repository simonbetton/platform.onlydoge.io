import { createLogger, resolveRequestId } from '@onlydoge/platform';
import { describe, expect, it } from 'vitest';

describe('logger', () => {
  it('accepts safe request ids and generates ids for unsafe values', () => {
    expect(resolveRequestId('req-safe-123')).toBe('req-safe-123');
    expect(resolveRequestId('  req-trimmed  ')).toBe('req-trimmed');

    const generated = resolveRequestId('not safe because spaces');
    expect(generated).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu,
    );
  });

  it('redacts sensitive fields from structured logs', () => {
    const lines: string[] = [];
    const logger = createLogger(
      { service: 'test' },
      {
        write(chunk: string) {
          lines.push(chunk);
        },
      },
    );

    logger.info({
      token: 'secret-token',
      rpcEndpoint: 'http://user:pass@dogecoin.example.com:22555/',
    });

    const output = lines.join('');
    expect(output).not.toContain('secret-token');
    expect(output).not.toContain('user:pass');
    expect(output).toContain('[REDACTED]');
  });

  it('emits stable service and component fields for background loggers', () => {
    const lines: string[] = [];
    const logger = createLogger(
      { component: 'core-indexer', service: 'onlydoge' },
      {
        write(chunk: string) {
          lines.push(chunk);
        },
      },
    );

    logger.info({ instanceId: 'inst-1' }, 'indexer loop started');

    const output = lines.join('');
    expect(output).toContain('"service":"onlydoge"');
    expect(output).toContain('"component":"core-indexer"');
    expect(output).toContain('"instanceId":"inst-1"');
    expect(output).toContain('indexer loop started');
  });
});
