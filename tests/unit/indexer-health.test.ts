import { describe, expect, it } from 'vitest';

import {
  type CoreIndexerHealthInput,
  evaluateCoreIndexerHealth,
} from '../../scripts/indexer-health';

const now = Date.parse('2026-07-15T07:00:00.000Z');

describe('core indexer health policy', () => {
  it('accepts a progressing backfill regardless of chain lag', () => {
    expect(health({ blockHeight: 1_000, state: state('sync_backfill', 10_000) })).toBeNull();
  });

  it('rejects stale backfill progress', () => {
    expect(health({ state: state('process_backfill', 180_001) })).toContain(
      'reason=stale_progress',
    );
  });

  it('accepts online lag at the configured threshold', () => {
    expect(
      health({
        blockHeight: 106,
        state: { ...state('online', 10_000), onlineTip: 104, processTail: 100 },
      }),
    ).toBeNull();
  });

  it('rejects online lag over the configured threshold', () => {
    expect(
      health({
        blockHeight: 107,
        state: { ...state('online', 10_000), onlineTip: 104, processTail: 100 },
      }),
    ).toContain('reason=online_lag');
  });

  it('rejects persisted errors and redacts credentials', () => {
    const result = health({
      state: {
        ...state('online', 10_000),
        lastError: 'failed http://user:pass@node/ token=private',
      },
    });
    expect(result).toContain('last_error=failed http://***:***@node/ token=***');
    expect(result).not.toContain('user:pass');
    expect(result).not.toContain('private');
  });

  it('rejects malformed timestamps and missing state', () => {
    expect(health({ state: { ...state('online', 0), updatedAt: 'invalid' } })).toContain(
      'reason=invalid_updated_at',
    );
    expect(health({ state: null })).toBe('state=missing');
  });

  it('becomes healthy after successful progress clears an error', () => {
    expect(
      health({
        state: {
          ...state('online', 0),
          lastError: null,
          onlineTip: 100,
          processTail: 100,
          syncTail: 100,
        },
      }),
    ).toBeNull();
  });
});

function health(overrides: Partial<CoreIndexerHealthInput> = {}): string | null {
  return evaluateCoreIndexerHealth({
    blockHeight: 100,
    now,
    onlineTipDistance: 6,
    state: state('online', 0),
    watchdogMs: 180_000,
    ...overrides,
  });
}

function state(stage: string, ageMs: number) {
  return {
    lastError: null,
    onlineTip: 100,
    processTail: 100,
    stage,
    syncTail: 100,
    updatedAt: new Date(now - ageMs).toISOString(),
  };
}
