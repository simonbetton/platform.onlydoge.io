import {
  configKeyCoreApplyRecovery,
  createCoreApplyRecoveryMarker,
  parseCoreApplyRecoveryMarker,
} from '@onlydoge/indexing-pipeline';
import { describe, expect, it } from 'vitest';

describe('core apply recovery marker', () => {
  it('round-trips a valid marker', () => {
    const marker = createCoreApplyRecoveryMarker({
      instanceId: 'instance-a',
      startHeight: 10,
      endHeight: 12,
      blockHashes: ['hash-10', 'hash-11', 'hash-12'],
      updateCurrentState: true,
    });

    expect(configKeyCoreApplyRecovery()).toBe('dogecoin_core_apply_recovery');
    expect(parseCoreApplyRecoveryMarker(marker)).toEqual(marker);
    expect(marker.startedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/u);
  });

  it('rejects malformed markers', () => {
    expect(() => parseCoreApplyRecoveryMarker(null)).toThrow(/must be an object/u);
    expect(() => parseCoreApplyRecoveryMarker({ version: 2 })).toThrow(
      /unsupported core apply recovery marker version/u,
    );
    expect(() =>
      parseCoreApplyRecoveryMarker({
        version: 1,
        instanceId: 'worker-a',
        startHeight: 1,
        endHeight: 0,
        blockHashes: [],
        updateCurrentState: true,
        startedAt: '2026-07-15T00:00:00.000Z',
      }),
    ).toThrow(/invalid height bounds/u);
  });
});
