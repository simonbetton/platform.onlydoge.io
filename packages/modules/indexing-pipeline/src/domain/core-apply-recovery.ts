export interface CoreApplyRecoveryMarkerV1 {
  version: 1;
  instanceId: string;
  startHeight: number;
  endHeight: number;
  blockHashes: string[];
  updateCurrentState: boolean;
  startedAt: string;
}

export function parseCoreApplyRecoveryMarker(value: unknown): CoreApplyRecoveryMarkerV1 {
  if (!isRecord(value)) {
    throw new Error('core apply recovery marker must be an object');
  }

  if (value.version !== 1) {
    throw new Error(`unsupported core apply recovery marker version: ${String(value.version)}`);
  }

  const instanceId = requireString(value.instanceId, 'instanceId');
  const startHeight = requireInteger(value.startHeight, 'startHeight');
  const endHeight = requireInteger(value.endHeight, 'endHeight');
  const blockHashes = requireStringArray(value.blockHashes, 'blockHashes');
  const updateCurrentState = requireBoolean(value.updateCurrentState, 'updateCurrentState');
  const startedAt = requireString(value.startedAt, 'startedAt');

  if (startHeight < 0 || endHeight < startHeight) {
    throw new Error('core apply recovery marker has invalid height bounds');
  }

  return {
    version: 1,
    instanceId,
    startHeight,
    endHeight,
    blockHashes,
    updateCurrentState,
    startedAt,
  };
}

export function createCoreApplyRecoveryMarker(input: {
  instanceId: string;
  startHeight: number;
  endHeight: number;
  blockHashes: string[];
  updateCurrentState: boolean;
}): CoreApplyRecoveryMarkerV1 {
  return {
    version: 1,
    instanceId: input.instanceId,
    startHeight: input.startHeight,
    endHeight: input.endHeight,
    blockHashes: input.blockHashes,
    updateCurrentState: input.updateCurrentState,
    startedAt: new Date().toISOString(),
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object';
}

function requireString(value: unknown, field: string): string {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new Error(`core apply recovery marker field ${field} must be a non-empty string`);
  }

  return value;
}

function requireInteger(value: unknown, field: string): number {
  if (typeof value !== 'number' || !Number.isInteger(value)) {
    throw new Error(`core apply recovery marker field ${field} must be an integer`);
  }

  return value;
}

function requireBoolean(value: unknown, field: string): boolean {
  if (typeof value !== 'boolean') {
    throw new Error(`core apply recovery marker field ${field} must be a boolean`);
  }

  return value;
}

function requireStringArray(value: unknown, field: string): string[] {
  if (!Array.isArray(value) || value.some((entry) => typeof entry !== 'string')) {
    throw new Error(`core apply recovery marker field ${field} must be a string array`);
  }

  return value;
}
