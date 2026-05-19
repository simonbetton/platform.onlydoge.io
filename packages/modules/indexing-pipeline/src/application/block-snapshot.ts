import { InfrastructureError } from '@onlydoge/shared-kernel';

export function readSnapshotString(
  record: Record<string, unknown>,
  key: string,
): string | undefined {
  const value = record[key];
  return typeof value === 'string' ? value : undefined;
}

export function readSnapshotItems<T>(value: unknown, predicate: (item: unknown) => item is T): T[] {
  return Array.isArray(value) ? value.filter(predicate) : [];
}

export function requireSnapshotRecord(
  value: unknown,
  field: string,
  source: 'dogecoin',
): Record<string, unknown> {
  if (!value || typeof value !== 'object') {
    throw new InfrastructureError(`invalid ${source} field: ${field}`);
  }

  return Object.fromEntries(Object.entries(value));
}
