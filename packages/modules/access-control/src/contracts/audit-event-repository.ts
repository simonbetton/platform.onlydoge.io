import type { PrimaryId } from '@onlydoge/shared-kernel';
import type {
  AuditEventFilters,
  AuditEventRecord,
  CreateAuditEventInput,
} from '../domain/audit-event';

export interface AuditEventRepository {
  createAuditEvent(record: CreateAuditEventInput): Promise<void>;
  deleteAuditEventsBefore(cutoffIso: string): Promise<void>;
  listAuditEvents(
    filters: AuditEventFilters & { actorApiKeyId?: PrimaryId },
  ): Promise<AuditEventRecord[]>;
}
