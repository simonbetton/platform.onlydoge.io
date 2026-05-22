import type { PrimaryId } from '@onlydoge/shared-kernel';
import type { ApiKeyRole } from './api-key';

export type AuditEventOutcome = 'success' | 'failure' | 'denied' | 'rate_limited';

export interface AuditEventRecord {
  actorApiKeyId: PrimaryId;
  actorApiKey: string;
  actorRole: ApiKeyRole;
  auditEventId: PrimaryId;
  createdAt: string;
  error: string | null;
  id: string;
  ip: string | null;
  method: string;
  operation: string;
  outcome: AuditEventOutcome;
  ownerApiKey: string | null;
  ownerApiKeyId: PrimaryId | null;
  path: string;
  requestId: string;
  resourceIds: string[];
  resourceType: string;
  route: string;
  statusCode: number;
  userAgent: string | null;
}

export type CreateAuditEventInput = Omit<AuditEventRecord, 'auditEventId' | 'id'> & {
  id?: string;
};

export interface AuditEventFilters {
  actor?: string;
  from?: string;
  method?: string;
  offset?: number;
  limit?: number;
  outcome?: AuditEventOutcome;
  resourceId?: string;
  resourceType?: string;
  statusCode?: number;
  to?: string;
}

export interface AuditEventResponse {
  actor: {
    id: string;
    role: ApiKeyRole;
  };
  createdAt: string;
  error: string | null;
  id: string;
  ip: string | null;
  method: string;
  operation: string;
  outcome: AuditEventOutcome;
  owner: {
    id: string;
  } | null;
  path: string;
  requestId: string;
  resourceIds: string[];
  resourceType: string;
  route: string;
  statusCode: number;
  userAgent: string | null;
}

export function auditEventToResponse(record: AuditEventRecord): AuditEventResponse {
  return {
    id: record.id,
    actor: {
      id: record.actorApiKey,
      role: record.actorRole,
    },
    method: record.method,
    path: record.path,
    route: record.route,
    operation: record.operation,
    resourceType: record.resourceType,
    resourceIds: record.resourceIds,
    owner: record.ownerApiKey ? { id: record.ownerApiKey } : null,
    statusCode: record.statusCode,
    outcome: record.outcome,
    error: record.error,
    requestId: record.requestId,
    ip: record.ip,
    userAgent: record.userAgent,
    createdAt: record.createdAt,
  };
}
