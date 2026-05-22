import {
  BlockTime,
  type ChainFamily,
  ExternalId,
  maskRpcEndpointAuth,
  type PrimaryId,
  RpcEndpoint,
  ValidationError,
} from '@onlydoge/shared-kernel';

export interface NetworkRecord {
  networkId: PrimaryId;
  id: string;
  name: string;
  architecture: ChainFamily;
  chainId: number;
  blockTime: number;
  rpcEndpoint: string;
  rps: number;
  zmqBlockEndpoint: string | null;
  isDeleted: boolean;
  updatedAt: string | null;
  createdAt: string;
}

export interface CreateNetworkInput {
  id?: string;
  name: string;
  architecture: ChainFamily;
  chainId?: number;
  blockTime: number;
  rpcEndpoint: string;
  rps?: number;
  zmqBlockEndpoint?: string | null;
}

export interface NetworkResponse {
  architecture: ChainFamily;
  blockTime: number;
  chainId: number;
  createdAt: string;
  id: string;
  name: string;
  rpcEndpoint: string;
  rps: number;
  zmqBlockEndpoint: string | null;
}

export type UpdateNetworkInput = Partial<
  Pick<
    NetworkRecord,
    'architecture' | 'blockTime' | 'chainId' | 'name' | 'rpcEndpoint' | 'rps' | 'zmqBlockEndpoint'
  >
>;

export class Network {
  public readonly record: NetworkRecord;

  private constructor(record: NetworkRecord) {
    this.record = record;
  }

  public static create(input: CreateNetworkInput, nextPrimaryId = 0): Network {
    validateNetworkInput(input);

    return new Network({
      networkId: nextPrimaryId,
      id: createNetworkExternalId(input.id),
      name: input.name.trim(),
      architecture: input.architecture,
      chainId: input.chainId ?? 0,
      blockTime: input.blockTime,
      rpcEndpoint: input.rpcEndpoint.trim(),
      rps: input.rps ?? 100,
      zmqBlockEndpoint: normalizeOptionalText(input.zmqBlockEndpoint),
      isDeleted: false,
      updatedAt: null,
      createdAt: new Date().toISOString(),
    });
  }
}

function validateNetworkInput(input: CreateNetworkInput): void {
  assertNetworkName(input.name);
  assertNetworkChainId(input.chainId);
  BlockTime.parse(input.blockTime);
  RpcEndpoint.parse(input.rpcEndpoint);
}

function assertNetworkName(value: string): void {
  if (!value.trim()) {
    throw new ValidationError('invalid parameter for `name`: ');
  }
}

function assertNetworkChainId(value: number | undefined): void {
  const chainId = value ?? 0;
  if (!Number.isInteger(chainId) || chainId < 0) {
    throw new ValidationError(`invalid parameter for \`chainId\`: ${chainId}`);
  }
}

function createNetworkExternalId(id: string | undefined): string {
  return id ? ExternalId.parse(id, 'net').value : ExternalId.create('net').value;
}

export function updateNetworkRecord(
  record: NetworkRecord,
  input: UpdateNetworkInput,
): NetworkRecord {
  validateNetworkUpdate(input);

  return {
    ...record,
    name: updatedText(input.name, record.name),
    architecture: updatedValue(input.architecture, record.architecture),
    chainId: updatedValue(input.chainId, record.chainId),
    blockTime: updatedValue(input.blockTime, record.blockTime),
    rpcEndpoint: updatedText(input.rpcEndpoint, record.rpcEndpoint),
    rps: updatedValue(input.rps, record.rps),
    zmqBlockEndpoint:
      input.zmqBlockEndpoint === undefined
        ? record.zmqBlockEndpoint
        : normalizeOptionalText(input.zmqBlockEndpoint),
    updatedAt: new Date().toISOString(),
  };
}

function validateNetworkUpdate(input: UpdateNetworkInput): void {
  if (input.blockTime !== undefined) {
    BlockTime.parse(input.blockTime);
  }
  if (input.rpcEndpoint !== undefined) {
    RpcEndpoint.parse(input.rpcEndpoint);
  }
}

function updatedValue<T>(value: T | undefined, fallback: T): T {
  return value === undefined ? fallback : value;
}

function updatedText(value: string | undefined, fallback: string): string {
  return value === undefined ? fallback : value.trim();
}

export function networkToResponse(record: NetworkRecord): NetworkResponse {
  return {
    id: record.id,
    name: record.name,
    architecture: record.architecture,
    chainId: record.chainId,
    blockTime: record.blockTime,
    rpcEndpoint: maskRpcEndpointAuth(RpcEndpoint.parse(record.rpcEndpoint)),
    rps: record.rps,
    zmqBlockEndpoint: record.zmqBlockEndpoint,
    createdAt: record.createdAt,
  };
}

function normalizeOptionalText(value: string | null | undefined): string | null {
  const trimmed = value?.trim() ?? '';
  return trimmed ? trimmed : null;
}
