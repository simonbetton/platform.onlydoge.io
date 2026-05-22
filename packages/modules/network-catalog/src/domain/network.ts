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

const defaultNetworkChainId = 0;
const defaultNetworkRps = 100;

export class Network {
  public readonly record: NetworkRecord;

  private constructor(record: NetworkRecord) {
    this.record = record;
  }

  public static create(input: CreateNetworkInput, nextPrimaryId = 0): Network {
    validateNetworkInput(input);
    return new Network(networkRecordFromInput(input, nextPrimaryId));
  }
}

function networkRecordFromInput(
  input: CreateNetworkInput,
  nextPrimaryId: PrimaryId,
): NetworkRecord {
  return {
    networkId: nextPrimaryId,
    id: createNetworkExternalId(input.id),
    name: input.name.trim(),
    architecture: input.architecture,
    chainId: networkChainId(input.chainId),
    blockTime: input.blockTime,
    rpcEndpoint: input.rpcEndpoint.trim(),
    rps: networkRps(input.rps),
    zmqBlockEndpoint: normalizeOptionalText(input.zmqBlockEndpoint),
    isDeleted: false,
    updatedAt: null,
    createdAt: new Date().toISOString(),
  };
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
  const chainId = networkChainId(value);
  if (!isNetworkChainId(chainId)) {
    throw new ValidationError(`invalid parameter for \`chainId\`: ${chainId}`);
  }
}

function isNetworkChainId(value: number): boolean {
  return [Number.isInteger(value), value >= 0].every(Boolean);
}

function createNetworkExternalId(id: string | undefined): string {
  if (!id) {
    return ExternalId.create('net').value;
  }

  return ExternalId.parse(id, 'net').value;
}

function networkChainId(value: number | undefined): number {
  return value ?? defaultNetworkChainId;
}

function networkRps(value: number | undefined): number {
  return value ?? defaultNetworkRps;
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
  validateOptionalBlockTime(input.blockTime);
  validateOptionalRpcEndpoint(input.rpcEndpoint);
}

function validateOptionalBlockTime(value: number | undefined): void {
  if (value !== undefined) {
    BlockTime.parse(value);
  }
}

function validateOptionalRpcEndpoint(value: string | undefined): void {
  if (value !== undefined) {
    RpcEndpoint.parse(value);
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
  const trimmed = trimOptionalText(value);
  return trimmed ? trimmed : null;
}

function trimOptionalText(value: string | null | undefined): string {
  return value?.trim() ?? '';
}
