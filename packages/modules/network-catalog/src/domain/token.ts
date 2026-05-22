import { ExternalId, type PrimaryId, ValidationError } from '@onlydoge/shared-kernel';

export interface TokenRecord {
  tokenId: PrimaryId;
  networkId: PrimaryId;
  id: string;
  name: string;
  symbol: string;
  address: string;
  decimals: number;
  updatedAt: string | null;
  createdAt: string;
}

export interface CreateTokenInput {
  id?: string;
  networkId: PrimaryId;
  name: string;
  symbol: string;
  address?: string;
  decimals: number;
}

export interface TokenResponse {
  address: string;
  createdAt: string;
  decimals: number;
  id: string;
  name: string;
  symbol: string;
}

export class Token {
  public readonly record: TokenRecord;

  private constructor(record: TokenRecord) {
    this.record = record;
  }

  public static create(input: CreateTokenInput, nextPrimaryId = 0): Token {
    assertTokenText(input.name, 'name');
    assertTokenText(input.symbol, 'symbol');
    assertTokenDecimals(input.decimals);

    return new Token(tokenRecordFromInput(input, nextPrimaryId));
  }
}

function tokenRecordFromInput(input: CreateTokenInput, nextPrimaryId: PrimaryId): TokenRecord {
  return {
    tokenId: nextPrimaryId,
    networkId: input.networkId,
    id: resolveTokenId(input.id),
    name: input.name.trim(),
    symbol: input.symbol.trim(),
    address: trimOptionalTokenText(input.address),
    decimals: input.decimals,
    updatedAt: null,
    createdAt: new Date().toISOString(),
  };
}

function resolveTokenId(id: string | undefined): string {
  return id ? ExternalId.parse(id, 'tok').value : ExternalId.create('tok').value;
}

function trimOptionalTokenText(value: string | undefined): string {
  return value?.trim() ?? '';
}

function assertTokenText(value: string, field: 'name' | 'symbol'): void {
  if (!value.trim()) {
    throw new ValidationError(`invalid parameter for \`${field}\`: `);
  }
}

function assertTokenDecimals(value: number): void {
  if (!Number.isInteger(value) || value < 0) {
    throw new ValidationError(`invalid parameter for \`decimals\`: ${value}`);
  }
}

export function tokenToResponse(record: TokenRecord): TokenResponse {
  return {
    id: record.id,
    name: record.name,
    symbol: record.symbol,
    address: record.address,
    decimals: record.decimals,
    createdAt: record.createdAt,
  };
}
