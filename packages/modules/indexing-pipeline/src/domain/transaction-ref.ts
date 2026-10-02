export interface TransactionRef {
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  source: 'raw_sync' | 'core_process';
  txIndex: number;
  txid: string;
  version: number;
}

export function deriveTransactionRefsFromBlock(input: {
  blockHash: string;
  blockHeight: number;
  blockTime: number;
  source: TransactionRef['source'];
  transactions: Array<{ txid: string }>;
}): TransactionRef[] {
  return input.transactions.map((transaction, txIndex) => ({
    blockHash: input.blockHash,
    blockHeight: input.blockHeight,
    blockTime: input.blockTime,
    source: input.source,
    txIndex,
    txid: transaction.txid,
    version: transactionRefVersion(input.blockHeight, txIndex, input.source),
  }));
}

export function transactionRefVersion(
  blockHeight: number,
  txIndex: number,
  source: TransactionRef['source'],
): number {
  const sourceOffset = source === 'raw_sync' ? 0 : 1;
  return blockHeight * 1_000_000 + txIndex * 10 + sourceOffset;
}
