# Dogecoin Projection Plan

## Current Gap

- `packages/modules/indexing-pipeline/src/application/indexing-pipeline-service.ts` currently does one thing well: fetch a block snapshot and persist it.
- The same loop then advances `indexer_process_tail` immediately, even though no projection work has happened yet.
- `packages/modules/indexing-pipeline/src/contracts/ports.ts` exposes a write-only raw storage port, so a failed projection pass cannot replay from stored snapshots.
- `packages/modules/indexing-pipeline/src/contracts/ports.ts` also exposes a projection warehouse port with no way to write transfers, balances, UTXO state, or path links.
- `processNewAddresses()` deletes relink requests instead of rebuilding links for newly labeled addresses.
- `packages/modules/investigation-query/src/application/investigation-query-service.ts` currently reads `balances` and `links`, but the existing `links` shape is really a placeholder and treats transfer count as hop count.

The immediate Dogecoin-first goal is to add a canonical projection layer that can:

1. resolve UTXO spends from raw snapshots,
2. derive deterministic address debits and credits,
3. derive directional transfer legs,
4. maintain current balances,
5. project outbound path links from labeled source addresses.

## Projection Layers

Keep the warehouse split into immutable facts plus query-oriented read models.

### 1. Immutable chain facts

#### `utxo_outputs`

Dogecoin-specific source of truth for output ownership and spend status.

- `network_id`
- `block_height`
- `block_hash`
- `block_time`
- `txid`
- `tx_index`
- `vout`
- `output_key`
  - deterministic `${txid}:${vout}`
- `address`
- `script_type`
- `value_base`
  - integer string in base units
- `is_coinbase`
- `is_spendable`
- `spent_by_txid`
- `spent_in_block`
- `spent_input_index`

Why it exists:

- input RPC payloads do not carry the previous output value or address,
- later transfer derivation needs exact spent output values,
- reprocessing must be idempotent per outpoint.

#### `address_movements`

Dogecoin debit and credit facts emitted from UTXO consumes and creates.

- `movement_id`
  - deterministic string, for example `${txid}:vin:${index}` or `${txid}:vout:${index}`
- `network_id`
- `block_height`
- `block_hash`
- `block_time`
- `txid`
- `tx_index`
- `entry_index`
- `address`
- `asset_address`
  - `''` for native DOGE
- `direction`
  - `debit` or `credit`
- `amount_base`
- `output_key`
  - nullable, points at the spent or created outpoint for Dogecoin
- `derivation_method`
  - for example `dogecoin_utxo_input_v1` or `dogecoin_utxo_output_v1`

Why it exists:

- balances become a pure aggregation problem,
- investigation APIs can later expose timelines without re-decoding blocks,
- each movement keeps deterministic provenance for idempotent rebuilds.

#### `transfers`

Directional value legs derived from canonical movements. This is the fact table used for graph traversal and downstream path expansion.

- `transfer_id`
  - deterministic string, for example `${txid}:${fromAddress}:${toAddress}:${index}`
- `network_id`
- `block_height`
- `block_hash`
- `block_time`
- `txid`
- `tx_index`
- `transfer_index`
- `asset_address`
  - `''` for native DOGE
- `from_address`
- `to_address`
- `amount_base`
- `derivation_method`
  - Dogecoin v1 should use a clearly named method such as `dogecoin_pro_rata_v1`
- `confidence`
  - float, starts conservative
- `is_change`
- `input_address_count`
- `output_address_count`

Why it exists:

- direct graph edges should not be rebuilt from raw UTXO every time,
- Dogecoin transfer derivation is heuristic and needs an explicit provenance field,
- graph traversal can read direct value legs without re-decoding raw blocks.

### 2. Mutable read models

#### `balances`

This can keep the current table name so the investigation module changes stay small.

- `network_id`
- `address`
- `asset_address`
  - `''` for native DOGE
- `balance`
- `as_of_block_height`

Write rule:

- upsert by `network_id + address + asset_address`,
- apply the block delta from `address_movements`,
- Dogecoin native balances remain exactly aligned with current unspent outputs.

#### `direct_links`

Aggregated one-hop adjacency derived from `transfers`.

- `network_id`
- `from_address`
- `to_address`
- `asset_address`
- `transfer_count`
- `total_amount_base`
- `first_seen_block_height`
- `last_seen_block_height`

Why it exists:

- source-path backfills should traverse a compact graph,
- relinking a newly labeled address should not require raw block replay.

#### `source_links`

Outbound path links from labeled source addresses. This is what `GET /v1/info` should read for the `sources` section.

- `network_id`
- `source_address_id`
  - internal metadata ID so relink requests can target a stable seed
- `source_address`
- `to_address`
- `hop_count`
- `path_transfer_count`
- `path_addresses`
  - ordered array from seed to destination
- `first_seen_block_height`
- `last_seen_block_height`

Why it exists:

- the current `links` table is trying to represent source evidence, not just direct adjacency,
- `hops` in the API should be real path length, not raw transfer count,
- new labels should be able to backfill historical reachability from already-derived transfer edges.

## Dogecoin Derivation Rules

Dogecoin processing should be deterministic and block-local.

### Block decoding

For each stored block snapshot:

1. Read the raw block from storage instead of re-fetching it from RPC.
2. Process transactions in block order.
3. Maintain a block-local outpoint map so same-block spends can resolve correctly.

### UTXO resolution

For each transaction input:

- if coinbase, skip spend resolution,
- else resolve the referenced outpoint from:
  1. the block-local outpoint map,
  2. previously stored `utxo_outputs`.

If an input cannot be resolved, the block projection should fail and leave `process_tail` unchanged.

### Address movement derivation

For each resolved input:

- emit a `debit` movement for the spent output owner and amount,
- mark the referenced `utxo_outputs` row as spent.

For each spendable output with a decodable address:

- emit a `credit` movement,
- insert an unspent `utxo_outputs` row.

Non-address or provably unspendable outputs should still be recorded in `utxo_outputs` with `is_spendable = false`, but they should not create balances or transfer legs.

### Transfer derivation

Dogecoin does not provide canonical sender-to-recipient legs, so the transfer table must record its derivation method.

Dogecoin v1 should use this deterministic rule:

1. Collect unique input addresses and their spent totals.
2. Classify obvious self-change outputs where `output.address` is already present in the input address set.
3. Treat every remaining spendable output as an external destination.
4. Allocate each external output amount across input addresses in proportion to their contributed input value.
5. Round deterministically in base units so the allocated legs sum exactly to the destination output amount.

This yields:

- complete outbound legs for graph work,
- explicit `is_change`,
- reproducible results for tests and replays.

The confidence can start as:

- `1.0` when all inputs belong to one address,
- lower when multiple input addresses contribute value.

Later heuristics can replace or augment `dogecoin_pro_rata_v1` without rewriting the rest of the pipeline.

### Balance derivation

Balances should be updated from the block delta, not recomputed from scratch.

- `balance += credits - debits`
- `asset_address = ''` for DOGE
- the warehouse adapter should clamp to exact integer strings and reject negative final balances for normal addresses

### Direct link derivation

For each derived transfer leg:

- upsert `direct_links` on `network_id + from_address + to_address + asset_address`,
- increment `transfer_count`,
- add to `total_amount_base`,
- move `last_seen_block_height`.

### Source path derivation

Source paths should only be created from labeled addresses, not from every address in the graph.

Two entry points matter:

1. Incremental block processing
   - if a new block emits a direct edge from a labeled seed, create a one-hop `source_links` row,
   - if it emits an edge from an address that is already reachable from a labeled seed, extend the path by one hop.
2. Relinking after a new label is added
   - delete existing `source_links` rows for that `source_address_id`,
   - replay reachability from `direct_links` with a bounded BFS or DFS,
   - write the shortest path per destination.

This is the part that fixes the current no-op `processNewAddresses()` behavior.

## Pipeline Changes

### Split sync from projection

The current service advances both tails together. That needs to become two explicit stages.

#### Stage 1: sync

- read `latestBlockHeight` from RPC,
- treat an unset tail as `-1` so the first synced block is height `0`,
- if `sync_tail < latestBlockHeight`, fetch `sync_tail + 1`,
- persist raw snapshot,
- advance `indexer_sync_tail`,
- update `indexer_sync_progress`.

#### Stage 2: project

- if `process_tail < sync_tail`, load the next stored raw snapshot,
- derive a block projection batch,
- atomically apply it to the warehouse,
- advance `indexer_process_tail`,
- update `indexer_process_progress`.

`process_tail` must never advance before warehouse projection succeeds.

### Add replayable raw-block reads

`RawBlockStoragePort` should grow a matching `getPart(...)` method so projection uses the stored snapshot as the source of truth.

That gives:

- crash recovery,
- deterministic reprocessing in tests,
- future backfills without RPC dependency.

### Replace the warehouse checkpoint with batch projection

`ProjectionWarehousePort` needs write semantics, not only `checkpointNetwork(...)`.

The minimal write-side surface is:

- `applyBlockProjection(batch)`
- `deleteSourceLinksForSeed(networkId, sourceAddressId)`
- `listOutgoingDirectLinks(networkId, fromAddresses, cursor?)`
- `upsertSourceLinks(rows)`

The existing read-side investigation queries can stay separate.

### Add a dedicated label seed port

The indexing module needs a clean way to resolve and drain newly labeled addresses.

Do not keep this as generic config deletion. Add a port that can:

- list pending relink seeds for a network,
- resolve `addressId -> address value`,
- clear a processed seed.

This can still be backed by the metadata config table, but the indexing module should not depend on prefix tricks.

### Keep coordinator configs focused on orchestration

Keep these config keys:

- `primary`
- `block_height_n{networkId}`
- `indexer_sync_tail_n{networkId}`
- `indexer_sync_progress_n{networkId}`
- `indexer_process_tail_n{networkId}`
- `indexer_process_progress_n{networkId}`

The current `indexer_link_n{networkId}_a{addressId}` cursor is optional. With a compact `direct_links` graph, relinking can replay from graph facts without a per-seed block cursor.

## Module Boundary Shape

Keep the modular-monolith boundaries by moving chain-specific derivation into the indexing module and leaving storage details in platform adapters.

Recommended application-layer split inside `indexing-pipeline`:

- `BlockSyncService`
- `DogecoinBlockProjector`
- `SourceLinkProjector`
- `IndexingPipelineService`
  - orchestration only

Recommended new contracts inside `indexing-pipeline`:

- replayable raw block storage port,
- projection warehouse write/read port for block batches and graph traversal,
- label seed port for pending relinks.

Platform remains responsible for:

- RPC calls,
- file or S3 raw storage,
- DuckDB or ClickHouse persistence,
- metadata-backed label queue implementation.

## Investigation Query Impact

The investigation module can stay mostly intact if the warehouse adapter maps the richer read model back onto the existing query contract.

- `getBalancesByAddresses(addresses)` should read `balances`
- `getDistinctLinksByAddresses(addresses)` should read `source_links`
- `transferCount` in the current contract should be replaced or reinterpreted as real `hop_count`

Short term, the contract can keep the same field name and map it from `hop_count`. Long term, the query contract should rename it.

## Implementation Order

1. Extend indexing-pipeline ports for replayable raw reads, label seed enumeration, and block projection writes.
2. Split the runtime loop into sync and process stages.
3. Implement `DogecoinBlockProjector` with deterministic fixtures:
   - coinbase-only block
   - simple spend
   - multi-input with change
   - same-block spend
4. Materialize `balances` and `direct_links`.
5. Implement labeled `source_links` replay for newly added addresses.
6. Switch `InvestigationQueryService` to consume real path links.
