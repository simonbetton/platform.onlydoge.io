CREATE DATABASE IF NOT EXISTS onlydoge;

CREATE TABLE IF NOT EXISTS onlydoge.applied_blocks
(
  network_id UInt64,
  block_height UInt64,
  block_hash String
)
ENGINE = MergeTree
ORDER BY (network_id, block_height, block_hash);

CREATE TABLE IF NOT EXISTS onlydoge.utxo_outputs
(
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  vout UInt64,
  output_key String,
  address String,
  script_type String,
  script_pub_key String,
  value_base String,
  is_coinbase UInt8,
  is_spendable UInt8,
  spent_by_txid Nullable(String),
  spent_in_block Nullable(UInt64),
  spent_input_index Nullable(UInt64)
)
ENGINE = MergeTree
ORDER BY (network_id, output_key);

CREATE TABLE IF NOT EXISTS onlydoge.address_movements
(
  movement_id String,
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  entry_index UInt64,
  address String,
  asset_address String,
  direction String,
  amount_base String,
  output_key Nullable(String),
  derivation_method String
)
ENGINE = MergeTree
ORDER BY (network_id, movement_id);

CREATE TABLE IF NOT EXISTS onlydoge.transfers
(
  transfer_id String,
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  transfer_index UInt64,
  asset_address String,
  from_address String,
  to_address String,
  amount_base String,
  derivation_method String,
  confidence Float64,
  is_change UInt8,
  input_address_count UInt64,
  output_address_count UInt64
)
ENGINE = MergeTree
ORDER BY (network_id, transfer_id);

CREATE TABLE IF NOT EXISTS onlydoge.balances
(
  network_id UInt64,
  address String,
  asset_address String,
  balance String,
  as_of_block_height UInt64
)
ENGINE = MergeTree
ORDER BY (network_id, address, asset_address);

CREATE TABLE IF NOT EXISTS onlydoge.direct_links
(
  network_id UInt64,
  from_address String,
  to_address String,
  asset_address String,
  transfer_count UInt64,
  total_amount_base String,
  first_seen_block_height UInt64,
  last_seen_block_height UInt64
)
ENGINE = MergeTree
ORDER BY (network_id, from_address, to_address, asset_address);

CREATE TABLE IF NOT EXISTS onlydoge.source_links
(
  network_id UInt64,
  source_address_id UInt64,
  source_address String,
  to_address String,
  hop_count UInt64,
  path_transfer_count UInt64,
  path_addresses Array(String),
  first_seen_block_height UInt64,
  last_seen_block_height UInt64
)
ENGINE = MergeTree
ORDER BY (network_id, source_address_id, to_address);

CREATE TABLE IF NOT EXISTS onlydoge.applied_blocks_v2
(
  network_id UInt64,
  block_height UInt64,
  block_hash String
)
ENGINE = MergeTree
ORDER BY (network_id, block_height, block_hash);

CREATE TABLE IF NOT EXISTS onlydoge.utxo_outputs_v2
(
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  vout UInt64,
  output_key String,
  address String,
  script_type String,
  script_pub_key String,
  value_base String,
  is_coinbase UInt8,
  is_spendable UInt8,
  spent_by_txid Nullable(String),
  spent_in_block Nullable(UInt64),
  spent_input_index Nullable(UInt64),
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (network_id, output_key)
SETTINGS old_parts_lifetime = 0;

CREATE TABLE IF NOT EXISTS onlydoge.utxo_outputs_current_v2
(
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  vout UInt64,
  output_key String,
  address String,
  script_type String,
  script_pub_key String,
  value_base String,
  is_coinbase UInt8,
  is_spendable UInt8,
  spent_by_txid Nullable(String),
  spent_in_block Nullable(UInt64),
  spent_input_index Nullable(UInt64),
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (network_id, output_key)
SETTINGS old_parts_lifetime = 0;

CREATE TABLE IF NOT EXISTS onlydoge.utxo_outputs_current_by_address_v2
(
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  vout UInt64,
  output_key String,
  address String,
  script_type String,
  script_pub_key String,
  value_base String,
  is_coinbase UInt8,
  is_spendable UInt8,
  spent_by_txid Nullable(String),
  spent_in_block Nullable(UInt64),
  spent_input_index Nullable(UInt64),
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (network_id, address, output_key)
SETTINGS old_parts_lifetime = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS onlydoge.utxo_outputs_current_by_address_v2_mv
TO onlydoge.utxo_outputs_current_by_address_v2
AS
SELECT
  network_id,
  block_height,
  block_hash,
  block_time,
  txid,
  tx_index,
  vout,
  output_key,
  address,
  script_type,
  script_pub_key,
  value_base,
  is_coinbase,
  is_spendable,
  spent_by_txid,
  spent_in_block,
  spent_input_index,
  version
FROM onlydoge.utxo_outputs_current_v2;

CREATE TABLE IF NOT EXISTS onlydoge.address_movements_v2
(
  movement_id String,
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  entry_index UInt64,
  address String,
  asset_address String,
  direction String,
  amount_base String,
  output_key Nullable(String),
  derivation_method String
)
ENGINE = MergeTree
ORDER BY (network_id, movement_id);

CREATE TABLE IF NOT EXISTS onlydoge.address_movements_by_address_v2
(
  movement_id String,
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  entry_index UInt64,
  address String,
  asset_address String,
  direction String,
  amount_base String,
  amount_base_i256 Int256 MATERIALIZED toInt256(amount_base),
  output_key Nullable(String),
  derivation_method String
)
ENGINE = MergeTree
ORDER BY (network_id, address, block_height, tx_index, entry_index, movement_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS onlydoge.address_movements_by_address_v2_mv
TO onlydoge.address_movements_by_address_v2
AS
SELECT
  movement_id,
  network_id,
  block_height,
  block_hash,
  block_time,
  txid,
  tx_index,
  entry_index,
  address,
  asset_address,
  direction,
  amount_base,
  output_key,
  derivation_method
FROM onlydoge.address_movements_v2;

CREATE TABLE IF NOT EXISTS onlydoge.transfers_v2
(
  transfer_id String,
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  transfer_index UInt64,
  asset_address String,
  from_address String,
  to_address String,
  amount_base String,
  derivation_method String,
  confidence Float64,
  is_change UInt8,
  input_address_count UInt64,
  output_address_count UInt64
)
ENGINE = MergeTree
ORDER BY (network_id, transfer_id);

CREATE TABLE IF NOT EXISTS onlydoge.balances_v2
(
  network_id UInt64,
  address String,
  asset_address String,
  balance String,
  as_of_block_height UInt64,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (network_id, address, asset_address);

CREATE TABLE IF NOT EXISTS onlydoge.direct_links_v2
(
  network_id UInt64,
  from_address String,
  to_address String,
  asset_address String,
  transfer_count UInt64,
  total_amount_base String,
  first_seen_block_height UInt64,
  last_seen_block_height UInt64,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (network_id, from_address, to_address, asset_address);

CREATE TABLE IF NOT EXISTS onlydoge.core_utxo_creates_v1
(
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  vout UInt64,
  output_key String,
  address String,
  script_type String,
  script_pub_key String,
  value_base String,
  is_coinbase UInt8,
  is_spendable UInt8,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (network_id, output_key);

CREATE TABLE IF NOT EXISTS onlydoge.core_utxo_spends_v1
(
  network_id UInt64,
  spent_output_key String,
  spent_by_txid String,
  spent_in_block UInt64,
  spent_input_index UInt64,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (network_id, spent_output_key);

CREATE TABLE IF NOT EXISTS onlydoge.core_processed_blocks_v1
(
  network_id UInt64,
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  tx_count UInt64,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (network_id, block_height);
