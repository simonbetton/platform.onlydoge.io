CREATE DATABASE IF NOT EXISTS onlydoge;

CREATE TABLE IF NOT EXISTS onlydoge.dogecoin_utxo_outputs_current_v1
(
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  vout UInt64,
  output_key String,
  address String,
  script_type String,
  value_base String,
  is_coinbase UInt8,
  is_spendable UInt8,
  spent_by_txid Nullable(String),
  spent_in_block Nullable(UInt64),
  spent_input_index Nullable(UInt64),
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY output_key
SETTINGS old_parts_lifetime = 0;

CREATE TABLE IF NOT EXISTS onlydoge.dogecoin_utxo_outputs_current_by_address_v1
(
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  vout UInt64,
  output_key String,
  address String,
  script_type String,
  value_base String,
  is_coinbase UInt8,
  is_spendable UInt8,
  spent_by_txid Nullable(String),
  spent_in_block Nullable(UInt64),
  spent_input_index Nullable(UInt64),
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (address, output_key)
SETTINGS old_parts_lifetime = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS onlydoge.dogecoin_utxo_outputs_current_by_address_v1_mv
TO onlydoge.dogecoin_utxo_outputs_current_by_address_v1
AS
SELECT
  block_height,
  block_hash,
  block_time,
  txid,
  tx_index,
  vout,
  output_key,
  address,
  script_type,
  value_base,
  is_coinbase,
  is_spendable,
  spent_by_txid,
  spent_in_block,
  spent_input_index,
  version
FROM onlydoge.dogecoin_utxo_outputs_current_v1;

CREATE TABLE IF NOT EXISTS onlydoge.dogecoin_address_movements_v1
(
  movement_id String,
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
ORDER BY movement_id;

CREATE TABLE IF NOT EXISTS onlydoge.dogecoin_address_movements_by_address_v1
(
  movement_id String,
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
ORDER BY (address, block_height, tx_index, entry_index, movement_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS onlydoge.dogecoin_address_movements_by_address_v1_mv
TO onlydoge.dogecoin_address_movements_by_address_v1
AS
SELECT
  movement_id,
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
FROM onlydoge.dogecoin_address_movements_v1;

CREATE TABLE IF NOT EXISTS onlydoge.dogecoin_balances_current_v1
(
  address String,
  asset_address String,
  balance String,
  as_of_block_height UInt64,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (address, asset_address);

CREATE TABLE IF NOT EXISTS onlydoge.dogecoin_applied_blocks_v1
(
  block_height UInt64,
  block_hash String
)
ENGINE = MergeTree
ORDER BY (block_height, block_hash);

CREATE TABLE IF NOT EXISTS onlydoge.dogecoin_core_utxo_creates_v1
(
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  vout UInt64,
  output_key String,
  address String,
  script_type String,
  value_base String,
  is_coinbase UInt8,
  is_spendable UInt8,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY output_key;

ALTER TABLE onlydoge.dogecoin_core_utxo_creates_v1
ADD INDEX IF NOT EXISTS core_utxo_creates_address_idx address TYPE bloom_filter(0.01) GRANULARITY 4;

CREATE TABLE IF NOT EXISTS onlydoge.dogecoin_core_utxo_spends_v1
(
  spent_output_key String,
  spent_by_txid String,
  spent_in_block UInt64,
  spent_input_index UInt64,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY spent_output_key;

CREATE TABLE IF NOT EXISTS onlydoge.dogecoin_core_processed_blocks_v1
(
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  tx_count UInt64,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY block_height;

CREATE TABLE IF NOT EXISTS onlydoge.analytics_transactions_v1
(
  block_height UInt64,
  block_hash String,
  block_time UInt64,
  txid String,
  tx_index UInt64,
  is_coinbase UInt8,
  input_count UInt64,
  output_count UInt64,
  total_input_base String,
  gross_output_base String,
  fee_base Nullable(String),
  total_input_base_i256 Int256 MATERIALIZED toInt256(total_input_base),
  gross_output_base_i256 Int256 MATERIALIZED toInt256(gross_output_base),
  fee_base_i256 Nullable(Int256) MATERIALIZED if(isNull(fee_base), NULL, toInt256(fee_base)),
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (block_time, block_height, tx_index, txid)
SETTINGS old_parts_lifetime = 0;

CREATE TABLE IF NOT EXISTS onlydoge.analytics_balances_current_v1
(
  address String,
  asset_address String,
  balance String,
  balance_i256 Int256 MATERIALIZED toInt256(balance),
  as_of_block_height UInt64,
  version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (asset_address, balance_i256, address)
SETTINGS old_parts_lifetime = 0;

CREATE TABLE IF NOT EXISTS onlydoge.mempool_samples_v1
(
  sampled_at DateTime,
  txid String,
  entry_time Nullable(UInt64),
  height Nullable(UInt64),
  size_bytes Nullable(UInt64),
  fee_base Nullable(String),
  fee_rate_base_per_kilobyte Nullable(String),
  raw_json String
)
ENGINE = MergeTree
ORDER BY (sampled_at, txid)
TTL sampled_at + INTERVAL 1 HOUR;
