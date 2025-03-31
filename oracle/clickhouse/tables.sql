-- Create the Kafka engine table to ingest Protobuf messages
CREATE TABLE IF NOT EXISTS kafka_qos_data
(
    gateway_id String,
    receipt_signer String,
    query_id String,
    api_key String,
    user_id String,
    subgraph String,
    result String,
    response_time_ms UInt32,
    request_bytes UInt32,
    response_bytes UInt32,
    total_fees_usd Float64,
    indexer_queries Nested(
        indexer String,
        deployment String,
        allocation String,
        indexed_chain String,
        url String,
        fee_grt Float64,
        response_time_ms UInt32,
        seconds_behind UInt32,
        result String,
        indexer_errors String,
        blocks_behind UInt64
    )
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'gateway_qos_topic',
    kafka_group_name = 'clickhouse_qos_consumer',
    kafka_format = 'ProtobufSingle',
    kafka_schema = 'schema.proto:qos.ClientQueryProtobuf',
    kafka_max_block_size = 1,
    kafka_poll_timeout_ms = 500;

-- Create the destination table for the raw data
CREATE TABLE IF NOT EXISTS raw_qos_data
(
    event_time DateTime,
    gateway_id String,
    receipt_signer String,
    query_id String,
    api_key String,
    user_id String,
    subgraph Nullable(String),
    result String,
    response_time_ms UInt32,
    request_bytes UInt32,
    response_bytes Nullable(UInt32),
    total_fees_usd Float64,
    indexer_queries Nested(
        indexer String,
        deployment String,
        allocation String,
        indexed_chain String,
        url String,
        fee_grt Float64,
        response_time_ms UInt32,
        seconds_behind UInt32,
        result String,
        indexer_errors String,
        blocks_behind UInt64
    )
) ENGINE = MergeTree()
ORDER BY (event_time, gateway_id)
PARTITION BY toYYYYMMDD(event_time)
TTL event_time + INTERVAL 7 DAY;

-- Create a simplified materialized view that also captures the timestamp
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_raw_qos_data TO raw_qos_data AS
SELECT
    now() as event_time,
    gateway_id,
    receipt_signer,
    query_id,
    api_key,
    user_id,
    subgraph,
    result,
    response_time_ms,
    request_bytes,
    response_bytes,
    total_fees_usd,
    indexer_queries
FROM kafka_qos_data;

-- Create the table for processed QoS data
CREATE TABLE IF NOT EXISTS qos_data
(
    event_time DateTime,
    gateway_id String,
    receipt_signer String,
    query_id String,
    api_key String,
    user_id String,
    subgraph Nullable(String),
    result String,
    response_time_ms UInt32,
    request_bytes UInt32,
    response_bytes Nullable(UInt32),
    total_fees_usd Float64,
    indexer_queries Nested(
        indexer String,
        deployment String,
        allocation String,
        indexed_chain String,
        url String,
        fee_grt Float64,
        response_time_ms UInt32,
        seconds_behind UInt32,
        result String,
        indexer_errors String,
        blocks_behind UInt64
    )
) ENGINE = MergeTree()
ORDER BY (event_time, gateway_id, query_id)
PARTITION BY toYYYYMMDD(event_time);

-- Create the materialized view that processes and transforms the data
CREATE MATERIALIZED VIEW IF NOT EXISTS qos_data_mv TO qos_data AS
SELECT
    now() as event_time,
    gateway_id,
    HEX(receipt_signer) AS receipt_signer,
    query_id,
    api_key,
    user_id,
    subgraph,
    result,
    response_time_ms,
    request_bytes,
    response_bytes,
    total_fees_usd,
    arrayMap(x -> HEX(x), indexer_queries.indexer) AS `indexer_queries.indexer`,
    arrayMap(x -> HEX(x), indexer_queries.deployment) AS `indexer_queries.deployment`,
    arrayMap(x -> HEX(x), indexer_queries.allocation) AS `indexer_queries.allocation`,
    indexer_queries.indexed_chain AS `indexer_queries.indexed_chain`,
    indexer_queries.url AS `indexer_queries.url`,
    indexer_queries.fee_grt AS `indexer_queries.fee_grt`,
    indexer_queries.response_time_ms AS `indexer_queries.response_time_ms`,
    indexer_queries.seconds_behind AS `indexer_queries.seconds_behind`,
    indexer_queries.result AS `indexer_queries.result`,
    indexer_queries.indexer_errors AS `indexer_queries.indexer_errors`,
    indexer_queries.blocks_behind AS `indexer_queries.blocks_behind`
FROM kafka_qos_data;
