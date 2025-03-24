-- Create the Kafka engine table to ingest Protobuf messages
CREATE TABLE IF NOT EXISTS kafka_qos_data
(
    event_time DateTime DEFAULT now(),
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
    kafka_format = 'Protobuf',
    kafka_schema = 'schema.proto:qos.ClientQueryProtobuf',
    kafka_max_block_size = 1,
    kafka_poll_timeout_ms = 500;

-- Create the destination table for the raw data
CREATE TABLE IF NOT EXISTS raw_qos_data
(
    event_time DateTime DEFAULT now(),
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
TTL event_time + INTERVAL 7 DAY;

-- Create a simplified materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_raw_qos_data TO raw_qos_data AS
SELECT * FROM kafka_qos_data;

-- Create the MaterializedView to process and store data from Kafka
CREATE TABLE IF NOT EXISTS qos_data
(
    event_time DateTime DEFAULT now(),
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
ORDER BY (event_time, gateway_id, query_id);

-- Only create the materialized view if it doesn't exist already
CREATE MATERIALIZED VIEW IF NOT EXISTS qos_data_mv TO qos_data AS
SELECT * FROM kafka_qos_data;
