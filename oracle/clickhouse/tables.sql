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

-- Create the destination table for the raw data (with TTL)
-- Note: This table seems less critical now with the qos_data_mv directly populating qos_data,
-- but we keep it for potential raw data inspection if needed.
CREATE TABLE IF NOT EXISTS raw_qos_data
(
    event_time DateTime,
    gateway_id String,
    receipt_signer String, -- Raw bytes
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
        indexer String, -- Raw bytes
        deployment String, -- Raw bytes
        allocation String, -- Raw bytes
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
TTL event_time + INTERVAL 7 DAY; -- Keep raw data for 7 days

-- Materialized view to populate raw_qos_data (optional, could be removed if qos_data is sufficient)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_raw_qos_data TO raw_qos_data AS
SELECT
    now() as event_time,
    gateway_id,
    receipt_signer, -- Store raw bytes here
    query_id,
    api_key,
    user_id,
    subgraph,
    result,
    response_time_ms,
    request_bytes,
    response_bytes,
    total_fees_usd,
    indexer_queries -- Store raw nested bytes here
FROM kafka_qos_data;

-- Create the table for processed QoS data (with HEX encoding)
CREATE TABLE IF NOT EXISTS qos_data
(
    event_time DateTime,
    gateway_id String,
    receipt_signer String, -- HEX encoded
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
        indexer String, -- HEX encoded
        deployment String, -- HEX encoded
        allocation String, -- HEX encoded
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
PARTITION BY toYYYYMMDD(event_time)
TTL event_time + INTERVAL 30 DAY; -- Keep processed data longer, e.g., 30 days

-- Create the materialized view that processes and transforms the data into qos_data
CREATE MATERIALIZED VIEW IF NOT EXISTS qos_data_mv TO qos_data AS
SELECT
    now() as event_time,
    gateway_id,
    HEX(receipt_signer) AS receipt_signer, -- Apply HEX encoding
    query_id,
    api_key,
    user_id,
    subgraph,
    result,
    response_time_ms,
    request_bytes,
    response_bytes,
    total_fees_usd,
    -- Apply HEX encoding to nested fields
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

-- ============================================================
-- AGGREGATION TABLES & MATERIALIZED VIEWS
-- ============================================================

-- ----------------------------------------
-- Deployment Level Aggregations (by subgraph)
-- ----------------------------------------

-- 5-Minute Deployment Aggregations
CREATE TABLE IF NOT EXISTS agg_deployment_5min
(
    time_bucket DateTime,
    subgraph String, -- Deployment ID is the subgraph ID
    gateway_id String,
    query_count UInt64,
    success_count UInt64,
    failure_count UInt64,
    avg_response_time_ms Float64,
    total_fees_usd Float64
) ENGINE = SummingMergeTree() -- Use SummingMergeTree for counts/sums
ORDER BY (time_bucket, subgraph, gateway_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_deployment_5min TO agg_deployment_5min AS
SELECT
    toStartOfFiveMinutes(event_time) AS time_bucket,
    assumeNotNull(subgraph) AS subgraph, -- Treat Nullable subgraph as String for grouping
    gateway_id,
    count() AS query_count,
    countIf(result = 'success') AS success_count,
    countIf(result != 'success') AS failure_count,
    avg(response_time_ms) AS avg_response_time_ms,
    sum(total_fees_usd) AS total_fees_usd
FROM qos_data
WHERE subgraph IS NOT NULL -- Only aggregate for requests with a subgraph
GROUP BY time_bucket, subgraph, gateway_id;

-- Hourly Deployment Aggregations
CREATE TABLE IF NOT EXISTS agg_deployment_hourly
(
    time_bucket DateTime,
    subgraph String,
    gateway_id String,
    query_count UInt64,
    success_count UInt64,
    failure_count UInt64,
    avg_response_time_ms Float64,
    total_fees_usd Float64
) ENGINE = SummingMergeTree()
ORDER BY (time_bucket, subgraph, gateway_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_deployment_hourly TO agg_deployment_hourly AS
SELECT
    toStartOfHour(event_time) AS time_bucket,
    assumeNotNull(subgraph) AS subgraph,
    gateway_id,
    count() AS query_count,
    countIf(result = 'success') AS success_count,
    countIf(result != 'success') AS failure_count,
    avg(response_time_ms) AS avg_response_time_ms,
    sum(total_fees_usd) AS total_fees_usd
FROM qos_data
WHERE subgraph IS NOT NULL
GROUP BY time_bucket, subgraph, gateway_id;

-- Daily Deployment Aggregations
CREATE TABLE IF NOT EXISTS agg_deployment_daily
(
    time_bucket DateTime,
    subgraph String,
    gateway_id String,
    query_count UInt64,
    success_count UInt64,
    failure_count UInt64,
    avg_response_time_ms Float64,
    total_fees_usd Float64
) ENGINE = SummingMergeTree()
ORDER BY (time_bucket, subgraph, gateway_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_deployment_daily TO agg_deployment_daily AS
SELECT
    toStartOfDay(event_time) AS time_bucket,
    assumeNotNull(subgraph) AS subgraph,
    gateway_id,
    count() AS query_count,
    countIf(result = 'success') AS success_count,
    countIf(result != 'success') AS failure_count,
    avg(response_time_ms) AS avg_response_time_ms,
    sum(total_fees_usd) AS total_fees_usd
FROM qos_data
WHERE subgraph IS NOT NULL
GROUP BY time_bucket, subgraph, gateway_id;


-- ----------------------------------------
-- Indexer Level Aggregations (by indexer_queries.indexer)
-- ----------------------------------------

-- 5-Minute Indexer Aggregations
CREATE TABLE IF NOT EXISTS agg_indexer_5min
(
    time_bucket DateTime,
    indexer String, -- HEX encoded indexer ID
    gateway_id String,
    query_count UInt64,
    success_count UInt64,
    failure_count UInt64,
    avg_indexer_response_time_ms Float64,
    total_fee_grt Float64,
    avg_seconds_behind Float64,
    avg_blocks_behind Float64
) ENGINE = SummingMergeTree()
ORDER BY (time_bucket, indexer, gateway_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_indexer_5min TO agg_indexer_5min AS
SELECT
    toStartOfFiveMinutes(event_time) AS time_bucket,
    iq.indexer AS indexer,
    gateway_id,
    count() AS query_count,
    countIf(iq.result = 'success') AS success_count,
    countIf(iq.result != 'success') AS failure_count,
    avg(iq.response_time_ms) AS avg_indexer_response_time_ms,
    sum(iq.fee_grt) AS total_fee_grt,
    avg(iq.seconds_behind) AS avg_seconds_behind,
    avg(iq.blocks_behind) AS avg_blocks_behind
FROM qos_data
ARRAY JOIN indexer_queries AS iq -- Flatten the nested structure
GROUP BY time_bucket, indexer, gateway_id;

-- Hourly Indexer Aggregations
CREATE TABLE IF NOT EXISTS agg_indexer_hourly
(
    time_bucket DateTime,
    indexer String,
    gateway_id String,
    query_count UInt64,
    success_count UInt64,
    failure_count UInt64,
    avg_indexer_response_time_ms Float64,
    total_fee_grt Float64,
    avg_seconds_behind Float64,
    avg_blocks_behind Float64
) ENGINE = SummingMergeTree()
ORDER BY (time_bucket, indexer, gateway_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_indexer_hourly TO agg_indexer_hourly AS
SELECT
    toStartOfHour(event_time) AS time_bucket,
    iq.indexer AS indexer,
    gateway_id,
    count() AS query_count,
    countIf(iq.result = 'success') AS success_count,
    countIf(iq.result != 'success') AS failure_count,
    avg(iq.response_time_ms) AS avg_indexer_response_time_ms,
    sum(iq.fee_grt) AS total_fee_grt,
    avg(iq.seconds_behind) AS avg_seconds_behind,
    avg(iq.blocks_behind) AS avg_blocks_behind
FROM qos_data
ARRAY JOIN indexer_queries AS iq
GROUP BY time_bucket, indexer, gateway_id;

-- Daily Indexer Aggregations
CREATE TABLE IF NOT EXISTS agg_indexer_daily
(
    time_bucket DateTime,
    indexer String,
    gateway_id String,
    query_count UInt64,
    success_count UInt64,
    failure_count UInt64,
    avg_indexer_response_time_ms Float64,
    total_fee_grt Float64,
    avg_seconds_behind Float64,
    avg_blocks_behind Float64
) ENGINE = SummingMergeTree()
ORDER BY (time_bucket, indexer, gateway_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_indexer_daily TO agg_indexer_daily AS
SELECT
    toStartOfDay(event_time) AS time_bucket,
    iq.indexer AS indexer,
    gateway_id,
    count() AS query_count,
    countIf(iq.result = 'success') AS success_count,
    countIf(iq.result != 'success') AS failure_count,
    avg(iq.response_time_ms) AS avg_indexer_response_time_ms,
    sum(iq.fee_grt) AS total_fee_grt,
    avg(iq.seconds_behind) AS avg_seconds_behind,
    avg(iq.blocks_behind) AS avg_blocks_behind
FROM qos_data
ARRAY JOIN indexer_queries AS iq
GROUP BY time_bucket, indexer, gateway_id;


-- ----------------------------------------
-- Allocation Level Aggregations (by subgraph + indexer_queries.indexer)
-- NOTE: The name "Allocation Aggregation" here refers to the grouping level,
--       not the specific `allocation` ID field, which is removed.
-- ----------------------------------------

-- 5-Minute Allocation Aggregations
CREATE TABLE IF NOT EXISTS agg_allocation_5min
(
    time_bucket DateTime,
    subgraph String, -- Deployment ID
    indexer String,  -- HEX encoded indexer ID
    gateway_id String,
    query_count UInt64,
    success_count UInt64,
    failure_count UInt64,
    avg_indexer_response_time_ms Float64,
    total_fee_grt Float64,
    avg_seconds_behind Float64,
    avg_blocks_behind Float64
) ENGINE = SummingMergeTree()
ORDER BY (time_bucket, subgraph, indexer, gateway_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_allocation_5min TO agg_allocation_5min AS
SELECT
    toStartOfFiveMinutes(event_time) AS time_bucket,
    assumeNotNull(subgraph) AS subgraph,
    iq.indexer AS indexer,
    gateway_id,
    count() AS query_count,
    countIf(iq.result = 'success') AS success_count,
    countIf(iq.result != 'success') AS failure_count,
    avg(iq.response_time_ms) AS avg_indexer_response_time_ms,
    sum(iq.fee_grt) AS total_fee_grt,
    avg(iq.seconds_behind) AS avg_seconds_behind,
    avg(iq.blocks_behind) AS avg_blocks_behind
FROM qos_data
ARRAY JOIN indexer_queries AS iq
WHERE subgraph IS NOT NULL -- Only aggregate for requests with a subgraph
GROUP BY time_bucket, subgraph, indexer, gateway_id;

-- Hourly Allocation Aggregations
CREATE TABLE IF NOT EXISTS agg_allocation_hourly
(
    time_bucket DateTime,
    subgraph String,
    indexer String,
    gateway_id String,
    query_count UInt64,
    success_count UInt64,
    failure_count UInt64,
    avg_indexer_response_time_ms Float64,
    total_fee_grt Float64,
    avg_seconds_behind Float64,
    avg_blocks_behind Float64
) ENGINE = SummingMergeTree()
ORDER BY (time_bucket, subgraph, indexer, gateway_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_allocation_hourly TO agg_allocation_hourly AS
SELECT
    toStartOfHour(event_time) AS time_bucket,
    assumeNotNull(subgraph) AS subgraph,
    iq.indexer AS indexer,
    gateway_id,
    count() AS query_count,
    countIf(iq.result = 'success') AS success_count,
    countIf(iq.result != 'success') AS failure_count,
    avg(iq.response_time_ms) AS avg_indexer_response_time_ms,
    sum(iq.fee_grt) AS total_fee_grt,
    avg(iq.seconds_behind) AS avg_seconds_behind,
    avg(iq.blocks_behind) AS avg_blocks_behind
FROM qos_data
ARRAY JOIN indexer_queries AS iq
WHERE subgraph IS NOT NULL
GROUP BY time_bucket, subgraph, indexer, gateway_id;

-- Daily Allocation Aggregations
CREATE TABLE IF NOT EXISTS agg_allocation_daily
(
    time_bucket DateTime,
    subgraph String,
    indexer String,
    gateway_id String,
    query_count UInt64,
    success_count UInt64,
    failure_count UInt64,
    avg_indexer_response_time_ms Float64,
    total_fee_grt Float64,
    avg_seconds_behind Float64,
    avg_blocks_behind Float64
) ENGINE = SummingMergeTree()
ORDER BY (time_bucket, subgraph, indexer, gateway_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_allocation_daily TO agg_allocation_daily AS
SELECT
    toStartOfDay(event_time) AS time_bucket,
    assumeNotNull(subgraph) AS subgraph,
    iq.indexer AS indexer,
    gateway_id,
    count() AS query_count,
    countIf(iq.result = 'success') AS success_count,
    countIf(iq.result != 'success') AS failure_count,
    avg(iq.response_time_ms) AS avg_indexer_response_time_ms,
    sum(iq.fee_grt) AS total_fee_grt,
    avg(iq.seconds_behind) AS avg_seconds_behind,
    avg(iq.blocks_behind) AS avg_blocks_behind
FROM qos_data
ARRAY JOIN indexer_queries AS iq
WHERE subgraph IS NOT NULL
GROUP BY time_bucket, subgraph, indexer, gateway_id;
