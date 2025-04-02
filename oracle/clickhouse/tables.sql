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
    result String, -- 'success' or other status
    response_time_ms UInt32,
    request_bytes UInt32,
    response_bytes Nullable(UInt32),
    total_fees_usd Float64,
    indexer_queries Nested (
        indexer String,
        result String, -- 'success' or other status
        response_time_ms UInt32,
        fee_grt Float64,
        seconds_behind UInt32,
        blocks_behind UInt64
    )
) ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (gateway_id, event_time);

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

-- 5-Minute Deployment Aggregations (Table Definition)
CREATE TABLE IF NOT EXISTS agg_deployment_5min
(
    time_bucket DateTime,
    subgraph String,
    gateway_id String,
    query_count AggregateFunction(count),
    success_count AggregateFunction(countIf, UInt8),
    failure_count AggregateFunction(countIf, UInt8),
    total_fees_usd AggregateFunction(sum, Float64),
    avg_response_time_ms AggregateFunction(avg, UInt32),
    max_response_time_ms AggregateFunction(max, UInt32),
    quantiles_response_time_ms AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_response_time_ms AggregateFunction(stddevSamp, UInt32),
    avg_fee_usd AggregateFunction(avg, Float64),
    max_fee_usd AggregateFunction(max, Float64),
    quantiles_fee_usd AggregateFunction(quantiles(0.90, 0.99), Float64),
    stddev_fee_usd AggregateFunction(stddevSamp, Float64)
)
ENGINE = AggregatingMergeTree
ORDER BY (time_bucket, subgraph, gateway_id);

-- Materialized View for 5-Minute Deployment Aggregations (Corrected SELECT with ALL -State functions and alias 'qd')
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_deployment_5min TO agg_deployment_5min AS
SELECT
    toStartOfFiveMinute(qd.event_time) AS time_bucket,
    qd.subgraph,
    qd.gateway_id,
    countState() AS query_count,
    countIfState(qd.result = 'success') AS success_count,
    countIfState(qd.result != 'success') AS failure_count,
    sumState(qd.total_fees_usd) AS total_fees_usd,
    avgState(qd.response_time_ms) AS avg_response_time_ms,
    maxState(qd.response_time_ms) AS max_response_time_ms,
    quantilesState(0.90, 0.99)(qd.response_time_ms) AS quantiles_response_time_ms,
    stddevSampState(qd.response_time_ms) AS stddev_response_time_ms,
    avgState(qd.total_fees_usd) AS avg_fee_usd,
    maxState(qd.total_fees_usd) AS max_fee_usd,
    quantilesState(0.90, 0.99)(qd.total_fees_usd) AS quantiles_fee_usd,
    stddevSampState(qd.total_fees_usd) AS stddev_fee_usd
FROM qos_data AS qd -- Added alias
WHERE qd.subgraph IS NOT NULL
GROUP BY time_bucket, qd.subgraph, qd.gateway_id; -- Use alias in GROUP BY

-- Hourly Deployment Aggregations (Table Definition)
CREATE TABLE IF NOT EXISTS agg_deployment_hourly
(
    time_bucket DateTime,
    subgraph String,
    gateway_id String,
    query_count AggregateFunction(count),
    success_count AggregateFunction(countIf, UInt8),
    failure_count AggregateFunction(countIf, UInt8),
    total_fees_usd AggregateFunction(sum, Float64),
    avg_response_time_ms AggregateFunction(avg, UInt32),
    max_response_time_ms AggregateFunction(max, UInt32),
    quantiles_response_time_ms AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_response_time_ms AggregateFunction(stddevSamp, UInt32),
    avg_fee_usd AggregateFunction(avg, Float64),
    max_fee_usd AggregateFunction(max, Float64),
    quantiles_fee_usd AggregateFunction(quantiles(0.90, 0.99), Float64),
    stddev_fee_usd AggregateFunction(stddevSamp, Float64)
)
ENGINE = AggregatingMergeTree
ORDER BY (time_bucket, subgraph, gateway_id);

-- Materialized View for Hourly Deployment Aggregations (Corrected SELECT with ALL -State functions and alias 'qd')
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_deployment_hourly TO agg_deployment_hourly AS
SELECT
    toStartOfHour(qd.event_time) AS time_bucket, -- Changed time function
    qd.subgraph,
    qd.gateway_id,
    countState() AS query_count,
    countIfState(qd.result = 'success') AS success_count,
    countIfState(qd.result != 'success') AS failure_count,
    sumState(qd.total_fees_usd) AS total_fees_usd,
    avgState(qd.response_time_ms) AS avg_response_time_ms,
    maxState(qd.response_time_ms) AS max_response_time_ms,
    quantilesState(0.90, 0.99)(qd.response_time_ms) AS quantiles_response_time_ms,
    stddevSampState(qd.response_time_ms) AS stddev_response_time_ms,
    avgState(qd.total_fees_usd) AS avg_fee_usd,
    maxState(qd.total_fees_usd) AS max_fee_usd,
    quantilesState(0.90, 0.99)(qd.total_fees_usd) AS quantiles_fee_usd,
    stddevSampState(qd.total_fees_usd) AS stddev_fee_usd
FROM qos_data AS qd -- Added alias
WHERE qd.subgraph IS NOT NULL
GROUP BY time_bucket, qd.subgraph, qd.gateway_id; -- Use alias in GROUP BY

-- Daily Deployment Aggregations (Table Definition)
CREATE TABLE IF NOT EXISTS agg_deployment_daily
(
    time_bucket DateTime,
    subgraph String,
    gateway_id String,
    query_count AggregateFunction(count),
    success_count AggregateFunction(countIf, UInt8),
    failure_count AggregateFunction(countIf, UInt8),
    total_fees_usd AggregateFunction(sum, Float64),
    avg_response_time_ms AggregateFunction(avg, UInt32),
    max_response_time_ms AggregateFunction(max, UInt32),
    quantiles_response_time_ms AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_response_time_ms AggregateFunction(stddevSamp, UInt32),
    avg_fee_usd AggregateFunction(avg, Float64),
    max_fee_usd AggregateFunction(max, Float64),
    quantiles_fee_usd AggregateFunction(quantiles(0.90, 0.99), Float64),
    stddev_fee_usd AggregateFunction(stddevSamp, Float64)
)
ENGINE = AggregatingMergeTree
ORDER BY (time_bucket, subgraph, gateway_id);

-- Materialized View for Daily Deployment Aggregations (Corrected SELECT with ALL -State functions and alias 'qd')
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_deployment_daily TO agg_deployment_daily AS
SELECT
    toStartOfDay(qd.event_time) AS time_bucket, -- Changed time function
    qd.subgraph,
    qd.gateway_id,
    countState() AS query_count,
    countIfState(qd.result = 'success') AS success_count,
    countIfState(qd.result != 'success') AS failure_count,
    sumState(qd.total_fees_usd) AS total_fees_usd,
    avgState(qd.response_time_ms) AS avg_response_time_ms,
    maxState(qd.response_time_ms) AS max_response_time_ms,
    quantilesState(0.90, 0.99)(qd.response_time_ms) AS quantiles_response_time_ms,
    stddevSampState(qd.response_time_ms) AS stddev_response_time_ms,
    avgState(qd.total_fees_usd) AS avg_fee_usd,
    maxState(qd.total_fees_usd) AS max_fee_usd,
    quantilesState(0.90, 0.99)(qd.total_fees_usd) AS quantiles_fee_usd,
    stddevSampState(qd.total_fees_usd) AS stddev_fee_usd
FROM qos_data AS qd -- Added alias
WHERE qd.subgraph IS NOT NULL
GROUP BY time_bucket, qd.subgraph, qd.gateway_id; -- Use alias in GROUP BY


-- ----------------------------------------
-- Allocation Level Aggregations (by subgraph + indexer_queries.indexer)
-- ----------------------------------------

-- 5-Minute Allocation Aggregations (Table Definition)
CREATE TABLE IF NOT EXISTS agg_allocation_5min
(
    time_bucket DateTime,
    subgraph String,
    indexer String,
    gateway_id String,
    query_count AggregateFunction(count),
    success_count AggregateFunction(countIf, UInt8),
    failure_count AggregateFunction(countIf, UInt8),
    total_fee_grt AggregateFunction(sum, Float64),
    avg_indexer_response_time_ms AggregateFunction(avg, UInt32),
    max_indexer_response_time_ms AggregateFunction(max, UInt32),
    quantiles_indexer_response_time_ms AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_indexer_response_time_ms AggregateFunction(stddevSamp, UInt32),
    avg_fee_grt AggregateFunction(avg, Float64),
    max_fee_grt AggregateFunction(max, Float64),
    quantiles_fee_grt AggregateFunction(quantiles(0.90, 0.99), Float64),
    stddev_fee_grt AggregateFunction(stddevSamp, Float64),
    avg_seconds_behind AggregateFunction(avg, UInt32),
    max_seconds_behind AggregateFunction(max, UInt32),
    quantiles_seconds_behind AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_seconds_behind AggregateFunction(stddevSamp, UInt32),
    avg_blocks_behind AggregateFunction(avg, UInt64),
    max_blocks_behind AggregateFunction(max, UInt64),
    quantiles_blocks_behind AggregateFunction(quantiles(0.90, 0.99), UInt64),
    stddev_blocks_behind AggregateFunction(stddevSamp, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (time_bucket, subgraph, indexer, gateway_id);

-- Materialized View for 5-Minute Allocation Aggregations (Corrected SELECT with ALL -State functions and aliases 'qd', 'iq')
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_allocation_5min TO agg_allocation_5min AS
SELECT
    toStartOfFiveMinute(qd.event_time) AS time_bucket,
    qd.subgraph,
    iq.indexer AS indexer,
    qd.gateway_id,
    countState() AS query_count,
    countIfState(iq.result = 'success') AS success_count,
    countIfState(iq.result != 'success') AS failure_count,
    sumState(iq.fee_grt) AS total_fee_grt,
    avgState(iq.response_time_ms) AS avg_indexer_response_time_ms,
    maxState(iq.response_time_ms) AS max_indexer_response_time_ms,
    quantilesState(0.90, 0.99)(iq.response_time_ms) AS quantiles_indexer_response_time_ms,
    stddevSampState(iq.response_time_ms) AS stddev_indexer_response_time_ms,
    avgState(iq.fee_grt) AS avg_fee_grt,
    maxState(iq.fee_grt) AS max_fee_grt,
    quantilesState(0.90, 0.99)(iq.fee_grt) AS quantiles_fee_grt,
    stddevSampState(iq.fee_grt) AS stddev_fee_grt,
    avgState(iq.seconds_behind) AS avg_seconds_behind,
    maxState(iq.seconds_behind) AS max_seconds_behind,
    quantilesState(0.90, 0.99)(iq.seconds_behind) AS quantiles_seconds_behind,
    stddevSampState(iq.seconds_behind) AS stddev_seconds_behind,
    avgState(iq.blocks_behind) AS avg_blocks_behind,
    maxState(iq.blocks_behind) AS max_blocks_behind,
    quantilesState(0.90, 0.99)(iq.blocks_behind) AS quantiles_blocks_behind,
    stddevSampState(iq.blocks_behind) AS stddev_blocks_behind
FROM qos_data AS qd -- Added alias
ARRAY JOIN indexer_queries AS iq
WHERE qd.subgraph IS NOT NULL AND length(iq.indexer) > 0
GROUP BY time_bucket, qd.subgraph, indexer, qd.gateway_id; -- Use aliases in GROUP BY

-- Hourly Allocation Aggregations (Table Definition)
CREATE TABLE IF NOT EXISTS agg_allocation_hourly
(
    time_bucket DateTime,
    subgraph String,
    indexer String,
    gateway_id String,
    query_count AggregateFunction(count),
    success_count AggregateFunction(countIf, UInt8),
    failure_count AggregateFunction(countIf, UInt8),
    total_fee_grt AggregateFunction(sum, Float64),
    avg_indexer_response_time_ms AggregateFunction(avg, UInt32),
    max_indexer_response_time_ms AggregateFunction(max, UInt32),
    quantiles_indexer_response_time_ms AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_indexer_response_time_ms AggregateFunction(stddevSamp, UInt32),
    avg_fee_grt AggregateFunction(avg, Float64),
    max_fee_grt AggregateFunction(max, Float64),
    quantiles_fee_grt AggregateFunction(quantiles(0.90, 0.99), Float64),
    stddev_fee_grt AggregateFunction(stddevSamp, Float64),
    avg_seconds_behind AggregateFunction(avg, UInt32),
    max_seconds_behind AggregateFunction(max, UInt32),
    quantiles_seconds_behind AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_seconds_behind AggregateFunction(stddevSamp, UInt32),
    avg_blocks_behind AggregateFunction(avg, UInt64),
    max_blocks_behind AggregateFunction(max, UInt64),
    quantiles_blocks_behind AggregateFunction(quantiles(0.90, 0.99), UInt64),
    stddev_blocks_behind AggregateFunction(stddevSamp, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (time_bucket, subgraph, indexer, gateway_id);

-- Materialized View for Hourly Allocation Aggregations (Corrected SELECT with ALL -State functions and aliases 'qd', 'iq')
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_allocation_hourly TO agg_allocation_hourly AS
SELECT
    toStartOfHour(qd.event_time) AS time_bucket, -- Changed time function
    qd.subgraph,
    iq.indexer AS indexer,
    qd.gateway_id,
    countState() AS query_count,
    countIfState(iq.result = 'success') AS success_count,
    countIfState(iq.result != 'success') AS failure_count,
    sumState(iq.fee_grt) AS total_fee_grt,
    avgState(iq.response_time_ms) AS avg_indexer_response_time_ms,
    maxState(iq.response_time_ms) AS max_indexer_response_time_ms,
    quantilesState(0.90, 0.99)(iq.response_time_ms) AS quantiles_indexer_response_time_ms,
    stddevSampState(iq.response_time_ms) AS stddev_indexer_response_time_ms,
    avgState(iq.fee_grt) AS avg_fee_grt,
    maxState(iq.fee_grt) AS max_fee_grt,
    quantilesState(0.90, 0.99)(iq.fee_grt) AS quantiles_fee_grt,
    stddevSampState(iq.fee_grt) AS stddev_fee_grt,
    avgState(iq.seconds_behind) AS avg_seconds_behind,
    maxState(iq.seconds_behind) AS max_seconds_behind,
    quantilesState(0.90, 0.99)(iq.seconds_behind) AS quantiles_seconds_behind,
    stddevSampState(iq.seconds_behind) AS stddev_seconds_behind,
    avgState(iq.blocks_behind) AS avg_blocks_behind,
    maxState(iq.blocks_behind) AS max_blocks_behind,
    quantilesState(0.90, 0.99)(iq.blocks_behind) AS quantiles_blocks_behind,
    stddevSampState(iq.blocks_behind) AS stddev_blocks_behind
FROM qos_data AS qd -- Added alias
ARRAY JOIN indexer_queries AS iq
WHERE qd.subgraph IS NOT NULL AND length(iq.indexer) > 0
GROUP BY time_bucket, qd.subgraph, indexer, qd.gateway_id; -- Use aliases in GROUP BY

-- Daily Allocation Aggregations (Table Definition)
CREATE TABLE IF NOT EXISTS agg_allocation_daily
(
    time_bucket DateTime,
    subgraph String,
    indexer String,
    gateway_id String,
    query_count AggregateFunction(count),
    success_count AggregateFunction(countIf, UInt8),
    failure_count AggregateFunction(countIf, UInt8),
    total_fee_grt AggregateFunction(sum, Float64),
    avg_indexer_response_time_ms AggregateFunction(avg, UInt32),
    max_indexer_response_time_ms AggregateFunction(max, UInt32),
    quantiles_indexer_response_time_ms AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_indexer_response_time_ms AggregateFunction(stddevSamp, UInt32),
    avg_fee_grt AggregateFunction(avg, Float64),
    max_fee_grt AggregateFunction(max, Float64),
    quantiles_fee_grt AggregateFunction(quantiles(0.90, 0.99), Float64),
    stddev_fee_grt AggregateFunction(stddevSamp, Float64),
    avg_seconds_behind AggregateFunction(avg, UInt32),
    max_seconds_behind AggregateFunction(max, UInt32),
    quantiles_seconds_behind AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_seconds_behind AggregateFunction(stddevSamp, UInt32),
    avg_blocks_behind AggregateFunction(avg, UInt64),
    max_blocks_behind AggregateFunction(max, UInt64),
    quantiles_blocks_behind AggregateFunction(quantiles(0.90, 0.99), UInt64),
    stddev_blocks_behind AggregateFunction(stddevSamp, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (time_bucket, subgraph, indexer, gateway_id);

-- Materialized View for Daily Allocation Aggregations (Corrected SELECT with ALL -State functions and aliases 'qd', 'iq')
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_allocation_daily TO agg_allocation_daily AS
SELECT
    toStartOfDay(qd.event_time) AS time_bucket, -- Changed time function
    qd.subgraph,
    iq.indexer AS indexer,
    qd.gateway_id,
    countState() AS query_count,
    countIfState(iq.result = 'success') AS success_count,
    countIfState(iq.result != 'success') AS failure_count,
    sumState(iq.fee_grt) AS total_fee_grt,
    avgState(iq.response_time_ms) AS avg_indexer_response_time_ms,
    maxState(iq.response_time_ms) AS max_indexer_response_time_ms,
    quantilesState(0.90, 0.99)(iq.response_time_ms) AS quantiles_indexer_response_time_ms,
    stddevSampState(iq.response_time_ms) AS stddev_indexer_response_time_ms,
    avgState(iq.fee_grt) AS avg_fee_grt,
    maxState(iq.fee_grt) AS max_fee_grt,
    quantilesState(0.90, 0.99)(iq.fee_grt) AS quantiles_fee_grt,
    stddevSampState(iq.fee_grt) AS stddev_fee_grt,
    avgState(iq.seconds_behind) AS avg_seconds_behind,
    maxState(iq.seconds_behind) AS max_seconds_behind,
    quantilesState(0.90, 0.99)(iq.seconds_behind) AS quantiles_seconds_behind,
    stddevSampState(iq.seconds_behind) AS stddev_seconds_behind,
    avgState(iq.blocks_behind) AS avg_blocks_behind,
    maxState(iq.blocks_behind) AS max_blocks_behind,
    quantilesState(0.90, 0.99)(iq.blocks_behind) AS quantiles_blocks_behind,
    stddevSampState(iq.blocks_behind) AS stddev_blocks_behind
FROM qos_data AS qd -- Added alias
ARRAY JOIN indexer_queries AS iq
WHERE qd.subgraph IS NOT NULL AND length(iq.indexer) > 0
GROUP BY time_bucket, qd.subgraph, indexer, qd.gateway_id; -- Use aliases in GROUP BY


-- ----------------------------------------
-- Indexer Level Aggregations (by indexer_queries.indexer)
-- ----------------------------------------

-- 5-Minute Indexer Aggregations (Table Definition)
CREATE TABLE IF NOT EXISTS agg_indexer_5min
(
    time_bucket DateTime,
    indexer String,
    gateway_id String,
    query_count AggregateFunction(count),
    success_count AggregateFunction(countIf, UInt8),
    failure_count AggregateFunction(countIf, UInt8),
    total_fee_grt AggregateFunction(sum, Float64),
    avg_indexer_response_time_ms AggregateFunction(avg, UInt32),
    max_indexer_response_time_ms AggregateFunction(max, UInt32),
    quantiles_indexer_response_time_ms AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_indexer_response_time_ms AggregateFunction(stddevSamp, UInt32),
    avg_fee_grt AggregateFunction(avg, Float64),
    max_fee_grt AggregateFunction(max, Float64),
    quantiles_fee_grt AggregateFunction(quantiles(0.90, 0.99), Float64),
    stddev_fee_grt AggregateFunction(stddevSamp, Float64),
    avg_seconds_behind AggregateFunction(avg, UInt32),
    max_seconds_behind AggregateFunction(max, UInt32),
    quantiles_seconds_behind AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_seconds_behind AggregateFunction(stddevSamp, UInt32),
    avg_blocks_behind AggregateFunction(avg, UInt64),
    max_blocks_behind AggregateFunction(max, UInt64),
    quantiles_blocks_behind AggregateFunction(quantiles(0.90, 0.99), UInt64),
    stddev_blocks_behind AggregateFunction(stddevSamp, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (time_bucket, indexer, gateway_id);

-- Materialized View for 5-Minute Indexer Aggregations (Corrected SELECT with ALL -State functions and aliases 'qd', 'iq')
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_indexer_5min TO agg_indexer_5min AS
SELECT
    toStartOfFiveMinute(qd.event_time) AS time_bucket,
    iq.indexer AS indexer,
    qd.gateway_id,
    countState() AS query_count,
    countIfState(iq.result = 'success') AS success_count,
    countIfState(iq.result != 'success') AS failure_count,
    sumState(iq.fee_grt) AS total_fee_grt,
    avgState(iq.response_time_ms) AS avg_indexer_response_time_ms,
    maxState(iq.response_time_ms) AS max_indexer_response_time_ms,
    quantilesState(0.90, 0.99)(iq.response_time_ms) AS quantiles_indexer_response_time_ms,
    stddevSampState(iq.response_time_ms) AS stddev_indexer_response_time_ms,
    avgState(iq.fee_grt) AS avg_fee_grt,
    maxState(iq.fee_grt) AS max_fee_grt,
    quantilesState(0.90, 0.99)(iq.fee_grt) AS quantiles_fee_grt,
    stddevSampState(iq.fee_grt) AS stddev_fee_grt,
    avgState(iq.seconds_behind) AS avg_seconds_behind,
    maxState(iq.seconds_behind) AS max_seconds_behind,
    quantilesState(0.90, 0.99)(iq.seconds_behind) AS quantiles_seconds_behind,
    stddevSampState(iq.seconds_behind) AS stddev_seconds_behind,
    avgState(iq.blocks_behind) AS avg_blocks_behind,
    maxState(iq.blocks_behind) AS max_blocks_behind,
    quantilesState(0.90, 0.99)(iq.blocks_behind) AS quantiles_blocks_behind,
    stddevSampState(iq.blocks_behind) AS stddev_blocks_behind
FROM qos_data AS qd -- Added alias
ARRAY JOIN indexer_queries AS iq
WHERE length(iq.indexer) > 0
GROUP BY time_bucket, indexer, qd.gateway_id; -- Use aliases in GROUP BY

-- Hourly Indexer Aggregations (Table Definition)
CREATE TABLE IF NOT EXISTS agg_indexer_hourly
(
    time_bucket DateTime,
    indexer String,
    gateway_id String,
    query_count AggregateFunction(count),
    success_count AggregateFunction(countIf, UInt8),
    failure_count AggregateFunction(countIf, UInt8),
    total_fee_grt AggregateFunction(sum, Float64),
    avg_indexer_response_time_ms AggregateFunction(avg, UInt32),
    max_indexer_response_time_ms AggregateFunction(max, UInt32),
    quantiles_indexer_response_time_ms AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_indexer_response_time_ms AggregateFunction(stddevSamp, UInt32),
    avg_fee_grt AggregateFunction(avg, Float64),
    max_fee_grt AggregateFunction(max, Float64),
    quantiles_fee_grt AggregateFunction(quantiles(0.90, 0.99), Float64),
    stddev_fee_grt AggregateFunction(stddevSamp, Float64),
    avg_seconds_behind AggregateFunction(avg, UInt32),
    max_seconds_behind AggregateFunction(max, UInt32),
    quantiles_seconds_behind AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_seconds_behind AggregateFunction(stddevSamp, UInt32),
    avg_blocks_behind AggregateFunction(avg, UInt64),
    max_blocks_behind AggregateFunction(max, UInt64),
    quantiles_blocks_behind AggregateFunction(quantiles(0.90, 0.99), UInt64),
    stddev_blocks_behind AggregateFunction(stddevSamp, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (time_bucket, indexer, gateway_id);

-- Materialized View for Hourly Indexer Aggregations (Corrected SELECT with ALL -State functions and aliases 'qd', 'iq')
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_indexer_hourly TO agg_indexer_hourly AS
SELECT
    toStartOfHour(qd.event_time) AS time_bucket, -- Changed time function
    iq.indexer AS indexer,
    qd.gateway_id,
    countState() AS query_count,
    countIfState(iq.result = 'success') AS success_count,
    countIfState(iq.result != 'success') AS failure_count,
    sumState(iq.fee_grt) AS total_fee_grt,
    avgState(iq.response_time_ms) AS avg_indexer_response_time_ms,
    maxState(iq.response_time_ms) AS max_indexer_response_time_ms,
    quantilesState(0.90, 0.99)(iq.response_time_ms) AS quantiles_indexer_response_time_ms,
    stddevSampState(iq.response_time_ms) AS stddev_indexer_response_time_ms,
    avgState(iq.fee_grt) AS avg_fee_grt,
    maxState(iq.fee_grt) AS max_fee_grt,
    quantilesState(0.90, 0.99)(iq.fee_grt) AS quantiles_fee_grt,
    stddevSampState(iq.fee_grt) AS stddev_fee_grt,
    avgState(iq.seconds_behind) AS avg_seconds_behind,
    maxState(iq.seconds_behind) AS max_seconds_behind,
    quantilesState(0.90, 0.99)(iq.seconds_behind) AS quantiles_seconds_behind,
    stddevSampState(iq.seconds_behind) AS stddev_seconds_behind,
    avgState(iq.blocks_behind) AS avg_blocks_behind,
    maxState(iq.blocks_behind) AS max_blocks_behind,
    quantilesState(0.90, 0.99)(iq.blocks_behind) AS quantiles_blocks_behind,
    stddevSampState(iq.blocks_behind) AS stddev_blocks_behind
FROM qos_data AS qd -- Added alias
ARRAY JOIN indexer_queries AS iq
WHERE length(iq.indexer) > 0
GROUP BY time_bucket, indexer, qd.gateway_id; -- Use aliases in GROUP BY

-- Daily Indexer Aggregations (Table Definition)
CREATE TABLE IF NOT EXISTS agg_indexer_daily
(
    time_bucket DateTime,
    indexer String,
    gateway_id String,
    query_count AggregateFunction(count),
    success_count AggregateFunction(countIf, UInt8),
    failure_count AggregateFunction(countIf, UInt8),
    total_fee_grt AggregateFunction(sum, Float64),
    avg_indexer_response_time_ms AggregateFunction(avg, UInt32),
    max_indexer_response_time_ms AggregateFunction(max, UInt32),
    quantiles_indexer_response_time_ms AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_indexer_response_time_ms AggregateFunction(stddevSamp, UInt32),
    avg_fee_grt AggregateFunction(avg, Float64),
    max_fee_grt AggregateFunction(max, Float64),
    quantiles_fee_grt AggregateFunction(quantiles(0.90, 0.99), Float64),
    stddev_fee_grt AggregateFunction(stddevSamp, Float64),
    avg_seconds_behind AggregateFunction(avg, UInt32),
    max_seconds_behind AggregateFunction(max, UInt32),
    quantiles_seconds_behind AggregateFunction(quantiles(0.90, 0.99), UInt32),
    stddev_seconds_behind AggregateFunction(stddevSamp, UInt32),
    avg_blocks_behind AggregateFunction(avg, UInt64),
    max_blocks_behind AggregateFunction(max, UInt64),
    quantiles_blocks_behind AggregateFunction(quantiles(0.90, 0.99), UInt64),
    stddev_blocks_behind AggregateFunction(stddevSamp, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (time_bucket, indexer, gateway_id);

-- Materialized View for Daily Indexer Aggregations (Corrected SELECT with ALL -State functions and aliases 'qd', 'iq')
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_indexer_daily TO agg_indexer_daily AS
SELECT
    toStartOfDay(qd.event_time) AS time_bucket, -- Changed time function
    iq.indexer AS indexer,
    qd.gateway_id,
    countState() AS query_count,
    countIfState(iq.result = 'success') AS success_count,
    countIfState(iq.result != 'success') AS failure_count,
    sumState(iq.fee_grt) AS total_fee_grt,
    avgState(iq.response_time_ms) AS avg_indexer_response_time_ms,
    maxState(iq.response_time_ms) AS max_indexer_response_time_ms,
    quantilesState(0.90, 0.99)(iq.response_time_ms) AS quantiles_indexer_response_time_ms,
    stddevSampState(iq.response_time_ms) AS stddev_indexer_response_time_ms,
    avgState(iq.fee_grt) AS avg_fee_grt,
    maxState(iq.fee_grt) AS max_fee_grt,
    quantilesState(0.90, 0.99)(iq.fee_grt) AS quantiles_fee_grt,
    stddevSampState(iq.fee_grt) AS stddev_fee_grt,
    avgState(iq.seconds_behind) AS avg_seconds_behind,
    maxState(iq.seconds_behind) AS max_seconds_behind,
    quantilesState(0.90, 0.99)(iq.seconds_behind) AS quantiles_seconds_behind,
    stddevSampState(iq.seconds_behind) AS stddev_seconds_behind,
    avgState(iq.blocks_behind) AS avg_blocks_behind,
    maxState(iq.blocks_behind) AS max_blocks_behind,
    quantilesState(0.90, 0.99)(iq.blocks_behind) AS quantiles_blocks_behind,
    stddevSampState(iq.blocks_behind) AS stddev_blocks_behind
FROM qos_data AS qd -- Added alias
ARRAY JOIN indexer_queries AS iq
WHERE length(iq.indexer) > 0
GROUP BY time_bucket, indexer, qd.gateway_id; -- Use aliases in GROUP BY

-- ----------------------------------------
-- Final Aggregation Views (Using -Merge with CTEs)
-- ----------------------------------------

-- View for Final 5-Minute Deployment Aggregations (Using CTE)
CREATE VIEW IF NOT EXISTS view_agg_deployment_5min AS
WITH AggregatedValues AS (
    -- Step 1: Perform all merges and grouping
    SELECT
        time_bucket,
        subgraph,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        avgMerge(avg_response_time_ms) AS final_avg_response_time_ms,
        maxMerge(max_response_time_ms) AS final_max_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_response_time_ms) AS final_quantiles_response_time_ms, -- Keep array
        stddevSampMerge(stddev_response_time_ms) AS final_stddev_response_time_ms,
        sumMerge(total_fees_usd) AS final_total_fees_usd,
        avgMerge(avg_fee_usd) AS final_avg_fee_usd,
        maxMerge(max_fee_usd) AS final_max_fee_usd,
        quantilesMerge(0.90, 0.99)(quantiles_fee_usd) AS final_quantiles_fee_usd, -- Keep array
        stddevSampMerge(stddev_fee_usd) AS final_stddev_fee_usd
    FROM agg_deployment_5min
    GROUP BY -- Group by dimensions only
        time_bucket,
        subgraph,
        gateway_id
)
-- Step 2: Select from CTE, extract percentiles, calculate proportion
SELECT
    time_bucket,
    subgraph,
    gateway_id,
    final_query_count AS query_count,
    final_success_count AS success_count,
    final_failure_count AS failure_count,
    final_avg_response_time_ms AS avg_response_time_ms,
    final_max_response_time_ms AS max_response_time_ms,
    final_quantiles_response_time_ms[1] AS p90_response_time_ms, -- Extract p90
    final_quantiles_response_time_ms[2] AS p99_response_time_ms, -- Extract p99
    final_stddev_response_time_ms AS stddev_response_time_ms,
    final_total_fees_usd AS total_fees_usd,
    final_avg_fee_usd AS avg_fee_usd,
    final_max_fee_usd AS max_fee_usd,
    final_quantiles_fee_usd[1] AS p90_fee_usd, -- Extract p90
    final_quantiles_fee_usd[2] AS p99_fee_usd, -- Extract p99
    final_stddev_fee_usd AS stddev_fee_usd,
    -- Calculate success proportion using aliases from CTE
    if(final_query_count = 0, 0.0, final_success_count / final_query_count) AS success_proportion
FROM AggregatedValues;

-- View for Final Hourly Deployment Aggregations (Using CTE)
CREATE VIEW IF NOT EXISTS view_agg_deployment_hourly AS
WITH AggregatedValues AS (
    SELECT
        time_bucket,
        subgraph,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        avgMerge(avg_response_time_ms) AS final_avg_response_time_ms,
        maxMerge(max_response_time_ms) AS final_max_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_response_time_ms) AS final_quantiles_response_time_ms,
        stddevSampMerge(stddev_response_time_ms) AS final_stddev_response_time_ms,
        sumMerge(total_fees_usd) AS final_total_fees_usd,
        avgMerge(avg_fee_usd) AS final_avg_fee_usd,
        maxMerge(max_fee_usd) AS final_max_fee_usd,
        quantilesMerge(0.90, 0.99)(quantiles_fee_usd) AS final_quantiles_fee_usd,
        stddevSampMerge(stddev_fee_usd) AS final_stddev_fee_usd
    FROM agg_deployment_hourly
    GROUP BY time_bucket, subgraph, gateway_id
)
SELECT
    time_bucket,
    subgraph,
    gateway_id,
    final_query_count AS query_count,
    final_success_count AS success_count,
    final_failure_count AS failure_count,
    final_avg_response_time_ms AS avg_response_time_ms,
    final_max_response_time_ms AS max_response_time_ms,
    final_quantiles_response_time_ms[1] AS p90_response_time_ms,
    final_quantiles_response_time_ms[2] AS p99_response_time_ms,
    final_stddev_response_time_ms AS stddev_response_time_ms,
    final_total_fees_usd AS total_fees_usd,
    final_avg_fee_usd AS avg_fee_usd,
    final_max_fee_usd AS max_fee_usd,
    final_quantiles_fee_usd[1] AS p90_fee_usd,
    final_quantiles_fee_usd[2] AS p99_fee_usd,
    final_stddev_fee_usd AS stddev_fee_usd,
    if(final_query_count = 0, 0.0, final_success_count / final_query_count) AS success_proportion
FROM AggregatedValues;

-- View for Final Daily Deployment Aggregations (Using CTE)
CREATE VIEW IF NOT EXISTS view_agg_deployment_daily AS
WITH AggregatedValues AS (
    SELECT
        time_bucket,
        subgraph,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        avgMerge(avg_response_time_ms) AS final_avg_response_time_ms,
        maxMerge(max_response_time_ms) AS final_max_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_response_time_ms) AS final_quantiles_response_time_ms,
        stddevSampMerge(stddev_response_time_ms) AS final_stddev_response_time_ms,
        sumMerge(total_fees_usd) AS final_total_fees_usd,
        avgMerge(avg_fee_usd) AS final_avg_fee_usd,
        maxMerge(max_fee_usd) AS final_max_fee_usd,
        quantilesMerge(0.90, 0.99)(quantiles_fee_usd) AS final_quantiles_fee_usd,
        stddevSampMerge(stddev_fee_usd) AS final_stddev_fee_usd
    FROM agg_deployment_daily
    GROUP BY time_bucket, subgraph, gateway_id
)
SELECT
    time_bucket,
    subgraph,
    gateway_id,
    final_query_count AS query_count,
    final_success_count AS success_count,
    final_failure_count AS failure_count,
    final_avg_response_time_ms AS avg_response_time_ms,
    final_max_response_time_ms AS max_response_time_ms,
    final_quantiles_response_time_ms[1] AS p90_response_time_ms,
    final_quantiles_response_time_ms[2] AS p99_response_time_ms,
    final_stddev_response_time_ms AS stddev_response_time_ms,
    final_total_fees_usd AS total_fees_usd,
    final_avg_fee_usd AS avg_fee_usd,
    final_max_fee_usd AS max_fee_usd,
    final_quantiles_fee_usd[1] AS p90_fee_usd,
    final_quantiles_fee_usd[2] AS p99_fee_usd,
    final_stddev_fee_usd AS stddev_fee_usd,
    if(final_query_count = 0, 0.0, final_success_count / final_query_count) AS success_proportion
FROM AggregatedValues;


-- View for Final 5-Minute Allocation Aggregations (Using CTE)
CREATE VIEW IF NOT EXISTS view_agg_allocation_5min AS
WITH AggregatedValues AS (
    SELECT
        time_bucket,
        subgraph,
        indexer,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        avgMerge(avg_indexer_response_time_ms) AS final_avg_indexer_response_time_ms,
        maxMerge(max_indexer_response_time_ms) AS final_max_indexer_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_indexer_response_time_ms) AS final_quantiles_indexer_response_time_ms,
        stddevSampMerge(stddev_indexer_response_time_ms) AS final_stddev_indexer_response_time_ms,
        sumMerge(total_fee_grt) AS final_total_fee_grt,
        avgMerge(avg_fee_grt) AS final_avg_fee_grt,
        maxMerge(max_fee_grt) AS final_max_fee_grt,
        quantilesMerge(0.90, 0.99)(quantiles_fee_grt) AS final_quantiles_fee_grt,
        stddevSampMerge(stddev_fee_grt) AS final_stddev_fee_grt,
        avgMerge(avg_seconds_behind) AS final_avg_seconds_behind,
        maxMerge(max_seconds_behind) AS final_max_seconds_behind,
        quantilesMerge(0.90, 0.99)(quantiles_seconds_behind) AS final_quantiles_seconds_behind,
        stddevSampMerge(stddev_seconds_behind) AS final_stddev_seconds_behind,
        avgMerge(avg_blocks_behind) AS final_avg_blocks_behind,
        maxMerge(max_blocks_behind) AS final_max_blocks_behind,
        quantilesMerge(0.90, 0.99)(quantiles_blocks_behind) AS final_quantiles_blocks_behind,
        stddevSampMerge(stddev_blocks_behind) AS final_stddev_blocks_behind
    FROM agg_allocation_5min
    GROUP BY time_bucket, subgraph, indexer, gateway_id
)
SELECT
    time_bucket,
    subgraph,
    indexer,
    gateway_id,
    final_query_count AS query_count,
    final_success_count AS success_count,
    final_failure_count AS failure_count,
    final_avg_indexer_response_time_ms AS avg_indexer_response_time_ms,
    final_max_indexer_response_time_ms AS max_indexer_response_time_ms,
    final_quantiles_indexer_response_time_ms[1] AS p90_indexer_response_time_ms,
    final_quantiles_indexer_response_time_ms[2] AS p99_indexer_response_time_ms,
    final_stddev_indexer_response_time_ms AS stddev_indexer_response_time_ms,
    final_total_fee_grt AS total_fee_grt,
    final_avg_fee_grt AS avg_fee_grt,
    final_max_fee_grt AS max_fee_grt,
    final_quantiles_fee_grt[1] AS p90_fee_grt,
    final_quantiles_fee_grt[2] AS p99_fee_grt,
    final_stddev_fee_grt AS stddev_fee_grt,
    final_avg_seconds_behind AS avg_seconds_behind,
    final_max_seconds_behind AS max_seconds_behind,
    final_quantiles_seconds_behind[1] AS p90_seconds_behind,
    final_quantiles_seconds_behind[2] AS p99_seconds_behind,
    final_stddev_seconds_behind AS stddev_seconds_behind,
    final_avg_blocks_behind AS avg_blocks_behind,
    final_max_blocks_behind AS max_blocks_behind,
    final_quantiles_blocks_behind[1] AS p90_blocks_behind,
    final_quantiles_blocks_behind[2] AS p99_blocks_behind,
    final_stddev_blocks_behind AS stddev_blocks_behind,
    if(final_query_count = 0, 0.0, final_success_count / final_query_count) AS success_proportion
FROM AggregatedValues;

-- View for Final Hourly Allocation Aggregations (Using CTE)
CREATE VIEW IF NOT EXISTS view_agg_allocation_hourly AS
WITH AggregatedValues AS (
    SELECT
        time_bucket,
        subgraph,
        indexer,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        avgMerge(avg_indexer_response_time_ms) AS final_avg_indexer_response_time_ms,
        maxMerge(max_indexer_response_time_ms) AS final_max_indexer_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_indexer_response_time_ms) AS final_quantiles_indexer_response_time_ms,
        stddevSampMerge(stddev_indexer_response_time_ms) AS final_stddev_indexer_response_time_ms,
        sumMerge(total_fee_grt) AS final_total_fee_grt,
        avgMerge(avg_fee_grt) AS final_avg_fee_grt,
        maxMerge(max_fee_grt) AS final_max_fee_grt,
        quantilesMerge(0.90, 0.99)(quantiles_fee_grt) AS final_quantiles_fee_grt,
        stddevSampMerge(stddev_fee_grt) AS final_stddev_fee_grt,
        avgMerge(avg_seconds_behind) AS final_avg_seconds_behind,
        maxMerge(max_seconds_behind) AS final_max_seconds_behind,
        quantilesMerge(0.90, 0.99)(quantiles_seconds_behind) AS final_quantiles_seconds_behind,
        stddevSampMerge(stddev_seconds_behind) AS final_stddev_seconds_behind,
        avgMerge(avg_blocks_behind) AS final_avg_blocks_behind,
        maxMerge(max_blocks_behind) AS final_max_blocks_behind,
        quantilesMerge(0.90, 0.99)(quantiles_blocks_behind) AS final_quantiles_blocks_behind,
        stddevSampMerge(stddev_blocks_behind) AS final_stddev_blocks_behind
    FROM agg_allocation_hourly
    GROUP BY time_bucket, subgraph, indexer, gateway_id
)
SELECT
    time_bucket,
    subgraph,
    indexer,
    gateway_id,
    final_query_count AS query_count,
    final_success_count AS success_count,
    final_failure_count AS failure_count,
    final_avg_indexer_response_time_ms AS avg_indexer_response_time_ms,
    final_max_indexer_response_time_ms AS max_indexer_response_time_ms,
    final_quantiles_indexer_response_time_ms[1] AS p90_indexer_response_time_ms,
    final_quantiles_indexer_response_time_ms[2] AS p99_indexer_response_time_ms,
    final_stddev_indexer_response_time_ms AS stddev_indexer_response_time_ms,
    final_total_fee_grt AS total_fee_grt,
    final_avg_fee_grt AS avg_fee_grt,
    final_max_fee_grt AS max_fee_grt,
    final_quantiles_fee_grt[1] AS p90_fee_grt,
    final_quantiles_fee_grt[2] AS p99_fee_grt,
    final_stddev_fee_grt AS stddev_fee_grt,
    final_avg_seconds_behind AS avg_seconds_behind,
    final_max_seconds_behind AS max_seconds_behind,
    final_quantiles_seconds_behind[1] AS p90_seconds_behind,
    final_quantiles_seconds_behind[2] AS p99_seconds_behind,
    final_stddev_seconds_behind AS stddev_seconds_behind,
    final_avg_blocks_behind AS avg_blocks_behind,
    final_max_blocks_behind AS max_blocks_behind,
    final_quantiles_blocks_behind[1] AS p90_blocks_behind,
    final_quantiles_blocks_behind[2] AS p99_blocks_behind,
    final_stddev_blocks_behind AS stddev_blocks_behind,
    if(final_query_count = 0, 0.0, final_success_count / final_query_count) AS success_proportion
FROM AggregatedValues;

-- View for Final Daily Allocation Aggregations (Using CTE)
CREATE VIEW IF NOT EXISTS view_agg_allocation_daily AS
WITH AggregatedValues AS (
    SELECT
        time_bucket,
        subgraph,
        indexer,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        avgMerge(avg_indexer_response_time_ms) AS final_avg_indexer_response_time_ms,
        maxMerge(max_indexer_response_time_ms) AS final_max_indexer_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_indexer_response_time_ms) AS final_quantiles_indexer_response_time_ms,
        stddevSampMerge(stddev_indexer_response_time_ms) AS final_stddev_indexer_response_time_ms,
        sumMerge(total_fee_grt) AS final_total_fee_grt,
        avgMerge(avg_fee_grt) AS final_avg_fee_grt,
        maxMerge(max_fee_grt) AS final_max_fee_grt,
        quantilesMerge(0.90, 0.99)(quantiles_fee_grt) AS final_quantiles_fee_grt,
        stddevSampMerge(stddev_fee_grt) AS final_stddev_fee_grt,
        avgMerge(avg_seconds_behind) AS final_avg_seconds_behind,
        maxMerge(max_seconds_behind) AS final_max_seconds_behind,
        quantilesMerge(0.90, 0.99)(quantiles_seconds_behind) AS final_quantiles_seconds_behind,
        stddevSampMerge(stddev_seconds_behind) AS final_stddev_seconds_behind,
        avgMerge(avg_blocks_behind) AS final_avg_blocks_behind,
        maxMerge(max_blocks_behind) AS final_max_blocks_behind,
        quantilesMerge(0.90, 0.99)(quantiles_blocks_behind) AS final_quantiles_blocks_behind,
        stddevSampMerge(stddev_blocks_behind) AS final_stddev_blocks_behind
    FROM agg_allocation_daily
    GROUP BY time_bucket, subgraph, indexer, gateway_id
)
SELECT
    time_bucket,
    subgraph,
    indexer,
    gateway_id,
    final_query_count AS query_count,
    final_success_count AS success_count,
    final_failure_count AS failure_count,
    final_avg_indexer_response_time_ms AS avg_indexer_response_time_ms,
    final_max_indexer_response_time_ms AS max_indexer_response_time_ms,
    final_quantiles_indexer_response_time_ms[1] AS p90_indexer_response_time_ms,
    final_quantiles_indexer_response_time_ms[2] AS p99_indexer_response_time_ms,
    final_stddev_indexer_response_time_ms AS stddev_indexer_response_time_ms,
    final_total_fee_grt AS total_fee_grt,
    final_avg_fee_grt AS avg_fee_grt,
    final_max_fee_grt AS max_fee_grt,
    final_quantiles_fee_grt[1] AS p90_fee_grt,
    final_quantiles_fee_grt[2] AS p99_fee_grt,
    final_stddev_fee_grt AS stddev_fee_grt,
    final_avg_seconds_behind AS avg_seconds_behind,
    final_max_seconds_behind AS max_seconds_behind,
    final_quantiles_seconds_behind[1] AS p90_seconds_behind,
    final_quantiles_seconds_behind[2] AS p99_seconds_behind,
    final_stddev_seconds_behind AS stddev_seconds_behind,
    final_avg_blocks_behind AS avg_blocks_behind,
    final_max_blocks_behind AS max_blocks_behind,
    final_quantiles_blocks_behind[1] AS p90_blocks_behind,
    final_quantiles_blocks_behind[2] AS p99_blocks_behind,
    final_stddev_blocks_behind AS stddev_blocks_behind,
    if(final_query_count = 0, 0.0, final_success_count / final_query_count) AS success_proportion
FROM AggregatedValues;


-- View for Final 5-Minute Indexer Aggregations (Using CTE)
CREATE VIEW IF NOT EXISTS view_agg_indexer_5min AS
WITH AggregatedValues AS (
    SELECT
        time_bucket,
        indexer,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        avgMerge(avg_indexer_response_time_ms) AS final_avg_indexer_response_time_ms,
        maxMerge(max_indexer_response_time_ms) AS final_max_indexer_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_indexer_response_time_ms) AS final_quantiles_indexer_response_time_ms,
        stddevSampMerge(stddev_indexer_response_time_ms) AS final_stddev_indexer_response_time_ms,
        sumMerge(total_fee_grt) AS final_total_fee_grt,
        avgMerge(avg_fee_grt) AS final_avg_fee_grt,
        maxMerge(max_fee_grt) AS final_max_fee_grt,
        quantilesMerge(0.90, 0.99)(quantiles_fee_grt) AS final_quantiles_fee_grt,
        stddevSampMerge(stddev_fee_grt) AS final_stddev_fee_grt,
        avgMerge(avg_seconds_behind) AS final_avg_seconds_behind,
        maxMerge(max_seconds_behind) AS final_max_seconds_behind,
        quantilesMerge(0.90, 0.99)(quantiles_seconds_behind) AS final_quantiles_seconds_behind,
        stddevSampMerge(stddev_seconds_behind) AS final_stddev_seconds_behind,
        avgMerge(avg_blocks_behind) AS final_avg_blocks_behind,
        maxMerge(max_blocks_behind) AS final_max_blocks_behind,
        quantilesMerge(0.90, 0.99)(quantiles_blocks_behind) AS final_quantiles_blocks_behind,
        stddevSampMerge(stddev_blocks_behind) AS final_stddev_blocks_behind
    FROM agg_indexer_5min
    GROUP BY time_bucket, indexer, gateway_id
)
SELECT
    time_bucket,
    indexer,
    gateway_id,
    final_query_count AS query_count,
    final_success_count AS success_count,
    final_failure_count AS failure_count,
    final_avg_indexer_response_time_ms AS avg_indexer_response_time_ms,
    final_max_indexer_response_time_ms AS max_indexer_response_time_ms,
    final_quantiles_indexer_response_time_ms[1] AS p90_indexer_response_time_ms,
    final_quantiles_indexer_response_time_ms[2] AS p99_indexer_response_time_ms,
    final_stddev_indexer_response_time_ms AS stddev_indexer_response_time_ms,
    final_total_fee_grt AS total_fee_grt,
    final_avg_fee_grt AS avg_fee_grt,
    final_max_fee_grt AS max_fee_grt,
    final_quantiles_fee_grt[1] AS p90_fee_grt,
    final_quantiles_fee_grt[2] AS p99_fee_grt,
    final_stddev_fee_grt AS stddev_fee_grt,
    final_avg_seconds_behind AS avg_seconds_behind,
    final_max_seconds_behind AS max_seconds_behind,
    final_quantiles_seconds_behind[1] AS p90_seconds_behind,
    final_quantiles_seconds_behind[2] AS p99_seconds_behind,
    final_stddev_seconds_behind AS stddev_seconds_behind,
    final_avg_blocks_behind AS avg_blocks_behind,
    final_max_blocks_behind AS max_blocks_behind,
    final_quantiles_blocks_behind[1] AS p90_blocks_behind,
    final_quantiles_blocks_behind[2] AS p99_blocks_behind,
    final_stddev_blocks_behind AS stddev_blocks_behind,
    if(final_query_count = 0, 0.0, final_success_count / final_query_count) AS success_proportion
FROM AggregatedValues;

-- View for Final Hourly Indexer Aggregations (Using CTE)
CREATE VIEW IF NOT EXISTS view_agg_indexer_hourly AS
WITH AggregatedValues AS (
    SELECT
        time_bucket,
        indexer,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        avgMerge(avg_indexer_response_time_ms) AS final_avg_indexer_response_time_ms,
        maxMerge(max_indexer_response_time_ms) AS final_max_indexer_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_indexer_response_time_ms) AS final_quantiles_indexer_response_time_ms,
        stddevSampMerge(stddev_indexer_response_time_ms) AS final_stddev_indexer_response_time_ms,
        sumMerge(total_fee_grt) AS final_total_fee_grt,
        avgMerge(avg_fee_grt) AS final_avg_fee_grt,
        maxMerge(max_fee_grt) AS final_max_fee_grt,
        quantilesMerge(0.90, 0.99)(quantiles_fee_grt) AS final_quantiles_fee_grt,
        stddevSampMerge(stddev_fee_grt) AS final_stddev_fee_grt,
        avgMerge(avg_seconds_behind) AS final_avg_seconds_behind,
        maxMerge(max_seconds_behind) AS final_max_seconds_behind,
        quantilesMerge(0.90, 0.99)(quantiles_seconds_behind) AS final_quantiles_seconds_behind,
        stddevSampMerge(stddev_seconds_behind) AS final_stddev_seconds_behind,
        avgMerge(avg_blocks_behind) AS final_avg_blocks_behind,
        maxMerge(max_blocks_behind) AS final_max_blocks_behind,
        quantilesMerge(0.90, 0.99)(quantiles_blocks_behind) AS final_quantiles_blocks_behind,
        stddevSampMerge(stddev_blocks_behind) AS final_stddev_blocks_behind
    FROM agg_indexer_hourly
    GROUP BY time_bucket, indexer, gateway_id
)
SELECT
    time_bucket,
    indexer,
    gateway_id,
    final_query_count AS query_count,
    final_success_count AS success_count,
    final_failure_count AS failure_count,
    final_avg_indexer_response_time_ms AS avg_indexer_response_time_ms,
    final_max_indexer_response_time_ms AS max_indexer_response_time_ms,
    final_quantiles_indexer_response_time_ms[1] AS p90_indexer_response_time_ms,
    final_quantiles_indexer_response_time_ms[2] AS p99_indexer_response_time_ms,
    final_stddev_indexer_response_time_ms AS stddev_indexer_response_time_ms,
    final_total_fee_grt AS total_fee_grt,
    final_avg_fee_grt AS avg_fee_grt,
    final_max_fee_grt AS max_fee_grt,
    final_quantiles_fee_grt[1] AS p90_fee_grt,
    final_quantiles_fee_grt[2] AS p99_fee_grt,
    final_stddev_fee_grt AS stddev_fee_grt,
    final_avg_seconds_behind AS avg_seconds_behind,
    final_max_seconds_behind AS max_seconds_behind,
    final_quantiles_seconds_behind[1] AS p90_seconds_behind,
    final_quantiles_seconds_behind[2] AS p99_seconds_behind,
    final_stddev_seconds_behind AS stddev_seconds_behind,
    final_avg_blocks_behind AS avg_blocks_behind,
    final_max_blocks_behind AS max_blocks_behind,
    final_quantiles_blocks_behind[1] AS p90_blocks_behind,
    final_quantiles_blocks_behind[2] AS p99_blocks_behind,
    final_stddev_blocks_behind AS stddev_blocks_behind,
    if(final_query_count = 0, 0.0, final_success_count / final_query_count) AS success_proportion
FROM AggregatedValues;

-- View for Final Daily Indexer Aggregations (Using CTE)
CREATE VIEW IF NOT EXISTS view_agg_indexer_daily AS
WITH AggregatedValues AS (
    SELECT
        time_bucket,
        indexer,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        avgMerge(avg_indexer_response_time_ms) AS final_avg_indexer_response_time_ms,
        maxMerge(max_indexer_response_time_ms) AS final_max_indexer_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_indexer_response_time_ms) AS final_quantiles_indexer_response_time_ms,
        stddevSampMerge(stddev_indexer_response_time_ms) AS final_stddev_indexer_response_time_ms,
        sumMerge(total_fee_grt) AS final_total_fee_grt,
        avgMerge(avg_fee_grt) AS final_avg_fee_grt,
        maxMerge(max_fee_grt) AS final_max_fee_grt,
        quantilesMerge(0.90, 0.99)(quantiles_fee_grt) AS final_quantiles_fee_grt,
        stddevSampMerge(stddev_fee_grt) AS final_stddev_fee_grt,
        avgMerge(avg_seconds_behind) AS final_avg_seconds_behind,
        maxMerge(max_seconds_behind) AS final_max_seconds_behind,
        quantilesMerge(0.90, 0.99)(quantiles_seconds_behind) AS final_quantiles_seconds_behind,
        stddevSampMerge(stddev_seconds_behind) AS final_stddev_seconds_behind,
        avgMerge(avg_blocks_behind) AS final_avg_blocks_behind,
        maxMerge(max_blocks_behind) AS final_max_blocks_behind,
        quantilesMerge(0.90, 0.99)(quantiles_blocks_behind) AS final_quantiles_blocks_behind,
        stddevSampMerge(stddev_blocks_behind) AS final_stddev_blocks_behind
    FROM agg_indexer_daily
    GROUP BY time_bucket, indexer, gateway_id
)
SELECT
    time_bucket,
    indexer,
    gateway_id,
    final_query_count AS query_count,
    final_success_count AS success_count,
    final_failure_count AS failure_count,
    final_avg_indexer_response_time_ms AS avg_indexer_response_time_ms,
    final_max_indexer_response_time_ms AS max_indexer_response_time_ms,
    final_quantiles_indexer_response_time_ms[1] AS p90_indexer_response_time_ms,
    final_quantiles_indexer_response_time_ms[2] AS p99_indexer_response_time_ms,
    final_stddev_indexer_response_time_ms AS stddev_indexer_response_time_ms,
    final_total_fee_grt AS total_fee_grt,
    final_avg_fee_grt AS avg_fee_grt,
    final_max_fee_grt AS max_fee_grt,
    final_quantiles_fee_grt[1] AS p90_fee_grt,
    final_quantiles_fee_grt[2] AS p99_fee_grt,
    final_stddev_fee_grt AS stddev_fee_grt,
    final_avg_seconds_behind AS avg_seconds_behind,
    final_max_seconds_behind AS max_seconds_behind,
    final_quantiles_seconds_behind[1] AS p90_seconds_behind,
    final_quantiles_seconds_behind[2] AS p99_seconds_behind,
    final_stddev_seconds_behind AS stddev_seconds_behind,
    final_avg_blocks_behind AS avg_blocks_behind,
    final_max_blocks_behind AS max_blocks_behind,
    final_quantiles_blocks_behind[1] AS p90_blocks_behind,
    final_quantiles_blocks_behind[2] AS p99_blocks_behind,
    final_stddev_blocks_behind AS stddev_blocks_behind,
    if(final_query_count = 0, 0.0, final_success_count / final_query_count) AS success_proportion
FROM AggregatedValues;
