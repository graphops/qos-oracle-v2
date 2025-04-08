-- Create the Kafka engine table to ingest Protobuf messages
CREATE TABLE IF NOT EXISTS kafka_qos_data
(
    gateway_id String,
    receipt_signer String,
    query_id String,
    api_key String,
    user_id String,
    subgraph Nullable(String),
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

-- ============================================================
-- MINUTE-LEVEL AGGREGATION TABLES & MATERIALIZED VIEWS
-- ============================================================
-- We store aggregate state only at the 1-minute level.
-- Coarser granularities (5min, hourly, daily) are calculated
-- on the fly in the final views using -Merge functions.
-- Materialized Views now read directly from kafka_qos_data.
-- ============================================================

-- ----------------------------------------
-- Deployment Level Aggregations (1-Minute State)
-- ----------------------------------------
CREATE TABLE IF NOT EXISTS agg_deployment_1min
(
    time_bucket DateTime, -- Minute-level bucket
    subgraph String,
    gateway_id String,
    -- Aggregate state columns (same as before)
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
PARTITION BY toYYYYMM(time_bucket) -- Partitioning is still useful
ORDER BY (time_bucket, subgraph, gateway_id) -- Grouping keys
TTL time_bucket + INTERVAL 7 DAY DELETE; -- TTL for 1-minute aggregate state (adjust as needed)

-- MV to populate 1-Minute Deployment Aggregations (Reads from Kafka)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_deployment_1min TO agg_deployment_1min AS
SELECT
    toStartOfMinute(now()) AS time_bucket, -- Use 1-minute bucket based on processing time
    kq.subgraph,
    kq.gateway_id,
    -- Calculate aggregate states directly from Kafka data
    countState() AS query_count,
    countIfState(kq.result = 'success') AS success_count,
    countIfState(kq.result != 'success') AS failure_count,
    sumState(kq.total_fees_usd) AS total_fees_usd,
    avgState(kq.response_time_ms) AS avg_response_time_ms,
    maxState(kq.response_time_ms) AS max_response_time_ms,
    quantilesState(0.90, 0.99)(kq.response_time_ms) AS quantiles_response_time_ms,
    stddevSampState(kq.response_time_ms) AS stddev_response_time_ms,
    avgState(kq.total_fees_usd) AS avg_fee_usd, -- Assuming total_fees_usd is per-query fee
    maxState(kq.total_fees_usd) AS max_fee_usd,
    quantilesState(0.90, 0.99)(kq.total_fees_usd) AS quantiles_fee_usd,
    stddevSampState(kq.total_fees_usd) AS stddev_fee_usd
FROM kafka_qos_data AS kq -- Read directly from Kafka table
WHERE kq.subgraph IS NOT NULL
GROUP BY time_bucket, kq.subgraph, kq.gateway_id;


-- ----------------------------------------
-- Allocation Level Aggregations (1-Minute State)
-- ----------------------------------------
CREATE TABLE IF NOT EXISTS agg_allocation_1min
(
    time_bucket DateTime, -- Minute-level bucket
    subgraph String,
    indexer String, -- HEX encoded
    gateway_id String,
    -- Aggregate state columns
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
PARTITION BY toYYYYMM(time_bucket)
ORDER BY (time_bucket, subgraph, indexer, gateway_id) -- Grouping keys
TTL time_bucket + INTERVAL 7 DAY DELETE; -- TTL for 1-minute aggregate state

-- MV to populate 1-Minute Allocation Aggregations (Reads from Kafka)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_allocation_1min TO agg_allocation_1min AS
SELECT
    toStartOfMinute(now()) AS time_bucket, -- Use 1-minute bucket based on processing time
    kq.subgraph,
    HEX(iq.indexer) AS indexer, -- Apply HEX encoding here
    kq.gateway_id,
    -- Calculate aggregate states directly from Kafka data
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
FROM kafka_qos_data AS kq -- Read directly from Kafka table
ARRAY JOIN indexer_queries AS iq
WHERE kq.subgraph IS NOT NULL AND length(iq.indexer) > 0
GROUP BY time_bucket, kq.subgraph, indexer, kq.gateway_id; -- Group by HEX encoded indexer


-- ----------------------------------------
-- Indexer Level Aggregations (1-Minute State)
-- ----------------------------------------
CREATE TABLE IF NOT EXISTS agg_indexer_1min
(
    time_bucket DateTime, -- Minute-level bucket
    indexer String, -- HEX encoded
    gateway_id String,
    -- Aggregate state columns (same structure as allocation, but grouped differently)
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
PARTITION BY toYYYYMM(time_bucket)
ORDER BY (time_bucket, indexer, gateway_id) -- Grouping keys
TTL time_bucket + INTERVAL 7 DAY DELETE; -- TTL for 1-minute aggregate state

-- MV to populate 1-Minute Indexer Aggregations (Reads from Kafka)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_indexer_1min TO agg_indexer_1min AS
SELECT
    toStartOfMinute(now()) AS time_bucket, -- Use 1-minute bucket based on processing time
    HEX(iq.indexer) AS indexer, -- Apply HEX encoding here
    kq.gateway_id,
    -- Calculate aggregate states directly from Kafka data
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
FROM kafka_qos_data AS kq -- Read directly from Kafka table
ARRAY JOIN indexer_queries AS iq
WHERE length(iq.indexer) > 0 -- Ensure indexer is present
GROUP BY time_bucket, indexer, kq.gateway_id; -- Group by HEX encoded indexer


-- ============================================================
-- FINAL AGGREGATION VIEWS (for GraphQL API)
-- ============================================================
-- These views read from the 1-minute aggregate state tables
-- and compute the final results for the desired time granularity
-- (5min, hourly, daily) on the fly using -Merge functions.
-- ============================================================

-- ----------------------------------------
-- Deployment Views (5min, Hourly, Daily)
-- ----------------------------------------

-- View for Final 5-Minute Deployment Aggregations
CREATE VIEW IF NOT EXISTS view_agg_deployment_5min AS
WITH AggregatedValues AS (
    SELECT
        toStartOfFiveMinute(time_bucket) AS final_time_bucket,
        subgraph,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        sumMerge(total_fees_usd) AS final_total_fees_usd,
        avgMerge(avg_response_time_ms) AS final_avg_response_time_ms,
        maxMerge(max_response_time_ms) AS final_max_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_response_time_ms) AS final_quantiles_response_time_ms,
        stddevSampMerge(stddev_response_time_ms) AS final_stddev_response_time_ms,
        avgMerge(avg_fee_usd) AS final_avg_fee_usd,
        maxMerge(max_fee_usd) AS final_max_fee_usd,
        quantilesMerge(0.90, 0.99)(quantiles_fee_usd) AS final_quantiles_fee_usd,
        stddevSampMerge(stddev_fee_usd) AS final_stddev_fee_usd
    FROM agg_deployment_1min
    GROUP BY final_time_bucket, subgraph, gateway_id
)
SELECT
    final_time_bucket AS time_bucket,
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

-- View for Final Hourly Deployment Aggregations
CREATE VIEW IF NOT EXISTS view_agg_deployment_hourly AS
WITH AggregatedValues AS (
    SELECT
        toStartOfHour(time_bucket) AS final_time_bucket,
        subgraph,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        sumMerge(total_fees_usd) AS final_total_fees_usd,
        avgMerge(avg_response_time_ms) AS final_avg_response_time_ms,
        maxMerge(max_response_time_ms) AS final_max_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_response_time_ms) AS final_quantiles_response_time_ms,
        stddevSampMerge(stddev_response_time_ms) AS final_stddev_response_time_ms,
        avgMerge(avg_fee_usd) AS final_avg_fee_usd,
        maxMerge(max_fee_usd) AS final_max_fee_usd,
        quantilesMerge(0.90, 0.99)(quantiles_fee_usd) AS final_quantiles_fee_usd,
        stddevSampMerge(stddev_fee_usd) AS final_stddev_fee_usd
    FROM agg_deployment_1min
    GROUP BY final_time_bucket, subgraph, gateway_id
)
SELECT
    final_time_bucket AS time_bucket,
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

-- View for Final Daily Deployment Aggregations
CREATE VIEW IF NOT EXISTS view_agg_deployment_daily AS
WITH AggregatedValues AS (
    SELECT
        toStartOfDay(time_bucket) AS final_time_bucket,
        subgraph,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        sumMerge(total_fees_usd) AS final_total_fees_usd,
        avgMerge(avg_response_time_ms) AS final_avg_response_time_ms,
        maxMerge(max_response_time_ms) AS final_max_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_response_time_ms) AS final_quantiles_response_time_ms,
        stddevSampMerge(stddev_response_time_ms) AS final_stddev_response_time_ms,
        avgMerge(avg_fee_usd) AS final_avg_fee_usd,
        maxMerge(max_fee_usd) AS final_max_fee_usd,
        quantilesMerge(0.90, 0.99)(quantiles_fee_usd) AS final_quantiles_fee_usd,
        stddevSampMerge(stddev_fee_usd) AS final_stddev_fee_usd
    FROM agg_deployment_1min
    GROUP BY final_time_bucket, subgraph, gateway_id
)
SELECT
    final_time_bucket AS time_bucket,
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


-- ----------------------------------------
-- Allocation Views (5min, Hourly, Daily)
-- ----------------------------------------

-- View for Final 5-Minute Allocation Aggregations
CREATE VIEW IF NOT EXISTS view_agg_allocation_5min AS
WITH AggregatedValues AS (
    SELECT
        toStartOfFiveMinute(time_bucket) AS final_time_bucket,
        subgraph,
        indexer,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        sumMerge(total_fee_grt) AS final_total_fee_grt,
        avgMerge(avg_indexer_response_time_ms) AS final_avg_indexer_response_time_ms,
        maxMerge(max_indexer_response_time_ms) AS final_max_indexer_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_indexer_response_time_ms) AS final_quantiles_indexer_response_time_ms,
        stddevSampMerge(stddev_indexer_response_time_ms) AS final_stddev_indexer_response_time_ms,
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
    FROM agg_allocation_1min
    GROUP BY final_time_bucket, subgraph, indexer, gateway_id
)
SELECT
    final_time_bucket AS time_bucket,
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

-- View for Final Hourly Allocation Aggregations
CREATE VIEW IF NOT EXISTS view_agg_allocation_hourly AS
WITH AggregatedValues AS (
    SELECT
        toStartOfHour(time_bucket) AS final_time_bucket,
        subgraph,
        indexer,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        sumMerge(total_fee_grt) AS final_total_fee_grt,
        avgMerge(avg_indexer_response_time_ms) AS final_avg_indexer_response_time_ms,
        maxMerge(max_indexer_response_time_ms) AS final_max_indexer_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_indexer_response_time_ms) AS final_quantiles_indexer_response_time_ms,
        stddevSampMerge(stddev_indexer_response_time_ms) AS final_stddev_indexer_response_time_ms,
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
    FROM agg_allocation_1min
    GROUP BY final_time_bucket, subgraph, indexer, gateway_id
)
SELECT
    final_time_bucket AS time_bucket,
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

-- View for Final Daily Allocation Aggregations
CREATE VIEW IF NOT EXISTS view_agg_allocation_daily AS
WITH AggregatedValues AS (
    SELECT
        toStartOfDay(time_bucket) AS final_time_bucket,
        subgraph,
        indexer,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        sumMerge(total_fee_grt) AS final_total_fee_grt,
        avgMerge(avg_indexer_response_time_ms) AS final_avg_indexer_response_time_ms,
        maxMerge(max_indexer_response_time_ms) AS final_max_indexer_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_indexer_response_time_ms) AS final_quantiles_indexer_response_time_ms,
        stddevSampMerge(stddev_indexer_response_time_ms) AS final_stddev_indexer_response_time_ms,
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
    FROM agg_allocation_1min
    GROUP BY final_time_bucket, subgraph, indexer, gateway_id
)
SELECT
    final_time_bucket AS time_bucket,
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


-- ----------------------------------------
-- Indexer Views (5min, Hourly, Daily)
-- ----------------------------------------

-- View for Final 5-Minute Indexer Aggregations
CREATE VIEW IF NOT EXISTS view_agg_indexer_5min AS
WITH AggregatedValues AS (
    SELECT
        toStartOfFiveMinute(time_bucket) AS final_time_bucket,
        indexer,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        sumMerge(total_fee_grt) AS final_total_fee_grt,
        avgMerge(avg_indexer_response_time_ms) AS final_avg_indexer_response_time_ms,
        maxMerge(max_indexer_response_time_ms) AS final_max_indexer_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_indexer_response_time_ms) AS final_quantiles_indexer_response_time_ms,
        stddevSampMerge(stddev_indexer_response_time_ms) AS final_stddev_indexer_response_time_ms,
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
    FROM agg_indexer_1min
    GROUP BY final_time_bucket, indexer, gateway_id
)
SELECT
    final_time_bucket AS time_bucket,
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

-- View for Final Hourly Indexer Aggregations
CREATE VIEW IF NOT EXISTS view_agg_indexer_hourly AS
WITH AggregatedValues AS (
    SELECT
        toStartOfHour(time_bucket) AS final_time_bucket,
        indexer,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        sumMerge(total_fee_grt) AS final_total_fee_grt,
        avgMerge(avg_indexer_response_time_ms) AS final_avg_indexer_response_time_ms,
        maxMerge(max_indexer_response_time_ms) AS final_max_indexer_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_indexer_response_time_ms) AS final_quantiles_indexer_response_time_ms,
        stddevSampMerge(stddev_indexer_response_time_ms) AS final_stddev_indexer_response_time_ms,
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
    FROM agg_indexer_1min
    GROUP BY final_time_bucket, indexer, gateway_id
)
SELECT
    final_time_bucket AS time_bucket,
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

-- View for Final Daily Indexer Aggregations
CREATE VIEW IF NOT EXISTS view_agg_indexer_daily AS
WITH AggregatedValues AS (
    SELECT
        toStartOfDay(time_bucket) AS final_time_bucket,
        indexer,
        gateway_id,
        countMerge(query_count) AS final_query_count,
        countIfMerge(success_count) AS final_success_count,
        countIfMerge(failure_count) AS final_failure_count,
        sumMerge(total_fee_grt) AS final_total_fee_grt,
        avgMerge(avg_indexer_response_time_ms) AS final_avg_indexer_response_time_ms,
        maxMerge(max_indexer_response_time_ms) AS final_max_indexer_response_time_ms,
        quantilesMerge(0.90, 0.99)(quantiles_indexer_response_time_ms) AS final_quantiles_indexer_response_time_ms,
        stddevSampMerge(stddev_indexer_response_time_ms) AS final_stddev_indexer_response_time_ms,
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
    FROM agg_indexer_1min
    GROUP BY final_time_bucket, indexer, gateway_id
)
SELECT
    final_time_bucket AS time_bucket,
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
