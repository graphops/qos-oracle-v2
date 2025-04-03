use actix_web::{web, App, HttpResponse, HttpServer, Result};
use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Enum, InputObject, Object, Schema, SimpleObject,
};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse};
use chrono::{DateTime, TimeZone, Utc};
use clickhouse::{Client, Row};
use serde::Deserialize;
use std::env;
use tokio::signal::unix::{signal, SignalKind};
// use bs58;
// use hex;

// Complete QoS Report matching the database schema
#[derive(SimpleObject, Deserialize, Debug, Clone)]
// #[graphql(complex)] // Mark complex to add computed fields if needed later
struct QosReport {
    event_time: String, // Keep as String for GraphQL output
    gateway_id: String,
    receipt_signer: String,
    query_id: String,
    api_key: String,
    user_id: String,
    subgraph: Option<String>,
    result: String,
    response_time_ms: u32,
    request_bytes: u32,
    response_bytes: Option<u32>,
    total_fees_usd: f64,
    // We might add indexer_queries here later if needed, requires nested struct handling
}

/*
#[derive(SimpleObject, Deserialize, Serialize)]
struct IndexerQuery {
    indexer: String,
    deployment: String,
    allocation: String,
    indexed_chain: String,
    url: String,
    fee_grt: f64,
    response_time_ms: u32,
    seconds_behind: u32,
    result: String,
    indexer_errors: String,
    blocks_behind: u64,
}
*/

// ClickHouse Row implementation for direct native protocol deserialization
#[derive(Row, Deserialize, Debug, Clone)]
struct QosReportRow {
    event_time: u32, // Use u32 for Unix timestamp
    gateway_id: String,
    receipt_signer: String,
    query_id: String,
    api_key: String,
    user_id: String,
    subgraph: Option<String>,
    result: String,
    response_time_ms: u32,
    request_bytes: u32,
    response_bytes: Option<u32>,
    total_fees_usd: f64,
    // indexer_queries: Vec<IndexerQueryRow>, // Needs proper handling for Nested type if selected
}

// Count query result struct
#[derive(Row, Deserialize, Debug, Clone)]
struct CountResult {
    #[allow(dead_code)] // Allow dead code as it's used for deserialization only
    count: u64,
}

// --- Structs for Aggregated Data ---

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
enum AggregationInterval {
    FiveMinutes,
    Hourly,
    Daily,
}

impl AggregationInterval {
    fn table_suffix(&self) -> &'static str {
        match self {
            AggregationInterval::FiveMinutes => "5min",
            AggregationInterval::Hourly => "hourly",
            AggregationInterval::Daily => "daily",
        }
    }
}

// --- Deployment Aggregation Structs ---

#[derive(Row, Deserialize, Debug, Clone, SimpleObject)]
struct DeploymentAggregationRow {
    time_bucket: u32,
    subgraph: String,
    gateway_id: String,
    query_count: u64,
    success_count: u64,
    failure_count: u64,
    avg_response_time_ms: f64,
    max_response_time_ms: u32,
    p90_response_time_ms: f64,
    p99_response_time_ms: f64,
    stddev_response_time_ms: f64,
    total_fees_usd: f64,
    avg_fee_usd: f64,
    max_fee_usd: f64,
    p90_fee_usd: f64,
    p99_fee_usd: f64,
    stddev_fee_usd: f64,
    success_proportion: f64,
}

#[derive(SimpleObject, Debug, Clone)]
struct DeploymentAggregationOutput {
    time_bucket: String,
    subgraph: String,
    gateway_id: String,
    query_count: u64,
    success_count: u64,
    failure_count: u64,
    // Latency
    #[graphql(name = "avgResponseTimeMs")]
    avg_response_time_ms: Option<f64>,
    #[graphql(name = "maxResponseTimeMs")]
    max_response_time_ms: Option<u32>,
    #[graphql(name = "p90ResponseTimeMs")]
    p90_response_time_ms: Option<f64>,
    #[graphql(name = "p99ResponseTimeMs")]
    p99_response_time_ms: Option<f64>,
    #[graphql(name = "stddevResponseTimeMs")]
    stddev_response_time_ms: Option<f64>,
    // Fees
    #[graphql(name = "totalFeesUsd")]
    total_fees_usd: Option<f64>,
    #[graphql(name = "avgFeeUsd")]
    avg_fee_usd: Option<f64>,
    #[graphql(name = "maxFeeUsd")]
    max_fee_usd: Option<f64>,
    #[graphql(name = "p90FeeUsd")]
    p90_fee_usd: Option<f64>,
    #[graphql(name = "p99FeeUsd")]
    p99_fee_usd: Option<f64>,
    #[graphql(name = "stddevFeeUsd")]
    stddev_fee_usd: Option<f64>,
    // Success
    #[graphql(name = "successProportion")]
    success_proportion: Option<f64>,
}

// --- Indexer Aggregation Structs (Updated) ---

#[derive(Row, Deserialize, Debug, Clone, SimpleObject)]
struct IndexerAggregationRow {
    time_bucket: u32,
    indexer: String,
    gateway_id: String,
    query_count: u64,
    success_count: u64,
    failure_count: u64,
    avg_indexer_response_time_ms: f64,
    max_indexer_response_time_ms: u32,
    p90_indexer_response_time_ms: f64,
    p99_indexer_response_time_ms: f64,
    stddev_indexer_response_time_ms: f64,
    total_fee_grt: f64,
    avg_fee_grt: f64,
    max_fee_grt: f64,
    p90_fee_grt: f64,
    p99_fee_grt: f64,
    stddev_fee_grt: f64,
    avg_seconds_behind: f64,
    max_seconds_behind: u32,
    p90_seconds_behind: f64,
    p99_seconds_behind: f64,
    stddev_seconds_behind: f64,
    avg_blocks_behind: f64,
    max_blocks_behind: u64,
    p90_blocks_behind: f64,
    p99_blocks_behind: f64,
    stddev_blocks_behind: f64,
    success_proportion: f64,
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(name = "IndexerAggregation")] // Keep existing name if desired
struct IndexerAggregationOutput {
    time_bucket: String,
    indexer: String,
    gateway_id: String,
    query_count: u64,
    success_count: u64,
    failure_count: u64,
    // Latency
    #[graphql(name = "avgIndexerResponseTimeMs")]
    avg_indexer_response_time_ms: Option<f64>,
    #[graphql(name = "maxIndexerResponseTimeMs")]
    max_indexer_response_time_ms: Option<u32>,
    #[graphql(name = "p90IndexerResponseTimeMs")]
    p90_indexer_response_time_ms: Option<f64>,
    #[graphql(name = "p99IndexerResponseTimeMs")]
    p99_indexer_response_time_ms: Option<f64>,
    #[graphql(name = "stddevIndexerResponseTimeMs")]
    stddev_indexer_response_time_ms: Option<f64>,
    // Fees
    #[graphql(name = "totalFeeGrt")]
    total_fee_grt: Option<f64>,
    #[graphql(name = "avgFeeGrt")]
    avg_fee_grt: Option<f64>,
    #[graphql(name = "maxFeeGrt")]
    max_fee_grt: Option<f64>,
    #[graphql(name = "p90FeeGrt")]
    p90_fee_grt: Option<f64>,
    #[graphql(name = "p99FeeGrt")]
    p99_fee_grt: Option<f64>,
    #[graphql(name = "stddevFeeGrt")]
    stddev_fee_grt: Option<f64>,
    // Behindness
    #[graphql(name = "avgSecondsBehind")]
    avg_seconds_behind: Option<f64>,
    #[graphql(name = "maxSecondsBehind")]
    max_seconds_behind: Option<u32>,
    #[graphql(name = "p90SecondsBehind")]
    p90_seconds_behind: Option<f64>,
    #[graphql(name = "p99SecondsBehind")]
    p99_seconds_behind: Option<f64>,
    #[graphql(name = "stddevSecondsBehind")]
    stddev_seconds_behind: Option<f64>,
    #[graphql(name = "avgBlocksBehind")]
    avg_blocks_behind: Option<f64>,
    #[graphql(name = "maxBlocksBehind")]
    max_blocks_behind: Option<u64>,
    #[graphql(name = "p90BlocksBehind")]
    p90_blocks_behind: Option<f64>,
    #[graphql(name = "p99BlocksBehind")]
    p99_blocks_behind: Option<f64>,
    #[graphql(name = "stddevBlocksBehind")]
    stddev_blocks_behind: Option<f64>,
    // Success
    #[graphql(name = "successProportion")]
    success_proportion: Option<f64>,
}

// --- Allocation Aggregation Structs ---

#[derive(Row, Deserialize, Debug, Clone, SimpleObject)]
struct AllocationAggregationRow {
    time_bucket: u32,
    subgraph: String,
    indexer: String,
    gateway_id: String,
    query_count: u64,
    success_count: u64,
    failure_count: u64,
    avg_indexer_response_time_ms: f64,
    max_indexer_response_time_ms: u32,
    p90_indexer_response_time_ms: f64,
    p99_indexer_response_time_ms: f64,
    stddev_indexer_response_time_ms: f64,
    total_fee_grt: f64,
    avg_fee_grt: f64,
    max_fee_grt: f64,
    p90_fee_grt: f64,
    p99_fee_grt: f64,
    stddev_fee_grt: f64,
    avg_seconds_behind: f64,
    max_seconds_behind: u32,
    p90_seconds_behind: f64,
    p99_seconds_behind: f64,
    stddev_seconds_behind: f64,
    avg_blocks_behind: f64,
    max_blocks_behind: u64,
    p90_blocks_behind: f64,
    p99_blocks_behind: f64,
    stddev_blocks_behind: f64,
    success_proportion: f64,
}

#[derive(SimpleObject, Debug, Clone)]
struct AllocationAggregationOutput {
    time_bucket: String,
    subgraph: String,
    indexer: String,
    gateway_id: String,
    query_count: u64,
    success_count: u64,
    failure_count: u64,
    // Latency
    #[graphql(name = "avgIndexerResponseTimeMs")]
    avg_indexer_response_time_ms: Option<f64>,
    #[graphql(name = "maxIndexerResponseTimeMs")]
    max_indexer_response_time_ms: Option<u32>,
    #[graphql(name = "p90IndexerResponseTimeMs")]
    p90_indexer_response_time_ms: Option<f64>,
    #[graphql(name = "p99IndexerResponseTimeMs")]
    p99_indexer_response_time_ms: Option<f64>,
    #[graphql(name = "stddevIndexerResponseTimeMs")]
    stddev_indexer_response_time_ms: Option<f64>,
    // Fees
    #[graphql(name = "totalFeeGrt")]
    total_fee_grt: Option<f64>,
    #[graphql(name = "avgFeeGrt")]
    avg_fee_grt: Option<f64>,
    #[graphql(name = "maxFeeGrt")]
    max_fee_grt: Option<f64>,
    #[graphql(name = "p90FeeGrt")]
    p90_fee_grt: Option<f64>,
    #[graphql(name = "p99FeeGrt")]
    p99_fee_grt: Option<f64>,
    #[graphql(name = "stddevFeeGrt")]
    stddev_fee_grt: Option<f64>,
    // Behindness
    #[graphql(name = "avgSecondsBehind")]
    avg_seconds_behind: Option<f64>,
    #[graphql(name = "maxSecondsBehind")]
    max_seconds_behind: Option<u32>,
    #[graphql(name = "p90SecondsBehind")]
    p90_seconds_behind: Option<f64>,
    #[graphql(name = "p99SecondsBehind")]
    p99_seconds_behind: Option<f64>,
    #[graphql(name = "stddevSecondsBehind")]
    stddev_seconds_behind: Option<f64>,
    #[graphql(name = "avgBlocksBehind")]
    avg_blocks_behind: Option<f64>,
    #[graphql(name = "maxBlocksBehind")]
    max_blocks_behind: Option<u64>,
    #[graphql(name = "p90BlocksBehind")]
    p90_blocks_behind: Option<f64>,
    #[graphql(name = "p99BlocksBehind")]
    p99_blocks_behind: Option<f64>,
    #[graphql(name = "stddevBlocksBehind")]
    stddev_blocks_behind: Option<f64>,
    // Success
    #[graphql(name = "successProportion")]
    success_proportion: Option<f64>,
}

// --- Input Objects for Filtering ---

#[derive(InputObject, Debug)]
struct TimeRangeInput {
    /// Start time (inclusive), RFC3339 format (e.g., "2023-01-01T00:00:00Z")
    from: String,
    /// End time (exclusive), RFC3339 format (e.g., "2023-01-02T00:00:00Z")
    to: String,
}

#[derive(InputObject, Debug)]
struct AggregationFilterInput {
    /// Optional: Filter by gateway ID
    gateway_id: Option<String>,
    /// Optional: Filter by subgraph ID (deployment ID)
    subgraph: Option<String>,
    /// Optional: Filter by indexer ID (HEX encoded)
    indexer: Option<String>,
    /// Optional: Filter by allocation ID (HEX encoded)
    allocation: Option<String>,
}

// --- GraphQL Query Root ---

struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Query for raw QoS reports
    async fn qos_reports(
        &self,
        _ctx: &Context<'_>, // Prefix ctx with underscore
        time_range: TimeRangeInput,
        #[graphql(desc = "Optional filters for the query")] filter: Option<AggregationFilterInput>,
        #[graphql(desc = "Maximum number of records to return (default 1000, max 10000)")]
        limit: Option<i32>,
        #[graphql(desc = "Number of records to skip (for pagination)")] offset: Option<i32>,
    ) -> Result<Vec<QosReport>, String> {
        let client = get_clickhouse_client()?;

        // Build WHERE clause
        let mut conditions = Vec::new();
        let from_dt = DateTime::parse_from_rfc3339(&time_range.from)
            .map_err(|e| format!("Invalid 'from' timestamp format: {}", e))?
            .with_timezone(&Utc);
        let to_dt = DateTime::parse_from_rfc3339(&time_range.to)
            .map_err(|e| format!("Invalid 'to' timestamp format: {}", e))?
            .with_timezone(&Utc);

        conditions.push(format!("event_time >= toDateTime({})", from_dt.timestamp()));
        conditions.push(format!("event_time < toDateTime({})", to_dt.timestamp()));

        if let Some(f) = filter {
            if let Some(gw) = &f.gateway_id {
                conditions.push(format!("gateway_id = '{}'", gw));
            }
            if let Some(sg) = &f.subgraph {
                // Handle nullable subgraph field correctly
                conditions.push(format!("subgraph = '{}'", sg));
            }
            // Filtering by indexer/allocation on raw reports requires arrayExists/has,
            // which can be slow. Skipping for now.
            // if let Some(ix) = &f.indexer {
            //     conditions.push(format!("has(indexer_queries.indexer, '{}')", ix));
            // }
            // if let Some(alloc) = &f.allocation {
            //     conditions.push(format!("has(indexer_queries.allocation, '{}')", alloc));
            // }
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        let query_offset = offset.unwrap_or(0).max(0);
        let query_limit = limit.unwrap_or(1000).clamp(0, 10000); // Use clamp

        let query = format!(
            "SELECT event_time, gateway_id, receipt_signer, query_id, api_key, user_id, \
                    subgraph, result, response_time_ms, request_bytes, response_bytes, total_fees_usd \
             FROM qos_data {} ORDER BY event_time DESC LIMIT {} OFFSET {}",
            where_clause, query_limit, query_offset
        );

        println!("Executing query: {}", query);

        let rows = client
            .query(&query)
            .fetch_all::<QosReportRow>()
            .await
            .map_err(|e| format!("Database query failed: {}", e))?;

        // Convert to GraphQL output type
        Ok(rows
            .into_iter()
            .map(|row| QosReport {
                event_time: Utc
                    .timestamp_opt(row.event_time as i64, 0)
                    .single()
                    .map_or_else(|| "Invalid Timestamp".to_string(), |dt| dt.to_rfc3339()),
                gateway_id: row.gateway_id,
                receipt_signer: row.receipt_signer,
                query_id: row.query_id,
                api_key: row.api_key,
                user_id: row.user_id,
                subgraph: row.subgraph,
                result: row.result,
                response_time_ms: row.response_time_ms,
                request_bytes: row.request_bytes,
                response_bytes: row.response_bytes,
                total_fees_usd: row.total_fees_usd,
            })
            .collect())
    }

    /// Query for the total count of QoS reports matching the criteria
    async fn qos_reports_count(
        &self,
        _ctx: &Context<'_>, // Prefix ctx with underscore
        time_range: TimeRangeInput,
        #[graphql(desc = "Optional filters for the query")] filter: Option<AggregationFilterInput>,
    ) -> Result<u64, String> {
        let client = get_clickhouse_client()?;

        // Build WHERE clause
        let mut conditions = Vec::new();
        let from_dt = DateTime::parse_from_rfc3339(&time_range.from)
            .map_err(|e| format!("Invalid 'from' timestamp format: {}", e))?
            .with_timezone(&Utc);
        let to_dt = DateTime::parse_from_rfc3339(&time_range.to)
            .map_err(|e| format!("Invalid 'to' timestamp format: {}", e))?
            .with_timezone(&Utc);

        conditions.push(format!("event_time >= toDateTime({})", from_dt.timestamp()));
        conditions.push(format!("event_time < toDateTime({})", to_dt.timestamp()));

        if let Some(f) = filter {
            if let Some(gw) = &f.gateway_id {
                conditions.push(format!("gateway_id = '{}'", gw));
            }
            if let Some(sg) = &f.subgraph {
                // Handle nullable subgraph field correctly
                conditions.push(format!("subgraph = '{}'", sg));
            }
            // Filtering by indexer/allocation on raw reports requires arrayExists/has,
            // which can be slow. Skipping for now.
            // if let Some(ix) = &f.indexer {
            //     conditions.push(format!("has(indexer_queries.indexer, '{}')", ix));
            // }
            // if let Some(alloc) = &f.allocation {
            //     conditions.push(format!("has(indexer_queries.allocation, '{}')", alloc));
            // }
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!("SELECT count(*) FROM qos_data {}", where_clause);

        println!("Executing query: {}", query);

        let count = client
            .query(&query)
            .fetch_one::<CountResult>()
            .await
            .map_err(|e| format!("Database query failed: {}", e))?;

        Ok(count.count)
    }

    /// Query for Deployment level aggregations
    async fn deployment_aggregations(
        &self,
        _ctx: &Context<'_>, // Prefix ctx with underscore
        interval: AggregationInterval,
        time_range: TimeRangeInput,
        #[graphql(desc = "Optional filters for the query")] filter: Option<AggregationFilterInput>,
        #[graphql(desc = "Maximum number of records to return (default 1000, max 10000)")]
        limit: Option<i32>,
    ) -> Result<Vec<DeploymentAggregationOutput>, String> {
        let client = get_clickhouse_client()?;
        let view_name = format!("view_agg_deployment_{}", interval.table_suffix());

        // Build WHERE clause
        let mut conditions = Vec::new();
        let from_dt = DateTime::parse_from_rfc3339(&time_range.from)
            .map_err(|e| format!("Invalid 'from' timestamp format: {}", e))?
            .with_timezone(&Utc);
        let to_dt = DateTime::parse_from_rfc3339(&time_range.to)
            .map_err(|e| format!("Invalid 'to' timestamp format: {}", e))?
            .with_timezone(&Utc);
        conditions.push(format!(
            "time_bucket >= toDateTime({})",
            from_dt.timestamp()
        ));
        conditions.push(format!("time_bucket < toDateTime({})", to_dt.timestamp()));

        if let Some(f) = filter {
            if let Some(gw) = &f.gateway_id {
                conditions.push(format!("gateway_id = '{}'", gw));
            }
            if let Some(sg) = &f.subgraph {
                conditions.push(format!("subgraph = '{}'", sg));
            }
            // No indexer/allocation filters applicable here
        }

        let where_clause = format!("WHERE {}", conditions.join(" AND "));
        let query_limit = limit.unwrap_or(1000).clamp(0, 10000); // Use clamp

        // Query directly selects pre-aggregated columns, no further aggregation or GROUP BY
        let query = format!(
            "SELECT time_bucket, subgraph, gateway_id, \
                    query_count, success_count, failure_count, \
                    avg_response_time_ms, max_response_time_ms, p90_response_time_ms, p99_response_time_ms, stddev_response_time_ms, \
                    total_fees_usd, avg_fee_usd, max_fee_usd, p90_fee_usd, p99_fee_usd, stddev_fee_usd, \
                    success_proportion \
             FROM {} {} \
             ORDER BY time_bucket DESC, subgraph, gateway_id \
             LIMIT {}",
            view_name, where_clause, query_limit
        );

        println!("Executing query: {}", query);

        let rows = client
            .query(&query)
            .fetch_all::<DeploymentAggregationRow>() // Uses updated struct
            .await
            .map_err(|e| format!("Database query failed: {}", e))?;

        // Map directly, row-by-row, including new fields
        Ok(rows
            .into_iter()
            .map(|row| DeploymentAggregationOutput {
                // Uses updated struct
                time_bucket: Utc
                    .timestamp_opt(row.time_bucket as i64, 0)
                    .single()
                    .map_or_else(|| "Invalid Timestamp".to_string(), |dt| dt.to_rfc3339()),
                subgraph: row.subgraph,
                gateway_id: row.gateway_id,
                query_count: row.query_count,
                success_count: row.success_count,
                failure_count: row.failure_count,
                // Map all new fields, handling Option for NaN/Inf safety
                avg_response_time_ms: Some(row.avg_response_time_ms),
                max_response_time_ms: Some(row.max_response_time_ms),
                p90_response_time_ms: Some(row.p90_response_time_ms),
                p99_response_time_ms: Some(row.p99_response_time_ms),
                stddev_response_time_ms: Some(row.stddev_response_time_ms),
                total_fees_usd: Some(row.total_fees_usd),
                avg_fee_usd: Some(row.avg_fee_usd),
                max_fee_usd: Some(row.max_fee_usd),
                p90_fee_usd: Some(row.p90_fee_usd),
                p99_fee_usd: Some(row.p99_fee_usd),
                stddev_fee_usd: Some(row.stddev_fee_usd),
                success_proportion: Some(row.success_proportion),
            })
            .collect())
    }

    /// Query for Indexer level aggregations
    async fn indexer_aggregations(
        &self,
        _ctx: &Context<'_>, // Prefix ctx with underscore
        interval: AggregationInterval,
        time_range: TimeRangeInput,
        #[graphql(desc = "Optional filters for the query")] filter: Option<AggregationFilterInput>,
        #[graphql(desc = "Maximum number of records to return (default 1000, max 10000)")]
        limit: Option<i32>,
    ) -> Result<Vec<IndexerAggregationOutput>, String> {
        // Return updated Output struct
        let client = get_clickhouse_client()?;
        let view_name = format!("view_agg_indexer_{}", interval.table_suffix());

        // Build WHERE clause (logic remains the same, lines 501-521)
        let mut conditions = Vec::new();
        let from_dt = DateTime::parse_from_rfc3339(&time_range.from)
            .map_err(|e| format!("Invalid 'from' timestamp format: {}", e))?
            .with_timezone(&Utc);
        let to_dt = DateTime::parse_from_rfc3339(&time_range.to)
            .map_err(|e| format!("Invalid 'to' timestamp format: {}", e))?
            .with_timezone(&Utc);
        conditions.push(format!(
            "time_bucket >= toDateTime({})",
            from_dt.timestamp()
        ));
        conditions.push(format!("time_bucket < toDateTime({})", to_dt.timestamp()));

        if let Some(f) = filter {
            if let Some(gw) = &f.gateway_id {
                conditions.push(format!("gateway_id = '{}'", gw));
            }
            if let Some(ix) = &f.indexer {
                conditions.push(format!("indexer = '{}'", ix));
            }
            // No subgraph/allocation filters applicable here
        }

        let where_clause = format!("WHERE {}", conditions.join(" AND "));
        let query_limit = limit.unwrap_or(1000).clamp(0, 10000); // Use clamp

        // Update SELECT list to include all new fields
        let query = format!(
            "SELECT time_bucket, indexer, gateway_id, \
                    query_count, success_count, failure_count, \
                    avg_indexer_response_time_ms, max_indexer_response_time_ms, \
                    p90_indexer_response_time_ms, p99_indexer_response_time_ms, \
                    stddev_indexer_response_time_ms, \
                    total_fee_grt, avg_fee_grt, max_fee_grt, p90_fee_grt, p99_fee_grt, stddev_fee_grt, \
                    avg_seconds_behind, max_seconds_behind, p90_seconds_behind, p99_seconds_behind, stddev_seconds_behind, \
                    avg_blocks_behind, max_blocks_behind, p90_blocks_behind, p99_blocks_behind, stddev_blocks_behind, \
                    success_proportion \
             FROM {} {} \
             ORDER BY time_bucket DESC, indexer ASC, gateway_id ASC \
             LIMIT {}",
            view_name, where_clause, query_limit
        );

        println!("Executing query: {}", query);

        let rows = client
            .query(&query)
            .fetch_all::<IndexerAggregationRow>() // Use updated Row struct
            .await
            .map_err(|e| format!("Database query failed: {}", e))?;

        // Map directly, row-by-row, including new fields
        Ok(rows
            .into_iter()
            .map(|row| IndexerAggregationOutput {
                // Use updated Output struct
                time_bucket: Utc
                    .timestamp_opt(row.time_bucket as i64, 0)
                    .single()
                    .map_or_else(|| "Invalid Timestamp".to_string(), |dt| dt.to_rfc3339()),
                indexer: row.indexer,
                gateway_id: row.gateway_id,
                query_count: row.query_count,
                success_count: row.success_count,
                failure_count: row.failure_count,
                // Map all new fields, handling Option for NaN/Inf safety
                avg_indexer_response_time_ms: Some(row.avg_indexer_response_time_ms),
                max_indexer_response_time_ms: Some(row.max_indexer_response_time_ms),
                p90_indexer_response_time_ms: Some(row.p90_indexer_response_time_ms),
                p99_indexer_response_time_ms: Some(row.p99_indexer_response_time_ms),
                stddev_indexer_response_time_ms: Some(row.stddev_indexer_response_time_ms),
                total_fee_grt: Some(row.total_fee_grt),
                avg_fee_grt: Some(row.avg_fee_grt),
                max_fee_grt: Some(row.max_fee_grt),
                p90_fee_grt: Some(row.p90_fee_grt),
                p99_fee_grt: Some(row.p99_fee_grt),
                stddev_fee_grt: Some(row.stddev_fee_grt),
                avg_seconds_behind: Some(row.avg_seconds_behind),
                max_seconds_behind: Some(row.max_seconds_behind),
                p90_seconds_behind: Some(row.p90_seconds_behind),
                p99_seconds_behind: Some(row.p99_seconds_behind),
                stddev_seconds_behind: Some(row.stddev_seconds_behind),
                avg_blocks_behind: Some(row.avg_blocks_behind),
                max_blocks_behind: Some(row.max_blocks_behind),
                p90_blocks_behind: Some(row.p90_blocks_behind),
                p99_blocks_behind: Some(row.p99_blocks_behind),
                stddev_blocks_behind: Some(row.stddev_blocks_behind),
                success_proportion: Some(row.success_proportion),
            })
            .collect())
    }

    /// Query for Allocation level aggregations (grouped by subgraph and indexer)
    async fn allocation_aggregations(
        &self,
        _ctx: &Context<'_>, // Prefix ctx with underscore
        interval: AggregationInterval,
        time_range: TimeRangeInput,
        #[graphql(desc = "Optional filters for the query")] filter: Option<AggregationFilterInput>,
        #[graphql(desc = "Maximum number of records to return (default 1000, max 10000)")]
        limit: Option<i32>,
    ) -> Result<Vec<AllocationAggregationOutput>, String> {
        let client = get_clickhouse_client()?;
        let view_name = format!("view_agg_allocation_{}", interval.table_suffix());

        // Build WHERE clause
        let mut conditions = Vec::new();
        let from_dt = DateTime::parse_from_rfc3339(&time_range.from)
            .map_err(|e| format!("Invalid 'from' timestamp format: {}", e))?
            .with_timezone(&Utc);
        let to_dt = DateTime::parse_from_rfc3339(&time_range.to)
            .map_err(|e| format!("Invalid 'to' timestamp format: {}", e))?
            .with_timezone(&Utc);
        conditions.push(format!(
            "time_bucket >= toDateTime({})",
            from_dt.timestamp()
        ));
        conditions.push(format!("time_bucket < toDateTime({})", to_dt.timestamp()));

        if let Some(f) = filter {
            if let Some(gw) = &f.gateway_id {
                conditions.push(format!("gateway_id = '{}'", gw));
            }
            if let Some(sg) = &f.subgraph {
                conditions.push(format!("subgraph = '{}'", sg));
            }
            if let Some(ix) = &f.indexer {
                conditions.push(format!("indexer = '{}'", ix));
            }
        }

        let where_clause = format!("WHERE {}", conditions.join(" AND "));
        let query_limit = limit.unwrap_or(1000).clamp(0, 10000); // Use clamp

        // Query directly selects pre-aggregated columns, no further aggregation or GROUP BY
        let query = format!(
            "SELECT time_bucket, subgraph, indexer, gateway_id, \
                    query_count, success_count, failure_count, \
                    avg_indexer_response_time_ms, max_indexer_response_time_ms, \
                    p90_indexer_response_time_ms, p99_indexer_response_time_ms, \
                    stddev_indexer_response_time_ms, \
                    total_fee_grt, avg_fee_grt, max_fee_grt, p90_fee_grt, p99_fee_grt, stddev_fee_grt, \
                    avg_seconds_behind, max_seconds_behind, p90_seconds_behind, p99_seconds_behind, stddev_seconds_behind, \
                    avg_blocks_behind, max_blocks_behind, p90_blocks_behind, p99_blocks_behind, stddev_blocks_behind, \
                    success_proportion \
             FROM {} {} \
             ORDER BY time_bucket DESC, subgraph ASC, indexer ASC, gateway_id ASC \
             LIMIT {}",
            view_name, where_clause, query_limit
        );

        println!("Executing query: {}", query);

        let rows = client
            .query(&query)
            .fetch_all::<AllocationAggregationRow>() // Uses existing struct
            .await
            .map_err(|e| format!("Database query failed: {}", e))?;

        // Map directly, row-by-row, including new fields
        Ok(rows
            .into_iter()
            .map(|row| AllocationAggregationOutput {
                // Uses existing struct
                time_bucket: Utc
                    .timestamp_opt(row.time_bucket as i64, 0)
                    .single()
                    .map_or_else(|| "Invalid Timestamp".to_string(), |dt| dt.to_rfc3339()),
                subgraph: row.subgraph,
                indexer: row.indexer,
                gateway_id: row.gateway_id,
                query_count: row.query_count,
                success_count: row.success_count,
                failure_count: row.failure_count,
                // Map all new fields (same mapping logic as Indexer)
                avg_indexer_response_time_ms: Some(row.avg_indexer_response_time_ms),
                max_indexer_response_time_ms: Some(row.max_indexer_response_time_ms),
                p90_indexer_response_time_ms: Some(row.p90_indexer_response_time_ms),
                p99_indexer_response_time_ms: Some(row.p99_indexer_response_time_ms),
                stddev_indexer_response_time_ms: Some(row.stddev_indexer_response_time_ms),
                total_fee_grt: Some(row.total_fee_grt),
                avg_fee_grt: Some(row.avg_fee_grt),
                max_fee_grt: Some(row.max_fee_grt),
                p90_fee_grt: Some(row.p90_fee_grt),
                p99_fee_grt: Some(row.p99_fee_grt),
                stddev_fee_grt: Some(row.stddev_fee_grt),
                avg_seconds_behind: Some(row.avg_seconds_behind),
                max_seconds_behind: Some(row.max_seconds_behind),
                p90_seconds_behind: Some(row.p90_seconds_behind),
                p99_seconds_behind: Some(row.p99_seconds_behind),
                stddev_seconds_behind: Some(row.stddev_seconds_behind),
                avg_blocks_behind: Some(row.avg_blocks_behind),
                max_blocks_behind: Some(row.max_blocks_behind),
                p90_blocks_behind: Some(row.p90_blocks_behind),
                p99_blocks_behind: Some(row.p99_blocks_behind),
                stddev_blocks_behind: Some(row.stddev_blocks_behind),
                success_proportion: Some(row.success_proportion),
            })
            .collect())
    }
}

// Helper function to create ClickHouse client
// Returns the configured client builder. Connection happens on first query.
fn get_clickhouse_client() -> Result<Client, String> {
    Ok(Client::default()
        .with_url(env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".into()))
        .with_database(env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "default".into()))
        .with_user(env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "graphql".into()))
        .with_password(
            env::var("CLICKHOUSE_PASSWORD").unwrap_or_else(|_| "graphql_password".into()),
        ))
    // Remove .try_into() and .map_err()
}

// GraphQL handlers
async fn graphql_handler(
    schema: web::Data<Schema<QueryRoot, EmptyMutation, EmptySubscription>>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

async fn graphql_playground() -> Result<HttpResponse> {
    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(async_graphql::http::playground_source(
            async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
        )))
}

// Health check endpoint
async fn health_check() -> HttpResponse {
    // Basic check: Can we create a client builder?
    let client_result = get_clickhouse_client();
    if client_result.is_err() {
        // Log the actual error if needed
        eprintln!(
            "Health check failed (client config): {:?}",
            client_result.err()
        );
        return HttpResponse::InternalServerError().body("ClickHouse client configuration error");
    }
    let client = client_result.unwrap(); // Safe unwrap after check above

    // More advanced check: Can we execute a simple query?
    match client.query("SELECT 1").execute().await {
        // Use execute() for simple queries
        Ok(_) => HttpResponse::Ok().body("OK"),
        Err(e) => {
            eprintln!("Health check failed (query execution): {}", e); // Log the error
            HttpResponse::ServiceUnavailable().body(format!("ClickHouse connection error: {}", e))
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging (optional but recommended)
    // You can use a simple logger like env_logger or tracing
    // env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    // Or using tracing if you prefer (ensure tracing/tracing-subscriber are deps)
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Initialize environment variables (e.g., from .env file if needed)
    // dotenv::dotenv().ok();

    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription).finish();

    tracing::info!("Starting GraphQL API server..."); // Use tracing/log

    // --- Server Initialization ---
    // Create the server instance but don't await .run() immediately
    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(schema.clone()))
            .route("/graphql", web::post().to(graphql_handler))
            .route("/", web::get().to(graphql_playground))
            .route("/health", web::get().to(health_check)) // Keep health check
    })
    .bind("0.0.0.0:8000")?
    .run(); // .run() returns a Server instance
            // --- End Server Initialization ---

    // --- Graceful Shutdown Logic ---
    // Get a handle to the server instance
    let server_handle = server.handle();

    // Spawn a separate task to listen for termination signals
    tokio::spawn(async move {
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");

        // Wait for either SIGINT or SIGTERM
        tokio::select! {
            _ = sigint.recv() => {
                tracing::info!("SIGINT received, initiating graceful shutdown...");
            },
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received, initiating graceful shutdown...");
            },
        };

        // Initiate graceful shutdown using the server handle.
        // stop(true) sends the stop signal gracefully.
        // We await it to ensure the signal is processed.
        server_handle.stop(true).await;
        tracing::info!("Shutdown signal sent to Actix server.");
    });
    // --- End Graceful Shutdown Logic ---

    tracing::info!("GraphQL server running at http://0.0.0.0:8000");
    tracing::info!("GraphQL playground available at http://localhost:8000"); // Updated log

    // Wait for the server to stop.
    // This will block until the server is shut down, either normally
    // or via the signal handler calling server_handle.stop().
    server.await?;

    tracing::info!("GraphQL server has stopped gracefully.");

    Ok(())
}
