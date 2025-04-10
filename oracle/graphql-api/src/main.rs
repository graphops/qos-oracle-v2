use actix_web::{web, App, HttpResponse, HttpServer, Result};
use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Enum, InputObject, Object, Schema, SimpleObject,
};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse};
use chrono::{DateTime, TimeZone, Utc};
use clickhouse::{Client, Row};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::Deserialize;
use std::env;
use tokio::signal::unix::{signal, SignalKind};

// --- Input Validation Regexes ---
// Define allowed characters/patterns. Adjust these based on actual expected formats.
static GATEWAY_ID_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[a-zA-Z0-9_-]+$").unwrap());
// Allow alphanumeric, hyphens, slashes (e.g., for ENS names/subgraph names)
static SUBGRAPH_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[a-zA-Z0-9_/-]+$").unwrap());
// Allow hex characters, optionally prefixed with 0x
static INDEXER_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^(0x)?[a-fA-F0-9]+$").unwrap());

// --- ClickHouse String Escaping ---
/// Escapes a string for safe inclusion within single quotes in a ClickHouse SQL query.
/// Replaces backslashes (`\`) with `\\` and single quotes (`'`) with `\'`.
fn escape_clickhouse_string_literal(input: &str) -> String {
    input.replace('\\', r"\\").replace('\'', r"\'")
}

// --- Time Parsing Helper ---
/// Parses a string as either a Unix timestamp (seconds) or an RFC3339 datetime.
fn parse_datetime_input(input: &str) -> Result<DateTime<Utc>, String> {
    // Try parsing as Unix timestamp (integer seconds) first
    if let Ok(ts) = input.parse::<i64>() {
        match Utc.timestamp_opt(ts, 0).single() {
            Some(dt) => Ok(dt),
            None => Err(format!("Invalid Unix timestamp value: {}", ts)),
        }
    } else {
        // Fallback to parsing as RFC3339
        DateTime::parse_from_rfc3339(input)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|e| {
                format!(
                    "Invalid format: Expected Unix timestamp or RFC3339. Parse error: {}",
                    e
                )
            })
    }
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

#[derive(InputObject, Debug, Clone)]
struct TimeRangeInput {
    from: Option<String>,
    to: Option<String>,
}

#[derive(InputObject, Debug, Clone)]
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

// --- Enums and Structs for Sorting ---

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
enum SortDirection {
    Asc,
    Desc,
}

// Define which fields can be sorted for Deployments
#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
enum DeploymentSortField {
    TimeBucket,
    Subgraph,
    GatewayId,
    QueryCount,
    SuccessCount,
    FailureCount,
    AvgResponseTimeMs,
    MaxResponseTimeMs,
    P90ResponseTimeMs,
    P99ResponseTimeMs,
    TotalFeesUsd,
    AvgFeeUsd,
    MaxFeeUsd,
    P90FeeUsd,
    P99FeeUsd,
    SuccessProportion,
}

impl DeploymentSortField {
    // Helper to get the corresponding ClickHouse column name
    fn column_name(&self) -> &'static str {
        match self {
            DeploymentSortField::TimeBucket => "time_bucket",
            DeploymentSortField::Subgraph => "subgraph",
            DeploymentSortField::GatewayId => "gateway_id",
            DeploymentSortField::QueryCount => "query_count",
            DeploymentSortField::SuccessCount => "success_count",
            DeploymentSortField::FailureCount => "failure_count",
            DeploymentSortField::AvgResponseTimeMs => "avg_response_time_ms",
            DeploymentSortField::MaxResponseTimeMs => "max_response_time_ms",
            DeploymentSortField::P90ResponseTimeMs => "p90_response_time_ms",
            DeploymentSortField::P99ResponseTimeMs => "p99_response_time_ms",
            DeploymentSortField::TotalFeesUsd => "total_fees_usd",
            DeploymentSortField::AvgFeeUsd => "avg_fee_usd",
            DeploymentSortField::MaxFeeUsd => "max_fee_usd",
            DeploymentSortField::P90FeeUsd => "p90_fee_usd",
            DeploymentSortField::P99FeeUsd => "p99_fee_usd",
            DeploymentSortField::SuccessProportion => "success_proportion",
        }
    }
}

// Define which fields can be sorted for Indexers
#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
enum IndexerSortField {
    TimeBucket,
    Indexer,
    GatewayId,
    QueryCount,
    SuccessCount,
    FailureCount,
    AvgIndexerResponseTimeMs,
    MaxIndexerResponseTimeMs,
    P90IndexerResponseTimeMs,
    P99IndexerResponseTimeMs,
    TotalFeeGrt,
    AvgFeeGrt,
    MaxFeeGrt,
    P90FeeGrt,
    P99FeeGrt,
    AvgSecondsBehind,
    MaxSecondsBehind,
    P90SecondsBehind,
    P99SecondsBehind,
    AvgBlocksBehind,
    MaxBlocksBehind,
    P90BlocksBehind,
    P99BlocksBehind,
    SuccessProportion,
}

impl IndexerSortField {
    // Helper to get the corresponding ClickHouse column name
    fn column_name(&self) -> &'static str {
        match self {
            IndexerSortField::TimeBucket => "time_bucket",
            IndexerSortField::Indexer => "indexer",
            IndexerSortField::GatewayId => "gateway_id",
            IndexerSortField::QueryCount => "query_count",
            IndexerSortField::SuccessCount => "success_count",
            IndexerSortField::FailureCount => "failure_count",
            IndexerSortField::AvgIndexerResponseTimeMs => "avg_indexer_response_time_ms",
            IndexerSortField::MaxIndexerResponseTimeMs => "max_indexer_response_time_ms",
            IndexerSortField::P90IndexerResponseTimeMs => "p90_indexer_response_time_ms",
            IndexerSortField::P99IndexerResponseTimeMs => "p99_indexer_response_time_ms",
            IndexerSortField::TotalFeeGrt => "total_fee_grt",
            IndexerSortField::AvgFeeGrt => "avg_fee_grt",
            IndexerSortField::MaxFeeGrt => "max_fee_grt",
            IndexerSortField::P90FeeGrt => "p90_fee_grt",
            IndexerSortField::P99FeeGrt => "p99_fee_grt",
            IndexerSortField::AvgSecondsBehind => "avg_seconds_behind",
            IndexerSortField::MaxSecondsBehind => "max_seconds_behind",
            IndexerSortField::P90SecondsBehind => "p90_seconds_behind",
            IndexerSortField::P99SecondsBehind => "p99_seconds_behind",
            IndexerSortField::AvgBlocksBehind => "avg_blocks_behind",
            IndexerSortField::MaxBlocksBehind => "max_blocks_behind",
            IndexerSortField::P90BlocksBehind => "p90_blocks_behind",
            IndexerSortField::P99BlocksBehind => "p99_blocks_behind",
            IndexerSortField::SuccessProportion => "success_proportion",
        }
    }
}

// Define which fields can be sorted for Allocations
#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
enum AllocationSortField {
    TimeBucket,
    Subgraph,
    Indexer,
    GatewayId,
    QueryCount,
    SuccessCount,
    FailureCount,
    AvgIndexerResponseTimeMs,
    MaxIndexerResponseTimeMs,
    P90IndexerResponseTimeMs,
    P99IndexerResponseTimeMs,
    TotalFeeGrt,
    AvgFeeGrt,
    MaxFeeGrt,
    P90FeeGrt,
    P99FeeGrt,
    AvgSecondsBehind,
    MaxSecondsBehind,
    P90SecondsBehind,
    P99SecondsBehind,
    AvgBlocksBehind,
    MaxBlocksBehind,
    P90BlocksBehind,
    P99BlocksBehind,
    SuccessProportion,
}

impl AllocationSortField {
    // Helper to get the corresponding ClickHouse column name
    fn column_name(&self) -> &'static str {
        match self {
            AllocationSortField::TimeBucket => "time_bucket",
            AllocationSortField::Subgraph => "subgraph",
            AllocationSortField::Indexer => "indexer",
            AllocationSortField::GatewayId => "gateway_id",
            AllocationSortField::QueryCount => "query_count",
            AllocationSortField::SuccessCount => "success_count",
            AllocationSortField::FailureCount => "failure_count",
            AllocationSortField::AvgIndexerResponseTimeMs => "avg_indexer_response_time_ms",
            AllocationSortField::MaxIndexerResponseTimeMs => "max_indexer_response_time_ms",
            AllocationSortField::P90IndexerResponseTimeMs => "p90_indexer_response_time_ms",
            AllocationSortField::P99IndexerResponseTimeMs => "p99_indexer_response_time_ms",
            AllocationSortField::TotalFeeGrt => "total_fee_grt",
            AllocationSortField::AvgFeeGrt => "avg_fee_grt",
            AllocationSortField::MaxFeeGrt => "max_fee_grt",
            AllocationSortField::P90FeeGrt => "p90_fee_grt",
            AllocationSortField::P99FeeGrt => "p99_fee_grt",
            AllocationSortField::AvgSecondsBehind => "avg_seconds_behind",
            AllocationSortField::MaxSecondsBehind => "max_seconds_behind",
            AllocationSortField::P90SecondsBehind => "p90_seconds_behind",
            AllocationSortField::P99SecondsBehind => "p99_seconds_behind",
            AllocationSortField::AvgBlocksBehind => "avg_blocks_behind",
            AllocationSortField::MaxBlocksBehind => "max_blocks_behind",
            AllocationSortField::P90BlocksBehind => "p90_blocks_behind",
            AllocationSortField::P99BlocksBehind => "p99_blocks_behind",
            AllocationSortField::SuccessProportion => "success_proportion",
        }
    }
}

// Input object for specifying sorting - Generic enough for all types?
// Let's make specific ones for type safety in the resolver signature.

#[derive(InputObject, Debug)]
struct DeploymentSortInput {
    field: DeploymentSortField,
    direction: Option<SortDirection>, // Default to Desc
}

#[derive(InputObject, Debug)]
struct IndexerSortInput {
    field: IndexerSortField,
    direction: Option<SortDirection>, // Default to Desc
}

#[derive(InputObject, Debug)]
struct AllocationSortInput {
    field: AllocationSortField,
    direction: Option<SortDirection>, // Default to Desc
}

// --- GraphQL Query Root ---

struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Query for Deployment level aggregations
    async fn deployment_aggregations(
        &self,
        _ctx: &Context<'_>,
        #[graphql(desc = "Aggregation interval (default: Hourly)")] interval: Option<
            AggregationInterval,
        >,
        #[graphql(
            desc = "Optional time range filter. If omitted, defaults to the last 24 hours. \
                              If provided, requires at least 'from' or 'to'."
        )]
        time_range: Option<TimeRangeInput>,
        #[graphql(desc = "Optional filters for the query")] filter: Option<AggregationFilterInput>,
        #[graphql(desc = "Maximum number of records to return (default 1000, max 10000)")]
        limit: Option<i32>,
        #[graphql(desc = "Optional sorting (default: time_bucket DESC)")] sort: Option<
            DeploymentSortInput,
        >,
    ) -> Result<Vec<DeploymentAggregationOutput>, String> {
        let client = get_clickhouse_client()?;
        let actual_interval = interval.unwrap_or(AggregationInterval::Hourly);
        let view_name = format!("view_agg_deployment_{}", actual_interval.table_suffix());

        // --- Build WHERE clause ---
        let mut conditions = Vec::new();

        // --- Handle Time Range ---
        match time_range {
            // Case 1: time_range argument is completely omitted - use default
            None => {
                let now = Utc::now();
                let default_from = now - chrono::Duration::hours(24);
                conditions.push(format!(
                    "time_bucket >= toDateTime({})",
                    default_from.timestamp()
                ));
                conditions.push(format!("time_bucket < toDateTime({})", now.timestamp()));
            }
            // Case 2: time_range argument is provided
            Some(tr) => {
                let parsed_from = tr.from.as_deref().map(parse_datetime_input).transpose()?;
                let parsed_to = tr.to.as_deref().map(parse_datetime_input).transpose()?;

                match (parsed_from, parsed_to) {
                    (None, None) => {
                        // If time_range was provided but both fields are null/missing
                        return Err(
                            "Time range filter requires at least 'from' or 'to' to be specified."
                                .to_string(),
                        );
                    }
                    (Some(from_dt), None) => {
                        conditions.push(format!(
                            "time_bucket >= toDateTime({})",
                            from_dt.timestamp()
                        ));
                    }
                    (None, Some(to_dt)) => {
                        conditions.push(format!("time_bucket < toDateTime({})", to_dt.timestamp()));
                    }
                    (Some(from_dt), Some(to_dt)) => {
                        if from_dt >= to_dt {
                            return Err("'from' time must be earlier than 'to' time.".to_string());
                        }
                        conditions.push(format!(
                            "time_bucket >= toDateTime({})",
                            from_dt.timestamp()
                        ));
                        conditions.push(format!("time_bucket < toDateTime({})", to_dt.timestamp()));
                    }
                }
            }
        }
        // --- End Handle Time Range ---

        // --- Handle Other Filters (gateway_id, subgraph) ---
        if let Some(f) = filter {
            if let Some(gw) = &f.gateway_id {
                // --- Validate and Escape gateway_id ---
                if !GATEWAY_ID_REGEX.is_match(gw) {
                    return Err("Invalid format for gateway_id filter".to_string());
                }
                let escaped_gw = escape_clickhouse_string_literal(gw);
                conditions.push(format!("gateway_id = '{}'", escaped_gw));
                // --- End Validation ---
            }
            if let Some(sg) = &f.subgraph {
                // --- Validate and Escape subgraph ---
                if !SUBGRAPH_REGEX.is_match(sg) {
                    return Err("Invalid format for subgraph filter".to_string());
                }
                let escaped_sg = escape_clickhouse_string_literal(sg);
                conditions.push(format!("subgraph = '{}'", escaped_sg));
                // --- End Validation ---
            }
            // No indexer filter applicable here
        }
        // --- End Handle Other Filters ---

        // Should always have at least one time condition now
        let where_clause = format!("WHERE {}", conditions.join(" AND "));
        let query_limit = limit.unwrap_or(1000).clamp(1, 10000);

        // --- Build ORDER BY Clause ---
        let order_by_clause = match sort {
            Some(s) => {
                let direction = match s.direction.unwrap_or(SortDirection::Desc) {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                };
                format!("ORDER BY {} {}", s.field.column_name(), direction)
            }
            None => "ORDER BY time_bucket DESC, subgraph ASC, gateway_id ASC".to_string(),
        };

        // --- Build and Execute Query ---
        let query = format!(
            "SELECT time_bucket, subgraph, gateway_id, \
                    query_count, success_count, failure_count, \
                    avg_response_time_ms, max_response_time_ms, p90_response_time_ms, p99_response_time_ms, stddev_response_time_ms, \
                    total_fees_usd, avg_fee_usd, max_fee_usd, p90_fee_usd, p99_fee_usd, stddev_fee_usd, \
                    success_proportion \
             FROM {} {} \
             {} \
             LIMIT {}",
            view_name, where_clause, order_by_clause, query_limit
        );

        println!("Executing query: {}", query);

        let rows = client
            .query(&query)
            .fetch_all::<DeploymentAggregationRow>()
            .await
            .map_err(|e| format!("Database query failed: {}", e))?;

        // Map results (logic remains the same)
        Ok(rows
            .into_iter()
            .map(|row| DeploymentAggregationOutput {
                time_bucket: Utc
                    .timestamp_opt(row.time_bucket as i64, 0)
                    .single()
                    .map_or_else(|| "Invalid Timestamp".to_string(), |dt| dt.to_rfc3339()),
                subgraph: row.subgraph,
                gateway_id: row.gateway_id,
                query_count: row.query_count,
                success_count: row.success_count,
                failure_count: row.failure_count,
                avg_response_time_ms: if row.avg_response_time_ms.is_finite() {
                    Some(row.avg_response_time_ms)
                } else {
                    None
                },
                max_response_time_ms: Some(row.max_response_time_ms),
                p90_response_time_ms: if row.p90_response_time_ms.is_finite() {
                    Some(row.p90_response_time_ms)
                } else {
                    None
                },
                p99_response_time_ms: if row.p99_response_time_ms.is_finite() {
                    Some(row.p99_response_time_ms)
                } else {
                    None
                },
                stddev_response_time_ms: if row.stddev_response_time_ms.is_finite() {
                    Some(row.stddev_response_time_ms)
                } else {
                    None
                },
                total_fees_usd: if row.total_fees_usd.is_finite() {
                    Some(row.total_fees_usd)
                } else {
                    None
                },
                avg_fee_usd: if row.avg_fee_usd.is_finite() {
                    Some(row.avg_fee_usd)
                } else {
                    None
                },
                max_fee_usd: if row.max_fee_usd.is_finite() {
                    Some(row.max_fee_usd)
                } else {
                    None
                },
                p90_fee_usd: if row.p90_fee_usd.is_finite() {
                    Some(row.p90_fee_usd)
                } else {
                    None
                },
                p99_fee_usd: if row.p99_fee_usd.is_finite() {
                    Some(row.p99_fee_usd)
                } else {
                    None
                },
                stddev_fee_usd: if row.stddev_fee_usd.is_finite() {
                    Some(row.stddev_fee_usd)
                } else {
                    None
                },
                success_proportion: if row.success_proportion.is_finite() {
                    Some(row.success_proportion)
                } else {
                    None
                },
            })
            .collect())
    }

    /// Query for Indexer level aggregations
    async fn indexer_aggregations(
        &self,
        _ctx: &Context<'_>,
        #[graphql(desc = "Aggregation interval (default: Hourly)")] interval: Option<
            AggregationInterval,
        >,
        #[graphql(
            desc = "Optional time range filter. If omitted, defaults to the last 24 hours. \
                              If provided, requires at least 'from' or 'to'."
        )]
        time_range: Option<TimeRangeInput>,
        #[graphql(desc = "Optional filters for the query")] filter: Option<AggregationFilterInput>,
        #[graphql(desc = "Maximum number of records to return (default 1000, max 10000)")]
        limit: Option<i32>,
        #[graphql(desc = "Optional sorting (default: time_bucket DESC)")] sort: Option<
            IndexerSortInput,
        >,
    ) -> Result<Vec<IndexerAggregationOutput>, String> {
        let client = get_clickhouse_client()?;
        let actual_interval = interval.unwrap_or(AggregationInterval::Hourly);
        let view_name = format!("view_agg_indexer_{}", actual_interval.table_suffix());

        // --- Build WHERE clause ---
        let mut conditions = Vec::new();

        // --- Handle Time Range (Apply same logic as deployment_aggregations) ---
        match time_range {
            None => {
                let now = Utc::now();
                let default_from = now - chrono::Duration::hours(24);
                conditions.push(format!(
                    "time_bucket >= toDateTime({})",
                    default_from.timestamp()
                ));
                conditions.push(format!("time_bucket < toDateTime({})", now.timestamp()));
            }
            Some(tr) => {
                let parsed_from = tr.from.as_deref().map(parse_datetime_input).transpose()?;
                let parsed_to = tr.to.as_deref().map(parse_datetime_input).transpose()?;

                match (parsed_from, parsed_to) {
                    (None, None) => {
                        return Err(
                            "Time range filter requires at least 'from' or 'to' to be specified."
                                .to_string(),
                        );
                    }
                    (Some(from_dt), None) => {
                        conditions.push(format!(
                            "time_bucket >= toDateTime({})",
                            from_dt.timestamp()
                        ));
                    }
                    (None, Some(to_dt)) => {
                        conditions.push(format!("time_bucket < toDateTime({})", to_dt.timestamp()));
                    }
                    (Some(from_dt), Some(to_dt)) => {
                        if from_dt >= to_dt {
                            return Err("'from' time must be earlier than 'to' time.".to_string());
                        }
                        conditions.push(format!(
                            "time_bucket >= toDateTime({})",
                            from_dt.timestamp()
                        ));
                        conditions.push(format!("time_bucket < toDateTime({})", to_dt.timestamp()));
                    }
                }
            }
        }
        // --- End Handle Time Range ---

        // --- Handle Other Filters (gateway_id, indexer) ---
        if let Some(f) = filter {
            if let Some(gw) = &f.gateway_id {
                // --- Validate and Escape gateway_id ---
                if !GATEWAY_ID_REGEX.is_match(gw) {
                    return Err("Invalid format for gateway_id filter".to_string());
                }
                let escaped_gw = escape_clickhouse_string_literal(gw);
                conditions.push(format!("gateway_id = '{}'", escaped_gw));
                // --- End Validation ---
            }
            if let Some(ix) = &f.indexer {
                // --- Validate and Escape indexer ---
                if !INDEXER_REGEX.is_match(ix) {
                    return Err("Invalid format for indexer filter".to_string());
                }
                let escaped_ix = escape_clickhouse_string_literal(ix);
                conditions.push(format!("indexer = '{}'", escaped_ix));
                // --- End Validation ---
            }
            // No subgraph filter applicable here
        }
        // --- End Handle Other Filters ---

        let where_clause = format!("WHERE {}", conditions.join(" AND "));
        let query_limit = limit.unwrap_or(1000).clamp(1, 10000);

        // --- Build ORDER BY Clause (Using IndexerSortField) ---
        let order_by_clause = match sort {
            Some(s) => {
                let direction = match s.direction.unwrap_or(SortDirection::Desc) {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                };
                format!("ORDER BY {} {}", s.field.column_name(), direction)
            }
            None => "ORDER BY time_bucket DESC, indexer ASC, gateway_id ASC".to_string(),
        };

        // --- Build and Execute Query ---
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
             {} \
             LIMIT {}",
            view_name, where_clause, order_by_clause, query_limit
        );

        println!("Executing query: {}", query);

        let rows = client
            .query(&query)
            .fetch_all::<IndexerAggregationRow>()
            .await
            .map_err(|e| format!("Database query failed: {}", e))?;

        // Map results (logic remains the same)
        Ok(rows
            .into_iter()
            .map(|row| IndexerAggregationOutput {
                time_bucket: Utc
                    .timestamp_opt(row.time_bucket as i64, 0)
                    .single()
                    .map_or_else(|| "Invalid Timestamp".to_string(), |dt| dt.to_rfc3339()),
                indexer: row.indexer,
                gateway_id: row.gateway_id,
                query_count: row.query_count,
                success_count: row.success_count,
                failure_count: row.failure_count,
                avg_indexer_response_time_ms: if row.avg_indexer_response_time_ms.is_finite() {
                    Some(row.avg_indexer_response_time_ms)
                } else {
                    None
                },
                max_indexer_response_time_ms: Some(row.max_indexer_response_time_ms),
                p90_indexer_response_time_ms: if row.p90_indexer_response_time_ms.is_finite() {
                    Some(row.p90_indexer_response_time_ms)
                } else {
                    None
                },
                p99_indexer_response_time_ms: if row.p99_indexer_response_time_ms.is_finite() {
                    Some(row.p99_indexer_response_time_ms)
                } else {
                    None
                },
                stddev_indexer_response_time_ms: if row.stddev_indexer_response_time_ms.is_finite()
                {
                    Some(row.stddev_indexer_response_time_ms)
                } else {
                    None
                },
                total_fee_grt: if row.total_fee_grt.is_finite() {
                    Some(row.total_fee_grt)
                } else {
                    None
                },
                avg_fee_grt: if row.avg_fee_grt.is_finite() {
                    Some(row.avg_fee_grt)
                } else {
                    None
                },
                max_fee_grt: if row.max_fee_grt.is_finite() {
                    Some(row.max_fee_grt)
                } else {
                    None
                },
                p90_fee_grt: if row.p90_fee_grt.is_finite() {
                    Some(row.p90_fee_grt)
                } else {
                    None
                },
                p99_fee_grt: if row.p99_fee_grt.is_finite() {
                    Some(row.p99_fee_grt)
                } else {
                    None
                },
                stddev_fee_grt: if row.stddev_fee_grt.is_finite() {
                    Some(row.stddev_fee_grt)
                } else {
                    None
                },
                avg_seconds_behind: if row.avg_seconds_behind.is_finite() {
                    Some(row.avg_seconds_behind)
                } else {
                    None
                },
                max_seconds_behind: Some(row.max_seconds_behind),
                p90_seconds_behind: if row.p90_seconds_behind.is_finite() {
                    Some(row.p90_seconds_behind)
                } else {
                    None
                },
                p99_seconds_behind: if row.p99_seconds_behind.is_finite() {
                    Some(row.p99_seconds_behind)
                } else {
                    None
                },
                stddev_seconds_behind: if row.stddev_seconds_behind.is_finite() {
                    Some(row.stddev_seconds_behind)
                } else {
                    None
                },
                avg_blocks_behind: if row.avg_blocks_behind.is_finite() {
                    Some(row.avg_blocks_behind)
                } else {
                    None
                },
                max_blocks_behind: Some(row.max_blocks_behind),
                p90_blocks_behind: if row.p90_blocks_behind.is_finite() {
                    Some(row.p90_blocks_behind)
                } else {
                    None
                },
                p99_blocks_behind: if row.p99_blocks_behind.is_finite() {
                    Some(row.p99_blocks_behind)
                } else {
                    None
                },
                stddev_blocks_behind: if row.stddev_blocks_behind.is_finite() {
                    Some(row.stddev_blocks_behind)
                } else {
                    None
                },
                success_proportion: if row.success_proportion.is_finite() {
                    Some(row.success_proportion)
                } else {
                    None
                },
            })
            .collect())
    }

    /// Query for Allocation level aggregations (grouped by subgraph and indexer)
    async fn allocation_aggregations(
        &self,
        _ctx: &Context<'_>,
        #[graphql(desc = "Aggregation interval (default: Hourly)")] interval: Option<
            AggregationInterval,
        >,
        #[graphql(
            desc = "Optional time range filter. If omitted, defaults to the last 24 hours. \
                              If provided, requires at least 'from' or 'to'."
        )]
        time_range: Option<TimeRangeInput>,
        #[graphql(desc = "Optional filters for the query")] filter: Option<AggregationFilterInput>,
        #[graphql(desc = "Maximum number of records to return (default 1000, max 10000)")]
        limit: Option<i32>,
        #[graphql(desc = "Optional sorting (default: time_bucket DESC)")] sort: Option<
            AllocationSortInput,
        >,
    ) -> Result<Vec<AllocationAggregationOutput>, String> {
        let client = get_clickhouse_client()?;
        let actual_interval = interval.unwrap_or(AggregationInterval::Hourly);
        let view_name = format!("view_agg_allocation_{}", actual_interval.table_suffix());

        // --- Build WHERE clause ---
        let mut conditions = Vec::new();

        // --- Handle Time Range (Apply same logic as deployment_aggregations) ---
        match time_range {
            None => {
                let now = Utc::now();
                let default_from = now - chrono::Duration::hours(24);
                conditions.push(format!(
                    "time_bucket >= toDateTime({})",
                    default_from.timestamp()
                ));
                conditions.push(format!("time_bucket < toDateTime({})", now.timestamp()));
            }
            Some(tr) => {
                let parsed_from = tr.from.as_deref().map(parse_datetime_input).transpose()?;
                let parsed_to = tr.to.as_deref().map(parse_datetime_input).transpose()?;

                match (parsed_from, parsed_to) {
                    (None, None) => {
                        return Err(
                            "Time range filter requires at least 'from' or 'to' to be specified."
                                .to_string(),
                        );
                    }
                    (Some(from_dt), None) => {
                        conditions.push(format!(
                            "time_bucket >= toDateTime({})",
                            from_dt.timestamp()
                        ));
                    }
                    (None, Some(to_dt)) => {
                        conditions.push(format!("time_bucket < toDateTime({})", to_dt.timestamp()));
                    }
                    (Some(from_dt), Some(to_dt)) => {
                        if from_dt >= to_dt {
                            return Err("'from' time must be earlier than 'to' time.".to_string());
                        }
                        conditions.push(format!(
                            "time_bucket >= toDateTime({})",
                            from_dt.timestamp()
                        ));
                        conditions.push(format!("time_bucket < toDateTime({})", to_dt.timestamp()));
                    }
                }
            }
        }
        // --- End Handle Time Range ---

        // --- Handle Other Filters (gateway_id, subgraph, indexer) ---
        if let Some(f) = filter {
            if let Some(gw) = &f.gateway_id {
                // --- Validate and Escape gateway_id ---
                if !GATEWAY_ID_REGEX.is_match(gw) {
                    return Err("Invalid format for gateway_id filter".to_string());
                }
                let escaped_gw = escape_clickhouse_string_literal(gw);
                conditions.push(format!("gateway_id = '{}'", escaped_gw));
                // --- End Validation ---
            }
            if let Some(sg) = &f.subgraph {
                // --- Validate and Escape subgraph ---
                if !SUBGRAPH_REGEX.is_match(sg) {
                    return Err("Invalid format for subgraph filter".to_string());
                }
                let escaped_sg = escape_clickhouse_string_literal(sg);
                conditions.push(format!("subgraph = '{}'", escaped_sg));
                // --- End Validation ---
            }
            if let Some(ix) = &f.indexer {
                // --- Validate and Escape indexer ---
                if !INDEXER_REGEX.is_match(ix) {
                    return Err("Invalid format for indexer filter".to_string());
                }
                let escaped_ix = escape_clickhouse_string_literal(ix);
                conditions.push(format!("indexer = '{}'", escaped_ix));
                // --- End Validation ---
            }
        }
        // --- End Handle Other Filters ---

        let where_clause = format!("WHERE {}", conditions.join(" AND "));
        let query_limit = limit.unwrap_or(1000).clamp(1, 10000);

        // --- Build ORDER BY Clause (Using AllocationSortField) ---
        let order_by_clause = match sort {
            Some(s) => {
                let direction = match s.direction.unwrap_or(SortDirection::Desc) {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                };
                format!("ORDER BY {} {}", s.field.column_name(), direction)
            }
            None => {
                "ORDER BY time_bucket DESC, subgraph ASC, indexer ASC, gateway_id ASC".to_string()
            }
        };

        // --- Build and Execute Query ---
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
             {} \
             LIMIT {}",
            view_name, where_clause, order_by_clause, query_limit
        );

        println!("Executing query: {}", query);

        let rows = client
            .query(&query)
            .fetch_all::<AllocationAggregationRow>()
            .await
            .map_err(|e| format!("Database query failed: {}", e))?;

        // Map results (logic remains the same)
        Ok(rows
            .into_iter()
            .map(|row| AllocationAggregationOutput {
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
                avg_indexer_response_time_ms: if row.avg_indexer_response_time_ms.is_finite() {
                    Some(row.avg_indexer_response_time_ms)
                } else {
                    None
                },
                max_indexer_response_time_ms: Some(row.max_indexer_response_time_ms),
                p90_indexer_response_time_ms: if row.p90_indexer_response_time_ms.is_finite() {
                    Some(row.p90_indexer_response_time_ms)
                } else {
                    None
                },
                p99_indexer_response_time_ms: if row.p99_indexer_response_time_ms.is_finite() {
                    Some(row.p99_indexer_response_time_ms)
                } else {
                    None
                },
                stddev_indexer_response_time_ms: if row.stddev_indexer_response_time_ms.is_finite()
                {
                    Some(row.stddev_indexer_response_time_ms)
                } else {
                    None
                },
                total_fee_grt: if row.total_fee_grt.is_finite() {
                    Some(row.total_fee_grt)
                } else {
                    None
                },
                avg_fee_grt: if row.avg_fee_grt.is_finite() {
                    Some(row.avg_fee_grt)
                } else {
                    None
                },
                max_fee_grt: if row.max_fee_grt.is_finite() {
                    Some(row.max_fee_grt)
                } else {
                    None
                },
                p90_fee_grt: if row.p90_fee_grt.is_finite() {
                    Some(row.p90_fee_grt)
                } else {
                    None
                },
                p99_fee_grt: if row.p99_fee_grt.is_finite() {
                    Some(row.p99_fee_grt)
                } else {
                    None
                },
                stddev_fee_grt: if row.stddev_fee_grt.is_finite() {
                    Some(row.stddev_fee_grt)
                } else {
                    None
                },
                avg_seconds_behind: if row.avg_seconds_behind.is_finite() {
                    Some(row.avg_seconds_behind)
                } else {
                    None
                },
                max_seconds_behind: Some(row.max_seconds_behind),
                p90_seconds_behind: if row.p90_seconds_behind.is_finite() {
                    Some(row.p90_seconds_behind)
                } else {
                    None
                },
                p99_seconds_behind: if row.p99_seconds_behind.is_finite() {
                    Some(row.p99_seconds_behind)
                } else {
                    None
                },
                stddev_seconds_behind: if row.stddev_seconds_behind.is_finite() {
                    Some(row.stddev_seconds_behind)
                } else {
                    None
                },
                avg_blocks_behind: if row.avg_blocks_behind.is_finite() {
                    Some(row.avg_blocks_behind)
                } else {
                    None
                },
                max_blocks_behind: Some(row.max_blocks_behind),
                p90_blocks_behind: if row.p90_blocks_behind.is_finite() {
                    Some(row.p90_blocks_behind)
                } else {
                    None
                },
                p99_blocks_behind: if row.p99_blocks_behind.is_finite() {
                    Some(row.p99_blocks_behind)
                } else {
                    None
                },
                stddev_blocks_behind: if row.stddev_blocks_behind.is_finite() {
                    Some(row.stddev_blocks_behind)
                } else {
                    None
                },
                success_proportion: if row.success_proportion.is_finite() {
                    Some(row.success_proportion)
                } else {
                    None
                },
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
