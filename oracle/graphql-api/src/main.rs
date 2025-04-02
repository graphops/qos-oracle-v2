use actix_web::{web, App, HttpResponse, HttpServer, Result};
use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Enum, InputObject, Object, Schema, SimpleObject,
};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse};
use chrono::{DateTime, TimeZone, Utc};
use clickhouse::{Client, Row};
use serde::Deserialize;
use std::env;
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
#[derive(Row, Deserialize, Debug)]
struct CountResult {
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

#[derive(SimpleObject, Deserialize, Debug, Clone)]
#[graphql(name = "DeploymentAggregation")] // Explicit name for GraphQL schema
struct DeploymentAggregationOutput {
    time_bucket: String, // Output as String
    subgraph: String,
    gateway_id: String,
    query_count: u64,
    success_count: u64,
    failure_count: u64,
    avg_response_time_ms: f64,
    total_fees_usd: f64,
}

#[derive(Row, Deserialize, Debug, Clone)]
struct DeploymentAggregationRow {
    time_bucket: u32, // Use u32 for Unix timestamp
    subgraph: String,
    gateway_id: String,
    query_count: u64,
    success_count: u64,
    failure_count: u64,
    avg_response_time_ms: f64,
    total_fees_usd: f64,
}

// --- Indexer Aggregation Structs ---

#[derive(SimpleObject, Deserialize, Debug, Clone)]
#[graphql(name = "IndexerAggregation")]
struct IndexerAggregationOutput {
    time_bucket: String,
    indexer: String,
    gateway_id: String,
    query_count: u64,
    success_count: u64,
    failure_count: u64,
    avg_indexer_response_time_ms: f64,
    total_fee_grt: f64,
    avg_seconds_behind: f64,
    avg_blocks_behind: f64,
}

#[derive(Row, Deserialize, Debug, Clone)]
struct IndexerAggregationRow {
    time_bucket: u32, // Use u32 for Unix timestamp
    indexer: String,
    gateway_id: String,
    query_count: u64,
    success_count: u64,
    failure_count: u64,
    avg_indexer_response_time_ms: f64,
    total_fee_grt: f64,
    avg_seconds_behind: f64,
    avg_blocks_behind: f64,
}

// --- Allocation Aggregation Structs ---

#[derive(Row, Deserialize, Debug, Clone)]
struct AllocationAggregationRow {
    time_bucket: u32,
    subgraph: String,
    indexer: String,
    gateway_id: String,
    query_count: u64,
    success_count: u64,
    failure_count: u64,
    avg_indexer_response_time_ms: f64,
    total_fee_grt: f64,
    avg_seconds_behind: f64,
    avg_blocks_behind: f64,
}

#[derive(SimpleObject, Debug, Clone)]
struct AllocationAggregationOutput {
    time_bucket: String, // Formatted as RFC3339
    subgraph: String,
    indexer: String,
    gateway_id: String,
    query_count: u64,
    success_count: u64,
    failure_count: u64,
    avg_indexer_response_time_ms: f64,
    total_fee_grt: f64,
    avg_seconds_behind: f64,
    avg_blocks_behind: f64,
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
    /// Query for raw QoS Reports (limited fields for now)
    async fn qos_reports(
        &self,
        ctx: &Context<'_>,
        time_range: TimeRangeInput,
        #[graphql(desc = "Optional filters for the query")] filter: Option<AggregationFilterInput>,
        #[graphql(desc = "Maximum number of records to return (default 1000, max 10000)")]
        limit: Option<i32>,
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
        let query_limit = limit.unwrap_or(1000).max(0).min(10000); // Apply limits

        let query = format!(
            "SELECT event_time, gateway_id, receipt_signer, query_id, api_key, user_id, \
                    subgraph, result, response_time_ms, request_bytes, response_bytes, total_fees_usd \
             FROM qos_data {} ORDER BY event_time DESC LIMIT {}",
            where_clause, query_limit
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

    /// Query for Deployment level aggregations
    async fn deployment_aggregations(
        &self,
        ctx: &Context<'_>,
        interval: AggregationInterval,
        time_range: TimeRangeInput,
        #[graphql(desc = "Optional filters for the query")] filter: Option<AggregationFilterInput>,
        #[graphql(desc = "Maximum number of records to return (default 1000, max 10000)")]
        limit: Option<i32>,
    ) -> Result<Vec<DeploymentAggregationOutput>, String> {
        let client = get_clickhouse_client()?;
        let table_name = format!("agg_deployment_{}", interval.table_suffix());

        // Build WHERE clause
        let mut conditions = Vec::new();
        let from_dt = DateTime::parse_from_rfc3339(&time_range.from)
            .map_err(|e| format!("Invalid 'from' timestamp format: {}", e))?
            .with_timezone(&Utc);
        let to_dt = DateTime::parse_from_rfc3339(&time_range.to)
            .map_err(|e| format!("Invalid 'to' timestamp format: {}", e))?
            .with_timezone(&Utc);
        conditions.push(format!("time_bucket >= toDateTime({})", from_dt.timestamp()));
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
        let query_limit = limit.unwrap_or(1000).max(0).min(10000);

        // Query directly selects pre-aggregated columns, no further aggregation or GROUP BY
        let query = format!(
            "SELECT time_bucket, subgraph, gateway_id, \
                    query_count, success_count, failure_count, \
                    avg_response_time_ms, total_fees_usd \
             FROM {} {} \
             ORDER BY time_bucket DESC, subgraph, gateway_id \
             LIMIT {}",
            table_name, where_clause, query_limit
        );

        println!("Executing query: {}", query);

        let rows = client
            .query(&query)
            .fetch_all::<DeploymentAggregationRow>() // Uses existing struct
            .await
            .map_err(|e| format!("Database query failed: {}", e))?;

        // Map directly, row-by-row (mapping logic remains the same)
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
                avg_response_time_ms: row.avg_response_time_ms,
                total_fees_usd: row.total_fees_usd,
            })
            .collect())
    }

    /// Query for Indexer level aggregations
    async fn indexer_aggregations(
        &self,
        ctx: &Context<'_>,
        interval: AggregationInterval,
        time_range: TimeRangeInput,
        #[graphql(desc = "Optional filters for the query")] filter: Option<AggregationFilterInput>,
        #[graphql(desc = "Maximum number of records to return (default 1000, max 10000)")]
        limit: Option<i32>,
    ) -> Result<Vec<IndexerAggregationOutput>, String> {
        let client = get_clickhouse_client()?;
        let table_name = format!("agg_indexer_{}", interval.table_suffix());

        // Build WHERE clause
        let mut conditions = Vec::new();
        let from_dt = DateTime::parse_from_rfc3339(&time_range.from)
            .map_err(|e| format!("Invalid 'from' timestamp format: {}", e))?
            .with_timezone(&Utc);
        let to_dt = DateTime::parse_from_rfc3339(&time_range.to)
            .map_err(|e| format!("Invalid 'to' timestamp format: {}", e))?
            .with_timezone(&Utc);
        conditions.push(format!("time_bucket >= toDateTime({})", from_dt.timestamp()));
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
        let query_limit = limit.unwrap_or(1000).max(0).min(10000);

        // Query directly selects pre-aggregated columns, no further aggregation or GROUP BY
        let query = format!(
            "SELECT time_bucket, indexer, gateway_id, \
                    query_count, success_count, failure_count, \
                    avg_indexer_response_time_ms, total_fee_grt, \
                    avg_seconds_behind, avg_blocks_behind \
             FROM {} {} \
             ORDER BY time_bucket DESC, indexer, gateway_id \
             LIMIT {}",
            table_name, where_clause, query_limit
        );

        println!("Executing query: {}", query);

        let rows = client
            .query(&query)
            .fetch_all::<IndexerAggregationRow>() // Uses existing struct
            .await
            .map_err(|e| format!("Database query failed: {}", e))?;

        // Map directly, row-by-row (mapping logic remains the same)
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
                avg_indexer_response_time_ms: row.avg_indexer_response_time_ms,
                total_fee_grt: row.total_fee_grt,
                avg_seconds_behind: row.avg_seconds_behind,
                avg_blocks_behind: row.avg_blocks_behind,
            })
            .collect())
    }

     /// Query for Allocation level aggregations
    async fn allocation_aggregations(
        &self,
        ctx: &Context<'_>,
        interval: AggregationInterval,
        time_range: TimeRangeInput,
        #[graphql(desc = "Optional filters for the query")] filter: Option<AggregationFilterInput>,
        #[graphql(desc = "Maximum number of records to return (default 1000, max 10000)")]
        limit: Option<i32>,
    ) -> Result<Vec<AllocationAggregationOutput>, String> {
        let client = get_clickhouse_client()?;
        let table_name = format!("agg_allocation_{}", interval.table_suffix());

        // Build WHERE clause
        let mut conditions = Vec::new();
        let from_dt = DateTime::parse_from_rfc3339(&time_range.from)
            .map_err(|e| format!("Invalid 'from' timestamp format: {}", e))?
            .with_timezone(&Utc);
        let to_dt = DateTime::parse_from_rfc3339(&time_range.to)
            .map_err(|e| format!("Invalid 'to' timestamp format: {}", e))?
            .with_timezone(&Utc);
        conditions.push(format!("time_bucket >= toDateTime({})", from_dt.timestamp()));
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
        let query_limit = limit.unwrap_or(1000).max(0).min(10000);

        // Query directly selects pre-aggregated columns, no further aggregation or GROUP BY
        let query = format!(
            "SELECT time_bucket, subgraph, indexer, gateway_id, \
                    query_count, success_count, failure_count, \
                    avg_indexer_response_time_ms, total_fee_grt, \
                    avg_seconds_behind, avg_blocks_behind \
             FROM {} {} \
             ORDER BY time_bucket DESC, subgraph, indexer, gateway_id \
             LIMIT {}",
            table_name, where_clause, query_limit
        );

        println!("Executing query: {}", query);

        let rows = client
            .query(&query)
            .fetch_all::<AllocationAggregationRow>() // Uses existing struct
            .await
            .map_err(|e| format!("Database query failed: {}", e))?;

        // Map directly, row-by-row (mapping logic remains the same)
        Ok(rows
            .into_iter()
            .map(|row| AllocationAggregationOutput { // Uses existing struct
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
                avg_indexer_response_time_ms: row.avg_indexer_response_time_ms,
                total_fee_grt: row.total_fee_grt,
                avg_seconds_behind: row.avg_seconds_behind,
                avg_blocks_behind: row.avg_blocks_behind,
            })
            .collect())
    }
}

// Helper function to create ClickHouse client
// Returns the configured client builder. Connection happens on first query.
fn get_clickhouse_client() -> Result<Client, String> {
    Ok(Client::default()
        .with_url(&env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".into()))
        .with_database(&env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "default".into()))
        .with_user(&env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "graphql".into()))
        .with_password(&env::var("CLICKHOUSE_PASSWORD").unwrap_or_else(|_| "graphql_password".into())))
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
        eprintln!("Health check failed (client config): {:?}", client_result.err());
        return HttpResponse::InternalServerError().body("ClickHouse client configuration error");
    }
    let client = client_result.unwrap(); // Safe unwrap after check above

    // More advanced check: Can we execute a simple query?
    match client.query("SELECT 1").execute().await { // Use execute() for simple queries
        Ok(_) => HttpResponse::Ok().body("OK"),
        Err(e) => {
            eprintln!("Health check failed (query execution): {}", e); // Log the error
            HttpResponse::ServiceUnavailable().body(format!("ClickHouse connection error: {}", e))
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize environment variables (e.g., from .env file if needed)
    // dotenv::dotenv().ok();

    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription).finish();

    println!("GraphQL playground: http://localhost:8000");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(schema.clone()))
            .route("/graphql", web::post().to(graphql_handler))
            .route("/", web::get().to(graphql_playground))
            .route("/health", web::get().to(health_check))
    })
    .bind("0.0.0.0:8000")?
    .run()
    .await
}
