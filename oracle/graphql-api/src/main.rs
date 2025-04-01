use actix_web::{web, App, HttpResponse, HttpServer, Result};
use async_graphql::{Context, EmptyMutation, EmptySubscription, Object, Schema, SimpleObject};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse};
use clickhouse::{Client, Row};
use serde::Deserialize;
// use bs58;
// use hex;

// Complete QoS Report matching the database schema
#[derive(SimpleObject, Deserialize)]
struct QosReport {
    event_time: String,
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
    event_time: u32,
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
}

// Count query result struct
#[derive(Row, Deserialize, Debug)]
struct CountResult {
    count: u64,
}

struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Query for QoS Reports with complete fields
    async fn qos_reports(
        &self,
        _ctx: &Context<'_>,
        from: Option<String>,
        to: Option<String>,
        limit: Option<i32>,
    ) -> async_graphql::Result<Vec<QosReport>> {
        // Configure client with proper settings and timeouts
        let client = Client::default()
            .with_url(
                &std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".into()),
            )
            .with_database(&std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "default".into()))
            .with_user(&std::env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "graphql".into()))
            .with_password(
                &std::env::var("CLICKHOUSE_PASSWORD").unwrap_or_else(|_| "graphql_password".into()),
            );

        // First, check if there's any data using a simple count query
        let count_query = "SELECT count() as count FROM qos_data";
        println!("Checking table count: {}", count_query);

        // Use the proper Row trait implementation for count
        let count_result = client.query(count_query).fetch_one::<CountResult>().await;

        match count_result {
            Ok(count_row) => {
                println!("Total records in qos_data: {}", count_row.count);
                if count_row.count == 0 {
                    return Ok(Vec::new());
                }
            }
            Err(e) => {
                println!("Error getting count: {}", e);
                return Err(async_graphql::Error::new(format!("Database error: {}", e)));
            }
        }

        // Build full query with all fields
        let mut query = "SELECT 
        event_time, 
        gateway_id, 
        query_id,
        api_key,
        user_id,
        receipt_signer,
        subgraph,
        result,
        response_time_ms,
        request_bytes,
        response_bytes,
        total_fees_usd
        FROM qos_data"
            .to_string();

        // event_time,
        // gateway_id,
        // receipt_signer,
        // query_id,
        // api_key,
        // user_id,
        // subgraph,
        // result,
        // response_time_ms,
        // request_bytes,
        // response_bytes,
        // total_fees_usd

        // Add date filtering if provided
        if from.is_some() || to.is_some() {
            query.push_str(" WHERE ");

            if let Some(from_val) = &from {
                query.push_str(&format!("event_time >= toDateTime('{}')", from_val));
                if to.is_some() {
                    query.push_str(" AND ");
                }
            }

            if let Some(to_val) = &to {
                query.push_str(&format!("event_time <= toDateTime('{}')", to_val));
            }
        }

        // Use the provided limit or default to 100
        let limit_value = limit.unwrap_or(100).max(1).min(1000);
        query.push_str(&format!(" ORDER BY event_time DESC LIMIT {}", limit_value));

        println!("Executing query: {}", query);

        // Use fetch_all with the Row trait implementation
        let rows_result = client.query(&query).fetch_all::<QosReportRow>().await;

        match rows_result {
            Ok(rows) => {
                println!("Query returned {} rows", rows.len());

                // Convert to GraphQL output type
                let reports: Vec<QosReport> = rows
                    .into_iter()
                    .map(|row| {
                        println!(
                            "Processing row: event_time={}, gateway_id={}, query_id={}",
                            row.event_time, row.gateway_id, row.query_id
                        );

                        QosReport {
                            event_time: row.event_time.to_string(),
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
                        }
                    })
                    .collect();

                println!("Returning {} reports", reports.len());
                Ok(reports)
            }
            Err(err) => {
                println!("Query failed: {}", err);
                Err(async_graphql::Error::new(format!(
                    "Database error: {}",
                    err
                )))
            }
        }
    }
}

/*
// Additional fields and nested data queries (commented out for debugging)
async fn fetch_nested_data(&self, client: &Client, query_id: &str) -> Result<Vec<IndexerQuery>, String> {
    // ...
}
*/

type GQLSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

async fn graphql_handler(schema: web::Data<GQLSchema>, req: GraphQLRequest) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

async fn graphql_playground() -> Result<HttpResponse> {
    let html = r#"
    <!DOCTYPE html>
    <html>
    <head>
        <title>GraphQL Playground</title>
        <meta charset="utf-8" />
        <meta name="viewport" content="user-scalable=no, initial-scale=1.0, minimum-scale=1.0, maximum-scale=1.0, minimal-ui">
        <link rel="stylesheet" href="//cdn.jsdelivr.net/npm/graphql-playground-react@1.7.22/build/static/css/index.css" />
        <script src="//cdn.jsdelivr.net/npm/graphql-playground-react@1.7.22/build/static/js/middleware.js"></script>
    </head>
    <body>
        <div id="root">
            <style>
                body { background-color: rgb(23, 42, 58); font-family: Open Sans, sans-serif; height: 90vh; }
                #root { height: 100%; width: 100%; display: flex; align-items: center; justify-content: center; }
                .loading { font-size: 32px; font-weight: 200; color: rgba(255, 255, 255, .6); margin-left: 20px; }
                img { width: 78px; height: 78px; }
                .title { font-weight: 400; }
            </style>
            <img src='//cdn.jsdelivr.net/npm/graphql-playground-react/build/logo.png' alt=''>
            <div class="loading"> Loading <span class="title">GraphQL Playground</span></div>
        </div>
        <script>window.addEventListener('load', function (event) {
              const loadingWrapper = document.getElementById('root');
              loadingWrapper.classList.add('playgroundIn');
              const root = document.getElementById('root');
              root.classList.add('playgroundIn');
              GraphQLPlayground.init(root, { endpoint: '/graphql' })
            })</script>
    </body>
    </html>
    "#;

    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html))
}

// Health check endpoint for Docker
async fn health_check() -> HttpResponse {
    HttpResponse::Ok().body("OK")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
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
