use actix_web::{web, App, HttpServer, HttpResponse, Result};
use async_graphql::{Schema, EmptyMutation, EmptySubscription, Context, Object, SimpleObject};
use async_graphql_actix_web::{GraphQLRequest, GraphQLResponse};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};

// Define the GraphQL types
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

#[derive(SimpleObject)]
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
    indexer_queries: Vec<IndexerQuery>,
}

// Structure to receive raw data from ClickHouse - needs both Row and Deserialize
#[derive(Row, Deserialize)]
struct ClickhouseRow {
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
    indexer_queries: Vec<(
        String, String, String, String, String, 
        f64, u32, u32, String, String, u64
    )>,
}

struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Query for QoS Reports within a time range.
    async fn qos_reports(&self, _ctx: &Context<'_>, from: String, to: String) -> async_graphql::Result<Vec<QosReport>> {
        let client = Client::default()
            .with_url(&std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://localhost:8123".into()))
            .with_database(&std::env::var("CLICKHOUSE_DB").unwrap_or_else(|_| "default".into()));

        let query = format!(
            "SELECT event_time, gateway_id, receipt_signer, query_id, api_key, user_id, subgraph, result, response_time_ms, request_bytes, response_bytes, total_fees_usd, indexer_queries \
             FROM raw_qos_data \
             WHERE event_time BETWEEN toDateTime('{}') AND toDateTime('{}') \
             ORDER BY event_time DESC",
            from, to
        );

        // Properly handle the ClickHouse cursor
        let cursor_result = client.query(&query).fetch::<ClickhouseRow>();
        
        match cursor_result {
            Ok(mut cursor) => {
                let mut reports = Vec::new();
                
                // Manually collect rows from the cursor
                while let Ok(Some(row)) = cursor.next().await {
                    // Convert the tuple array to IndexerQuery structs
                    let indexer_queries = row.indexer_queries.into_iter().map(|tuple| {
                        IndexerQuery {
                            indexer: tuple.0,
                            deployment: tuple.1,
                            allocation: tuple.2,
                            indexed_chain: tuple.3,
                            url: tuple.4,
                            fee_grt: tuple.5,
                            response_time_ms: tuple.6,
                            seconds_behind: tuple.7,
                            result: tuple.8,
                            indexer_errors: tuple.9,
                            blocks_behind: tuple.10,
                        }
                    }).collect();

                    reports.push(QosReport {
                        event_time: row.event_time,
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
                        indexer_queries,
                    });
                }

                Ok(reports)
            },
            Err(err) => Err(async_graphql::Error::new(err.to_string())),
        }
    }
}

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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .finish();

    println!("GraphQL playground: http://localhost:8000");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(schema.clone()))
            .route("/graphql", web::post().to(graphql_handler))
            .route("/", web::get().to(graphql_playground))
    })
    .bind("0.0.0.0:8000")?
    .run()
    .await
}