use chrono::{DateTime, Utc};
use sea_orm::{ConnectionTrait, DatabaseConnection, Statement, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::logs::insert_log;

pub async fn get_gateway_indexer_query_results_for_time_bucket(
    db: &DatabaseConnection,
    bucket_start_time: DateTime<Utc>,
) -> anyhow::Result<HashMap<String, IndexerQueryResultBucket>> {
    let bucket_end_time = bucket_start_time + chrono::Duration::minutes(5);

    let sql = r#"
    SELECT
        indexer,
        COUNT(*) as count,
        MIN(query_id) as query_id,
        MAX(status_code::text) as status_code,
        MAX(status) as status,
        AVG(response_time_ms) as response_time_ms,
        MAX(user_address) as user_address,
        MAX(api_key) as api_key,
        MAX(deployment) as deployment,
        MAX(graph_env) as graph_env,
        MAX(network) as network,
        SUM(fee) as fee,
        MAX(ray_id) as ray_id,
        MAX(timestamp) as timestamp,
        MAX(gateway_id) as gateway_id,
        MAX(network_chain) as network_chain,
        MAX(indexed_chain) as indexed_chain,
        MAX(url) as url,
        MAX(allocation) as allocation,
        MAX(indexer_errors) as indexer_errors,
        AVG(seconds_behind) as seconds_behind,
        AVG(blocks_behind) as blocks_behind
    FROM indexer_query_results
    WHERE timestamp >= $1 AND timestamp < $2
    GROUP BY indexer
    "#;

    let result = Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Postgres,
        sql,
        vec![
            Value::BigInt(Some(bucket_start_time.timestamp_millis())),
            Value::BigInt(Some(bucket_end_time.timestamp_millis())),
        ],
    );

    tracing::info!("About to query indexer");
    let query_result: Vec<IndexerQueryResultBucket> = db
        .query_all(result)
        .await?
        .into_iter()
        .map(|row| IndexerQueryResultBucket {
            indexer: row.try_get::<String>("", "indexer").unwrap_or_default(),
            count: row.try_get::<i64>("", "count").unwrap_or(0) as u32,
            query_id: row.try_get::<String>("", "query_id").unwrap_or_default(),
            status_code: row
                .try_get::<String>("", "status_code")
                .unwrap_or_default()
                .parse()
                .unwrap_or(0),
            status: row.try_get::<String>("", "status").unwrap_or_default(),
            response_time_ms: row.try_get::<f64>("", "response_time_ms").unwrap_or(0.0) as u32,
            user_address: row
                .try_get::<String>("", "user_address")
                .unwrap_or_default(),
            api_key: row.try_get::<String>("", "api_key").unwrap_or_default(),
            deployment: row.try_get::<String>("", "deployment").unwrap_or_default(),
            graph_env: row
                .try_get::<Option<String>>("", "graph_env")
                .unwrap_or(None),
            network: row.try_get::<Option<String>>("", "network").unwrap_or(None),
            fee: row.try_get::<f32>("", "fee").unwrap_or(0.0),
            ray_id: row.try_get::<Option<String>>("", "ray_id").unwrap_or(None),
            timestamp: row.try_get::<i64>("", "timestamp").unwrap_or(0),
            gateway_id: row.try_get::<String>("", "gateway_id").unwrap_or_default(),
            network_chain: row
                .try_get::<Option<String>>("", "network_chain")
                .unwrap_or(None),
            indexed_chain: row
                .try_get::<Option<String>>("", "indexed_chain")
                .unwrap_or(None),
            url: row.try_get::<Option<String>>("", "url").unwrap_or(None),
            allocation: row
                .try_get::<Option<String>>("", "allocation")
                .unwrap_or(None),
            indexer_errors: row
                .try_get::<Option<String>>("", "indexer_errors")
                .unwrap_or(None),
            seconds_behind: row.try_get::<f64>("", "seconds_behind").unwrap_or(0.0) as u32,
            blocks_behind: row.try_get::<f64>("", "blocks_behind").unwrap_or(0.0) as u32,
        })
        .collect();

        tracing::info!("Result indexer: {:?}", query_result);
    Ok(query_result
        .into_iter()
        .map(|bucket| (bucket.indexer.clone(), bucket))
        .collect())
}

pub async fn get_gateway_client_query_results_for_time_bucket(
    db: &DatabaseConnection,
    bucket_start_time: DateTime<Utc>,
) -> anyhow::Result<HashMap<String, ClientQueryResultBucket>> {
    let bucket_end_time = bucket_start_time + chrono::Duration::minutes(5);

    let sql = r#"
    SELECT
        deployment,
        COUNT(*) as count,
        MIN(query_id) as query_id,
        MAX(status_code::text) as status_code,
        MAX(status) as status,
        AVG(response_time_ms) as response_time_ms,
        MAX(user_address) as user_address,
        MAX(api_key) as api_key,
        MAX(graph_env) as graph_env,
        MAX(network) as network,
        SUM(query_count) as query_count,
        MAX(budget) as budget,
        SUM(budget_float) as budget_float,
        SUM(fee) as fee,
        SUM(fee_usd) as fee_usd,
        MAX(ray_id) as ray_id,
        MAX(timestamp) as timestamp,
        MAX(gateway_id) as gateway_id,
        MAX(network_chain) as network_chain,
        MAX(indexed_chain) as indexed_chain
    FROM client_query_result
    WHERE timestamp >= $1 AND timestamp < $2
    GROUP BY deployment
    "#;

    let result = Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Postgres,
        sql,
        vec![
            Value::BigInt(Some(bucket_start_time.timestamp_millis())),
            Value::BigInt(Some(bucket_end_time.timestamp_millis())),
        ],
    );

    tracing::info!("About to query client");
    let query_result: Vec<ClientQueryResultBucket> = db
        .query_all(result)
        .await?
        .into_iter()
        .map(|row| ClientQueryResultBucket {
            deployment: row.try_get::<String>("", "deployment").unwrap_or_default(),
            count: row.try_get::<i64>("", "count").unwrap_or(0) as u32,
            query_id: row.try_get::<String>("", "query_id").unwrap_or_default(),
            status_code: row
                .try_get::<String>("", "status_code")
                .unwrap_or_default()
                .parse()
                .unwrap_or(0),
            status: row.try_get::<String>("", "status").unwrap_or_default(),
            response_time_ms: row.try_get::<f64>("", "response_time_ms").unwrap_or(0.0) as u32,
            user_address: row
                .try_get::<String>("", "user_address")
                .unwrap_or_default(),
            api_key: row.try_get::<String>("", "api_key").unwrap_or_default(),
            graph_env: row
                .try_get::<Option<String>>("", "graph_env")
                .unwrap_or(None),
            network: row.try_get::<Option<String>>("", "network").unwrap_or(None),
            query_count: row.try_get::<i32>("", "query_count").unwrap_or(0),
            budget: row.try_get::<Option<String>>("", "budget").unwrap_or(None),
            budget_float: row.try_get::<f32>("", "budget_float").unwrap_or(0.0),
            fee: row.try_get::<f32>("", "fee").unwrap_or(0.0),
            fee_usd: row.try_get::<f32>("", "fee_usd").unwrap_or(0.0),
            ray_id: row.try_get::<Option<String>>("", "ray_id").unwrap_or(None),
            timestamp: row.try_get::<i64>("", "timestamp").unwrap_or(0),
            gateway_id: row.try_get::<String>("", "gateway_id").unwrap_or_default(),
            network_chain: row
                .try_get::<Option<String>>("", "network_chain")
                .unwrap_or(None),
            indexed_chain: row
                .try_get::<Option<String>>("", "indexed_chain")
                .unwrap_or(None),
        })
        .collect();

        tracing::info!("Result client: {:?}", query_result);
    Ok(query_result
        .into_iter()
        .map(|bucket| (bucket.deployment.clone(), bucket))
        .collect())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerQueryResultBucket {
    pub indexer: String,
    pub count: u32,
    pub query_id: String,
    pub status_code: i32,
    pub status: String,
    pub response_time_ms: u32,
    pub user_address: String,
    pub api_key: String,
    pub deployment: String,
    pub graph_env: Option<String>,
    pub network: Option<String>,
    pub fee: f32,
    pub ray_id: Option<String>,
    pub timestamp: i64,
    pub gateway_id: String,
    pub network_chain: Option<String>,
    pub indexed_chain: Option<String>,
    pub url: Option<String>,
    pub allocation: Option<String>,
    pub indexer_errors: Option<String>,
    pub seconds_behind: u32,
    pub blocks_behind: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientQueryResultBucket {
    pub deployment: String,
    pub count: u32,
    pub query_id: String,
    pub status_code: i32,
    pub status: String,
    pub response_time_ms: u32,
    pub user_address: String,
    pub api_key: String,
    pub graph_env: Option<String>,
    pub network: Option<String>,
    pub query_count: i32,
    pub budget: Option<String>,
    pub budget_float: f32,
    pub fee: f32,
    pub fee_usd: f32,
    pub ray_id: Option<String>,
    pub timestamp: i64,
    pub gateway_id: String,
    pub network_chain: Option<String>,
    pub indexed_chain: Option<String>,
}

pub async fn process_and_publish_indexer_data(
    db: &DatabaseConnection,
    indexer: String,
    bucket: IndexerQueryResultBucket,
) -> anyhow::Result<()> {
    // Convert bucket data to JSON
    let json_data = serde_json::to_string(&bucket)?;

    tracing::info!("JSON data to be stored for indexer '{}': {:?}", indexer, json_data);
    // Insert log with posted = false
    let log = insert_log(
        db,
        Utc::now(),
        json_data,
        "IndexerQueryResult".to_string(),
        false,
    )
    .await;

    // // Publish to IPFS (you need to implement this function)
    // if let Ok(ipfs_hash) = publish_to_ipfs(&bucket).await {
    //     // Update log to posted = true
    //     //update_log_posted_status(db, log.id, true).await?;
    //     tracing::info!("Published indexer data to IPFS: {}", ipfs_hash);
    // } else {
    //     tracing::error!("Failed to publish indexer data to IPFS");
    // }

    Ok(())
}

pub async fn process_and_publish_client_data(
    db: &DatabaseConnection,
    deployment: String,
    bucket: ClientQueryResultBucket,
) -> anyhow::Result<()> {
    // Convert bucket data to JSON
    let json_data = serde_json::to_string(&bucket)?;

    tracing::info!("JSON data to be stored for deployment '{}': {:?}", deployment, json_data);
    // Insert log with posted = false
    let log = insert_log(
        db,
        Utc::now(),
        json_data,
        "ClientQueryResult".to_string(),
        false,
    )
    .await;

    // // Publish to IPFS (you need to implement this function)
    // if let Ok(ipfs_hash) = publish_to_ipfs(&bucket).await {
    //     // Update log to posted = true
    //     //update_log_posted_status(db, log.id, true).await?;
    //     tracing::info!("Published client data to IPFS: {}", ipfs_hash);
    // } else {
    //     tracing::error!("Failed to publish client data to IPFS");
    // }

    Ok(())
}

async fn publish_to_ipfs<T: serde::Serialize>(data: &T) -> anyhow::Result<String> {
    // Implement IPFS publishing logic here
    // This is a placeholder implementation
    Ok("QmHashPlaceholder".to_string())
}
