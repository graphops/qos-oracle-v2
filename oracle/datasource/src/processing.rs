use chrono::{DateTime, Duration, Timelike, Utc};
use sea_orm::{ConnectionTrait, DatabaseConnection, EntityTrait, QueryOrder, Statement, Value};
use serde::{Deserialize, Serialize};

use crate::logs::insert_log;

pub async fn get_gateway_indexer_query_results_for_time_bucket(
    db: &DatabaseConnection,
    bucket_start_time: DateTime<Utc>,
) -> anyhow::Result<Vec<IndexerQueryResultBucket>> {
    let bucket_end_time = bucket_start_time + chrono::Duration::minutes(5);

    let sql = r#"
    SELECT
        indexer,
        COUNT(*) as count,
        MIN(query_id) as query_id,
        MAX(status_code) as status_code,
        MAX(status) as status,
        MAX(user_address) as user_address,
        MAX(api_key) as api_key,
        MAX(deployment) as deployment,
        MAX(graph_env) as graph_env,
        MAX(network) as network,
        MAX(ray_id) as ray_id,
        MAX(timestamp) as timestamp,
        MAX(gateway_id) as gateway_id,
        MAX(network_chain) as network_chain,
        MAX(indexed_chain) as indexed_chain,
        MAX(url) as url,
        MAX(allocation) as allocation,
        MAX(indexer_errors) as indexer_errors,
        AVG(response_time_ms)::float as avg_indexer_latency_ms,
        MAX(response_time_ms)::float as max_indexer_latency_ms,
        percentile_cont(0.9) WITHIN GROUP (ORDER BY response_time_ms)::float as p90_indexer_latency_ms,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY response_time_ms)::float as p99_indexer_latency_ms,
        AVG(fee) as avg_query_fee,
        MAX(fee) as max_query_fee,
        percentile_cont(0.9) WITHIN GROUP (ORDER BY fee) as p90_query_fee,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY fee) as p99_query_fee,
        SUM(fee) as total_query_fee,
        AVG(blocks_behind)::float as avg_blocks_behind,
        MAX(blocks_behind)::float as max_blocks_behind,
        percentile_cont(0.9) WITHIN GROUP (ORDER BY blocks_behind)::float as p90_blocks_behind,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY blocks_behind)::float as p99_blocks_behind,
        AVG(seconds_behind)::float as avg_seconds_behind,
        MAX(seconds_behind)::float as max_seconds_behind,
        percentile_cont(0.9) WITHIN GROUP (ORDER BY seconds_behind)::float as p90_seconds_behind,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY seconds_behind)::float as p99_seconds_behind,
        SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END)::bigint as num_indexer_successful_responses,
        (SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END)::float / COUNT(*)::float) as proportion_indexer_successful_responses
    FROM indexer_query_results
    WHERE timestamp >= $1 AND timestamp < $2
    GROUP BY indexer, deployment
    "#;

    let result = Statement::from_sql_and_values(
        sea_orm::DatabaseBackend::Postgres,
        sql,
        vec![
            Value::BigInt(Some(bucket_start_time.timestamp_millis())),
            Value::BigInt(Some(bucket_end_time.timestamp_millis())),
        ],
    );

    tracing::info!(
        "About to query indexer, interval start: {}, interval end: {}",
        bucket_start_time.timestamp_millis(),
        bucket_end_time.timestamp_millis()
    );
    let query_result: Vec<IndexerQueryResultBucket> = db
        .query_all(result)
        .await?
        .into_iter()
        .map(|row| IndexerQueryResultBucket {
            indexer: row.try_get::<String>("", "indexer").unwrap_or_default(),
            count: row.try_get::<i64>("", "count").unwrap_or(0),
            query_id: row.try_get::<String>("", "query_id").unwrap_or_default(),
            status_code: row.try_get::<i32>("", "status_code").unwrap_or(0),
            status: row.try_get::<String>("", "status").unwrap_or_default(),
            user_address: row
                .try_get::<String>("", "user_address")
                .unwrap_or_default(),
            api_key: row.try_get::<String>("", "api_key").unwrap_or_default(),
            deployment: row.try_get::<String>("", "deployment").unwrap_or_default(),
            graph_env: row
                .try_get::<Option<String>>("", "graph_env")
                .unwrap_or(None),
            network: row.try_get::<Option<String>>("", "network").unwrap_or(None),
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
            avg_indexer_latency_ms: row
                .try_get::<f64>("", "avg_indexer_latency_ms")
                .unwrap_or(0.0),
            max_indexer_latency_ms: row
                .try_get::<f64>("", "max_indexer_latency_ms")
                .unwrap_or(0.0),
            p90_indexer_latency_ms: row
                .try_get::<f64>("", "p90_indexer_latency_ms")
                .unwrap_or(0.0),
            p99_indexer_latency_ms: row
                .try_get::<f64>("", "p99_indexer_latency_ms")
                .unwrap_or(0.0),
            avg_query_fee: row.try_get::<f64>("", "avg_query_fee").unwrap_or(0.0),
            max_query_fee: row.try_get::<f32>("", "max_query_fee").unwrap_or(0.0),
            p90_query_fee: row.try_get::<f64>("", "p90_query_fee").unwrap_or(0.0),
            p99_query_fee: row.try_get::<f64>("", "p99_query_fee").unwrap_or(0.0),
            total_query_fee: row.try_get::<f32>("", "total_query_fee").unwrap_or(0.0),
            avg_blocks_behind: row.try_get::<f64>("", "avg_blocks_behind").unwrap_or(0.0),
            max_blocks_behind: row.try_get::<f64>("", "max_blocks_behind").unwrap_or(0.0),
            p90_blocks_behind: row.try_get::<f64>("", "p90_blocks_behind").unwrap_or(0.0),
            p99_blocks_behind: row.try_get::<f64>("", "p99_blocks_behind").unwrap_or(0.0),
            avg_seconds_behind: row.try_get::<f64>("", "avg_seconds_behind").unwrap_or(0.0),
            max_seconds_behind: row.try_get::<f64>("", "max_seconds_behind").unwrap_or(0.0),
            p90_seconds_behind: row.try_get::<f64>("", "p90_seconds_behind").unwrap_or(0.0),
            p99_seconds_behind: row.try_get::<f64>("", "p99_seconds_behind").unwrap_or(0.0),
            num_indexer_successful_responses: row
                .try_get::<i64>("", "num_indexer_successful_responses")
                .unwrap_or(0),
            proportion_indexer_successful_responses: row
                .try_get::<f64>("", "proportion_indexer_successful_responses")
                .unwrap_or(0.0),
        })
        .collect();

    tracing::info!("Result indexer: {:?}", query_result);
    Ok(query_result)
}

pub async fn get_gateway_client_query_results_for_time_bucket(
    db: &DatabaseConnection,
    bucket_start_time: DateTime<Utc>,
) -> anyhow::Result<Vec<ClientQueryResultBucket>> {
    let bucket_end_time = bucket_start_time + chrono::Duration::minutes(5);

    let sql = r#"
    SELECT
        deployment,
        COUNT(*) as count,
        MIN(query_id) as query_id,
        MAX(status_code) as status_code,
        MAX(status) as status,
        MAX(user_address) as user_address,
        MAX(api_key) as api_key,
        MAX(graph_env) as graph_env,
        MAX(network) as network,
        SUM(query_count) as query_count,
        MAX(budget) as budget,
        SUM(fee_usd) as fee_usd,
        MAX(ray_id) as ray_id,
        MAX(timestamp) as timestamp,
        MAX(gateway_id) as gateway_id,
        MAX(network_chain) as network_chain,
        MAX(indexed_chain) as indexed_chain,
        AVG(response_time_ms)::float as avg_response_time_ms,
        MAX(response_time_ms)::float as max_response_time_ms,
        percentile_cont(0.9) WITHIN GROUP (ORDER BY response_time_ms)::float as p90_response_time_ms,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY response_time_ms)::float as p99_response_time_ms,
        AVG(fee) as avg_query_fee,
        MAX(fee) as max_query_fee,
        percentile_cont(0.9) WITHIN GROUP (ORDER BY fee) as p90_query_fee,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY fee) as p99_query_fee,
        SUM(fee) as total_query_fee,
        SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END)::bigint as num_successful_responses,
        (SUM(CASE WHEN status_code = 200 THEN 1 ELSE 0 END)::float / COUNT(*)::float) as proportion_successful_responses
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

    tracing::info!(
        "About to query client, interval start: {}, interval end: {}",
        bucket_start_time.timestamp_millis(),
        bucket_end_time.timestamp_millis()
    );
    let query_result: Vec<ClientQueryResultBucket> = db
        .query_all(result)
        .await?
        .into_iter()
        .map(|row| ClientQueryResultBucket {
            deployment: row.try_get::<String>("", "deployment").unwrap_or_default(),
            count: row.try_get::<i64>("", "count").unwrap_or(0),
            query_id: row.try_get::<String>("", "query_id").unwrap_or_default(),
            status_code: row.try_get::<i32>("", "status_code").unwrap_or(0),
            status: row.try_get::<String>("", "status").unwrap_or_default(),
            user_address: row
                .try_get::<String>("", "user_address")
                .unwrap_or_default(),
            api_key: row.try_get::<String>("", "api_key").unwrap_or_default(),
            graph_env: row
                .try_get::<Option<String>>("", "graph_env")
                .unwrap_or(None),
            network: row.try_get::<Option<String>>("", "network").unwrap_or(None),
            query_count: row.try_get::<i64>("", "query_count").unwrap_or(0),
            budget: row.try_get::<Option<String>>("", "budget").unwrap_or(None),
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
            avg_response_time_ms: row
                .try_get::<f64>("", "avg_response_time_ms")
                .unwrap_or(0.0),
            max_response_time_ms: row
                .try_get::<f64>("", "max_response_time_ms")
                .unwrap_or(0.0),
            p90_response_time_ms: row
                .try_get::<f64>("", "p90_response_time_ms")
                .unwrap_or(0.0),
            p99_response_time_ms: row
                .try_get::<f64>("", "p99_response_time_ms")
                .unwrap_or(0.0),
            avg_query_fee: row.try_get::<f64>("", "avg_query_fee").unwrap_or(0.0),
            max_query_fee: row.try_get::<f32>("", "max_query_fee").unwrap_or(0.0),
            p90_query_fee: row.try_get::<f64>("", "p90_query_fee").unwrap_or(0.0),
            p99_query_fee: row.try_get::<f64>("", "p99_query_fee").unwrap_or(0.0),
            total_query_fee: row.try_get::<f32>("", "total_query_fee").unwrap_or(0.0),
            num_successful_responses: row
                .try_get::<i64>("", "num_successful_responses")
                .unwrap_or(0),
            proportion_successful_responses: row
                .try_get::<f64>("", "proportion_successful_responses")
                .unwrap_or(0.0),
        })
        .collect();

    tracing::info!("Result client: {:?}", query_result);
    Ok(query_result)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerQueryResultBucket {
    pub indexer: String,
    pub count: i64,
    pub query_id: String,
    pub status_code: i32,
    pub status: String,
    pub user_address: String,
    pub api_key: String,
    pub deployment: String,
    pub graph_env: Option<String>,
    pub network: Option<String>,
    pub ray_id: Option<String>,
    pub timestamp: i64,
    pub gateway_id: String,
    pub network_chain: Option<String>,
    pub indexed_chain: Option<String>,
    pub url: Option<String>,
    pub allocation: Option<String>,
    pub indexer_errors: Option<String>,
    pub avg_indexer_latency_ms: f64,
    pub max_indexer_latency_ms: f64,
    pub p90_indexer_latency_ms: f64,
    pub p99_indexer_latency_ms: f64,
    pub avg_query_fee: f64,
    pub max_query_fee: f32,
    pub p90_query_fee: f64,
    pub p99_query_fee: f64,
    pub total_query_fee: f32,
    pub avg_blocks_behind: f64,
    pub max_blocks_behind: f64,
    pub p90_blocks_behind: f64,
    pub p99_blocks_behind: f64,
    pub avg_seconds_behind: f64,
    pub max_seconds_behind: f64,
    pub p90_seconds_behind: f64,
    pub p99_seconds_behind: f64,
    pub num_indexer_successful_responses: i64,
    pub proportion_indexer_successful_responses: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientQueryResultBucket {
    pub deployment: String,
    pub count: i64,
    pub query_id: String,
    pub status_code: i32,
    pub status: String,
    pub user_address: String,
    pub api_key: String,
    pub graph_env: Option<String>,
    pub network: Option<String>,
    pub query_count: i64,
    pub budget: Option<String>,
    pub fee_usd: f32,
    pub ray_id: Option<String>,
    pub timestamp: i64,
    pub gateway_id: String,
    pub network_chain: Option<String>,
    pub indexed_chain: Option<String>,
    pub avg_response_time_ms: f64,
    pub max_response_time_ms: f64,
    pub p90_response_time_ms: f64,
    pub p99_response_time_ms: f64,
    pub avg_query_fee: f64,
    pub max_query_fee: f32,
    pub p90_query_fee: f64,
    pub p99_query_fee: f64,
    pub total_query_fee: f32,
    pub num_successful_responses: i64,
    pub proportion_successful_responses: f64,
}

pub async fn process_and_publish_indexer_data(
    db: &DatabaseConnection,
    buckets: Vec<IndexerQueryResultBucket>,
    bucket_time: DateTime<Utc>,
) -> anyhow::Result<()> {
    // Convert bucket data to JSON array
    let json_data = serde_json::to_string(&buckets)?;

    // tracing::info!(
    //     "JSON data to be stored for indexer buckets: {:?}",
    //     json_data
    // );
    // Insert log with posted = false
    let _log = insert_log(
        db,
        bucket_time,
        json_data,
        "IndexerQueryResult".to_string(),
        false,
    )
    .await?;

    // Publish to IPFS
    if let Ok(ipfs_hash) = publish_to_ipfs(&buckets).await {
        // Update log to posted = true
        //update_log_posted_status(db, log.id, true).await?;
        tracing::info!("Published indexer data to IPFS: {}", ipfs_hash);
    } else {
        tracing::error!("Failed to publish indexer data to IPFS");
    }

    Ok(())
}

pub async fn process_and_publish_client_data(
    db: &DatabaseConnection,
    buckets: Vec<ClientQueryResultBucket>,
    bucket_time: DateTime<Utc>,
) -> anyhow::Result<()> {
    // Convert bucket data to JSON array
    let json_data = serde_json::to_string(&buckets)?;

    //tracing::info!("JSON data to be stored for client buckets: {:?}", json_data);
    // Insert log with posted = false
    let _log = insert_log(
        db,
        bucket_time,
        json_data,
        "ClientQueryResult".to_string(),
        false,
    )
    .await?;

    // Publish to IPFS
    if let Ok(ipfs_hash) = publish_to_ipfs(&buckets).await {
        // Update log to posted = true
        //update_log_posted_status(db, log.id, true).await?;
        tracing::info!("Published client data to IPFS: {}", ipfs_hash);
    } else {
        tracing::error!("Failed to publish client data to IPFS");
    }

    Ok(())
}
async fn publish_to_ipfs<T: serde::Serialize>(_data: &T) -> anyhow::Result<String> {
    // Implement IPFS publishing logic here
    // This is a placeholder implementation
    Ok("QmHashPlaceholder".to_string())
}

pub async fn get_starting_timestamp(db: &DatabaseConnection) -> anyhow::Result<DateTime<Utc>> {
    // Check for the latest IPFS log timestamp
    let latest_ipfs_log = entity::ipfs_logs::Entity::find()
        .order_by_desc(entity::ipfs_logs::Column::Id)
        .one(db)
        .await?;

    let oldest_timestamp = if let Some(log) = latest_ipfs_log {
        tracing::info!(
            "Latest timestamp for ipfs log: {}",
            log.bucket_start_timestamp.and_utc()
        );
        log.bucket_start_timestamp.and_utc()
    } else {
        // If no IPFS log found, find the oldest timestamp from indexer and client data
        let oldest_indexer_timestamp = entity::indexer_query_results::Entity::find()
            .order_by_asc(entity::indexer_query_results::Column::Timestamp)
            .one(db)
            .await?
            .and_then(|record| record.timestamp)
            .map(|ts| DateTime::<Utc>::from_timestamp(ts / 1000, 0).unwrap());

        let oldest_client_timestamp = entity::client_query_result::Entity::find()
            .order_by_asc(entity::client_query_result::Column::Timestamp)
            .one(db)
            .await?
            .and_then(|record| record.timestamp)
            .map(|ts| DateTime::<Utc>::from_timestamp(ts / 1000, 0).unwrap());

        tracing::info!(
            "Oldest client/indexer timestamp found! Indexer {:?}, Client {:?}",
            oldest_indexer_timestamp,
            oldest_client_timestamp
        );
        match (oldest_indexer_timestamp, oldest_client_timestamp) {
            (Some(indexer), Some(client)) => indexer.min(client),
            (Some(indexer), None) => indexer,
            (None, Some(client)) => client,
            (None, None) => Utc::now() - Duration::hours(1), // Default to 1 hour ago if no data found
        }
    };

    Ok(align_to_bucket(oldest_timestamp))
}

pub fn align_to_bucket(timestamp: DateTime<Utc>) -> DateTime<Utc> {
    let minutes = timestamp.minute();
    let bucket_start = (minutes / 5) * 5;
    timestamp
        .with_minute(bucket_start)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap()
}
