use std::{env, time::Duration};

use async_trait::async_trait;
use chrono::Utc;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use rdkafka::{
    consumer::{DefaultConsumerContext, StreamConsumer},
    error::KafkaError,
    Message,
};
use sea_orm::{prelude::Uuid, ActiveModelTrait, ConnectOptions, Database, DatabaseConnection, Set};

use migration::MigratorTrait;

use crate::{Datasource, DatasourceWriter, GatewayClientQueryResult, GatewayIndexerQueryResult};

lazy_static! {
    static ref CHAIN_ID: u64 = env::var("CHAIN_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(421614);
}

/// The rdbms (this implementation uses postgres) datasource implements both the `Datasource` and `DatasourceWriter` traits.
/// Allows a user to query and store `GatewayClientQueryResult` records stored in the postgres database instance.
#[derive(Clone)]
pub struct DatasourceClientQueryPostgres {
    pub db_conn: DatabaseConnection,
}

/// The rdbms (this implementation uses postgres) datasource implements the `DatasourceWriter` trait.
/// Allows a user to query and store `GatewayIndexerQueryResult` records stored in the postgres database instance.
#[derive(Clone)]
pub struct DatasourceIndexerQueryPostgres {
    pub db_conn: DatabaseConnection,
}

#[async_trait]
impl Datasource for DatasourceClientQueryPostgres {
    /// Create a DatasourcePostgres instance by instantiating a postgres db connection.
    ///
    /// # Arguments
    ///
    /// * `db_url` - the postgres connection url. ex: postgress://username:password@host:port/database
    async fn create(db_url: String) -> Result<&'static Self, sea_orm::DbErr> {
        let mut opt = ConnectOptions::new(db_url);
        opt.max_connections(100)
            .min_connections(5)
            .connect_timeout(Duration::from_secs(30))
            .acquire_timeout(Duration::from_secs(10))
            .idle_timeout(Duration::from_secs(10))
            .max_lifetime(Duration::from_secs(10))
            .sqlx_logging(true)
            .sqlx_logging_level(tracing::log::LevelFilter::Info);
        let db_conn = Database::connect(opt).await.map_err(sea_orm::DbErr::from)?;
        // run the migrations on the instance
        migration::Migrator::up(&db_conn, None).await?;

        Ok(Box::leak(Box::new(Self { db_conn })))
    }
}

#[async_trait]
impl Datasource for DatasourceIndexerQueryPostgres {
    /// Create a DatasourcePostgres instance by instantiating a postgres db connection.
    ///
    /// # Arguments
    ///
    /// * `db_url` - the postgres connection url. ex: postgress://username:password@host:port/database
    async fn create(db_url: String) -> Result<&'static Self, sea_orm::DbErr> {
        let mut opt = ConnectOptions::new(db_url);
        opt.max_connections(100)
            .min_connections(5)
            .connect_timeout(Duration::from_secs(30))
            .acquire_timeout(Duration::from_secs(10))
            .idle_timeout(Duration::from_secs(10))
            .max_lifetime(Duration::from_secs(10))
            .sqlx_logging(true)
            .sqlx_logging_level(tracing::log::LevelFilter::Info);
        let db_conn = Database::connect(opt).await.map_err(sea_orm::DbErr::from)?;
        // run the migrations on the instance
        migration::Migrator::up(&db_conn, None).await?;

        Ok(Box::leak(Box::new(Self { db_conn })))
    }
}

#[async_trait]
impl DatasourceWriter for DatasourceClientQueryPostgres {
    async fn write(&self, consumer: &StreamConsumer<DefaultConsumerContext>) {
        let stream_processor = consumer.stream().try_for_each(move |borrowed_msg| {
            async move {
                let msg = borrowed_msg.detach();
                let query_result_msg = match serde_json::from_slice::<GatewayClientQueryResult>(
                    msg.payload().unwrap_or_default(),
                ) {
                    Err(err) => {
                        tracing::warn!("DatasourcePostgres.store_client_query_result_record()::cannot deserialize message. skipping offset: [{}]. {}", msg.offset(), err);
                        return Result::<(), KafkaError>::Ok(());
                    }
                    Ok(payload) => payload,
                };

                // build a `GraphClientQueryResultRecord`
                let _timestamp = msg
                    .timestamp()
                    .to_millis()
                    .map(|ms| ms / 1000)
                    .unwrap_or(Utc::now().timestamp());
                let offset = msg.offset();
                let _key = String::from_utf8_lossy(msg.key().unwrap_or_default()).to_string();

                let result_record = entity::client_query_result::ActiveModel {
                    id: Set(Uuid::new_v4()),
                    query_id: Set(query_result_msg.query_id),
                    user_address: Set(query_result_msg.user),
                    api_key: Set(query_result_msg.api_key),
                    deployment: Set(Some(query_result_msg.deployment)),
                    query_count: Set(query_result_msg.query_count),
                    status_code: Set(entity::sea_orm_active_enums::ClientQueryResultStatus::from(query_result_msg.status_code)),
                    status: Set(Some(query_result_msg.status)),
                    graph_env: Set(query_result_msg.graph_env.clone()),
                    network: Set(query_result_msg.network.clone()),
                    response_time_ms: Set(query_result_msg.response_time_ms.try_into().unwrap_or(0)),
                    budget: Set(query_result_msg.budget),
                    budget_float: Set(query_result_msg.budget_float),
                    fee: Set(query_result_msg.fee),
                    fee_usd: Set(query_result_msg.fee_usd),
                    ray_id: Set(query_result_msg.ray_id),
                    timestamp: Set(query_result_msg.timestamp),
                    gateway_id: Set(Some(query_result_msg.gateway_id)),
                    network_chain: Set(query_result_msg.graph_env.clone()),
                    indexed_chain: Set(query_result_msg.network.clone()),
                };

                //tracing::info!("Result record generated. query_id: {}", &result_record.query_id.unwrap());
                match result_record.insert(&self.db_conn).await {
                    Ok(_) => tracing::info!("successfully stored message [{}] in db...", offset),
                    Err(err) => {
                        tracing::error!("failure storing message in db [{:#?}]. skipping...", err);
                        return Result::<(), KafkaError>::Ok(());
                    }
                }

                Result::<(), KafkaError>::Ok(())
            }
        });

        tracing::info!(
            "DatasourcePostgres.write()::initializing message stream consumer processing..."
        );
        stream_processor
            .await
            .expect("DatasourcePostgres.write()::failure processing the stream messages");
        tracing::info!("DatasourcePostgres.write()::message stream consumer terminated");
    }
}

#[async_trait]
impl DatasourceWriter for DatasourceIndexerQueryPostgres {
    async fn write(&self, consumer: &StreamConsumer<DefaultConsumerContext>) {
        let stream_processor = consumer.stream().try_for_each(move |borrowed_msg| {
            async move {
                let msg = borrowed_msg.detach();
                let query_result_msg = match serde_json::from_slice::<GatewayIndexerQueryResult>(
                    msg.payload().unwrap_or_default(),
                ) {
                    Err(err) => {
                        tracing::warn!("DatasourcePostgres.store_client_query_result_record()::cannot deserialize message. skipping offset: [{}]. {}", msg.offset(), err);
                        return Result::<(), KafkaError>::Ok(());
                    }
                    Ok(payload) => payload,
                };

                // build a `GraphClientQueryResultRecord`
                let _timestamp = msg
                    .timestamp()
                    .to_millis()
                    .map(|ms| ms / 1000)
                    .unwrap_or(Utc::now().timestamp());
                let offset = msg.offset();
                let _key = String::from_utf8_lossy(msg.key().unwrap_or_default()).to_string();

                let result_record = entity::indexer_query_results::ActiveModel {
                    id: Set(Uuid::new_v4()),
                    query_id: Set(query_result_msg.query_id),
                    user_address: Set(query_result_msg.user_address),
                    api_key: Set(query_result_msg.api_key),
                    deployment: Set(Some(query_result_msg.deployment)),
                    status_code: Set(entity::sea_orm_active_enums::IndexerQueryResultsStatus::from(query_result_msg.status_code)),
                    status: Set(Some(query_result_msg.status)),
                    graph_env: Set(query_result_msg.graph_env.clone()),
                    network: Set(query_result_msg.network.clone()),
                    response_time_ms: Set(query_result_msg.response_time_ms.try_into().unwrap_or(0)),
                    fee: Set(query_result_msg.fee),
                    ray_id: Set(query_result_msg.ray_id),
                    timestamp: Set(query_result_msg.timestamp),
                    gateway_id: Set(Some(query_result_msg.gateway_id)),
                    network_chain: Set(query_result_msg.graph_env.clone()),
                    indexed_chain: Set(query_result_msg.network.clone()),
                    indexer: Set(query_result_msg.indexer.clone()),
                    url: Set(query_result_msg.url.clone()),
                    allocation: Set(query_result_msg.allocation.clone()),
                    indexer_errors: Set(query_result_msg.indexer_errors.clone()),
                    seconds_behind: Set(query_result_msg.seconds_behind.try_into().unwrap_or(0)),
                    blocks_behind: Set(query_result_msg.blocks_behind.try_into().unwrap_or(0)),
                };

                //tracing::info!("Result record generated. query_id: {}", &result_record.query_id.unwrap());
                match result_record.insert(&self.db_conn).await {
                    Ok(_) => tracing::info!("successfully stored message [{}] in db...", offset),
                    Err(err) => {
                        tracing::error!("failure storing message in db [{:#?}]. skipping...", err);
                        return Result::<(), KafkaError>::Ok(());
                    }
                }

                Result::<(), KafkaError>::Ok(())
            }
        });

        tracing::info!(
            "DatasourcePostgres.write()::initializing message stream consumer processing..."
        );
        stream_processor
            .await
            .expect("DatasourcePostgres.write()::failure processing the stream messages");
        tracing::info!("DatasourcePostgres.write()::message stream consumer terminated");
    }
}
