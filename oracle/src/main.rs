use anyhow::Context;
use chrono::Utc;
use datasource::{logs::*, processing::*, CreateWithDatasourcePgArgs, LogConsumer};
use std::{env, fs::read_to_string, path::PathBuf};
use tokio::time::{interval, Duration};
use tracing_subscriber::{self, layer::SubscriberExt as _, util::SubscriberInitExt as _};

use crate::config::Config;

mod config;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load configuration
    let config_path = env::args()
        .nth(1)
        .expect("Missing argument for config path")
        .parse::<PathBuf>()
        .unwrap();
    let config_file_text = read_to_string(config_path.clone()).expect("Failed to open config");
    let conf = serde_json::from_str::<Config>(&config_file_text)
        .context("Failed to parse JSON config")
        .unwrap();

    let config_repr = format!("{conf:#?}");

    init_tracing(conf.log_json);

    tracing::info!("Graph Service Analytics API starting...");
    tracing::debug!(conf = %config_repr);

    // Connect to the database
    let db_config = DbConfig {
        url: conf.db_url.clone(),
    };
    let db_conn = connect(&db_config).await?;

    // Start Kafka consumers
    let _datasource_client_query =
        LogConsumer::create_with_client_datasource_pg(CreateWithDatasourcePgArgs {
            kafka_config: conf.kafka.0.clone(),
            kafka_topic_ids: [conf.kafka_topic_ids[0].clone()].to_vec(),
            postgres_db_url: conf.db_url.clone(),
            num_workers: Some(conf.consumer_num as usize),
        })
        .await
        .expect("Failure instantiating GatewayQueryClientConsumer");

    let _datasource_indexer_query =
        LogConsumer::create_with_indexer_datasource_pg(CreateWithDatasourcePgArgs {
            kafka_config: conf.kafka.0.clone(),
            kafka_topic_ids: [conf.kafka_topic_ids[1].clone()].to_vec(),
            postgres_db_url: conf.db_url.clone(),
            num_workers: Some(conf.consumer_num as usize),
        })
        .await
        .expect("Failure instantiating GatewayIndexerClientConsumer");

    let start_timestamp = get_starting_timestamp(&db_conn).await?;
    // Start the processing loop
    let mut interval = interval(Duration::from_secs(300)); // 5 minutes
    let mut current_timestamp = start_timestamp;
    loop {
        interval.tick().await;
        let now = align_to_bucket(Utc::now()); // With this we ensure we don't process ongoing buckets

        while current_timestamp < now {
            let bucket_end_time = current_timestamp + Duration::from_secs(300);

            // Process indexer query results
            match get_gateway_indexer_query_results_for_time_bucket(&db_conn, current_timestamp)
                .await
            {
                Ok(indexer_results) => {
                    if !indexer_results.is_empty() {
                        if let Err(e) = process_and_publish_indexer_data(
                            &db_conn,
                            indexer_results,
                            current_timestamp,
                        )
                        .await
                        {
                            tracing::error!("Error processing indexer data: {:?}", e);
                        }
                    }
                }
                Err(e) => tracing::error!("Error fetching indexer query results: {:?}", e),
            }

            // Process client query results
            match get_gateway_client_query_results_for_time_bucket(&db_conn, current_timestamp)
                .await
            {
                Ok(client_results) => {
                    if !client_results.is_empty() {
                        if let Err(e) = process_and_publish_client_data(
                            &db_conn,
                            client_results,
                            current_timestamp,
                        )
                        .await
                        {
                            tracing::error!("Error processing client data: {:?}", e);
                        }
                    }
                }
                Err(e) => tracing::error!("Error fetching client query results: {:?}", e),
            }

            current_timestamp = bucket_end_time;
        }
    }
}

fn init_tracing(json: bool) {
    let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::try_new("info,qos_oracle_v2=debug").unwrap()
    });
    let defaults = tracing_subscriber::registry().with(filter_layer);
    let fmt_layer = tracing_subscriber::fmt::layer();
    if json {
        defaults
            .with(fmt_layer.json().with_current_span(false))
            .init();
    } else {
        defaults.with(fmt_layer).init();
    }
}
