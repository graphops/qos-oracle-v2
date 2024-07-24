use std::{env, fs::read_to_string, path::PathBuf, thread, time};

use anyhow::Context;

use tracing_subscriber::{self, layer::SubscriberExt as _, util::SubscriberInitExt as _};

use datasource::{CreateWithDatasourcePgArgs, LogConsumer};

use crate::config::Config;

mod config;

#[tokio::main]
pub async fn main() {
    // the mounted config location is passed as an arg to the build.
    // grab the config path value from the arg and attempt to load the config JSON and parse into a [`crate::config::Config`] instance.`
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

    // instantiate the postgres datasource instance and begin consuming messages
    let _datasource_client_query =
        LogConsumer::create_with_client_datasource_pg(CreateWithDatasourcePgArgs {
            kafka_config: conf.kafka.0.clone(),
            kafka_topic_ids: [conf.kafka_topic_ids[0].clone()].to_vec(),
            postgres_db_url: conf.db_url.clone(),
            num_workers: Some(2),
        })
        .await
        .expect("Failure instantiating GatewayQueryClientConsumer");
    // instantiate the postgres datasource instance and begin consuming messages
    let _datasource_indexer_query =
        LogConsumer::create_with_indexer_datasource_pg(CreateWithDatasourcePgArgs {
            kafka_config: conf.kafka.0.clone(),
            kafka_topic_ids: [conf.kafka_topic_ids[1].clone()].to_vec(),
            postgres_db_url: conf.db_url.clone(),
            num_workers: Some(2),
        })
        .await
        .expect("Failure instantiating GatewayIndexerClientConsumer");

    loop {
        thread::sleep(time::Duration::from_millis(1000));
    }
}

fn init_tracing(json: bool) {
    let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        tracing_subscriber::EnvFilter::try_new("info,graph_subscriptions_api=debug").unwrap()
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
