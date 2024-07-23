use std::{collections::BTreeMap, fmt};

use serde::Deserialize;
use serde_with::{serde_as, DisplayFromStr};
use toolshed::url::Url;

#[serde_as]
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// See https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
    ///
    /// # Examples
    ///
    ///
    /// ```
    /// // builds default, basic auth settings for connecting to a local kafka instance
    /// {
    ///     "kafka": {
    ///         "bootstrap.servers": "PLAINTEXT://127.0.0.1:9092",
    ///         "group.id": "graph-gateway",
    ///         "enable.partition.eof": "false",
    ///         "enable.auto.commit": "false",
    ///     }
    /// }
    /// // with SSL/SASL authentication mechanism configured as well
    /// {
    ///     "kafka": {
    ///         "bootstrap.servers": "PLAINTEXT://127.0.0.1:9092",
    ///         "security.protocol": "sasl_ssl",
    ///         "sasl.mechanism": "SCRAM-SHA-256",
    ///         "sasl.username": "username",
    ///         "sasl.password": "pwd",
    ///         "ssl.ca.location": "/path/to/ca.crt",
    ///         "ssl.certificate.location": "/path/to/ssl.crt",
    ///         "ssl.key.location": "/path/to/ssl.key",
    ///         "group.id": "graph-gateway",
    ///         "enable.partition.eof": "false",
    ///         "enable.auto.commit": "false",
    ///     }
    /// }
    /// ```
    #[serde(default)]
    pub kafka: KafkaConfig,
    /// The Kafka topics the gateway GSP query logs will be published to
    pub kafka_topic_ids: Vec<String>,
    /// Postgres database url where the logs are stored.
    /// Uses format: "postgres://{user}:{pwd}@{host}:{port}/{database}"
    pub db_url: String,  
    /// Format log output as JSON
    pub log_json: bool,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct KafkaConfig(pub BTreeMap<String, String>);

impl Default for KafkaConfig {
    fn default() -> Self {
        let settings = [
            ("bootstrap.servers", "PLAINTEXT://127.0.0.1:9092"),
            ("group.id", "graph-gateway"),
            ("enable.partition.eof", "false"),
            ("enable.auto.commit", "false"),
        ];
        Self(
            settings
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_json_config_into_config_instance() {
        let config_raw = r#"
        {
            "kafka": {
                "bootstrap.servers": "PLAINTEXT://127.0.0.1:9092",
                "group.id": "graph-gateway",
                "enable.partition.eof": "false",
                "enable.auto.commit": "false"
            },
            "kafka_topic_ids": [
                "gateway_client_query_results",
                "gateway_indexer_attempts"
            ],
            "db_url": "postgres://dev:dev@localhost:5432/gateway_client_query_results",
        }
        "#;
        let expected_kafka_config = KafkaConfig::default();
        let expected = Config {
            kafka_topic_ids: Vec::from(["gateway_client_query_results".to_string(),"gateway_indexer_attempts".to_string()]),
            db_url: "postgres://dev:dev@localhost:5432/gateway_client_query_results".to_string(),
            kafka: expected_kafka_config,
            log_json: true,
        };

        match serde_json::from_str::<Config>(config_raw) {
            Ok(actual) => {
                // spot check
                assert_eq!(actual.kafka_topic_ids, expected.kafka_topic_ids);
                assert_eq!(actual.db_url, expected.db_url);
                assert_eq!(actual.kafka.clone(), expected.kafka.clone());
            }
            Err(err) => {
                panic!("Failure parsing JSON -> Config {:#?}", err);
            }
        }
    }
}
