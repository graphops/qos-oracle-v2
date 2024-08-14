mod consumer;
mod datasource_postgres;
mod datasource_trait;
pub mod logs;
mod models;
pub mod processing;

pub use consumer::*;
pub use datasource_postgres::{DatasourceClientQueryPostgres, DatasourceIndexerQueryPostgres};
pub use datasource_trait::{Datasource, DatasourceWriter};
pub use models::*;

pub struct CreateWithDatasourcePgArgs {
    /// The graph gateway query logs topic id
    pub kafka_topic_ids: Vec<String>,
    /// Connection/configuration parameters.
    /// For example, to connect via SSL to the kafka broker, etc.
    /// Thesese key-values are _required_:
    /// - "bootstrap.servers"
    /// - "group.id"
    ///
    /// # Examples
    ///
    /// ```
    /// // instantiate the config, to connect locally, no auth mech
    /// let mut config = std::collections::BTreeMap::<String, String>::new();
    /// config.insert("bootstrap.servers".to_string(), "PLAINTEXT://127.0.0.1:9092".to_string());
    /// config.insert("group.id".to_string(), "graph-gateway".to_string());
    /// config.insert("enable.partition.eof".to_string(), "false".to_string());
    /// config.insert("enable.auto.commit".to_string(), "false".to_string());
    /// // instantiate the config to connect via ssl
    /// let mut config = std::collections::BTreeMap::<String, String>::new();
    /// config.insert("bootstrap.servers".to_string(), "PLAINTEXT://127.0.0.1:9092".to_string());
    /// config.insert("group.id".to_string(), "graph-gateway".to_string());
    /// config.insert("security.protocol".to_string(), "sasl_ssl".to_string());
    /// config.insert("sasl.mechanism".to_string(), "SCRAM-SHA-256".to_string());
    /// config.insert("sasl.username".to_string(), "username".to_string());
    /// config.insert("sasl.password".to_string(), "password".to_string());
    /// config.insert("ssl.ca.location".to_string(), "/path/to/ca/cert".to_string());
    /// config.insert("ssl.certificate.location".to_string(), "/path/to/ssl/cert".to_string());
    /// config.insert("ssl.key.location".to_string(), "/path/to/ssl/key".to_string());
    /// config.insert("enable.partition.eof".to_string(), "false".to_string());
    /// config.insert("enable.auto.commit".to_string(), "false".to_string());
    /// ```
    pub kafka_config: std::collections::BTreeMap<String, String>,
    /// Postgres db url.
    /// Format: `postgres://{user}:{password}@{host}:{port}/{database}
    ///
    pub postgres_db_url: String,
    /// Number of work threads to spin up which listen on the kafka message consumer and write to the db.
    /// Default value is: 1
    pub num_workers: Option<usize>,
}
