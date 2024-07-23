use anyhow::Result;
use async_trait::async_trait;
use rdkafka::consumer::{DefaultConsumerContext, StreamConsumer};


#[async_trait]
/// Define an extentable trait that defines common creation methods
pub trait Datasource {
    async fn create(db_url: String) -> Result<&'static Self, sea_orm::DbErr>;
}

#[async_trait]
/// Define an extendable trait that exposes a write method that takes the received message
/// from the Kafka StreamConsumer and stores the data in the sources data storage mechanism.
pub trait DatasourceWriter {
    /// Use the passed in reference to the `StreamConsumer` to stream/consume messages on the initialized consumer.
    async fn write(&self, consumer: &StreamConsumer<DefaultConsumerContext>);
}
