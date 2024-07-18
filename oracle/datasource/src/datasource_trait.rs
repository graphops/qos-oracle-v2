use std::collections::HashMap;

use alloy_primitives::Address;
use anyhow::Result;
use async_trait::async_trait;
use rdkafka::consumer::{DefaultConsumerContext, StreamConsumer};

use crate::models::{self, *};

#[async_trait]
/// Define an extentable trait that defines common methods to retrieve query key, and stats, data.
pub trait Datasource {
    /// Retrieve a derived, distinct, User record for the given user address from the datasource.
    async fn user(&self, user: Address) -> Result<User>;
    /// Retrieve a list of distinct query keys for the given user address from the datasource.
    #[allow(clippy::too_many_arguments)]
    async fn query_keys(
        &self,
        user: Address,
        first: Option<i32>,
        skip: Option<i32>,
        order_by: Option<QueryKeyOrderBy>,
        order_direction: Option<OrderDirection>,
    ) -> Result<Vec<QueryKey>>;
    /// Derive aggregated query stats, per the given interval, in a given timeframe for the user.
    async fn query_rate_per_interval(
        &self,
        user: Address,
        interval: QueryRateInterval,
        timeframe_start: i64,
        timeframe_end: i64,
        only_successful: Option<bool>,
    ) -> Result<Vec<UserServiceQueryRateStat>>;
    /// Derive a cumulative, per 1hr interval, total of aggregated SUCCESSFUL queries for the user, from the beginning of their billing period until the current timestamp.
    /// This returns a `Vec<UserServiceQueryRateStat>` where each `UserServiceQueryRateStat.queries_per_interval` is a sum
    /// of queries from time 0 -> the end of the interval in the response.
    ///
    /// # Arguments
    ///
    /// * `user` - [REQUIRED] the user to pull the aggregated stats for
    /// * `billing_period_start` - [REQUIRED] the start of the given Billing Period to pull stats for.
    /// * `billing_period_end` - [OPTIONAL; default now] the end of the given Billing period - or the current timestamp - to pull stats for.
    async fn cumulative_queries_in_period(
        &self,
        user: Address,
        period_start: i64,
        period_end: i64,
    ) -> Result<Vec<UserServiceCumulativeQueryStat>>;

    async fn last_notifications(
        &self,
        user: &Address,
    ) -> anyhow::Result<HashMap<NotificationTopic, models::Notification>>;

    async fn save_notification(
        &self,
        address: Address,
        topic: NotificationTopic,
    ) -> Result<Notification>;

    async fn archive_notification(&self, notification_id: Uuid) -> Result<()>;
}

#[async_trait]
/// Define an extendable trait that exposes a write method that takes the received message
/// from the Kafka StreamConsumer and stores the data in the sources data storage mechanism.
pub trait DatasourceWriter {
    /// Use the passed in reference to the `StreamConsumer` to stream/consume messages on the initialized consumer.
    async fn write(&self, consumer: &StreamConsumer<DefaultConsumerContext>);
}
