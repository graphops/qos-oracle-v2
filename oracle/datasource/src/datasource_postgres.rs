use std::{collections::HashMap, env, time::Duration};

use alloy_primitives::Address;
use async_trait::async_trait;
use chrono::Utc;
use futures::TryStreamExt;
use lazy_static::lazy_static;
use rdkafka::{
    consumer::{DefaultConsumerContext, StreamConsumer},
    error::KafkaError,
    Message,
};
use sea_orm::{
    prelude::Uuid, ActiveModelTrait, ActiveValue::NotSet, ColumnTrait, ConnectOptions, Database,
    DatabaseConnection, EntityTrait, FromQueryResult, QueryFilter, Set, Statement,
};

use thegraph::types::DeploymentId;

use entity::{notification, prelude::Notification};
use migration::MigratorTrait;

use crate::{
    models, Datasource, DatasourceWriter, GatewayClientQueryResult, NotificationTopic,
    OrderDirection, QueryKey, QueryKeyOrderBy, QueryRateInterval, UniqQueryKeyDeploymentQmHash,
    User, UserHasKeyResult, UserServiceCumulativeQueryStat, UserServiceQueryRateStat,
};

lazy_static! {
    static ref CHAIN_ID: u64 = env::var("CHAIN_ID")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(421614);
}

impl FromQueryResult for UniqQueryKeyDeploymentQmHash {
    fn from_query_result(res: &sea_orm::QueryResult, pre: &str) -> Result<Self, migration::DbErr> {
        let deployment_id = res.try_get::<String>(pre, "deployment").and_then(|val| {
            val.parse::<DeploymentId>()
                .map_err(|err| migration::DbErr::Custom(format!("deployment malformed: {}", err)))
        })?;

        Ok(Self {
            deployment: deployment_id,
        })
    }
}

impl FromQueryResult for User {
    fn from_query_result(res: &sea_orm::QueryResult, pre: &str) -> Result<Self, sea_orm::DbErr> {
        let id = res
            .try_get::<String>(pre, "id")
            .map(|val| val.parse::<Address>())?;
        let id = id.map_err(|err| migration::DbErr::Custom(err.to_string()))?;

        Ok(Self {
            id,
            total_query_count: res.try_get(pre, "total_query_count")?,
            total_query_count_in_billing_period: res
                .try_get(pre, "total_query_count_in_billing_period")?,
            query_key_count: res.try_get(pre, "query_key_count")?,
            max_queries_per_minute: res.try_get(pre, "max_queries_per_minute")?,
            max_queries_per_minute_in_billing_period: res
                .try_get(pre, "max_queries_per_minute_in_billing_period")?,
        })
    }
}

impl FromQueryResult for QueryKey {
    fn from_query_result(res: &sea_orm::QueryResult, pre: &str) -> Result<Self, migration::DbErr> {
        let user_address = res
            .try_get::<String>(pre, "user_address")
            .map(|val| val.parse::<Address>())?;
        let user_address = user_address.map_err(|err| migration::DbErr::Custom(err.to_string()))?;
        // ticket_payload comes as a JSON array value.
        // previously it gets parsed into AuthTokenClaim, now temporarily take it as a string
        // let ticket_payload = res.try_get::<String>(pre, "ticket_payload")?;

        Ok(Self {
            api_key: res.try_get(pre, "api_key")?,
            user_address,
            total_query_count: res.try_get(pre, "total_query_count")?,
            total_query_count_in_billing_period: res
                .try_get(pre, "total_query_count_in_billing_period")?,
            queried_subgraphs_count: res.try_get(pre, "queried_subgraphs_count")?,
            last_query_timestamp: res.try_get(pre, "last_query_timestamp")?,
            // ticket_payload,
        })
    }
}

/// The rdbms (this implementation uses postgres) datasource implements both the `Datasource` and `DatasourceWriter` traits.
/// Allows a user to query and store `GatewayClientQueryResult` records stored in the postgres database instance.
#[derive(Clone)]
pub struct DatasourcePostgres {
    pub db_conn: DatabaseConnection,
}
impl DatasourcePostgres {
    /// Create a DatasourcePostgres instance by instantiating a postgres db connection.
    ///
    /// # Arguments
    ///
    /// * `db_url` - the postgres connection url. ex: postgress://username:password@host:port/database
    pub async fn create(db_url: String) -> Result<&'static Self, sea_orm::DbErr> {
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
impl Datasource for DatasourcePostgres {
    /// Return a derived User record.
    /// The `id` value returned on the User record is the `user` record pulled from the `client_query_results.
    ///
    /// # Arguments
    ///
    /// - `user` - [REQUIRED] the user wallet address who performed the stored queries
    async fn user(&self, user: Address) -> anyhow::Result<User> {
        let user = User::find_by_statement(Statement::from_sql_and_values(
            sea_orm::DbBackend::Postgres,
            r#"
            WITH query_per_minute_breakdown (user_address, minute_start, max_queries_per_minute, rank) AS (
                SELECT
                    user_address,
                    date_trunc('minute', timestamp 'epoch' + (timestamp * interval '1 milliseconds')) AS minute_start,
                    CAST(SUM(query_count) AS bigint) AS max_queries_per_minute,
                    ROW_NUMBER() OVER (PARTITION BY user_address, date_trunc('minute', timestamp 'epoch' + (timestamp * interval '1 milliseconds')) ORDER BY MAX(query_count) DESC) AS rank
                FROM client_query_result
                WHERE user_address = $1
                GROUP BY 1, 2
                ORDER BY SUM(query_count) DESC
                LIMIT 1
            ), queries_in_billing_period(user_address, total_query_count_in_billing_period) AS (
                SELECT
                    user_address,
                    CAST(SUM(query_count) AS BIGINT) AS total_query_count_in_billing_period
                FROM client_query_result
                WHERE user_address = $1
                GROUP BY 1
            ), max_queries_per_minute_in_billing_period (user_address, minute_start, max_queries_per_minute, rank) AS (
                SELECT
                    user_address,
                    DATE_TRUNC('minute', timestamp 'epoch' + (timestamp * interval '1 second')) AS minute_start,
                    CAST(SUM(query_count) AS bigint) AS max_queries_per_minute,
                    ROW_NUMBER() OVER (PARTITION BY user_address, date_trunc('minute', timestamp 'epoch' + (timestamp * interval '1 second')) ORDER BY MAX(query_count) DESC) AS rank
                FROM client_query_result
                WHERE user_address = $1
                GROUP BY 1, 2
                ORDER BY SUM(query_count) DESC
                LIMIT 1
            )
            SELECT
                client_query_result.user_address AS id,
                CAST(SUM(client_query_result.query_count) AS bigint) as total_query_count,
                CAST(MAX(COALESCE(queries_in_billing_period.total_query_count_in_billing_period, 0)) AS BIGINT) AS total_query_count_in_billing_period,
                CAST(COUNT(DISTINCT api_key) AS int4) AS query_key_count,
                CAST(MAX(query_per_minute_breakdown.max_queries_per_minute) AS bigint) AS max_queries_per_minute,
                CAST(MAX(COALESCE(max_queries_per_minute_in_billing_period.max_queries_per_minute, 0)) AS bigint) AS max_queries_per_minute_in_billing_period
            FROM client_query_result
            INNER JOIN query_per_minute_breakdown
                ON query_per_minute_breakdown.user_address = client_query_result.user_address
                AND query_per_minute_breakdown.rank = 1
            LEFT OUTER JOIN queries_in_billing_period
                ON queries_in_billing_period.user_address = client_query_result.user_address
            LEFT OUTER JOIN max_queries_per_minute_in_billing_period
                ON max_queries_per_minute_in_billing_period.user_address = client_query_result.user_address
                AND max_queries_per_minute_in_billing_period.rank = 1
            WHERE client_query_result.user_address = $1
            GROUP BY 1
            "#,
            [format!("{user:#x}").into()],
        ))
        .one(&self.db_conn)
        .await
        .map_err(anyhow::Error::from)?
        .unwrap_or(User {
            id: user,
            total_query_count: 0,
            total_query_count_in_billing_period: 0,
            query_key_count: 0,
            max_queries_per_minute: 0,
            max_queries_per_minute_in_billing_period: 0
        });

        Ok(user)
    }
    /// Retrieve the user's unique `QueryKey` records derived from the stored query result records from the postgres database.
    ///
    /// # Arguments
    ///
    /// - `user` - the user wallet address who performed the stored queries
    /// - `first` - [OPTIONAL:default 100] the number of records, after sorting, to return
    /// - `skip` - [OPTIONAL:default 0] the number of records, after sorting, to skip
    /// - `order_by` - [OPTIONAL] what field on the `QueryKey` to sort by
    /// - `order_direction` [OPTIONAL] the sort direction
    async fn query_keys(
        &self,
        user: Address,
        first: Option<i32>,
        skip: Option<i32>,
        order_by: Option<QueryKeyOrderBy>,
        order_direction: Option<OrderDirection>,
    ) -> anyhow::Result<Vec<QueryKey>> {
        let order_by = order_by.unwrap_or(QueryKeyOrderBy::Name);
        let order_direction = order_direction.unwrap_or(OrderDirection::Asc);
        let limit = first.unwrap_or(100);
        let offset = skip.unwrap_or(0);

        QueryKey::find_by_statement(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            r#"
            WITH queried_subgraphs_count AS (
                SELECT
                    CONCAT(api_key, user_address) AS ticket,
                    COUNT(DISTINCT deployment) AS queried_subgraphs_count
                FROM client_query_result
                WHERE 
                    user_address = $1
                    AND deployment IS NOT NULL
                GROUP BY api_key, user_address
            ), query_count_in_billing_period AS (
                SELECT
                    CONCAT(api_key, user_address) AS ticket,
                    SUM(query_count) AS total_query_count_in_billing_period
                FROM client_query_result
                WHERE
                    user_address = $1
                    AND status_code = 'SUCCESS'
                GROUP BY api_key, user_address
            )
            SELECT
                result.api_key,
                result.user_address,
                CAST(SUM(result.query_count) AS bigint) AS total_query_count,
                CAST(COALESCE(query_count_in_billing_period.total_query_count_in_billing_period, 0) AS bigint) AS total_query_count_in_billing_period,
                COALESCE(queried_subgraphs_count.queried_subgraphs_count, 0) AS queried_subgraphs_count,
                MAX(result.timestamp) AS last_query_timestamp
            FROM client_query_result AS result
            LEFT OUTER JOIN queried_subgraphs_count
                ON queried_subgraphs_count.ticket = CONCAT(result.api_key, result.user_address)
            LEFT OUTER JOIN query_count_in_billing_period
                ON query_count_in_billing_period.ticket = CONCAT(result.api_key, result.user_address)
            WHERE result.user_address = $1 AND result.api_key IS NOT NULL
            GROUP BY result.api_key, result.user_address, queried_subgraphs_count.queried_subgraphs_count, query_count_in_billing_period.total_query_count_in_billing_period
            ORDER BY $2
            LIMIT $3
            OFFSET $4;
            "#,
            [
                format!("{user:#x}").into(),
                format!("{} {}", order_by.as_str(), order_direction.as_str()).into(),
                limit.into(),
                offset.into(),
            ],
        ))
        .all(&self.db_conn)
        .await
        .map_err(anyhow::Error::from)
    }

    /// Aggregate `client_query_results` record for the given user.
    /// Sum the `client_query_results.query_count` on the interval over the given timeframe.
    ///
    /// # Arguments
    ///
    /// * `user` - [REQUIRED] the user to pull the aggregated stats for
    /// * `interval` - [REQUIRED] the interval to aggregate the stats on.
    /// * `timeframe_start` - [REQUIRED] the start of the interval to pull stats for. either specified by the end-user, or the Subscription start
    /// * `timeframe_end` - [REQUIRED] the end of the interval to pull stats for. either specified by the end-user, or the Subscription end
    /// * `only_successful` - [OPTIONAL] if true, only aggregate queries where the status was SUCCESS
    async fn query_rate_per_interval(
        &self,
        user: Address,
        interval: QueryRateInterval,
        timeframe_start: i64,
        timeframe_end: i64,
        only_successful: Option<bool>,
    ) -> anyhow::Result<Vec<UserServiceQueryRateStat>> {
        let success_where_cond = only_successful
            .unwrap_or(false)
            // .then(|| format!("AND status_code = {:?}", ClientQueryResultStatus::Success))
            .then(|| "AND status_code = 'SUCCESS'".to_string())
            .unwrap_or_default();
        let interval = interval.to_sql_interval_str();
        let sql = format!(
            r#"
            SELECT
                CAST(date_trunc('{}', timestamp 'epoch' + (timestamp * interval '1 milliseconds')) AS TIMESTAMPTZ) AS interval_start,
                CAST((date_trunc('{}', timestamp 'epoch' + (timestamp * interval '1 milliseconds')) + interval '1 {}' - interval '1 milliseconds') AS TIMESTAMPTZ) AS interval_end,
                CAST(SUM(query_count) AS bigint) AS queries_per_interval
            FROM client_query_result
            WHERE
                user_address = $1
                AND timestamp BETWEEN $2 AND $3
                {}
            GROUP BY 1, 2
            ORDER BY 1
            "#,
            interval, interval, interval, success_where_cond,
        );
        UserServiceQueryRateStat::find_by_statement(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            sql.as_str(),
            [
                format!("{user:#x}").into(),
                (timeframe_start * 1000).into(),
                (timeframe_end * 1000).into(),
            ],
        ))
        .all(&self.db_conn)
        .await
        .map_err(anyhow::Error::from)
    }

    async fn cumulative_queries_in_period(
        &self,
        user: Address,
        period_start: i64,
        period_end: i64,
    ) -> anyhow::Result<Vec<UserServiceCumulativeQueryStat>> {
        UserServiceCumulativeQueryStat::find_by_statement(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            r#"
            WITH intervals AS (
                SELECT
                    CAST(date_trunc('hour', timestamp 'epoch' + (timestamp * interval '1 milliseconds')) AS TIMESTAMPTZ) AS interval_start,
                    CAST((date_trunc('hour', timestamp 'epoch' + (timestamp * interval '1 milliseconds')) + interval '1 hour' - interval '1 milliseconds') AS TIMESTAMPTZ) AS interval_end,
                    SUM(query_count) AS queries_per_interval
                FROM client_query_result
                WHERE
                    user_address = $1
                    AND timestamp BETWEEN $2 AND $3
                    AND status_code = 'SUCCESS'
                GROUP BY 1, 2
            )
            SELECT
                interval_start,
                interval_end,
                CAST(SUM(queries_per_interval) OVER (ORDER BY interval_start) AS BIGINT) AS cumulative_queries
            FROM intervals
            ORDER BY interval_start;
            "#,
            [
                format!("{user:#x}").into(),
                (period_start* 1000).into(),
                (period_end* 1000).into()
            ],
        ))
        .all(&self.db_conn)
        .await
        .map_err(anyhow::Error::from)
    }

    async fn last_notifications(
        &self,
        address: &Address,
    ) -> anyhow::Result<HashMap<NotificationTopic, models::Notification>> {
        models::Notification::find_by_statement(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            r#"
                SELECT * FROM (
                    SELECT 
                        *,
                        ROW_NUMBER() OVER(PARTITION BY topic ORDER BY created_at DESC) AS rn
                    FROM 
                        notification
                    WHERE address = $1 and archived_at IS NULL
                ) LastNotifications WHERE rn = 1;
            "#,
            [format!("{address:#x}").into()],
        ))
        .all(&self.db_conn)
        .await
        .map_err(anyhow::Error::from)
        .map(|v| HashMap::from_iter(v.into_iter().map(|m| (m.topic, m))))
    }

    async fn save_notification(
        &self,
        address: Address,
        topic: NotificationTopic,
    ) -> anyhow::Result<models::Notification> {
        notification::ActiveModel {
            id: Set(Uuid::new_v4()),
            address: Set(format!("{address:#x}")),
            topic: Set(topic.into()),
            created_at: NotSet,
            archived_at: NotSet,
        }
        .insert(&self.db_conn)
        .await
        .map_err(anyhow::Error::from)
        .map(|n| n.into())
    }

    async fn archive_notification(&self, notification_id: Uuid) -> anyhow::Result<()> {
        Notification::update(notification::ActiveModel {
            id: Set(notification_id),
            archived_at: Set(Some(Utc::now().into())),
            ..Default::default()
        })
        .filter(notification::Column::Id.eq(notification_id))
        .exec(&self.db_conn)
        .await
        .map_err(anyhow::Error::from)
        .map(|_| ())
    }
}

#[async_trait]
impl DatasourceWriter for DatasourcePostgres {
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
