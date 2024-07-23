use std::io::Cursor;

use alloy_primitives::Address;
use chrono::{DateTime, FixedOffset, Utc};
use prost::Message;
use sea_orm::FromQueryResult;
use sea_orm::TryGetableFromJson;
use serde::{Deserialize, Serialize};
use thegraph::types::DeploymentId;

use entity::notification;

pub type Uuid = sea_orm::prelude::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Copy, Hash, Eq)]
pub enum NotificationTopic {
    BalanceRunningLow,
    BalanceRanOut,
}

impl From<NotificationTopic> for i32 {
    fn from(val: NotificationTopic) -> Self {
        match val {
            NotificationTopic::BalanceRunningLow => 0,
            NotificationTopic::BalanceRanOut => 1,
        }
    }
}

impl From<i32> for NotificationTopic {
    fn from(val: i32) -> Self {
        match val {
            0 => NotificationTopic::BalanceRunningLow,
            1 => NotificationTopic::BalanceRanOut,
            _ => panic!("Invalid notification topic"),
        }
    }
}

impl TryGetableFromJson for NotificationTopic {
    fn try_get_from_json<I: sea_orm::ColIdx>(
        res: &sea_orm::QueryResult,
        idx: I,
    ) -> Result<Self, sea_orm::TryGetError> {
        let val: i32 = res.try_get_by(idx)?;
        Ok(NotificationTopic::from(val))
    }
}

#[derive(Clone, Deserialize, PartialEq, ::prost::Message)]
pub struct GatewayClientQueryResult {
    /// Set to the value of the CF-Ray header, otherwise a generated UUID
    #[prost(string, tag = "1")]
    pub query_id: ::prost::alloc::string::String,
    #[prost(enumeration = "StatusCode", tag = "2")]
    pub status_code: i32,
    #[prost(string, tag = "3")]
    pub status: ::prost::alloc::string::String,
    #[prost(uint32, tag = "4")]
    pub response_time_ms: u32,
    #[prost(string, tag = "5")]
    pub user: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub api_key: ::prost::alloc::string::String,
    /// Subgraph Deployment ID, CIDv0 ("Qm" hash)
    #[prost(string, tag = "7")]
    pub deployment: ::prost::alloc::string::String,
    /// Chain name indexed by subgraph deployment
    #[prost(string, optional, tag = "8")]
    pub graph_env: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "9")]
    pub network: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(int32, optional, tag = "10")]
    pub query_count: ::core::option::Option<i32>,
    #[prost(string, optional, tag = "11")]
    pub budget: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(float, optional, tag = "12")]
    pub budget_float: ::core::option::Option<f32>,
    #[prost(float, optional, tag = "13")]
    pub fee: ::core::option::Option<f32>,
    #[prost(float, optional, tag = "14")]
    pub fee_usd: ::core::option::Option<f32>,
    #[prost(string, optional, tag = "16")]
    pub ray_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(int64, optional, tag = "17")]
    pub timestamp: ::core::option::Option<i64>,
    #[prost(string, tag = "18")]
    pub gateway_id: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "19")]
    pub network_chain: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "20")]
    pub indexed_chain: ::core::option::Option<::prost::alloc::string::String>,
}

#[derive(Clone, Deserialize, PartialEq, ::prost::Message)]
pub struct GatewayIndexerQueryResult {
    /// Set to the value of the CF-Ray header, otherwise a generated UUID
    #[prost(string, tag = "1")]
    pub query_id: ::prost::alloc::string::String,
    #[prost(enumeration = "StatusCode", tag = "2")]
    pub status_code: i32,
    #[prost(string, tag = "3")]
    pub status: ::prost::alloc::string::String,
    #[prost(uint32, tag = "4")]
    pub response_time_ms: u32,
    #[prost(string, tag = "5")]
    pub user_address: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub indexer: ::prost::alloc::string::String,
    #[prost(string, tag = "7")]
    pub allocation: ::prost::alloc::string::String,
    #[prost(string, tag = "8")]
    pub url: ::prost::alloc::string::String,
    #[prost(string, tag = "9")]
    pub indexer_errors: ::prost::alloc::string::String,
    #[prost(string, tag = "10")]
    pub api_key: ::prost::alloc::string::String,
    /// Subgraph Deployment ID, CIDv0 ("Qm" hash)
    #[prost(string, tag = "11")]
    pub deployment: ::prost::alloc::string::String,
    /// Chain name indexed by subgraph deployment
    #[prost(string, optional, tag = "12")]
    pub graph_env: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "13")]
    pub network: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(float, optional, tag = "14")]
    pub fee: ::core::option::Option<f32>,
    #[prost(float, optional, tag = "15")]
    pub fee_usd: ::core::option::Option<f32>,
    #[prost(string, optional, tag = "16")]
    pub ray_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(int64, optional, tag = "17")]
    pub timestamp: ::core::option::Option<i64>,
    #[prost(int64, optional, tag = "18")]
    pub seconds_behind: ::core::option::Option<i64>,
    #[prost(int64, optional, tag = "19")]
    pub blocks_behind: ::core::option::Option<i64>,
    #[prost(string, tag = "20")]
    pub gateway_id: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "21")]
    pub network_chain: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "22")]
    pub indexed_chain: ::core::option::Option<::prost::alloc::string::String>,
}

#[derive(
    Clone, Copy, Debug, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration,
)]
#[repr(i32)]
pub enum StatusCode {
    Success = 0,
    InternalError = 1,
    UserError = 2,
    NotFound = 3,
}
impl StatusCode {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            StatusCode::Success => "SUCCESS",
            StatusCode::InternalError => "INTERNAL_ERROR",
            StatusCode::UserError => "USER_ERROR",
            StatusCode::NotFound => "NOT_FOUND",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SUCCESS" => Some(Self::Success),
            "INTERNAL_ERROR" => Some(Self::InternalError),
            "USER_ERROR" => Some(Self::UserError),
            "NOT_FOUND" => Some(Self::NotFound),
            _ => None,
        }
    }
}
impl GatewayClientQueryResult {
    pub fn from_slice(slice: &[u8]) -> anyhow::Result<Self> {
        Self::decode(&mut Cursor::new(slice)).map_err(anyhow::Error::from)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}
impl OrderDirection {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderDirection::Asc => "ASC",
            OrderDirection::Desc => "DESC",
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct QueryKey {
    /// User-selected, friendly name of the query key. Part of the EIP-712 domain signed message.
    /// Parsed from the `ticket_payload` JSON object from the kafka topic.
    pub api_key: String,
    /// Wallet address of the user who owns the query key.
    /// Pulled directly from the kafka topic log from The Graph Gateway.
    pub user_address: Address,
    /// Total all-time count of queries performed, across all deployments, by the query key
    pub total_query_count: i64,
    /// Total count of queries performed, across all deployments, by the query key, in the given Billing Period
    pub total_query_count_in_billing_period: i64,
    /// Count of _unique_ deployments queried by the query key
    pub queried_subgraphs_count: i64,
    /// Unix-timestamp of when the latest query was process, for any deployment, by the query key
    pub last_query_timestamp: i64,
    // /// The ticket payload value with allowed_domains, allowed_deployments, allowed_subgraphs, etc
    // pub ticket_payload: String,
}

#[derive(Debug, PartialEq)]
pub struct UniqQueryKeyDeploymentQmHash {
    pub deployment: DeploymentId,
}

#[derive(Debug, PartialEq, Eq, Default, Clone)]
pub struct User {
    /// The User eth wallet address.
    /// Stored as `ticket_user` in the client_query_result table.
    pub id: Address,
    /// The total count of queries performed by the User across the entire lifetime of their API service
    pub total_query_count: i64,
    /// The total count of queries performed by the User since the start of the current billing period
    pub total_query_count_in_billing_period: i64,
    /// Count of unique query keys generated by the User, and used to make queries
    pub query_key_count: i32,
    /// The maximum amount of queries performed, in any given minute, over the entire lifetime of the user's API service.
    pub max_queries_per_minute: i64,
    /// The maximum amount of queries performed, in any given minute, since the start of the current billing period
    pub max_queries_per_minute_in_billing_period: i64,
}

#[derive(Debug, PartialEq, FromQueryResult, Default)]
pub struct UserHasKeyResult {
    pub user_has_key: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum QueryKeyOrderBy {
    Owner,
    Name,
}
impl QueryKeyOrderBy {
    pub fn as_str(&self) -> &'static str {
        match self {
            QueryKeyOrderBy::Name => "api_key",
            QueryKeyOrderBy::Owner => "user",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum QueryRateInterval {
    Minute,
    Hour,
    Month,
}
impl QueryRateInterval {
    pub fn to_sql_interval_str(&self) -> &str {
        match self {
            QueryRateInterval::Minute => "minute",
            QueryRateInterval::Hour => "hour",
            QueryRateInterval::Month => "month",
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, FromQueryResult)]
pub struct Notification {
    pub id: Uuid,
    pub topic: NotificationTopic,
    pub address: String,
    pub created_at: DateTime<FixedOffset>,
    pub archived_at: Option<DateTime<FixedOffset>>,
}

impl Default for Notification {
    fn default() -> Self {
        Notification {
            id: Uuid::default(),
            topic: NotificationTopic::BalanceRunningLow,
            address: "".to_string(),
            created_at: Utc::now().into(),
            archived_at: Some(Utc::now().into()),
        }
    }
}

impl From<Notification> for notification::Model {
    fn from(val: Notification) -> Self {
        notification::Model {
            id: val.id,
            address: val.address,
            archived_at: val.archived_at,
            created_at: val.created_at,
            topic: val.topic.into(),
        }
    }
}

impl From<notification::Model> for Notification {
    fn from(val: notification::Model) -> Self {
        Notification {
            id: val.id,
            address: val.address,
            archived_at: val.archived_at,
            created_at: val.created_at,
            topic: val.topic.into(),
        }
    }
}

/// Represents the number of queries performed, by the User, on their API service,
/// aggregated in a given timeframe.
#[derive(Debug, Serialize, Deserialize, FromQueryResult)]
pub struct UserServiceQueryRateStat {
    /// Unix-timestamp of when the timeframe for the record _starts_.
    /// The `QueryRateStat.queries_per_interval` are aggregated between this value and the `QueryRateStat.interval_end`.
    pub interval_start: DateTime<Utc>,
    /// Unix-timestamp of when the timeframe for the record _ends_.
    /// The `QueryRateStat.queries_per_interval` are aggregated between this value and the `QueryRateStat.interval_start`.
    pub interval_end: DateTime<Utc>,
    /// Aggregated query count per {timeframe} where timeframe is decided by the user at query time,
    /// aggregated between the `QueryRateStat.interval_start` and `QueryRateStat.interval_end`.
    pub queries_per_interval: i64,
}

/// Represents the number of queries performed, by the User, on their Service,
/// cumulated from time 0 -> `interval_end`
#[derive(Debug, Serialize, Deserialize, FromQueryResult)]
pub struct UserServiceCumulativeQueryStat {
    /// Unix-timestamp of when the timeframe for the record _starts_.
    /// The `QueryRateStat.queries_per_interval` are aggregated between this value and the `QueryRateStat.interval_end`.
    pub interval_start: DateTime<Utc>,
    /// Unix-timestamp of when the timeframe for the record _ends_.
    /// The `QueryRateStat.queries_per_interval` are aggregated between this value and the `QueryRateStat.interval_start`.
    pub interval_end: DateTime<Utc>,
    /// Aggregated query count per {timeframe} where timeframe is decided by the user at query time,
    /// aggregated from the `UserService.start` -> `QueryRateStat.interval_end`.
    pub cumulative_queries: i64,
}
