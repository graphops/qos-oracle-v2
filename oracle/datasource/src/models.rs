use std::io::Cursor;

use prost::Message;
use serde::Deserialize;
pub type Uuid = sea_orm::prelude::Uuid;

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
    #[prost(string, optional, tag = "6")]
    pub indexer: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "7")]
    pub allocation: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "8")]
    pub url: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "9")]
    pub indexer_errors: ::core::option::Option<::prost::alloc::string::String>,
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
    #[prost(string, optional, tag = "15")]
    pub ray_id: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(int64, optional, tag = "16")]
    pub timestamp: ::core::option::Option<i64>,
    #[prost(uint32, tag = "17")]
    pub seconds_behind: u32,
    #[prost(uint32, tag = "18")]
    pub blocks_behind: u32,
    #[prost(string, tag = "19")]
    pub gateway_id: ::prost::alloc::string::String,
    #[prost(string, optional, tag = "20")]
    pub network_chain: ::core::option::Option<::prost::alloc::string::String>,
    #[prost(string, optional, tag = "21")]
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
