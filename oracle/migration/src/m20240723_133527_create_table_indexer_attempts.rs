use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, EnumIter},
    sea_query::extension::postgres::Type,
};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();

        manager
            .create_type(
                Type::create()
                    .as_enum(IndexerQueryResultStatus::Table)
                    .values([
                        IndexerQueryResultStatus::Success,
                        IndexerQueryResultStatus::InternalError,
                        IndexerQueryResultStatus::UserError,
                        IndexerQueryResultStatus::NotFound,
                    ])
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(IndexerQueryResult::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(IndexerQueryResult::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(IndexerQueryResult::QueryId).text().not_null())
                    .col(
                        ColumnDef::new(IndexerQueryResult::UserAddress)
                            .text()
                            .not_null(),
                    )
                    .col(ColumnDef::new(IndexerQueryResult::ApiKey).text().not_null())
                    .col(ColumnDef::new(IndexerQueryResult::Deployment).string_len(46))
                    .col(
                        ColumnDef::new(IndexerQueryResult::StatusCode)
                            .enumeration(
                                IndexerQueryResultStatus::Table,
                                [
                                    IndexerQueryResultStatus::Success,
                                    IndexerQueryResultStatus::InternalError,
                                    IndexerQueryResultStatus::UserError,
                                    IndexerQueryResultStatus::NotFound,
                                ],
                            )
                            .not_null()
                            .default("SUCCESS"),
                    )
                    .col(ColumnDef::new(IndexerQueryResult::Status).text().null())
                    .col(ColumnDef::new(IndexerQueryResult::GraphEnv).text().null())
                    .col(ColumnDef::new(IndexerQueryResult::Network).text().null())
                    .col(
                        ColumnDef::new(IndexerQueryResult::ResponseTimeMs)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(IndexerQueryResult::Fee)
                            .float()
                            .null()
                            .default(0.00),
                    )
                    .col(ColumnDef::new(IndexerQueryResult::RayId).text().null())
                    .col(
                        ColumnDef::new(IndexerQueryResult::Timestamp)
                            .big_integer()
                            .null(),
                    )
                    .col(ColumnDef::new(IndexerQueryResult::GatewayId).text().null())
                    .col(ColumnDef::new(IndexerQueryResult::NetworkChain).text().null())
                    .col(ColumnDef::new(IndexerQueryResult::IndexedChain).text().null())
                    .col(ColumnDef::new(IndexerQueryResult::Indexer).text().null())
                    .col(ColumnDef::new(IndexerQueryResult::Url).text().null())
                    .col(ColumnDef::new(IndexerQueryResult::Allocation).text().null())
                    .col(ColumnDef::new(IndexerQueryResult::IndexerErrors).text().null())
                    .col(
                        ColumnDef::new(IndexerQueryResult::SecondsBehind)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .col(
                        ColumnDef::new(IndexerQueryResult::BlocksBehind)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .to_owned(),
            )
            .await?;
        
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(IndexerQueryResult::Table).to_owned())
            .await?;

        manager
            .drop_type(
                Type::drop()
                    .name(IndexerQueryResultStatus::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(Iden)]
pub enum IndexerQueryResult {
    #[iden = "indexer_query_results"]
    Table,
    Id,
    #[iden = "query_id"]
    QueryId,
    #[iden = "status_code"]
    StatusCode,
    #[iden = "status"]
    Status,
    #[iden = "response_time_ms"]
    ResponseTimeMs,
    #[iden = "user_address"]
    UserAddress,
    #[iden = "api_key"]
    ApiKey,
    #[iden = "deployment"]
    Deployment,
    #[iden = "graph_env"]
    GraphEnv,
    #[iden = "network"]
    Network,
    // #[iden = "query_count"]
    // QueryCount,
    // #[iden = "budget"]
    // Budget,
    // #[iden = "budget_float"]
    // BudgetFloat,
    #[iden = "fee"]
    Fee,
    #[iden = "fee_usd"]
    // FeeUsd,
    // #[iden = "ray_id"]
    RayId,
    #[iden = "timestamp"]
    Timestamp,
    #[iden = "gateway_id"]
    GatewayId,
    #[iden = "network_chain"]
    NetworkChain,
    #[iden = "indexed_chain"]
    IndexedChain,
    #[iden = "indexer"]
    Indexer,
    #[iden = "url"]
    Url,
    #[iden = "seconds_behind"]
    SecondsBehind,
    #[iden = "blocks_behind"]
    BlocksBehind,
    #[iden = "allocation"]
    Allocation,
    #[iden = "indexer_errors"]
    IndexerErrors,
}

// interface IndexerQueryFormat {
//     "gateway_id": string, +
//     "query_id": string, +
//     "ray_id": string, +
//     "network_chain": string, +
//     "graph_env": string, +
//     "timestamp": number, +
//     "api_key": string, +
//     "user_address": string, +
//     "deployment": string, +
//     "network": string, +
//     "indexed_chain": string, +
//     "indexer": string, +
//     "url": string, +
//     "fee": number, +
//     "legacy_scalar": boolean, +
//     "utility": number, +
//     "seconds_behind": number, +
//     "blocks_behind": number, +
//     "response_time_ms": number, +
//     "allocation": string, +
//     "indexer_errors": string, +
//     "status": string, +
//     "status_code": string, +
// }

#[derive(Iden, EnumIter)]
pub enum IndexerQueryResultStatus {
    #[iden = "indexer_query_results_status"]
    Table,
    #[iden = "SUCCESS"]
    Success,
    #[iden = "USER_ERROR"]
    UserError,
    #[iden = "INTERNAL_ERROR"]
    InternalError,
    #[iden = "NOT_FOUND"]
    NotFound,
}
