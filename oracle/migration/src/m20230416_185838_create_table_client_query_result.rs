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
                    .as_enum(ClientQueryResultStatus::Table)
                    .values([
                        ClientQueryResultStatus::Success,
                        ClientQueryResultStatus::InternalError,
                        ClientQueryResultStatus::UserError,
                        ClientQueryResultStatus::NotFound,
                    ])
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(ClientQueryResult::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ClientQueryResult::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(ClientQueryResult::QueryId).text().not_null())
                    .col(
                        ColumnDef::new(ClientQueryResult::UserAddress)
                            .text()
                            .not_null(),
                    )
                    .col(ColumnDef::new(ClientQueryResult::ApiKey).text().not_null())
                    .col(ColumnDef::new(ClientQueryResult::Deployment).string_len(46))
                    .col(
                        ColumnDef::new(ClientQueryResult::QueryCount)
                            .integer()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(ClientQueryResult::StatusCode)
                            .enumeration(
                                ClientQueryResultStatus::Table,
                                [
                                    ClientQueryResultStatus::Success,
                                    ClientQueryResultStatus::InternalError,
                                    ClientQueryResultStatus::UserError,
                                    ClientQueryResultStatus::NotFound,
                                ],
                            )
                            .not_null()
                            .default("SUCCESS"),
                    )
                    .col(ColumnDef::new(ClientQueryResult::Status).text().null())
                    .col(ColumnDef::new(ClientQueryResult::GraphEnv).text().null())
                    .col(ColumnDef::new(ClientQueryResult::Network).text().null())
                    .col(
                        ColumnDef::new(ClientQueryResult::ResponseTimeMs)
                            .integer()
                            .not_null()
                            .default(0),
                    )
                    .col(ColumnDef::new(ClientQueryResult::Budget).text().null())
                    .col(
                        ColumnDef::new(ClientQueryResult::BudgetFloat)
                            .float()
                            .null()
                            .default(0.00),
                    )
                    .col(
                        ColumnDef::new(ClientQueryResult::Fee)
                            .float()
                            .null()
                            .default(0.00),
                    )
                    .col(
                        ColumnDef::new(ClientQueryResult::FeeUsd)
                            .float()
                            .null()
                            .default(0.00),
                    )
                    .col(ColumnDef::new(ClientQueryResult::RayId).text().null())
                    .col(
                        ColumnDef::new(ClientQueryResult::Timestamp)
                            .big_integer()
                            .null(),
                    )
                    .col(ColumnDef::new(ClientQueryResult::GatewayId).text().null())
                    .col(
                        ColumnDef::new(ClientQueryResult::NetworkChain)
                            .text()
                            .null(),
                    )
                    .col(
                        ColumnDef::new(ClientQueryResult::IndexedChain)
                            .text()
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;

        // create an index on the: user & deployment
        manager
            .create_index(
                Index::create()
                    .name("idx__client_query_result__user_address")
                    .if_not_exists()
                    .table(ClientQueryResult::Table)
                    .col(ClientQueryResult::UserAddress)
                    .to_owned(),
            )
            .await?;
        // only create an index on the `api_key` & `deployment` values where the value is not null
        db.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS idx__client_query_result__api_key ON client_query_result (api_key) WHERE api_key IS NOT NULL"
        )
        .await?;
        db.execute_unprepared(
            "CREATE INDEX IF NOT EXISTS idx__client_query_result__deployment ON client_query_result (deployment) WHERE deployment IS NOT NULL"
        )
        .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
                Index::drop()
                    .name("idx__client_query_result__user_address")
                    .table(ClientQueryResult::Table)
                    .to_owned(),
            )
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .name("idx__client_query_result__api_key")
                    .table(ClientQueryResult::Table)
                    .to_owned(),
            )
            .await?;
        manager
            .drop_index(
                Index::drop()
                    .name("idx__client_query_result__deployment")
                    .table(ClientQueryResult::Table)
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(Table::drop().table(ClientQueryResult::Table).to_owned())
            .await?;

        manager
            .drop_type(
                Type::drop()
                    .name(ClientQueryResultStatus::Table)
                    .if_exists()
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(Iden)]
pub enum ClientQueryResult {
    #[iden = "client_query_result"]
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
    #[iden = "query_count"]
    QueryCount,
    #[iden = "budget"]
    Budget,
    #[iden = "budget_float"]
    BudgetFloat,
    #[iden = "fee"]
    Fee,
    #[iden = "fee_usd"]
    FeeUsd,
    #[iden = "ray_id"]
    RayId,
    #[iden = "timestamp"]
    Timestamp,
    #[iden = "gateway_id"]
    GatewayId,
    #[iden = "network_chain"]
    NetworkChain,
    #[iden = "indexed_chain"]
    IndexedChain,
}

#[derive(Iden, EnumIter)]
pub enum ClientQueryResultStatus {
    #[iden = "client_query_result_status"]
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
