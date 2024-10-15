use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let _db = manager.get_connection();

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
                            .integer()
                            .not_null()
                            .default(0),
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

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(ClientQueryResult::Table).to_owned())
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
