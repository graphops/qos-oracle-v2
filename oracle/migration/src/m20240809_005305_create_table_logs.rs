use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(IpfsLogs::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(IpfsLogs::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(IpfsLogs::BucketStartTimestamp)
                            .timestamp()
                            .not_null(),
                    )
                    .col(ColumnDef::new(IpfsLogs::JsonData).text().not_null())
                    .col(ColumnDef::new(IpfsLogs::DataType).string().not_null())
                    .col(ColumnDef::new(IpfsLogs::Posted).boolean().not_null())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(IpfsLogs::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
enum IpfsLogs {
    Table,
    Id,
    BucketStartTimestamp,
    JsonData,
    DataType,
    Posted,
}