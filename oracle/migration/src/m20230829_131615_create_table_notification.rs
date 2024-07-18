use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Notification::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Notification::Id)
                            .uuid()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Notification::Topic).integer().not_null())
                    .col(ColumnDef::new(Notification::Address).string().not_null())
                    .col(
                        ColumnDef::new(Notification::CreatedAt)
                            .timestamp_with_time_zone()
                            .default(Expr::current_timestamp())
                            .not_null(),
                    )
                    .col(ColumnDef::new(Notification::ArchivedAt).timestamp_with_time_zone())
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Notification::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
pub enum Notification {
    #[iden = "notification"]
    Table,
    #[iden = "id"]
    Id,
    #[iden = "address"]
    Address,
    #[iden = "topic"]
    Topic,
    #[iden = "created_at"]
    CreatedAt,
    /// We're not tracking if the user has read the notification, but if it has been "acted upon",
    /// we archive it. Only the newest archived notification is used, the rest can be deleted periodically.
    #[iden = "archived_at"]
    ArchivedAt,
}
