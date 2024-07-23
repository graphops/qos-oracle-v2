pub use sea_orm_migration::prelude::*;

mod m20230416_185838_create_table_client_query_result;
mod m20230829_131615_create_table_notification;
mod m20240723_133527_create_table_indexer_attempts;
pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20230416_185838_create_table_client_query_result::Migration),
            Box::new(m20230829_131615_create_table_notification::Migration),
            Box::new(m20240723_133527_create_table_indexer_attempts::Migration),
        ]
    }
}
