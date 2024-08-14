pub use sea_orm_migration::prelude::*;

mod m20230416_185838_create_table_client_query_result;
mod m20240723_133527_create_table_indexer_attempts;
mod m20240809_005305_create_table_logs;
pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20230416_185838_create_table_client_query_result::Migration),
            Box::new(m20240723_133527_create_table_indexer_attempts::Migration),
            Box::new(m20240809_005305_create_table_logs::Migration),
        ]
    }
}
