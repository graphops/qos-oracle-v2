use chrono::{DateTime, Utc};
use entity::ipfs_logs::{self, Entity as IpfsLog};
use sea_orm::ActiveModelTrait;
use sea_orm::{
    ColumnTrait, ConnectionTrait, Database, DatabaseConnection, EntityTrait, InsertResult,
    QueryFilter, QueryOrder,
};

pub struct DbConfig {
    pub url: String,
}

pub async fn connect(config: &DbConfig) -> Result<DatabaseConnection, sea_orm::DbErr> {
    Database::connect(&config.url).await
}

pub async fn insert_log(
    db: &DatabaseConnection,
    timestamp: DateTime<Utc>,
    json_data: String,
    data_type: String,
    posted: bool,
) -> Result<InsertResult<ipfs_logs::ActiveModel>, sea_orm::DbErr> {
    let log = ipfs_logs::ActiveModel {
        id: sea_orm::Set(timestamp.naive_utc()),
        json_data: sea_orm::Set(json_data),
        data_type: sea_orm::Set(data_type),
        posted: sea_orm::Set(posted),
    };

    IpfsLog::insert(log).exec(db).await
}

pub async fn query_logs(
    db: &DatabaseConnection,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Result<Vec<ipfs_logs::Model>, sea_orm::DbErr> {
    IpfsLog::find()
        .filter(ipfs_logs::Column::Id.between(start_time.naive_utc(), end_time.naive_utc()))
        .order_by_asc(ipfs_logs::Column::Id)
        .all(db)
        .await
}

pub async fn query_logs_by_type(
    db: &DatabaseConnection,
    data_type: String,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Result<Vec<ipfs_logs::Model>, sea_orm::DbErr> {
    IpfsLog::find()
        .filter(ipfs_logs::Column::Id.between(start_time.naive_utc(), end_time.naive_utc()))
        .filter(ipfs_logs::Column::DataType.eq(data_type))
        .order_by_asc(ipfs_logs::Column::Id)
        .all(db)
        .await
}

pub async fn update_log_posted_status(
    db: &DatabaseConnection,
    id: DateTime<Utc>,
    posted: bool,
) -> Result<(), sea_orm::DbErr> {
    let log = IpfsLog::find_by_id(id.naive_utc()).one(db).await?;
    if let Some(log) = log {
        let mut active_model: ipfs_logs::ActiveModel = log.into();
        active_model.posted = sea_orm::Set(posted);
        active_model.update(db).await?;
    }
    Ok(())
}
