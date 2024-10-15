use chrono::{DateTime, Utc};
use entity::ipfs_logs::{self, Entity as IpfsLog};
use sea_orm::{ActiveModelTrait, Set};
use sea_orm::{
    ColumnTrait, Database, DatabaseConnection, EntityTrait, InsertResult, QueryFilter, QueryOrder,
};

pub struct DbConfig {
    pub url: String,
}

pub async fn connect(config: &DbConfig) -> Result<DatabaseConnection, sea_orm::DbErr> {
    Database::connect(&config.url).await
}

pub async fn insert_log(
    db: &DatabaseConnection,
    bucket_start_timestamp: DateTime<Utc>,
    json_data: String,
    data_type: String,
    posted: bool,
) -> Result<InsertResult<ipfs_logs::ActiveModel>, sea_orm::DbErr> {
    let log = ipfs_logs::ActiveModel {
        bucket_start_timestamp: Set(bucket_start_timestamp.naive_utc()),
        json_data: Set(json_data),
        data_type: Set(data_type),
        posted: Set(posted),
        ..Default::default() // This will set id to NotSet, allowing auto-increment
    };

    IpfsLog::insert(log).exec(db).await
}

pub async fn query_logs(
    db: &DatabaseConnection,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Result<Vec<ipfs_logs::Model>, sea_orm::DbErr> {
    IpfsLog::find()
        .filter(
            ipfs_logs::Column::BucketStartTimestamp
                .between(start_time.naive_utc(), end_time.naive_utc()),
        )
        .order_by_asc(ipfs_logs::Column::BucketStartTimestamp)
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
        .filter(
            ipfs_logs::Column::BucketStartTimestamp
                .between(start_time.naive_utc(), end_time.naive_utc()),
        )
        .filter(ipfs_logs::Column::DataType.eq(data_type))
        .order_by_asc(ipfs_logs::Column::BucketStartTimestamp)
        .all(db)
        .await
}

pub async fn update_log_posted_status(
    db: &DatabaseConnection,
    id: i32,
    posted: bool,
) -> Result<(), sea_orm::DbErr> {
    let log = IpfsLog::find_by_id(id).one(db).await?;
    if let Some(log) = log {
        let mut active_model: ipfs_logs::ActiveModel = log.into();
        active_model.posted = Set(posted);
        active_model.update(db).await?;
    }
    Ok(())
}
