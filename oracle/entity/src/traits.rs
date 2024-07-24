use crate::sea_orm_active_enums::{ClientQueryResultStatus, IndexerQueryResultsStatus};

impl From<i32> for ClientQueryResultStatus {
    fn from(val: i32) -> Self {
        match val {
            0 => ClientQueryResultStatus::Success,
            1 => ClientQueryResultStatus::InternalError,
            2 => ClientQueryResultStatus::UserError,
            _ => ClientQueryResultStatus::NotFound,
        }
    }
}

impl From<i32> for IndexerQueryResultsStatus {
    fn from(val: i32) -> Self {
        match val {
            0 => IndexerQueryResultsStatus::Success,
            1 => IndexerQueryResultsStatus::InternalError,
            2 => IndexerQueryResultsStatus::UserError,
            _ => IndexerQueryResultsStatus::NotFound,
        }
    }
}
