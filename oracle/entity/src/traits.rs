use crate::sea_orm_active_enums::ClientQueryResultStatus;

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
