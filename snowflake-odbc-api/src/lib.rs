use thiserror::Error;
use crate::SnowflakeApiError::DeserializationError;

pub mod auth;

#[derive(Error, Debug)]
pub enum SnowflakeApiError {
    #[error("response deserialization error: `{0}`")]
    DeserializationError(String),

    #[error(transparent)]
    RequestError(#[from] ureq::Error),
}

pub struct SnowflakeOdbcApi {
    account_identifier: String,
}

impl SnowflakeOdbcApi {
    pub fn new(
        account_identifier: &str,
    ) -> Result<Self, SnowflakeApiError> {
        let account_identifier = account_identifier.to_uppercase();
        Ok(SnowflakeOdbcApi {
            account_identifier
        })
    }
}
