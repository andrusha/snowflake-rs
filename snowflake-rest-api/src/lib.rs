use thiserror::Error;
use response::SnowflakeResponse;
use snowflake_jwt::{generate_jwt_token, JwtError};
use crate::SnowflakeApiError::DeserializationError;

mod response;

#[derive(Error, Debug)]
pub enum SnowflakeApiError {
    #[error(transparent)]
    JwtError(#[from] JwtError),

    #[error("response deserialization error: `{0}`")]
    DeserializationError(String),

    #[error(transparent)]
    RequestError(#[from] ureq::Error),
}

pub struct SnowflakeRestApi {
    jwt_token: String,
    account_identifier: String,
    warehouse: String,
    role: Option<String>
}

impl SnowflakeRestApi {
    pub fn new(
        private_key_pem: &[u8],
        username: &str,
        role: Option<&str>,
        account_identifier: &str,
        warehouse: &str,
    ) -> Result<Self, SnowflakeApiError> {
        let username = username.to_uppercase();
        let account_identifier = account_identifier.to_uppercase();
        let warehouse = warehouse.to_uppercase();

        let full_identifier = format!("{}.{}", &account_identifier, &username);
        // todo: token rotation on subsequent API calls
        let jwt_token = generate_jwt_token(private_key_pem, &full_identifier)?;
        let role = role.map(|r| r.to_string());

        Ok(SnowflakeRestApi {
            jwt_token,
            account_identifier,
            warehouse,
            role
        })
    }

    pub fn run_query(&self, sql: &str) -> Result<SnowflakeResponse, SnowflakeApiError> {
        let url = format!(
            "https://{}.snowflakecomputing.com/api/v2/statements",
            self.account_identifier
        );
        let auth = format!("Bearer {}", self.jwt_token);

        log::info!("running query: {}", sql);

        let resp = ureq::post(&url)
            .set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .set("Authorization", &auth)
            .set("User-Agent", "sqlite-snowflake/0.1")
            .send_json(ureq::json!({
                "statement": sql,
                "timeout": 60,
                "warehouse": self.warehouse,
                "role": self.role
            }))?;

        serde_json::from_reader(resp.into_reader())
            .map_err(|e| DeserializationError(e.to_string()))
    }
}
