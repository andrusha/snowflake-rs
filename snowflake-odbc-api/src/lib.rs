use thiserror::Error;
use url::Url;
use uuid::Uuid;
use crate::auth::{AuthError, SnowflakeAuth};
use crate::SnowflakeApiError::DeserializationError;

pub mod auth;

#[derive(Error, Debug)]
pub enum SnowflakeApiError {
    #[error("response deserialization error: `{0}`")]
    DeserializationError(String),

    #[error(transparent)]
    RequestError(#[from] ureq::Error),

    #[error(transparent)]
    AuthError(#[from] AuthError),

    #[error(transparent)]
    UrlParseError(#[from] url::ParseError),
}

pub struct SnowflakeOdbcApi {
    auth: Box<dyn SnowflakeAuth>,
    account_identifier: String,
}

impl SnowflakeOdbcApi {
    pub fn new(
        auth: Box<dyn SnowflakeAuth>,
        account_identifier: &str,
    ) -> Result<Self, SnowflakeApiError> {
        let account_identifier = account_identifier.to_uppercase();
        Ok(SnowflakeOdbcApi {
            auth,
            account_identifier,
        })
    }

    // todo: unify query requests with retries in the single place (with auth)
    // todo: what exec query should return if successful?
    pub fn exec(&self, sql: &str) -> Result<String, SnowflakeApiError> {
        // todo: rotate tokens and keep until lifetime
        let tokens = self.auth.get_master_token()?;

        let url = format!("https://{}.snowflakecomputing.com/queries/v1/query-request", &self.account_identifier);

        // todo: increment subsequent request ids
        let request_id = Uuid::now_v1(&[0, 0, 0, 0, 0, 0]);
        let (client_start_time, _nanos) = request_id.get_timestamp().unwrap().to_unix();
        let request_guid = Uuid::new_v4();
        let url = Url::parse_with_params(
            &url,
            &[
                ("clientStartTime", client_start_time.to_string()),
                ("requestId", request_id.to_string()),
                ("request_guid", request_guid.to_string()),
            ])?;

        let auth = format!("Snowflake Token=\"{}\"", &tokens.session_token);
        let resp = ureq::request_url("POST", &url)
            .set("Authorization", &auth)
            .set("User-Agent", "Rust/0.0.1")
            .set("accept", "application/snowflake")
            .send_json(ureq::json!({
                "sqlText": &sql,
                // todo: async support needed?
                "asyncExec": false,
                // todo: why is it needed?
                "sequenceId": 1,
                "isInternal": false
        }))?;

        // serde_json::from_reader(resp.into_reader())
        // todo: properly handle error responses in messages
        resp.into_string().map_err(|e| DeserializationError(e.to_string()))
    }
}
