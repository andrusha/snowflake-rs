use thiserror::Error;
use url::Url;
use uuid::Uuid;
use serde::{Deserialize, Serialize};
use snowflake_jwt::{generate_jwt_token, JwtError};
use crate::auth::AuthError::DeserializationError;


#[derive(Error, Debug)]
pub enum AuthError {
    #[error(transparent)]
    JwtError(#[from] JwtError),

    #[error("response deserialization error: `{0}`")]
    DeserializationError(String),

    #[error(transparent)]
    RequestError(#[from] ureq::Error),

    #[error(transparent)]
    UrlParseError(#[from] url::ParseError),
}


// todo: contain all the tokens and their expiration times
#[derive(Debug)]
pub struct AuthTokens {
    pub session_token: String,
    pub master_token: String,
}

// todo: allow to query for configuration parameters as well
pub trait SnowflakeAuth {
    fn get_master_token(&self) -> Result<AuthTokens, AuthError>;
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Parameter {
    name: String,
    value: serde_json::Value, // As value can be of different types (bool, String, i64, etc.), we'll use serde_json::Value
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SessionInfo {
    #[serde(rename = "databaseName")]
    database_name: Option<String>,
    #[serde(rename = "schemaName")]
    schema_name: Option<String>,
    #[serde(rename = "warehouseName")]
    warehouse_name: String,
    #[serde(rename = "roleName")]
    role_name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AuthData {
    #[serde(rename = "masterToken")]
    master_token: String,
    token: String,
    #[serde(rename = "validityInSeconds")]
    validity_in_seconds: i64,
    #[serde(rename = "masterValidityInSeconds")]
    master_validity_in_seconds: i64,
    #[serde(rename = "displayUserName")]
    display_user_name: String,
    #[serde(rename = "serverVersion")]
    server_version: String,
    #[serde(rename = "firstLogin")]
    first_login: bool,
    #[serde(rename = "remMeToken")]
    rem_me_token: Option<String>,
    #[serde(rename = "remMeValidityInSeconds")]
    rem_me_validity_in_seconds: i64,
    #[serde(rename = "healthCheckInterval")]
    health_check_interval: i64,
    #[serde(rename = "newClientForUpgrade")]
    new_client_for_upgrade: Option<String>,
    #[serde(rename = "sessionId")]
    session_id: i64,
    parameters: Vec<Parameter>,
    #[serde(rename = "sessionInfo")]
    session_info: SessionInfo,
    #[serde(rename = "idToken")]
    id_token: Option<String>,
    #[serde(rename = "idTokenValidityInSeconds")]
    id_token_validity_in_seconds: i64,
    #[serde(rename = "responseData")]
    response_data: Option<serde_json::Value>,
    #[serde(rename = "mfaToken")]
    mfa_token: Option<String>,
    #[serde(rename = "mfaTokenValidityInSeconds")]
    mfa_token_validity_in_seconds: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct AuthResponse {
    // todo: make data optional (what if message fails?)
    data: AuthData,
    code: Option<String>,
    message: Option<String>,
    success: bool,
}

pub struct SnowflakeCertAuth {
    private_key_pem: Vec<u8>,
    account_identifier: String,
    warehouse: String,
    database: String,
    username: String,
    role: String,
}

impl SnowflakeCertAuth {
    pub fn new(
        private_key_pem: &[u8],
        username: &str,
        role: &str,
        account_identifier: &str,
        warehouse: &str,
        database: &str,
    ) -> Result<Self, AuthError> {
        let username = username.to_uppercase();
        let account_identifier = account_identifier.to_uppercase();
        let warehouse = warehouse.to_uppercase();
        let database = database.to_uppercase();
        let role = role.to_uppercase();
        let private_key_pem = private_key_pem.to_vec();

        Ok(SnowflakeCertAuth {
            private_key_pem,
            account_identifier,
            warehouse,
            database,
            username,
            role,
        })
    }

    // todo: close session after it's over
    fn auth_query(&self) -> Result<AuthResponse, AuthError> {
        let full_identifier = format!("{}.{}", &self.account_identifier, &self.username);
        let jwt_token = generate_jwt_token(&self.private_key_pem, &full_identifier)?;

        let url = format!("https://{}.snowflakecomputing.com/session/v1/login-request", &self.account_identifier);

        // todo: increment subsequent requst ids (on retry?)
        let request_id = Uuid::now_v1(&[0, 0, 0, 0, 0, 0]);
        let request_guid = Uuid::new_v4();
        let url = Url::parse_with_params(
            &url,
            &[
                ("requestId", request_id.to_string()),
                ("request_guid", request_guid.to_string()),
                // todo: make database optional
                ("databaseName", self.database.clone()),
                ("roleName", self.role.clone()),
                // ("schemaName", self.schema),
                ("warehouse", self.warehouse.clone())
            ])?;

        let auth = format!("Bearer {}", &jwt_token);
        let resp = ureq::request_url("POST", &url)
            .set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .set("Authorization", &auth)
            .set("User-Agent", "Rust/0.0.1")
            .set("accept", "application/json")
            .send_json(ureq::json!({
            "data": {
                // pretend to be Go client in order to default to Arrow output format
                "CLIENT_APP_ID": "Go",
                "CLIENT_APP_VERSION": "1.6.22",
                "SVN_REVISION": "",
                "ACCOUNT_NAME": &self.account_identifier,
                "LOGIN_NAME": &self.username,
                "AUTHENTICATOR": "SNOWFLAKE_JWT",
                "TOKEN": &jwt_token,
                "SESSION_PARAMETERS": {
                    "CLIENT_VALIDATE_DEFAULT_PARAMETERS": true
                },
                "CLIENT_ENVIRONMENT": {
                    "APPLICATION": "Rust",
                    "OS": "darwin",
                    "OS_VERSION": "gc-arm64",
                    "OCSP_MODE": "FAIL_OPEN"
                }
            }
        }))?;

        // todo: properly handle error responses in messages
        serde_json::from_reader(resp.into_reader())
            .map_err(|e| DeserializationError(e.to_string()))
    }
}

impl SnowflakeAuth for SnowflakeCertAuth {
    fn get_master_token(&self) -> Result<AuthTokens, AuthError> {
        let resp = self.auth_query()?;

        Ok(AuthTokens {
            session_token: resp.data.token,
            master_token: resp.data.master_token,
        })
    }
}

pub struct SnowflakePasswordAuth {
    account_identifier: String,
    warehouse: String,
    database: String,
    username: String,
    password: String,
    role: String,
}

impl SnowflakePasswordAuth {
    pub fn new(
        username: &str,
        password: &str,
        role: &str,
        account_identifier: &str,
        warehouse: &str,
        database: &str,
    ) -> Result<Self, AuthError> {
        let username = username.to_uppercase();
        let password = password.to_string();
        let account_identifier = account_identifier.to_uppercase();
        let warehouse = warehouse.to_uppercase();
        let database = database.to_uppercase();
        let role = role.to_uppercase();

        Ok(SnowflakePasswordAuth {
            account_identifier,
            warehouse,
            database,
            username,
            password,
            role,
        })
    }

    // todo: close session after it's over
    fn auth_query(&self) -> Result<AuthResponse, AuthError> {
        let url = format!("https://{}.snowflakecomputing.com/session/v1/login-request", &self.account_identifier);

        // todo: increment subsequent requst ids (on retry?)
        let request_id = Uuid::now_v1(&[0, 0, 0, 0, 0, 0]);
        let request_guid = Uuid::new_v4();
        let url = Url::parse_with_params(
            &url,
            &[
                ("requestId", request_id.to_string()),
                ("request_guid", request_guid.to_string()),
                // todo: make database optional
                ("databaseName", self.database.clone()),
                ("roleName", self.role.clone()),
                // ("schemaName", self.schema),
                ("warehouse", self.warehouse.clone())
            ])?;

        let resp = ureq::request_url("POST", &url)
            .set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
            .set("User-Agent", "Rust/0.0.1")
            .set("accept", "application/json")
            .send_json(ureq::json!({
            "data": {
                // pretend to be Go client in order to default to Arrow output format
                "CLIENT_APP_ID": "Go",
                "CLIENT_APP_VERSION": "1.6.22",
                "SVN_REVISION": "",
                "ACCOUNT_NAME": &self.account_identifier,
                "LOGIN_NAME": &self.username,
                "PASSWORD": &self.password,
                "SESSION_PARAMETERS": {
                    "CLIENT_VALIDATE_DEFAULT_PARAMETERS": true
                },
                "CLIENT_ENVIRONMENT": {
                    "APPLICATION": "Rust",
                    "OS": "darwin",
                    "OS_VERSION": "gc-arm64",
                    "OCSP_MODE": "FAIL_OPEN"
                }
            }
        }))?;

        // todo: properly handle error responses in messages
        serde_json::from_reader(resp.into_reader())
            .map_err(|e| DeserializationError(e.to_string()))
    }
}

impl SnowflakeAuth for SnowflakePasswordAuth {
    fn get_master_token(&self) -> Result<AuthTokens, AuthError> {
        let resp = self.auth_query()?;

        Ok(AuthTokens {
            session_token: resp.data.token,
            master_token: resp.data.master_token,
        })
    }
}
