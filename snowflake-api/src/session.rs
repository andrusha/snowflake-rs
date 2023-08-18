use std::sync::Arc;
use std::time::{Duration, Instant};

use snowflake_jwt::generate_jwt_token;
use thiserror::Error;

use crate::{AsyncRuntime, connection};
use crate::auth_response::AuthResponse;
use crate::connection::{Connection, QueryType};
use crate::DefaultRuntime;

#[derive(Error, Debug)]
pub enum AuthError {
    #[error(transparent)]
    JwtError(#[from] snowflake_jwt::JwtError),

    #[error(transparent)]
    RequestError(#[from] connection::ConnectionError),

    #[error("Password auth was requested, but password wasn't provided")]
    MissingPassword,

    #[error("Certificate auth was requested, but certificate wasn't provided")]
    MissingCertificate,
}

pub struct AuthToken {
    pub session_token: String,
    validity_in_seconds: Duration,
    issued_on: Instant,
}

impl AuthToken {
    pub fn new(session_token: &str, validity_in_seconds: i64) -> Self {
        let session_token = session_token.to_string();
        // todo: validate that seconds are signed
        let validity_in_seconds = if validity_in_seconds < 0 {
            Duration::from_secs(u64::MAX)
        } else {
            Duration::from_secs(validity_in_seconds as u64)
        };
        let issued_on = Instant::now();

        AuthToken {
            session_token,
            validity_in_seconds,
            issued_on,
        }
    }

    pub fn is_expired(&self) -> bool {
        if let Some(upper_bound) = self.issued_on.checked_add(self.validity_in_seconds) {
            upper_bound >= Instant::now()
        } else {
            false
        }
    }
}

enum AuthType {
    Certificate,
    Password,
}

/// Requests, caches, and renews authentication tokens.
/// Tokens are given as response to creating new session in Snowflake. Session persists
/// the configuration state and temporary objects (tables, procedures, etc).
// todo: split warehouse-database-schema and username-role-key into its own structs
// todo: close session after object is dropped
pub struct Session<R = DefaultRuntime> {
    connection: Arc<Connection<R>>,

    auth_token_cached: Option<AuthToken>,
    auth_type: AuthType,
    account_identifier: String,

    warehouse: String,
    database: Option<String>,
    schema: Option<String>,

    username: String,
    role: Option<String>,
    private_key_pem: Option<Vec<u8>>,
    password: Option<String>,
}

// todo: make builder
impl<R> Session<R>
    where
        R: AsyncRuntime {
    /// Authenticate using private certificate and JWT
    // fixme: add builder or introduce structs
    #[allow(clippy::too_many_arguments)]
    pub fn cert_auth(
        connection: Arc<Connection<R>>,
        account_identifier: &str,
        warehouse: &str,
        database: Option<&str>,
        schema: Option<&str>,
        username: &str,
        role: Option<&str>,
        private_key_pem: &[u8],
    ) -> Self {
        // uppercase everything as this is the convention
        let account_identifier = account_identifier.to_uppercase();

        let warehouse = warehouse.to_uppercase();
        let database = database.map(str::to_uppercase);
        let schema = schema.map(str::to_uppercase);

        let username = username.to_uppercase();
        let role = role.map(str::to_uppercase);
        let private_key_pem = Some(private_key_pem.to_vec());

        Session {
            connection,
            auth_token_cached: None,
            auth_type: AuthType::Certificate,
            private_key_pem,
            account_identifier,
            warehouse,
            database,
            username,
            role,
            schema,
            password: None,
        }
    }

    /// Authenticate using password
    // fixme: add builder or introduce structs
    #[allow(clippy::too_many_arguments)]
    pub fn password_auth(
        connection: Arc<Connection<R>>,
        account_identifier: &str,
        warehouse: &str,
        database: Option<&str>,
        schema: Option<&str>,
        username: &str,
        role: Option<&str>,
        password: &str,
    ) -> Self {
        let account_identifier = account_identifier.to_uppercase();

        let warehouse = warehouse.to_uppercase();
        let database = database.map(str::to_uppercase);
        let schema = schema.map(str::to_uppercase);

        let username = username.to_uppercase();
        let password = Some(password.to_string());
        let role = role.map(str::to_uppercase);

        Session {
            connection,
            auth_token_cached: None,
            auth_type: AuthType::Password,
            account_identifier,
            warehouse,
            database,
            username,
            role,
            password,
            schema,
            private_key_pem: None,
        }
    }

    /// Get cached token or request a new one if old one has expired.
    // todo: do token exchange instead of recreating a session as it loses temporary objects
    pub async fn get_token(&mut self) -> Result<String, AuthError> {
        if let Some(token) = self.auth_token_cached.as_ref().filter(|at| at.is_expired()) {
            return Ok(token.session_token.clone());
        }

        // todo: implement token exchange using master token instead of requesting new one
        // todo: close session when over
        let token = match self.auth_type {
            AuthType::Certificate => {
                log::info!("Starting session with certificate authentication");
                self.token_request(self.cert_request_body()?).await
            }
            AuthType::Password => {
                log::info!("Starting session with password authentication");
                self.token_request(self.passwd_request_body()?).await
            }
        }?;
        let session_token = token.session_token.clone();
        self.auth_token_cached = Some(token);

        Ok(session_token)
    }

    fn cert_request_body(&self) -> Result<serde_json::Value, AuthError> {
        let full_identifier = format!("{}.{}", &self.account_identifier, &self.username);
        let private_key_pem = self
            .private_key_pem
            .as_ref()
            .ok_or(AuthError::MissingCertificate)?;
        let jwt_token = generate_jwt_token(private_key_pem, &full_identifier)?;

        // todo: can refactor common parts from both bodies?
        Ok(serde_json::json!({
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
        }))
    }

    fn passwd_request_body(&self) -> Result<serde_json::Value, AuthError> {
        let password = self.password.as_ref().ok_or(AuthError::MissingPassword)?;

        Ok(serde_json::json!({
            "data": {
                // pretend to be Go client in order to default to Arrow output format
                "CLIENT_APP_ID": "Go",
                "CLIENT_APP_VERSION": "1.6.22",
                "SVN_REVISION": "",
                "ACCOUNT_NAME": &self.account_identifier,
                "LOGIN_NAME": &self.username,
                "PASSWORD": password,
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
        }))
    }

    async fn token_request(&self, body: serde_json::Value) -> Result<AuthToken, AuthError> {
        let mut get_params = vec![("warehouse", self.warehouse.as_str())];

        if let Some(database) = &self.database {
            get_params.push(("databaseName", database.as_str()));
        }

        if let Some(schema) = &self.schema {
            get_params.push(("schemaName", schema.as_str()));
        }

        if let Some(role) = &self.role {
            get_params.push(("roleName", role.as_str()))
        }

        let resp = self
            .connection
            .request::<AuthResponse>(
                QueryType::Auth,
                &self.account_identifier,
                &get_params,
                None,
                body,
            )
            .await?;
        log::debug!("Auth response: {:?}", resp);

        Ok(AuthToken::new(
            &resp.data.token,
            resp.data.validity_in_seconds,
        ))
    }
}
