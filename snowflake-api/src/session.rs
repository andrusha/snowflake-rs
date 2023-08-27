use std::sync::Arc;
use std::time::{Duration, Instant};

use snowflake_jwt::generate_jwt_token;
use thiserror::Error;

use crate::connection;
use crate::connection::{Connection, QueryType};
use crate::requests::{
    CertLoginRequest, CertRequestData, ClientEnvironment, LoginRequest, LoginRequestCommon,
    PasswordLoginRequest, PasswordRequestData, SessionParameters,
};
use crate::responses::AuthResponse;

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

    #[error("Unexpected API response")]
    UnexpectedResponse,

    #[error("Failed to authenticate. Error code: {0}. Message: {1}")]
    AuthFailed(String, String),
}

#[derive(Debug, Clone)]
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
pub struct Session {
    connection: Arc<Connection>,

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
impl Session {
    /// Authenticate using private certificate and JWT
    // fixme: add builder or introduce structs
    #[allow(clippy::too_many_arguments)]
    pub fn cert_auth(
        connection: Arc<Connection>,
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
        connection: Arc<Connection>,
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

    pub async fn close(&mut self) -> Result<(), AuthError> {
        if let Some(token) = self.auth_token_cached.clone() {
            let auth = format!("Snowflake Token=\"{}\"", &token.session_token);
            self.auth_token_cached = None;

            let resp = self
                .connection
                .request::<AuthResponse>(
                    QueryType::CloseSession,
                    &self.account_identifier,
                    &[("delete", "true")],
                    Some(&auth),
                    serde_json::Value::default(),
                )
                .await?;

            match resp {
                AuthResponse::Close(_) => Ok(()),
                AuthResponse::Error(e) => Err(AuthError::AuthFailed(
                    e.data.error_code,
                    e.message.unwrap_or_default(),
                )),
                _ => Err(AuthError::UnexpectedResponse),
            }
        } else {
            Ok(())
        }
    }

    fn cert_request_body(&self) -> Result<CertLoginRequest, AuthError> {
        let full_identifier = format!("{}.{}", &self.account_identifier, &self.username);
        let private_key_pem = self
            .private_key_pem
            .as_ref()
            .ok_or(AuthError::MissingCertificate)?;
        let jwt_token = generate_jwt_token(private_key_pem, &full_identifier)?;

        Ok(CertLoginRequest {
            data: CertRequestData {
                login_request_common: self.login_request_common(),
                authenticator: "SNOWFLAKE_JWT".to_string(),
                token: jwt_token,
            },
        })
    }

    fn passwd_request_body(&self) -> Result<PasswordLoginRequest, AuthError> {
        let password = self.password.as_ref().ok_or(AuthError::MissingPassword)?;

        Ok(PasswordLoginRequest {
            data: PasswordRequestData {
                login_request_common: self.login_request_common(),
                password: password.to_string(),
            },
        })
    }

    async fn token_request<T: serde::ser::Serialize>(
        &self,
        body: LoginRequest<T>,
    ) -> Result<AuthToken, AuthError> {
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
                QueryType::LoginRequest,
                &self.account_identifier,
                &get_params,
                None,
                body,
            )
            .await?;
        log::debug!("Auth response: {:?}", resp);

        match resp {
            AuthResponse::Login(lr) => Ok(AuthToken::new(
                &lr.data.token,
                lr.data.master_validity_in_seconds,
            )),
            AuthResponse::Error(e) => Err(AuthError::AuthFailed(
                e.data.error_code,
                e.message.unwrap_or_default(),
            )),
            _ => Err(AuthError::UnexpectedResponse),
        }
    }

    fn login_request_common(&self) -> LoginRequestCommon {
        LoginRequestCommon {
            client_app_id: "Go".to_string(),
            client_app_version: "1.6.22".to_string(),
            svn_revision: "".to_string(),
            account_name: self.account_identifier.clone(),
            login_name: self.username.clone(),
            session_parameters: SessionParameters {
                client_validate_default_parameters: true,
            },
            client_environment: ClientEnvironment {
                application: "Rust".to_string(),
                // todo: detect os
                os: "darwin".to_string(),
                os_version: "gc-arm64".to_string(),
                ocsp_mode: "FAIL_OPEN".to_string(),
            },
        }
    }
}
