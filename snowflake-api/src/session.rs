use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::lock::Mutex;
#[cfg(feature = "cert-auth")]
use snowflake_jwt::generate_jwt_token;
use thiserror::Error;

use crate::connection;
use crate::connection::{Connection, QueryType};
#[cfg(feature = "cert-auth")]
use crate::requests::{CertLoginRequest, CertRequestData};
use crate::requests::{
    ClientEnvironment, LoginRequest, LoginRequestCommon, OAuthLoginRequest, OAuthRequestData,
    PasswordLoginRequest, PasswordRequestData, RenewSessionRequest, SessionParameters,
};
use crate::responses::AuthResponse;

#[derive(Error, Debug)]
pub enum AuthError {
    #[error(transparent)]
    #[cfg(feature = "cert-auth")]
    JwtError(#[from] snowflake_jwt::JwtError),

    #[error(transparent)]
    RequestError(#[from] connection::ConnectionError),

    #[error("Environment variable `{0}` is required, but were not set")]
    MissingEnvArgument(String),

    #[error("Password auth was requested, but password wasn't provided")]
    MissingPassword,

    #[error("Certificate auth was requested, but certificate wasn't provided")]
    MissingCertificate,

    #[error("Unexpected API response")]
    UnexpectedResponse,

    // todo: add code mapping to meaningful message and/or refer to docs
    //   eg https://docs.snowflake.com/en/user-guide/key-pair-auth-troubleshooting
    #[error("Failed to authenticate. Error code: {0}. Message: {1}")]
    AuthFailed(String, String),

    #[error("Can not renew closed session token")]
    OutOfOrderRenew,

    #[error("Failed to exchange or request a new token")]
    TokenFetchFailed,

    #[error("Enable the cert-auth feature to use certificate authentication")]
    CertAuthNotEnabled,

    #[error("The authentication type has not been set yet on the connection!")]
    AuthTypeUnset,
}

#[derive(Debug)]
struct AuthTokens {
    session_token: AuthToken,
    master_token: AuthToken,
    /// expected by snowflake api for all requests within session to follow sequence id
    sequence_id: u64,
}

#[derive(Debug, Clone)]
struct AuthToken {
    token: String,
    valid_for: Duration,
    issued_on: Instant,
}

#[derive(Debug, Clone)]
pub struct AuthParts {
    pub session_token_auth_header: String,
    pub sequence_id: u64,
}

impl AuthToken {
    pub fn new(token: &str, validity_in_seconds: i64) -> Self {
        let token = token.to_string();

        let valid_for = if validity_in_seconds < 0 {
            Duration::from_secs(u64::MAX)
        } else {
            // Note for reviewer: I beliebe this only fails on negative numbers. I imagine we will
            // never get negative numbers, but if we do, is MAX or 0 a more sane default?
            Duration::from_secs(u64::try_from(validity_in_seconds).unwrap_or(u64::MAX))
        };
        let issued_on = Instant::now();

        Self {
            token,
            valid_for,
            issued_on,
        }
    }

    pub fn is_expired(&self) -> bool {
        Instant::now().duration_since(self.issued_on) >= self.valid_for
    }

    pub fn auth_header(&self) -> String {
        format!("Snowflake Token=\"{}\"", &self.token)
    }
}

enum AuthType {
    Certificate(String),
    Password(String),
    OAuth(String),
}

/// Requests, caches, and renews authentication tokens.
/// Tokens are given as response to creating new session in Snowflake. Session persists
/// the configuration state and temporary objects (tables, procedures, etc).

// todo: close session after object is dropped
pub struct Session {
    connection: Arc<Connection>,

    auth_tokens: Mutex<Option<AuthTokens>>,
    auth_type: AuthType,
    account_identifier: String,

    username: String,
    role: Option<String>,

    object_details: SessionObjectDetails,
}

#[must_use]
pub struct SessionBuilder {
    account_identifier: String,
    object_details: SessionObjectDetails,
    username: String,
    role: Option<String>,
}

impl SessionBuilder {
    pub fn new(account_identifier: &str, username: &str) -> Self {
        let username = username.to_uppercase();

        Self {
            account_identifier: account_identifier.to_string(),
            object_details: SessionObjectDetails::default(),
            username,
            role: None,
        }
    }

    pub fn warehouse(mut self, warehouse: Option<&str>) -> Self {
        self.object_details.warehouse = warehouse.map(str::to_string);
        self
    }

    pub fn database(mut self, database: Option<&str>) -> Self {
        self.object_details.database = database.map(str::to_string);
        self
    }

    pub fn schema(mut self, schema: Option<&str>) -> Self {
        self.object_details.schema = schema.map(str::to_string);
        self
    }

    pub fn role(mut self, role: Option<&str>) -> Self {
        self.role = role.map(str::to_string);
        self
    }

    pub fn build_oauth(&self, connection: Arc<Connection>, oauth_access_token: &str) -> Session {
        Session::new(
            connection,
            &self.account_identifier,
            AuthType::OAuth(oauth_access_token.to_string()),
            self.object_details.clone(),
            &self.username,
            self.role.as_deref(),
        )
    }

    pub fn build_password(&self, connection: Arc<Connection>, password: &str) -> Session {
        Session::new(
            connection,
            &self.account_identifier,
            AuthType::Password(password.to_string()),
            self.object_details.clone(),
            &self.username,
            self.role.as_deref(),
        )
    }

    pub fn build_cert(&self, connection: Arc<Connection>, private_key_pem: &str) -> Session {
        Session::new(
            connection,
            &self.account_identifier,
            AuthType::Certificate(private_key_pem.to_string()),
            self.object_details.clone(),
            &self.username,
            self.role.as_deref(),
        )
    }
}

#[derive(Debug, Default, Clone)]
struct SessionObjectDetails {
    warehouse: Option<String>,
    database: Option<String>,
    schema: Option<String>,
}

impl Session {
    fn new(
        connection: Arc<Connection>,
        account_identifier: &str,
        auth_type: AuthType,
        object_details: SessionObjectDetails,
        username: &str,
        role: Option<&str>,
    ) -> Self {
        Self {
            connection,
            auth_tokens: Mutex::new(None),
            auth_type,
            account_identifier: account_identifier.to_string(),
            username: username.to_string(),
            role: role.map(str::to_string),
            object_details,
        }
    }

    /// Get cached token or request a new one if old one has expired.
    pub async fn get_token(&self) -> Result<AuthParts, AuthError> {
        let mut auth_tokens = self.auth_tokens.lock().await;
        if auth_tokens.is_none()
            || auth_tokens
                .as_ref()
                .is_some_and(|at| at.master_token.is_expired())
        {
            // Create new session if tokens are absent or can not be exchange
            let tokens = match &self.auth_type {
                AuthType::Certificate(pem) => {
                    log::info!("Starting session with certificate authentication");
                    if cfg!(feature = "cert-auth") {
                        self.create(self.cert_request_body(pem)?).await
                    } else {
                        Err(AuthError::MissingCertificate)?
                    }
                }
                AuthType::Password(pwd) => {
                    log::info!("Starting session with password authentication");
                    self.create(self.passwd_request_body(pwd)).await
                }
                AuthType::OAuth(token) => {
                    log::info!("Starting session with oauth authentication");
                    self.create(self.oauth_request_body(token)).await
                }
            }?;
            *auth_tokens = Some(tokens);
        } else if auth_tokens
            .as_ref()
            .is_some_and(|at| at.session_token.is_expired())
        {
            // Renew old session token
            let old_token = auth_tokens.take().unwrap();
            let tokens = self.renew(old_token).await?;
            *auth_tokens = Some(tokens);
        }
        auth_tokens.as_mut().unwrap().sequence_id += 1;
        Ok(AuthParts {
            session_token_auth_header: auth_tokens.as_ref().unwrap().session_token.auth_header(),
            sequence_id: auth_tokens.as_ref().unwrap().sequence_id,
        })
    }

    pub async fn close(&mut self) -> Result<(), AuthError> {
        if let Some(tokens) = self.auth_tokens.lock().await.take() {
            log::debug!("Closing sessions");

            let resp = self
                .connection
                .request::<AuthResponse>(
                    QueryType::CloseSession,
                    &self.account_identifier,
                    &[("delete", "true")],
                    Some(&tokens.session_token.auth_header()),
                    serde_json::Value::default(),
                )
                .await?;

            match resp {
                AuthResponse::Close(_) => Ok(()),
                AuthResponse::Error(e) => Err(AuthError::AuthFailed(
                    e.code.unwrap_or_default(),
                    e.message.unwrap_or_default(),
                )),
                _ => Err(AuthError::UnexpectedResponse),
            }
        } else {
            Ok(())
        }
    }

    #[cfg(feature = "cert-auth")]
    fn cert_request_body(&self, private_key_pem: &str) -> Result<CertLoginRequest, AuthError> {
        let full_identifier = format!("{}.{}", &self.account_identifier, &self.username);

        let jwt_token = generate_jwt_token(private_key_pem, &full_identifier)?;

        Ok(CertLoginRequest {
            data: CertRequestData {
                login_request_common: self.login_request_common(),
                authenticator: "SNOWFLAKE_JWT".to_string(),
                token: jwt_token,
            },
        })
    }

    fn passwd_request_body(&self, password: &str) -> PasswordLoginRequest {
        PasswordLoginRequest {
            data: PasswordRequestData {
                login_request_common: self.login_request_common(),
                password: password.to_string(),
            },
        }
    }

    fn oauth_request_body(&self, oauth_access_token: &str) -> OAuthLoginRequest {
        OAuthLoginRequest {
            data: OAuthRequestData {
                login_request_common: self.login_request_common(),
                authenticator: "OAUTH".to_string(),
                token: oauth_access_token.to_string(),
            },
        }
    }

    /// Start new session, all the Snowflake temporary objects will be scoped towards it,
    /// as well as temporary configuration parameters
    async fn create<T: serde::ser::Serialize>(
        &self,
        body: LoginRequest<T>,
    ) -> Result<AuthTokens, AuthError> {
        let mut get_params = Vec::new();
        if let Some(warehouse) = &self.object_details.warehouse {
            get_params.push(("warehouse", warehouse.as_str()));
        }

        if let Some(database) = &self.object_details.database {
            get_params.push(("databaseName", database.as_str()));
        }

        if let Some(schema) = &self.object_details.schema {
            get_params.push(("schemaName", schema.as_str()));
        }

        if let Some(role) = &self.role {
            get_params.push(("roleName", role.as_str()));
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
            AuthResponse::Login(lr) => {
                let session_token = AuthToken::new(&lr.data.token, lr.data.validity_in_seconds);
                let master_token =
                    AuthToken::new(&lr.data.master_token, lr.data.master_validity_in_seconds);

                Ok(AuthTokens {
                    session_token,
                    master_token,
                    sequence_id: 0,
                })
            }
            AuthResponse::Error(e) => Err(AuthError::AuthFailed(
                e.code.unwrap_or_default(),
                e.message.unwrap_or_default(),
            )),
            _ => Err(AuthError::UnexpectedResponse),
        }
    }

    fn login_request_common(&self) -> LoginRequestCommon {
        LoginRequestCommon {
            client_app_id: "Go".to_string(),
            client_app_version: "1.6.22".to_string(),
            svn_revision: String::new(),
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

    async fn renew(&self, token: AuthTokens) -> Result<AuthTokens, AuthError> {
        log::debug!("Renewing the token");
        let auth = token.master_token.auth_header();
        let body = RenewSessionRequest {
            old_session_token: token.session_token.token.clone(),
            request_type: "RENEW".to_string(),
        };

        let resp = self
            .connection
            .request(
                QueryType::TokenRequest,
                &self.account_identifier,
                &[],
                Some(&auth),
                body,
            )
            .await?;

        match resp {
            AuthResponse::Renew(rs) => {
                let session_token =
                    AuthToken::new(&rs.data.session_token, rs.data.validity_in_seconds_s_t);
                let master_token =
                    AuthToken::new(&rs.data.master_token, rs.data.validity_in_seconds_m_t);

                Ok(AuthTokens {
                    session_token,
                    master_token,
                    sequence_id: token.sequence_id,
                })
            }
            AuthResponse::Error(e) => Err(AuthError::AuthFailed(
                e.code.unwrap_or_default(),
                e.message.unwrap_or_default(),
            )),
            _ => Err(AuthError::UnexpectedResponse),
        }
    }
}
