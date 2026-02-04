use serde::Serialize;

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ExecRequest {
    pub sql_text: String,
    pub async_exec: bool,
    pub sequence_id: u64,
    pub is_internal: bool,
}

#[derive(Serialize, Debug)]
pub struct LoginRequest<T> {
    pub data: T,
}

pub type PasswordLoginRequest = LoginRequest<PasswordRequestData>;
#[cfg(feature = "cert-auth")]
pub type CertLoginRequest = LoginRequest<CertRequestData>;
#[cfg(feature = "browser-auth")]
pub type BrowserLoginRequest = LoginRequest<BrowserRequestData>;

#[derive(Serialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct LoginRequestCommon {
    pub client_app_id: String,
    pub client_app_version: String,
    pub svn_revision: String,
    pub account_name: String,
    pub login_name: String,
    pub session_parameters: SessionParameters,
    pub client_environment: ClientEnvironment,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct SessionParameters {
    pub client_validate_default_parameters: bool,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct ClientEnvironment {
    pub application: String,
    pub os: String,
    pub os_version: String,
    pub ocsp_mode: String,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct PasswordRequestData {
    #[serde(flatten)]
    pub login_request_common: LoginRequestCommon,
    pub password: String,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct CertRequestData {
    #[serde(flatten)]
    pub login_request_common: LoginRequestCommon,
    pub authenticator: String,
    pub token: String,
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RenewSessionRequest {
    pub old_session_token: String,
    pub request_type: String,
}

/// Request for the first phase of browser SSO authentication.
/// Sent to /session/authenticator-request to get the SSO URL.
#[cfg(feature = "browser-auth")]
#[derive(Serialize, Debug)]
pub struct AuthenticatorRequest {
    pub data: AuthenticatorRequestData,
}

#[cfg(feature = "browser-auth")]
#[derive(Serialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct AuthenticatorRequestData {
    pub client_app_id: String,
    pub client_app_version: String,
    pub svn_revision: String,
    pub account_name: String,
    pub login_name: String,
    pub authenticator: String,
    /// Port for the local callback listener
    pub browser_mode_redirect_port: String,
    /// Proof key for the SSO challenge
    pub proof_key: String,
    pub client_environment: AuthenticatorClientEnvironment,
}

#[cfg(feature = "browser-auth")]
#[derive(Serialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct AuthenticatorClientEnvironment {
    pub application: String,
    pub os: String,
    pub os_version: String,
}

/// Request data for the second phase of browser SSO authentication.
/// Sent to /session/v1/login-request with the token from the browser callback.
#[cfg(feature = "browser-auth")]
#[derive(Serialize, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct BrowserRequestData {
    #[serde(flatten)]
    pub login_request_common: LoginRequestCommon,
    pub authenticator: String,
    pub token: String,
    pub proof_key: String,
}
