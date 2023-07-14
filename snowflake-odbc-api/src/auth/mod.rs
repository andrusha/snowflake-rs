use thiserror::Error;
use crate::request;

pub use cert::SnowflakeCertAuth;
pub use password::SnowflakePasswordAuth;

mod cert;
mod password;
mod response;

#[derive(Error, Debug)]
pub enum AuthError {
    #[error(transparent)]
    JwtError(#[from] snowflake_jwt::JwtError),

    #[error(transparent)]
    RequestError(#[from] request::RequestError),
}

// todo: contain all the tokens and their expiration times
#[derive(Debug)]
pub struct AuthTokens {
    pub session_token: String,
    pub master_token: String,
}

// todo: allow to query for configuration parameters as well
// todo: close session after it's over
pub trait SnowflakeAuth {
    fn get_master_token(&self) -> Result<AuthTokens, AuthError>;
}
