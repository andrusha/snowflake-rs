use crate::auth::response::AuthResponse;
use crate::auth::{AuthError, AuthTokens, SnowflakeAuth};
use crate::request::{request, QueryType};

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
}

impl SnowflakeAuth for SnowflakePasswordAuth {
    fn get_master_token(&self) -> Result<AuthTokens, AuthError> {
        log::info!("Logging in using password authentication");

        let get_params = vec![
            // todo: make database optional
            ("databaseName", self.database.as_str()),
            ("roleName", self.role.as_str()),
            // ("schemaName", self.schema),
            ("warehouse", self.warehouse.as_str()),
        ];

        let body = ureq::json!({
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
        });

        let resp = request::<AuthResponse>(
            QueryType::Auth,
            &self.account_identifier,
            &get_params,
            &[],
            body,
        )?;
        log::debug!("Auth response: {:?}", resp);

        Ok(AuthTokens {
            session_token: resp.data.token,
            master_token: resp.data.master_token,
        })
    }
}
