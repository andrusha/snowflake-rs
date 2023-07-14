use crate::auth::response::AuthResponse;
use crate::auth::{AuthTokens, SnowflakeAuth};
use crate::request::{request, QueryType};
use crate::AuthError;
use snowflake_jwt::generate_jwt_token;


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
}

impl SnowflakeAuth for SnowflakeCertAuth {
    fn get_master_token(&self) -> Result<AuthTokens, AuthError> {
        let full_identifier = format!("{}.{}", &self.account_identifier, &self.username);
        let jwt_token = generate_jwt_token(&self.private_key_pem, &full_identifier)?;

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
        });

        let resp = request::<AuthResponse>(
            QueryType::Auth,
            &self.account_identifier,
            &get_params,
            &[],
            body,
        )?;

        Ok(AuthTokens {
            session_token: resp.data.token,
            master_token: resp.data.master_token,
        })
    }
}
