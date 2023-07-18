use async_trait::async_trait;
use snowflake_jwt::generate_jwt_token;
use std::sync::Arc;

use crate::auth::response::AuthResponse;
use crate::auth::{AuthTokens, SnowflakeAuth};
use crate::connection::{Connection, QueryType};
use crate::AuthError;

// todo: split warehouse-database-schema and username-role-key into its own structs
pub struct SnowflakeCertAuth {
    connection: Arc<Connection>,
    account_identifier: String,
    warehouse: String,
    database: Option<String>,
    schema: Option<String>,
    username: String,
    role: Option<String>,
    private_key_pem: Vec<u8>,
}

impl SnowflakeCertAuth {
    pub fn new(
        connection: Arc<Connection>,
        private_key_pem: &[u8],
        username: &str,
        role: Option<&str>,
        account_identifier: &str,
        warehouse: &str,
        database: Option<&str>,
        schema: Option<&str>,
    ) -> Result<Self, AuthError> {
        // uppercase everything as this is the convention
        let account_identifier = account_identifier.to_uppercase();

        let warehouse = warehouse.to_uppercase();
        let database = database.map(str::to_uppercase);
        let schema = schema.map(str::to_uppercase);

        let username = username.to_uppercase();
        let role = role.map(str::to_uppercase);
        let private_key_pem = private_key_pem.to_vec();

        Ok(SnowflakeCertAuth {
            connection,
            private_key_pem,
            account_identifier,
            warehouse,
            database,
            username,
            role,
            schema,
        })
    }
}

#[async_trait]
impl SnowflakeAuth for SnowflakeCertAuth {
    async fn get_master_token(&self) -> Result<AuthTokens, AuthError> {
        log::info!("Logging in using certificate authentication");

        let full_identifier = format!("{}.{}", &self.account_identifier, &self.username);
        let jwt_token = generate_jwt_token(&self.private_key_pem, &full_identifier)?;

        // todo: extract this into common function
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

        let body = serde_json::json!({
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

        Ok(AuthTokens {
            session_token: resp.data.token,
            master_token: resp.data.master_token,
        })
    }
}
