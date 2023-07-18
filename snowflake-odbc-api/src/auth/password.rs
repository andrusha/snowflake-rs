use async_trait::async_trait;
use std::sync::Arc;

use crate::auth::response::AuthResponse;
use crate::auth::{AuthError, AuthTokens, SnowflakeAuth};
use crate::connection::{Connection, QueryType};

pub struct SnowflakePasswordAuth {
    connection: Arc<Connection>,
    account_identifier: String,
    warehouse: String,
    database: Option<String>,
    schema: Option<String>,
    username: String,
    password: String,
    role: Option<String>,
}

impl SnowflakePasswordAuth {
    pub fn new(
        connection: Arc<Connection>,
        username: &str,
        password: &str,
        role: Option<&str>,
        account_identifier: &str,
        warehouse: &str,
        database: Option<&str>,
        schema: Option<&str>,
    ) -> Result<Self, AuthError> {
        let account_identifier = account_identifier.to_uppercase();

        let warehouse = warehouse.to_uppercase();
        let database = database.map(str::to_uppercase);
        let schema = schema.map(str::to_uppercase);

        let username = username.to_uppercase();
        let password = password.to_string();
        let role = role.map(str::to_uppercase);

        Ok(SnowflakePasswordAuth {
            connection,
            account_identifier,
            warehouse,
            database,
            username,
            password,
            role,
            schema,
        })
    }
}

#[async_trait]
impl SnowflakeAuth for SnowflakePasswordAuth {
    async fn get_master_token(&self) -> Result<AuthTokens, AuthError> {
        log::info!("Logging in using password authentication");

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
