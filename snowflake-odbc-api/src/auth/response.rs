use serde::{Deserialize, Serialize};

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
    pub master_token: String,
    pub token: String,
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
    pub data: AuthData,
    code: Option<String>,
    message: Option<String>,
    success: bool,
}
