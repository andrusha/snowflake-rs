use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct Parameter {
    pub name: String,
    pub value: serde_json::Value, // As value can be of different types (bool, String, i64, etc.), we'll use serde_json::Value
}

#[derive(Deserialize, Debug)]
pub struct SessionInfo {
    #[serde(rename = "databaseName")]
    pub database_name: Option<String>,
    #[serde(rename = "schemaName")]
    pub schema_name: Option<String>,
    #[serde(rename = "warehouseName")]
    pub warehouse_name: String,
    #[serde(rename = "roleName")]
    pub role_name: String,
}

#[derive(Deserialize, Debug)]
pub struct AuthData {
    #[serde(rename = "masterToken")]
    pub master_token: String,
    pub token: String,
    #[serde(rename = "validityInSeconds")]
    pub validity_in_seconds: i64,
    #[serde(rename = "masterValidityInSeconds")]
    pub master_validity_in_seconds: i64,
    #[serde(rename = "displayUserName")]
    pub display_user_name: String,
    #[serde(rename = "serverVersion")]
    pub server_version: String,
    #[serde(rename = "firstLogin")]
    pub first_login: bool,
    #[serde(rename = "remMeToken")]
    pub rem_me_token: Option<String>,
    #[serde(rename = "remMeValidityInSeconds")]
    pub rem_me_validity_in_seconds: i64,
    #[serde(rename = "healthCheckInterval")]
    pub health_check_interval: i64,
    #[serde(rename = "newClientForUpgrade")]
    pub new_client_for_upgrade: Option<String>,
    #[serde(rename = "sessionId")]
    pub session_id: i64,
    pub parameters: Vec<Parameter>,
    #[serde(rename = "sessionInfo")]
    pub session_info: SessionInfo,
    #[serde(rename = "idToken")]
    pub id_token: Option<String>,
    #[serde(rename = "idTokenValidityInSeconds")]
    pub id_token_validity_in_seconds: i64,
    #[serde(rename = "responseData")]
    pub response_data: Option<serde_json::Value>,
    #[serde(rename = "mfaToken")]
    pub mfa_token: Option<String>,
    #[serde(rename = "mfaTokenValidityInSeconds")]
    pub mfa_token_validity_in_seconds: i64,
}

#[derive(Deserialize, Debug)]
pub struct AuthResponse {
    // todo: make data optional (what if message fails?)
    pub data: AuthData,
    pub code: Option<String>,
    pub message: Option<String>,
    pub success: bool,
}
