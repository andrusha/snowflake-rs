use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct ErrorResponse {
    pub code: String,
    pub data: Data,
    pub headers: Option<String>,
    pub message: String,
    pub success: bool,
}

#[derive(Deserialize, Debug)]
pub struct Data {
    pub age: u64,
    #[serde(rename = "errorCode")]
    pub error_code: String,
    #[serde(rename = "internalError")]
    pub internal_error: bool,

    // come when query is invalid
    pub line: Option<i64>,
    pub pos: Option<i64>,

    #[serde(rename = "queryId")]
    pub query_id: String,
    #[serde(rename = "sqlState")]
    pub sql_state: String,

    // todo: when this appears? what it means?
    #[serde(rename = "type")]
    pub type_: Option<String>,
}
