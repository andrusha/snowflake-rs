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
    pub age: u32,
    #[serde(rename = "errorCode")]
    pub error_code: String,
    #[serde(rename = "internalError")]
    pub internal_error: bool,
    pub line: i32,
    pub pos: i32,
    #[serde(rename = "queryId")]
    pub query_id: String,
    #[serde(rename = "sqlState")]
    pub sql_state: String,
    #[serde(rename = "type")]
    pub type_: String,
}
