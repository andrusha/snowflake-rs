use serde::Deserialize;
use crate::error_response::ErrorResponse;

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum QueryResponse {
    Result(QueryResponseInt),
    Empty(EmptyResponse),
    Error(ErrorResponse),
}

// placeholder to deserialize empty object
#[derive(Deserialize, Debug)]
pub struct EmptyResponse {}

#[derive(Deserialize, Debug)]
pub struct QueryResponseInt {
    pub data: QueryData,
    pub code: Option<String>,
    pub message: Option<String>,
    pub success: bool,
}

#[derive(Deserialize, Debug)]
pub struct QueryData {
    pub parameters: Vec<Parameter>,
    pub rowtype: Vec<RowType>,
    #[serde(rename = "rowsetBase64")]
    pub rowset_base64: String,
    pub total: u32,
    pub returned: u32,
    #[serde(rename = "queryId")]
    pub query_id: String,
    #[serde(rename = "databaseProvider")]
    pub database_provider: Option<String>,
    #[serde(rename = "finalDatabaseName")]
    pub final_database_name: String,
    #[serde(rename = "finalSchemaName")]
    pub final_schema_name: Option<String>,
    #[serde(rename = "finalWarehouseName")]
    pub final_warehouse_name: String,
    #[serde(rename = "finalRoleName")]
    pub final_role_name: String,
    #[serde(rename = "numberOfBinds")]
    pub number_of_binds: u32,
    #[serde(rename = "arrayBindSupported")]
    pub array_bind_supported: bool,
    #[serde(rename = "statementTypeId")]
    pub statement_type_id: u32,
    pub version: u32,
    #[serde(rename = "sendResultTime")]
    pub send_result_time: u64,
    #[serde(rename = "queryResultFormat")]
    pub query_result_format: String,
    #[serde(rename = "queryContext")]
    pub query_context: QueryContext,
}

#[derive(Deserialize, Debug)]
pub struct Parameter {
    pub name: String,
    // todo: parse parameters correctly
    pub value: serde_json::Value,
}

#[derive(Deserialize, Debug)]
pub struct RowType {
    pub name: String,
    pub database: String,
    pub schema: String,
    pub table: String,
    pub scale: Option<u32>,
    #[serde(rename = "type")]
    pub type_: String,
    pub precision: Option<u32>,
    #[serde(rename = "byteLength")]
    pub byte_length: Option<u32>,
    pub nullable: bool,
    pub collation: Option<String>,
    pub length: Option<u32>,
}

#[derive(Deserialize, Debug)]
pub struct QueryContext {
    pub entries: Vec<Entry>,
}

#[derive(Deserialize, Debug)]
pub struct Entry {
    pub id: u32,
    pub timestamp: u64,
    pub priority: u32,
}
