use crate::error_response::ErrorResponse;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum QueryResponse {
    Result(QueryResponseInt),
    Error(ErrorResponse),
}

#[derive(Deserialize, Debug)]
pub struct QueryResponseInt {
    // JSON-responses capitalize Data field for some reason
    #[serde(alias = "Data")]
    pub data: QueryData,
    // might come as empty string instead of None
    pub code: Option<String>,
    // might come as empty string instead of None
    pub message: Option<String>,
    pub success: bool,
}

// todo: verify structs against Go / C++ implementation
// fixme: should all ints be signed?
#[derive(Deserialize, Debug)]
pub struct QueryData {
    pub parameters: Vec<Parameter>,
    pub rowtype: Vec<RowType>,

    /// would exist when JSON response is given
    /// default for non-SELECT queries
    /// GET / PUT has their own response format
    pub rowset: Option<serde_json::Value>,

    /// only exists when binary response is given, eg Arrow
    /// default for all SELECT queries
    #[serde(rename = "rowsetBase64")]
    pub rowset_base64: Option<String>,

    pub total: u64,
    pub returned: u64,
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

    // only present on SELECT queries
    #[serde(rename = "numberOfBinds")]
    pub number_of_binds: Option<u64>,
    #[serde(rename = "arrayBindSupported")]
    pub array_bind_supported: Option<bool>,
    #[serde(rename = "queryContext")]
    pub query_context: Option<QueryContext>,
    #[serde(rename = "sendResultTime")]
    pub send_result_time: Option<u64>,

    #[serde(rename = "statementTypeId")]
    pub statement_type_id: u64,
    pub version: u64,
    // todo: add Enum for json or arrow
    #[serde(rename = "queryResultFormat")]
    pub query_result_format: String,

    // present on file transfer / format related queries
    // todo: add correct types
    #[serde(rename = "uploadInfo")]
    pub upload_info: Option<serde_json::Value>,
    #[serde(rename = "encryptionMaterial")]
    pub encryption_material: Option<serde_json::Value>,
    #[serde(rename = "stageInfo")]
    pub stage_info: Option<serde_json::Value>,
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
    pub scale: Option<i64>,
    #[serde(rename = "type")]
    pub type_: String,
    pub precision: Option<i64>,
    #[serde(rename = "byteLength")]
    pub byte_length: Option<i64>,
    pub nullable: bool,
    pub collation: Option<String>,
    pub length: Option<i64>,
}

#[derive(Deserialize, Debug)]
pub struct QueryContext {
    pub entries: Vec<Entry>,
}

#[derive(Deserialize, Debug)]
pub struct Entry {
    pub id: u64,
    pub timestamp: u64,
    pub priority: u64,
}
