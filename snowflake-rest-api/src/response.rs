use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize, Deserialize)]
pub struct ResultSetMetaData {
    #[serde(rename = "numRows")]
    num_rows: u32,
    format: String,
    #[serde(rename = "partitionInfo")]
    partition_info: Vec<PartitionInfo>,
    #[serde(rename = "rowType")]
    row_type: Vec<RowType>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionInfo {
    #[serde(rename = "rowCount")]
    row_count: u32,
    #[serde(rename = "uncompressedSize")]
    uncompressed_size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RowType {
    name: String,
    database: String,
    schema: String,
    table: String,
    precision: Option<u32>,
    scale: Option<u32>,
    #[serde(rename = "type")]
    type_: String,
    #[serde(rename = "byteLength")]
    byte_length: Option<u64>,
    nullable: bool,
    collation: Option<String>,
    length: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Stats {
    #[serde(rename = "numRowsInserted")]
    num_rows_inserted: u32,
    #[serde(rename = "numRowsDeleted")]
    num_rows_deleted: u32,
    #[serde(rename = "numRowsUpdated")]
    num_rows_updated: u32,
    #[serde(rename = "numDmlDuplicates")]
    num_dml_duplicates: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SnowflakeResponse {
    #[serde(rename = "resultSetMetaData")]
    result_set_meta_data: ResultSetMetaData,
    pub data: Vec<Vec<Value>>,
    code: String,
    #[serde(rename = "statementStatusUrl")]
    statement_status_url: String,
    #[serde(rename = "requestId")]
    request_id: String,
    #[serde(rename = "sqlState")]
    sql_state: String,
    #[serde(rename = "statementHandle")]
    statement_handle: String,
    message: String,
    #[serde(rename = "createdOn")]
    created_on: u64,
    stats: Option<Stats>
}
