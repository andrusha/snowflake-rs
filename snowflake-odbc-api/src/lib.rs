use arrow::datatypes::ToByteSlice;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use base64::Engine;
use thiserror::Error;
use url::Url;
use uuid::Uuid;
use crate::auth::{AuthError, SnowflakeAuth};
use crate::SnowflakeApiError::DeserializationError;

pub mod auth;

#[derive(Error, Debug)]
pub enum SnowflakeApiError {
    #[error("response deserialization error: `{0}`")]
    DeserializationError(String),

    #[error(transparent)]
    RequestError(#[from] ureq::Error),

    #[error(transparent)]
    AuthError(#[from] AuthError),

    #[error(transparent)]
    UrlParseError(#[from] url::ParseError),

    #[error(transparent)]
    ResponseDeserializationError(#[from] base64::DecodeError),

    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),
}

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryResponse {
    pub data: Data,
    pub code: Option<String>,
    pub message: Option<String>,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Data {
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

#[derive(Serialize, Deserialize, Debug)]
pub struct Parameter {
    pub name: String,
    // todo: parse parameters correctly
    pub value: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryContext {
    pub entries: Vec<Entry>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Entry {
    pub id: u32,
    pub timestamp: u64,
    pub priority: u32,
}

pub struct SnowflakeOdbcApi {
    auth: Box<dyn SnowflakeAuth>,
    account_identifier: String,
}

impl SnowflakeOdbcApi {
    pub fn new(
        auth: Box<dyn SnowflakeAuth + Send>,
        account_identifier: &str,
    ) -> Result<Self, SnowflakeApiError> {
        let account_identifier = account_identifier.to_uppercase();
        Ok(SnowflakeOdbcApi {
            auth,
            account_identifier,
        })
    }

    // todo: unify query requests with retries in the single place (with auth)
    // todo: what exec query should return if successful?
    pub fn exec(&self, sql: &str) -> Result<QueryResponse, SnowflakeApiError> {
        // todo: rotate tokens and keep until lifetime
        let tokens = self.auth.get_master_token()?;

        let url = format!("https://{}.snowflakecomputing.com/queries/v1/query-request", &self.account_identifier);

        // todo: increment subsequent request ids
        let request_id = Uuid::now_v1(&[0, 0, 0, 0, 0, 0]);
        let (client_start_time, _nanos) = request_id.get_timestamp().unwrap().to_unix();
        let request_guid = Uuid::new_v4();
        let url = Url::parse_with_params(
            &url,
            &[
                ("clientStartTime", client_start_time.to_string()),
                ("requestId", request_id.to_string()),
                ("request_guid", request_guid.to_string()),
            ])?;
        // alter session set C_API_QUERY_RESULT_FORMAT=ARROW_FORCE
        // alter session set query_result_format=arrow_force
        let auth = format!("Snowflake Token=\"{}\"", &tokens.session_token);
        let resp = ureq::request_url("POST", &url)
            .set("Authorization", &auth)
            // pretend to be C-API to get compatible responses
            //.set("User-Agent", "C API/1.0.0")
            .set("User-Agent", "Rust/0.0.1")
            .set("accept", "application/snowflake")
            .send_json(ureq::json!({
                "sqlText": &sql,
                // todo: async support needed?
                "asyncExec": false,
                // todo: why is it needed?
                "sequenceId": 1,
                "isInternal": false
        }))?;

        // todo: properly handle error responses in messages
        serde_json::from_reader(resp.into_reader()).map_err(|e| DeserializationError(e.to_string()))
    }

    pub fn exec_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, SnowflakeApiError> {
        let resp = self.exec(sql)?;
        let bytes = base64::engine::general_purpose::STANDARD.decode(resp.data.rowset_base64)?;

        let fr = StreamReader::try_new_unbuffered(bytes.to_byte_slice(), None)?;

        let mut res = Vec::new();
        for batch in fr {
            res.push(batch?);
        }

        Ok(res)
    }
}
