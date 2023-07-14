use crate::auth::{AuthError, SnowflakeAuth};
use crate::request::{request, QueryType};
use arrow::datatypes::ToByteSlice;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use base64::Engine;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub use auth::{SnowflakeCertAuth, SnowflakePasswordAuth};

mod auth;
mod request;

#[derive(Error, Debug)]
pub enum SnowflakeApiError {
    #[error(transparent)]
    RequestError(#[from] request::RequestError),

    #[error(transparent)]
    AuthError(#[from] AuthError),

    #[error(transparent)]
    ResponseDeserializationError(#[from] base64::DecodeError),

    #[error(transparent)]
    ArrowError(#[from] arrow::error::ArrowError),
}

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
    auth: Box<dyn SnowflakeAuth + Send>,
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

    pub fn exec_response(&self, sql: &str) -> Result<QueryResponse, SnowflakeApiError> {
        let tokens = self.auth.get_master_token()?;
        let auth = format!("Snowflake Token=\"{}\"", &tokens.session_token);
        let body = ureq::json!({
                "sqlText": &sql,
                "asyncExec": false,
                "sequenceId": 1,
                "isInternal": false
        });

        let resp = request::<QueryResponse>(
            QueryType::ArrowQuery,
            &self.account_identifier,
            &[],
            &[("Authorization", &auth)],
            body,
        )?;

        Ok(resp)
    }

    pub fn exec_json(&self, sql: &str) -> Result<serde_json::Value, SnowflakeApiError> {
        let tokens = self.auth.get_master_token()?;
        let auth = format!("Snowflake Token=\"{}\"", &tokens.session_token);
        let body = ureq::json!({
                "sqlText": &sql,
                "asyncExec": false,
                "sequenceId": 1,
                "isInternal": false
        });

        let resp = request::<serde_json::Value>(
            QueryType::JsonQuery,
            &self.account_identifier,
            &[],
            &[("Authorization", &auth)],
            body,
        )?;

        Ok(resp)
    }

    pub fn exec_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, SnowflakeApiError> {
        let resp = self.exec_response(sql)?;

        let bytes = base64::engine::general_purpose::STANDARD.decode(resp.data.rowset_base64)?;
        let fr = StreamReader::try_new_unbuffered(bytes.to_byte_slice(), None)?;

        let mut res = Vec::new();
        for batch in fr {
            res.push(batch?);
        }

        Ok(res)
    }
}
