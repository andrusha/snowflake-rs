use crate::auth::{AuthError, SnowflakeAuth};
use crate::request::{request, QueryType};
use arrow::datatypes::ToByteSlice;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use base64::Engine;
use regex::Regex;
use serde::Deserialize;
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

// select query response

#[derive(Deserialize, Debug)]
pub struct QueryResponse {
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

// put response

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum PutResponse {
    S3(S3PutResponse)
}

#[derive(Deserialize, Debug)]
pub struct S3PutResponse {
    pub data: S3PutData,
    pub code: Option<String>,
    pub message: Option<String>,
    pub success: bool,
}

#[derive(Deserialize, Debug)]
pub struct S3PutData {
    #[serde(rename = "uploadInfo")]
    pub upload_info: Info,
    #[serde(rename = "src_locations")]
    pub src_locations: Vec<String>,
    pub parallel: u32,
    pub threshold: u64,
    #[serde(rename = "autoCompress")]
    pub auto_compress: bool,
    pub overwrite: bool,
    #[serde(rename = "sourceCompression")]
    pub source_compression: String,
    #[serde(rename = "clientShowEncryptionParameter")]
    pub client_show_encryption_parameter: bool,
    #[serde(rename = "queryId")]
    pub query_id: String,
    #[serde(rename = "encryptionMaterial")]
    pub encryption_material: EncryptionMaterial,
    #[serde(rename = "stageInfo")]
    pub stage_info: Info,
    pub command: String,
    pub kind: Option<String>,
    pub operation: String,
}

#[derive(Deserialize, Debug)]
pub struct Info {
    #[serde(rename = "locationType")]
    pub location_type: String,
    pub location: String,
    pub path: String,
    pub region: String,
    #[serde(rename = "storageAccount")]
    pub storage_account: Option<String>,
    #[serde(rename = "isClientSideEncrypted")]
    pub is_client_side_encrypted: bool,
    pub creds: Creds,
    #[serde(rename = "presignedUrl")]
    pub presigned_url: Option<String>,
    #[serde(rename = "endPoint")]
    pub end_point: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct Creds {
    #[serde(rename = "AWS_KEY_ID")]
    pub aws_key_id: String,
    #[serde(rename = "AWS_SECRET_KEY")]
    pub aws_secret_key: String,
    #[serde(rename = "AWS_TOKEN")]
    pub aws_token: String,
    #[serde(rename = "AWS_ID")]
    pub aws_id: String,
    #[serde(rename = "AWS_KEY")]
    pub aws_key: String,
}

#[derive(Deserialize, Debug)]
pub struct EncryptionMaterial {
    #[serde(rename = "queryStageMasterKey")]
    pub query_stage_master_key: String,
    #[serde(rename = "queryId")]
    pub query_id: String,
    #[serde(rename = "smkId")]
    pub smk_id: u64,
}

// actual api

pub enum QueryResult {
    Arrow(Vec<RecordBatch>),
    Put(S3PutResponse),
    Empty,
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

    pub fn exec(&self, sql: &str) -> Result<QueryResult, SnowflakeApiError> {
        let put_re = Regex::new(r"(?i)^(?:/\*.*\*/\s*)*put\s+").unwrap();

        // put commands go through a different flow and result is side-effect
        if put_re.is_match(sql) {
            self
                .exec_put(sql)
                .map(QueryResult::Put)
        } else {
            self
                .exec_arrow(sql)
                .map(QueryResult::Arrow)
        }
    }

    fn exec_put(&self, sql: &str) -> Result<S3PutResponse, SnowflakeApiError> {
        self.run_sql::<S3PutResponse>(sql, QueryType::JsonQuery)
    }

    #[cfg(debug_assertions)]
    pub fn exec_response(&self, sql: &str) -> Result<QueryResponse, SnowflakeApiError> {
        self.run_sql::<QueryResponse>(sql, QueryType::ArrowQuery)
    }

    #[cfg(debug_assertions)]
    pub fn exec_json(&self, sql: &str) -> Result<serde_json::Value, SnowflakeApiError> {
        self.run_sql::<serde_json::Value>(sql, QueryType::JsonQuery)
    }

    fn exec_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, SnowflakeApiError> {
        let resp = self.run_sql::<QueryResponse>(sql, QueryType::ArrowQuery)?;

        let bytes = base64::engine::general_purpose::STANDARD.decode(resp.data.rowset_base64)?;
        let fr = StreamReader::try_new_unbuffered(bytes.to_byte_slice(), None)?;

        // fixme: loads everything into memory
        let mut res = Vec::new();
        for batch in fr {
            res.push(batch?);
        }

        Ok(res)
    }

    fn run_sql<R: serde::de::DeserializeOwned>(&self, sql: &str, query_type: QueryType) -> Result<R, SnowflakeApiError> {
        let tokens = self.auth.get_master_token()?;
        let auth = format!("Snowflake Token=\"{}\"", &tokens.session_token);
        let body = ureq::json!({
                "sqlText": &sql,
                "asyncExec": false,
                "sequenceId": 1,
                "isInternal": false
        });

        let resp = request::<R>(
            query_type,
            &self.account_identifier,
            &[],
            &[("Authorization", &auth)],
            body,
        )?;

        Ok(resp)
    }
}
