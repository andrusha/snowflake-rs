use std::io;
use std::path::Path;

use arrow::datatypes::ToByteSlice;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use base64::Engine;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use regex::Regex;
use serde::Deserialize;
use thiserror::Error;

pub use auth::{SnowflakeCertAuth, SnowflakePasswordAuth};

use crate::auth::{AuthError, SnowflakeAuth};
use crate::request::{QueryType, request};

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

    #[error("S3 bucket path in PUT request is invalid: `{0}`")]
    InvalidBucketPath(String),

    #[error("Couldn't extract filename from the local path: `{0}`")]
    InvalidLocalPath(String),

    #[error(transparent)]
    LocalIoError(#[from] io::Error),

    #[error(transparent)]
    ObjectStoreError(#[from] object_store::Error),

    #[error(transparent)]
    ObjectStorePathError(#[from] object_store::path::Error),
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
    Empty,
}

pub struct SnowflakeOdbcApi {
    auth: Box<dyn SnowflakeAuth + Send>,
    account_identifier: String,
}

impl SnowflakeOdbcApi {
    pub fn new(
        auth: Box<impl SnowflakeAuth + Send + 'static>,
        account_identifier: &str,
    ) -> Result<Self, SnowflakeApiError> {
        let account_identifier = account_identifier.to_uppercase();
        Ok(SnowflakeOdbcApi {
            auth,
            account_identifier,
        })
    }

    pub async fn exec(&self, sql: &str) -> Result<QueryResult, SnowflakeApiError> {
        let put_re = Regex::new(r"(?i)^(?:/\*.*\*/\s*)*put\s+").unwrap();

        // put commands go through a different flow and result is side-effect
        if put_re.is_match(sql) {
            log::info!("Detected PUT query");

            self
                .exec_put(sql)
                .await
                .map(|_| QueryResult::Empty)
        } else {
            self
                .exec_arrow(sql)
                .await
                .map(QueryResult::Arrow)
        }
    }

    async fn exec_put(&self, sql: &str) -> Result<(), SnowflakeApiError> {
        let resp = self.run_sql::<PutResponse>(sql, QueryType::JsonQuery).await?;
        log::debug!("Got put response: {:?}", resp);

        match resp {
            PutResponse::S3(r) => self.put_to_s3(r).await?,
        }

        Ok(())
    }

    async fn put_to_s3(&self, r: S3PutResponse) -> Result<(), SnowflakeApiError> {
        let info = r.data.upload_info;
        let (bucket_name, bucket_path) = info
            .location
            .split_once("/")
            .ok_or(SnowflakeApiError::InvalidBucketPath(info.location.clone()))?;

        let s3 = AmazonS3Builder::new()
            .with_region(info.region)
            .with_bucket_name(bucket_name)
            .with_access_key_id(info.creds.aws_key_id)
            .with_secret_access_key(info.creds.aws_secret_key)
            .with_token(info.creds.aws_token)
            .build()?;

        // todo: security vulnerability, external system tells you which files to upload
        for src_path in r.data.src_locations.iter() {
            let path = Path::new(src_path);
            let filename = path
                .file_name()
                .ok_or(SnowflakeApiError::InvalidLocalPath(src_path.clone()))?;

            // fixme: unwrap
            let dest_path = format!("{}{}", bucket_path, filename.to_str().unwrap());
            let dest_path = object_store::path::Path::parse(dest_path)?;

            let src_path = object_store::path::Path::parse(src_path)?;

            let fs = LocalFileSystem::new()
                .get(&src_path)
                .await?;

            s3
                .put(&dest_path, fs.bytes().await?)
                .await?;
        }

        Ok(())
    }

    #[cfg(debug_assertions)]
    pub async fn exec_response(&self, sql: &str) -> Result<QueryResponse, SnowflakeApiError> {
        self.run_sql::<QueryResponse>(sql, QueryType::ArrowQuery).await
    }

    #[cfg(debug_assertions)]
    pub async fn exec_json(&self, sql: &str) -> Result<serde_json::Value, SnowflakeApiError> {
        self.run_sql::<serde_json::Value>(sql, QueryType::JsonQuery).await
    }

    async fn exec_arrow(&self, sql: &str) -> Result<Vec<RecordBatch>, SnowflakeApiError> {
        let resp = self.run_sql::<QueryResponse>(sql, QueryType::ArrowQuery).await?;
        log::debug!("Got query response: {:?}", resp);

        log::info!("Decoding Arrow");
        let bytes = base64::engine::general_purpose::STANDARD.decode(resp.data.rowset_base64)?;
        let fr = StreamReader::try_new_unbuffered(bytes.to_byte_slice(), None)?;

        // fixme: loads everything into memory
        let mut res = Vec::new();
        for batch in fr {
            res.push(batch?);
        }

        Ok(res)
    }

    async fn run_sql<R: serde::de::DeserializeOwned>(&self, sql: &str, query_type: QueryType) -> Result<R, SnowflakeApiError> {
        log::debug!("Executing: {}", sql);

        let tokens = self.auth.get_master_token().await?;
        let auth = format!("Snowflake Token=\"{}\"", &tokens.session_token);
        let body = serde_json::json!({
                "sqlText": &sql,
                "asyncExec": false,
                "sequenceId": 1,
                "isInternal": false
        });

        let resp = request::<R>(
            query_type,
            &self.account_identifier,
            &[],
            Some(&auth),
            body,
        ).await?;

        Ok(resp)
    }
}
