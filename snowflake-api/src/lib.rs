#![doc(
issue_tracker_base_url = "https://github.com/mycelial/snowflake-rs/issues",
test(no_crate_inject)
)]
#![doc = include_str ! ("../README.md")]

use std::io;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::ToByteSlice;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use base64::Engine;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use regex::Regex;
use thiserror::Error;

use crate::connection::{Connection, ConnectionError};
use put_response::{PutResponse, S3PutResponse};
use query_response::QueryResponse;
use session::{AuthError, Session};

use crate::connection::QueryType;

mod auth_response;
mod connection;
mod error_response;
mod put_response;
mod query_response;
mod session;

#[derive(Error, Debug)]
pub enum SnowflakeApiError {
    #[error(transparent)]
    RequestError(#[from] ConnectionError),

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

    #[error("Snowflake API error: `{0}`")]
    ApiError(String),

    #[error("Snowflake API empty response could mean that query wasn't executed correctly or API call was faulty")]
    EmptyResponse,

    #[error("No usable rowsets were included in the response")]
    BrokenResponse,
}

pub enum QueryResult {
    Arrow(Vec<RecordBatch>),
    Json(serde_json::Value),
    Empty,
}

/// Snowflake API, keeps connection pool and manages session for you
pub struct SnowflakeApi {
    connection: Arc<Connection>,
    session: Session,
    account_identifier: String,
    sequence_id: u64,
}

impl SnowflakeApi {
    /// Initialize object with password auth. Authentication happens on the first request.
    pub fn with_password_auth(
        account_identifier: &str,
        warehouse: &str,
        database: Option<&str>,
        schema: Option<&str>,
        username: &str,
        role: Option<&str>,
        password: &str,
    ) -> Result<Self, SnowflakeApiError> {
        let connection = Arc::new(Connection::new()?);

        let session = Session::password_auth(
            Arc::clone(&connection),
            account_identifier,
            warehouse,
            database,
            schema,
            username,
            role,
            password,
        );

        let account_identifier = account_identifier.to_uppercase();
        Ok(SnowflakeApi {
            connection: Arc::clone(&connection),
            session,
            account_identifier,
            sequence_id: 0,
        })
    }

    /// Initialize object with private certificate auth. Authentication happens on the first request.
    pub fn with_certificate_auth(
        account_identifier: &str,
        warehouse: &str,
        database: Option<&str>,
        schema: Option<&str>,
        username: &str,
        role: Option<&str>,
        private_key_pem: &[u8],
    ) -> Result<Self, SnowflakeApiError> {
        let connection = Arc::new(Connection::new()?);

        let session = Session::cert_auth(
            Arc::clone(&connection),
            account_identifier,
            warehouse,
            database,
            schema,
            username,
            role,
            private_key_pem,
        );

        let account_identifier = account_identifier.to_uppercase();
        Ok(SnowflakeApi {
            connection: Arc::clone(&connection),
            session,
            account_identifier,
            sequence_id: 0,
        })
    }

    /// Execute a single query against API.
    /// If statement is PUT, then file will be uploaded to the Snowflake-managed storage
    pub async fn exec(&mut self, sql: &str) -> Result<QueryResult, SnowflakeApiError> {
        let put_re = Regex::new(r"(?i)^(?:/\*.*\*/\s*)*put\s+").unwrap();

        // put commands go through a different flow and result is side-effect
        if put_re.is_match(sql) {
            log::info!("Detected PUT query");

            self.exec_put(sql).await.map(|_| QueryResult::Empty)
        } else {
            self.exec_arrow(sql).await
        }
    }

    async fn exec_put(&mut self, sql: &str) -> Result<(), SnowflakeApiError> {
        let resp = self
            .run_sql::<PutResponse>(sql, QueryType::JsonQuery)
            .await?;
        log::debug!("Got put response: {:?}", resp);

        match resp {
            PutResponse::S3(r) => self.put_to_s3(r).await?,
            PutResponse::Error(e) => return Err(SnowflakeApiError::ApiError(e.message)),
        }

        Ok(())
    }

    async fn put_to_s3(&self, r: S3PutResponse) -> Result<(), SnowflakeApiError> {
        let info = r.data.stage_info;
        let (bucket_name, bucket_path) = info
            .location
            .split_once('/')
            .ok_or(SnowflakeApiError::InvalidBucketPath(info.location.clone()))?;

        let s3 = AmazonS3Builder::new()
            .with_region(info.region)
            .with_bucket_name(bucket_name)
            .with_access_key_id(info.creds.aws_key_id)
            .with_secret_access_key(info.creds.aws_secret_key)
            .with_token(info.creds.aws_token)
            .build()?;

        // todo: security vulnerability, external system tells you which local files to upload
        for src_path in r.data.src_locations.iter() {
            let path = Path::new(src_path);
            let filename = path
                .file_name()
                .ok_or(SnowflakeApiError::InvalidLocalPath(src_path.clone()))?;

            // fixme: unwrap
            let dest_path = format!("{}{}", bucket_path, filename.to_str().unwrap());
            let dest_path = object_store::path::Path::parse(dest_path)?;

            let src_path = object_store::path::Path::parse(src_path)?;

            let fs = LocalFileSystem::new().get(&src_path).await?;

            s3.put(&dest_path, fs.bytes().await?).await?;
        }

        Ok(())
    }

    /// Useful for debugging to get the straight query response
    #[cfg(debug_assertions)]
    pub async fn exec_response(&mut self, sql: &str) -> Result<QueryResponse, SnowflakeApiError> {
        self.run_sql::<QueryResponse>(sql, QueryType::ArrowQuery)
            .await
    }

    /// Useful for debugging to get raw JSON response
    #[cfg(debug_assertions)]
    pub async fn exec_json(&mut self, sql: &str) -> Result<serde_json::Value, SnowflakeApiError> {
        self.run_sql::<serde_json::Value>(sql, QueryType::JsonQuery)
            .await
    }

    async fn exec_arrow(&mut self, sql: &str) -> Result<QueryResult, SnowflakeApiError> {
        let resp = self
            .run_sql::<QueryResponse>(sql, QueryType::ArrowQuery)
            .await?;
        log::debug!("Got query response: {:?}", resp);

        let resp = match resp {
            // processable response
            QueryResponse::Result(r) => r,
            QueryResponse::Error(e) => return Err(SnowflakeApiError::ApiError(e.message)),
        };

        // if response was empty, base64 data is empty string
        // todo: still return empty arrow batch with proper schema? (schema always included)
        if resp.data.returned == 0 {
            log::info!("Got response with 0 rows");

            Ok(QueryResult::Empty)
        } else if let Some(json) = resp.data.rowset {
            log::info!("Got JSON response");

            Ok(QueryResult::Json(json))
        } else if let Some(base64) = resp.data.rowset_base64 {
            log::info!("Got base64 encoded response");
            let bytes = base64::engine::general_purpose::STANDARD.decode(base64)?;
            let fr = StreamReader::try_new_unbuffered(bytes.to_byte_slice(), None)?;

            // fixme: loads everything into memory
            let mut res = Vec::new();
            for batch in fr {
                res.push(batch?);
            }

            Ok(QueryResult::Arrow(res))
        } else {
            Err(SnowflakeApiError::BrokenResponse)
        }
    }

    async fn run_sql<R: serde::de::DeserializeOwned>(
        &mut self,
        sql: &str,
        query_type: QueryType,
    ) -> Result<R, SnowflakeApiError> {
        log::debug!("Executing: {}", sql);

        let token = self.session.get_token().await?;
        // expected by snowflake api for all requests within session to follow sequence id
        // fixme: race condition
        self.sequence_id += 1;

        let auth = format!("Snowflake Token=\"{}\"", &token);
        let body = serde_json::json!({
                "sqlText": &sql,
                "asyncExec": false,
                "sequenceId": self.sequence_id,
                "isInternal": false
        });

        let resp = self
            .connection
            .request::<R>(query_type, &self.account_identifier, &[], Some(&auth), body)
            .await?;

        Ok(resp)
    }
}
