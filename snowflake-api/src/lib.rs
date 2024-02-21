#![doc(
    issue_tracker_base_url = "https://github.com/mycelial/snowflake-rs/issues",
    test(no_crate_inject)
)]
#![doc = include_str ! ("../README.md")]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
    clippy::must_use_candidate,
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::struct_field_names,
    clippy::future_not_send, // This one seems like something we should eventually fix
    clippy::missing_panics_doc
)]

use std::io;
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::ToByteSlice;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use base64::Engine;
use futures::future::try_join_all;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use regex::Regex;
use thiserror::Error;

use crate::connection::{Connection, ConnectionError};
use responses::ExecResponse;
use session::{AuthError, Session};

use crate::connection::QueryType;
use crate::requests::ExecRequest;
use crate::responses::{AwsPutGetStageInfo, PutGetExecResponse, PutGetStageInfo};

mod connection;
mod requests;
mod responses;
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

    #[error("Snowflake API error. Code: `{0}`. Message: `{1}`")]
    ApiError(String, String),

    #[error("Snowflake API empty response could mean that query wasn't executed correctly or API call was faulty")]
    EmptyResponse,

    #[error("No usable rowsets were included in the response")]
    BrokenResponse,

    #[error("Following feature is not implemented yet: {0}")]
    Unimplemented(String),

    #[error("Unexpected API response")]
    UnexpectedResponse,
}

/// Container for query result.
/// Arrow is returned by-default for all SELECT statements,
/// unless there is session configuration issue or it's a different statement type.
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
}

impl SnowflakeApi {
    /// Initialize object with password auth. Authentication happens on the first request.
    pub fn with_password_auth(
        account_identifier: &str,
        warehouse: Option<&str>,
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
        Ok(Self {
            connection: Arc::clone(&connection),
            session,
            account_identifier,
        })
    }

    /// Initialize object with private certificate auth. Authentication happens on the first request.
    pub fn with_certificate_auth(
        account_identifier: &str,
        warehouse: Option<&str>,
        database: Option<&str>,
        schema: Option<&str>,
        username: &str,
        role: Option<&str>,
        private_key_pem: &str,
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
        Ok(Self {
            connection: Arc::clone(&connection),
            session,
            account_identifier,
        })
    }

    /// Closes the current session, this is necessary to clean up temporary objects (tables, functions, etc)
    /// which are Snowflake session dependent.
    /// If another request is made the new session will be initiated.
    pub async fn close_session(&mut self) -> Result<(), SnowflakeApiError> {
        self.session.close().await?;
        Ok(())
    }

    /// Execute a single query against API.
    /// If statement is PUT, then file will be uploaded to the Snowflake-managed storage
    pub async fn exec(&self, sql: &str) -> Result<QueryResult, SnowflakeApiError> {
        let put_re = Regex::new(r"(?i)^(?:/\*.*\*/\s*)*put\s+").unwrap();

        // put commands go through a different flow and result is side-effect
        if put_re.is_match(sql) {
            log::info!("Detected PUT query");

            self.exec_put(sql).await.map(|()| QueryResult::Empty)
        } else {
            self.exec_arrow(sql).await
        }
    }

    async fn exec_put(&self, sql: &str) -> Result<(), SnowflakeApiError> {
        let resp = self
            .run_sql::<ExecResponse>(sql, QueryType::JsonQuery)
            .await?;
        log::debug!("Got PUT response: {:?}", resp);

        match resp {
            ExecResponse::Query(_) => Err(SnowflakeApiError::UnexpectedResponse),
            ExecResponse::PutGet(pg) => self.put(pg).await,
            ExecResponse::Error(e) => Err(SnowflakeApiError::ApiError(
                e.data.error_code,
                e.message.unwrap_or_default(),
            )),
        }
    }

    async fn put(&self, resp: PutGetExecResponse) -> Result<(), SnowflakeApiError> {
        match resp.data.stage_info {
            PutGetStageInfo::Aws(info) => self.put_to_s3(&resp.data.src_locations, info).await,
            PutGetStageInfo::Azure(_) => Err(SnowflakeApiError::Unimplemented(
                "PUT local file requests for Azure".to_string(),
            )),
            PutGetStageInfo::Gcs(_) => Err(SnowflakeApiError::Unimplemented(
                "PUT local file requests for GCS".to_string(),
            )),
        }
    }

    async fn put_to_s3(
        &self,
        src_locations: &[String],
        info: AwsPutGetStageInfo,
    ) -> Result<(), SnowflakeApiError> {
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
        for src_path in src_locations {
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
    pub async fn exec_response(&mut self, sql: &str) -> Result<ExecResponse, SnowflakeApiError> {
        self.run_sql::<ExecResponse>(sql, QueryType::ArrowQuery)
            .await
    }

    /// Useful for debugging to get raw JSON response
    #[cfg(debug_assertions)]
    pub async fn exec_json(&mut self, sql: &str) -> Result<serde_json::Value, SnowflakeApiError> {
        self.run_sql::<serde_json::Value>(sql, QueryType::JsonQuery)
            .await
    }

    async fn exec_arrow(&self, sql: &str) -> Result<QueryResult, SnowflakeApiError> {
        let resp = self
            .run_sql::<ExecResponse>(sql, QueryType::ArrowQuery)
            .await?;
        log::debug!("Got query response: {:?}", resp);

        let resp = match resp {
            // processable response
            ExecResponse::Query(qr) => qr,
            ExecResponse::PutGet(_) => return Err(SnowflakeApiError::UnexpectedResponse),
            ExecResponse::Error(e) => {
                return Err(SnowflakeApiError::ApiError(
                    e.data.error_code,
                    e.message.unwrap_or_default(),
                ))
            }
        };

        // if response was empty, base64 data is empty string
        // todo: still return empty arrow batch with proper schema? (schema always included)
        if resp.data.returned == 0 {
            log::debug!("Got response with 0 rows");
            Ok(QueryResult::Empty)
        } else if let Some(json) = resp.data.rowset {
            log::debug!("Got JSON response");
            // NOTE: json response could be chunked too. however, go clients should receive arrow by-default,
            // unless user sets session variable to return json. This case was added for debugging and status
            // information being passed through that fields.
            Ok(QueryResult::Json(json))
        } else if let Some(base64) = resp.data.rowset_base64 {
            // fixme: loads everything into memory
            let mut res = vec![];
            if !base64.is_empty() {
                log::debug!("Got base64 encoded response");
                let bytes = base64::engine::general_purpose::STANDARD.decode(base64)?;
                let fr = StreamReader::try_new_unbuffered(bytes.to_byte_slice(), None)?;
                for batch in fr {
                    res.push(batch?);
                }
            }
            let chunks = try_join_all(resp.data.chunks.iter().map(|chunk| {
                self.connection
                    .get_chunk(&chunk.url, &resp.data.chunk_headers)
            }))
            .await?;
            for bytes in chunks {
                let fr = StreamReader::try_new_unbuffered(&*bytes, None)?;
                for batch in fr {
                    res.push(batch?);
                }
            }

            Ok(QueryResult::Arrow(res))
        } else {
            Err(SnowflakeApiError::BrokenResponse)
        }
    }

    async fn run_sql<R: serde::de::DeserializeOwned>(
        &self,
        sql_text: &str,
        query_type: QueryType,
    ) -> Result<R, SnowflakeApiError> {
        log::debug!("Executing: {}", sql_text);

        let parts = self.session.get_token().await?;

        let body = ExecRequest {
            sql_text: sql_text.to_string(),
            async_exec: false,
            sequence_id: parts.sequence_id,
            is_internal: false,
        };

        let resp = self
            .connection
            .request::<R>(
                query_type,
                &self.account_identifier,
                &[],
                Some(&parts.session_token_auth_header),
                body,
            )
            .await?;

        Ok(resp)
    }
}
