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

use arrow::datatypes::ToByteSlice;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use base64::Engine;
use futures::future::try_join_all;
use object_store::aws::AmazonS3Builder;
use regex::Regex;
use reqwest_middleware::ClientWithMiddleware;
use std::io;
use thiserror::Error;

use crate::connection::{Connection, ConnectionError};
use crate::upload_files::{get_files, upload_files_parallel, upload_files_sequential};
use responses::ExecResponse;
use session::{AuthError, Session};

use crate::connection::QueryType;
use crate::requests::ExecRequest;
#[cfg(feature = "file")]
use crate::responses::{AwsPutGetStageInfo, PutGetExecResponse, PutGetStageInfo};
#[cfg(feature = "file")]
use std::sync::Arc;

pub mod connection;
mod requests;
mod responses;
mod session;
mod upload_files;

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
    #[cfg(feature = "file")]
    ObjectStoreError(#[from] object_store::Error),

    #[error(transparent)]
    #[cfg(feature = "file")]
    ObjectStorePathError(#[from] object_store::path::Error),

    #[error(transparent)]
    #[cfg(feature = "file")]
    TokioTaskJoinError(#[from] tokio::task::JoinError),

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

    #[error("Missing feature: {0}")]
    MissingFeature(String),
}

/// Container for query result.
/// Arrow is returned by-default for all SELECT statements,
/// unless there is session configuration issue or it's a different statement type.
pub enum QueryResult {
    Arrow(Vec<RecordBatch>),
    Json(serde_json::Value),
    Empty,
}

pub struct AuthArgs {
    pub account_identifier: String,
    pub warehouse: Option<String>,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub username: String,
    pub role: Option<String>,
    pub auth_type: AuthType,
}

pub enum AuthType {
    Password(PasswordArgs),
    Certificate(CertificateArgs),
}

pub struct PasswordArgs {
    pub password: String,
}

pub struct CertificateArgs {
    pub private_key_pem: String,
}

#[must_use]
pub struct SnowflakeApiBuilder {
    pub auth: AuthArgs,
    client: Option<ClientWithMiddleware>,
}

impl SnowflakeApiBuilder {
    pub fn new(auth: AuthArgs) -> Self {
        Self { auth, client: None }
    }

    pub fn with_client(mut self, client: ClientWithMiddleware) -> Self {
        self.client = Some(client);
        self
    }

    pub fn build(self) -> Result<SnowflakeApi, SnowflakeApiError> {
        let connection = match self.client {
            Some(client) => Arc::new(Connection::new_with_middware(client)),
            None => Arc::new(Connection::new()?),
        };

        let session = match self.auth.auth_type {
            AuthType::Password(args) => Session::password_auth(
                Arc::clone(&connection),
                &self.auth.account_identifier,
                self.auth.warehouse.as_deref(),
                self.auth.database.as_deref(),
                self.auth.schema.as_deref(),
                &self.auth.username,
                self.auth.role.as_deref(),
                &args.password,
            ),
            AuthType::Certificate(args) => Session::cert_auth(
                Arc::clone(&connection),
                &self.auth.account_identifier,
                self.auth.warehouse.as_deref(),
                self.auth.database.as_deref(),
                self.auth.schema.as_deref(),
                &self.auth.username,
                self.auth.role.as_deref(),
                &args.private_key_pem,
            ),
        };

        let account_identifier = self.auth.account_identifier.to_uppercase();

        Ok(SnowflakeApi::new(
            Arc::clone(&connection),
            session,
            account_identifier,
        ))
    }
}

/// Snowflake API, keeps connection pool and manages session for you
pub struct SnowflakeApi {
    connection: Arc<Connection>,
    session: Session,
    account_identifier: String,
    // These two fields are used for PUT requests
    // The defaults used can be found here:
    // https://docs.snowflake.com/en/sql-reference/sql/put
    max_parallel_uploads: usize,
    max_file_size_threshold: i64,
}

impl SnowflakeApi {
    /// Create a new `SnowflakeApi` object with an existing connection and session.
    pub fn new(connection: Arc<Connection>, session: Session, account_identifier: String) -> Self {
        Self {
            connection,
            session,
            account_identifier,
            max_parallel_uploads: 4,
            max_file_size_threshold: 64_000_000,
        }
    }
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
        Ok(Self::new(
            Arc::clone(&connection),
            session,
            account_identifier,
        ))
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
        Ok(Self::new(
            Arc::clone(&connection),
            session,
            account_identifier,
        ))
    }

    /// Set the maximum number of parallel uploads for PUT requests. The default is 4.
    pub fn set_max_parallel_uploads(&mut self, max_parallel_uploads: usize) {
        self.max_parallel_uploads = max_parallel_uploads;
    }

    /// Set the maximum file size threshold parallel PUT requests. The default is 64MB.
    pub fn set_file_size_threshold(&mut self, max_file_size_threshold: i64) {
        self.max_file_size_threshold = max_file_size_threshold;
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

            #[cfg(feature = "file")]
            return self.exec_put(sql).await.map(|()| QueryResult::Empty);
            #[cfg(not(feature = "file"))]
            return Err(SnowflakeApiError::MissingFeature("file".to_string()));
        }
        self.exec_arrow(sql).await
    }

    #[cfg(feature = "file")]
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

    #[cfg(feature = "file")]
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

    #[cfg(feature = "file")]
    async fn put_to_s3(
        &self,
        src_locations: &[String],
        info: AwsPutGetStageInfo,
    ) -> Result<(), SnowflakeApiError> {
        // These constants are based on the snowflake website
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

        let s3_arc = Arc::new(s3);

        let files = get_files(src_locations, self.max_file_size_threshold);
        let bucket_path = bucket_path.to_string();
        upload_files_parallel(
            files.small_files,
            &bucket_path,
            &s3_arc,
            self.max_parallel_uploads,
        )
        .await?;
        upload_files_sequential(files.large_files, &bucket_path, &s3_arc).await?;
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
