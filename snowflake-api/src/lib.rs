#![doc(
    issue_tracker_base_url = "https://github.com/mycelial/snowflake-rs/issues",
    test(no_crate_inject)
)]
#![doc = include_str!("../README.md")]
#![warn(clippy::all, clippy::pedantic)]
#![allow(
clippy::must_use_candidate,
clippy::missing_errors_doc,
clippy::module_name_repetitions,
clippy::struct_field_names,
clippy::future_not_send, // This one seems like something we should eventually fix
clippy::missing_panics_doc
)]

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Decimal128Array, Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::error::ArrowError;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use base64::Engine;
use bytes::{Buf, Bytes};
use futures::future::try_join_all;
use futures::Stream;
use regex::Regex;
use reqwest_middleware::ClientWithMiddleware;
use thiserror::Error;

use responses::ExecResponse;
use session::{AuthError, Session};

use crate::connection::QueryType;
use crate::connection::{Connection, ConnectionError};
use crate::requests::ExecRequest;
use crate::responses::{ExecResponseRowType, SnowflakeType};
use crate::session::AuthError::MissingEnvArgument;

pub mod connection;
#[cfg(feature = "polars")]
mod polars;
mod put;
mod requests;
pub mod responses;
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

    #[error(transparent)]
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

    #[error(transparent)]
    GlobPatternError(#[from] glob::PatternError),

    #[error(transparent)]
    GlobError(#[from] glob::GlobError),
}

/// Even if Arrow is specified as a return type non-select queries
/// will return Json array of arrays: `[[42, "answer"], [43, "non-answer"]]`.
pub struct JsonResult {
    // todo: can it _only_ be a json array of arrays or something else too?
    pub value: serde_json::Value,
    /// Field ordering matches the array ordering
    pub schema: Vec<FieldSchema>,
}

impl Display for JsonResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.value)
    }
}

/// Based on the [`ExecResponseRowType`]
pub struct FieldSchema {
    pub name: String,
    // todo: is it a good idea to expose internal response struct to the user?
    pub type_: SnowflakeType,
    pub scale: Option<i64>,
    pub precision: Option<i64>,
    pub nullable: bool,
}

impl From<ExecResponseRowType> for FieldSchema {
    fn from(value: ExecResponseRowType) -> Self {
        FieldSchema {
            name: value.name,
            type_: value.type_,
            scale: value.scale,
            precision: value.precision,
            nullable: value.nullable,
        }
    }
}

/// Container for query result.
/// Arrow is returned by-default for all SELECT statements,
/// unless there is session configuration issue or it's a different statement type.
pub enum QueryResult {
    Arrow(Vec<RecordBatch>),
    Json(JsonResult),
    Empty,
}

/// Raw query result
/// Can be transformed into [`QueryResult`]
pub enum RawQueryResult {
    /// Arrow IPC chunks
    /// see: <https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc>
    Bytes(Vec<Bytes>),
    /// Arrow IPC chunks with Snowflake schema metadata for decimal conversion
    BytesWithSchema(Vec<Bytes>, Vec<FieldSchema>),
    Stream(
        Pin<Box<dyn Stream<Item = std::result::Result<Bytes, reqwest::Error>> + std::marker::Send>>,
    ),
    /// Json payload is deserialized,
    /// as it's already a part of REST response
    Json(JsonResult),
    Empty,
}

impl RawQueryResult {
    pub fn deserialize_arrow(self) -> Result<QueryResult, ArrowError> {
        match self {
            RawQueryResult::Bytes(bytes) => {
                Self::flat_bytes_to_batches(bytes).map(QueryResult::Arrow)
            }
            RawQueryResult::BytesWithSchema(bytes, schema) => {
                Self::flat_bytes_to_batches_with_schema(bytes, schema).map(QueryResult::Arrow)
            }
            RawQueryResult::Stream(_) => unimplemented!(),
            RawQueryResult::Json(j) => Ok(QueryResult::Json(j)),
            RawQueryResult::Empty => Ok(QueryResult::Empty),
        }
    }

    pub fn flat_bytes_to_batches(bytes: Vec<Bytes>) -> Result<Vec<RecordBatch>, ArrowError> {
        let mut res = vec![];
        for b in bytes {
            let mut batches = Self::bytes_to_batches(b)?;
            res.append(&mut batches);
        }
        Ok(res)
    }

    pub fn flat_bytes_to_batches_with_schema(
        bytes: Vec<Bytes>,
        schema: Vec<FieldSchema>,
    ) -> Result<Vec<RecordBatch>, ArrowError> {
        let mut res = vec![];
        for b in bytes {
            let batches = Self::bytes_to_batches(b)?;
            for batch in batches {
                let converted_batch = Self::convert_decimal_columns(batch, &schema)?;
                res.push(converted_batch);
            }
        }
        Ok(res)
    }

    fn bytes_to_batches(bytes: Bytes) -> Result<Vec<RecordBatch>, ArrowError> {
        let record_batches = StreamReader::try_new(bytes.reader(), None)?;
        record_batches.into_iter().collect()
    }

    /// Convert integer columns to decimal columns based on Snowflake schema metadata
    fn convert_decimal_columns(
        batch: RecordBatch,
        schema: &[FieldSchema],
    ) -> Result<RecordBatch, ArrowError> {
        let original_schema = batch.schema();
        let mut new_fields: Vec<Arc<Field>> = Vec::new();
        let mut new_columns = Vec::new();

        // Create a mapping of field names to their Snowflake schema info
        let schema_map: HashMap<&String, &FieldSchema> =
            schema.iter().map(|field| (&field.name, field)).collect();

        for (i, field) in original_schema.fields().iter().enumerate() {
            let column = batch.column(i);

            if let Some(snowflake_field) = schema_map.get(field.name()) {
                if matches!(snowflake_field.type_, SnowflakeType::Fixed) {
                    if let (Some(precision), Some(scale)) =
                        (snowflake_field.precision, snowflake_field.scale)
                    {
                        // Convert integer column to decimal
                        let decimal_column =
                            Self::convert_integer_to_decimal(column, precision as u8, scale as i8)?;
                        let decimal_field = Arc::new(Field::new(
                            field.name(),
                            DataType::Decimal128(precision as u8, scale as i8),
                            field.is_nullable(),
                        ));
                        new_fields.push(decimal_field);
                        new_columns.push(decimal_column);
                        continue;
                    }
                }
            }

            // Keep the original column if no conversion needed
            new_fields.push(Arc::clone(field));
            new_columns.push(Arc::clone(column));
        }

        let new_schema = Arc::new(ArrowSchema::new(new_fields));
        RecordBatch::try_new(new_schema, new_columns)
    }

    /// Convert an integer array to a decimal array with the given precision and scale
    fn convert_integer_to_decimal(
        array: &ArrayRef,
        precision: u8,
        scale: i8,
    ) -> Result<ArrayRef, ArrowError> {
        match array.data_type() {
            DataType::Int32 => {
                let int_array = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                    ArrowError::CastError("Failed to downcast to Int32Array".to_string())
                })?;

                let decimal_values: Result<Vec<Option<i128>>, ArrowError> = int_array
                    .iter()
                    .map(|opt_val| opt_val.map(|val| val as i128).map(Ok).transpose())
                    .collect();

                let decimal_array = Decimal128Array::from(decimal_values?)
                    .with_precision_and_scale(precision, scale)?;
                Ok(Arc::new(decimal_array))
            }
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    ArrowError::CastError("Failed to downcast to Int64Array".to_string())
                })?;

                let decimal_values: Result<Vec<Option<i128>>, ArrowError> = int_array
                    .iter()
                    .map(|opt_val| opt_val.map(|val| val as i128).map(Ok).transpose())
                    .collect();

                let decimal_array = Decimal128Array::from(decimal_values?)
                    .with_precision_and_scale(precision, scale)?;
                Ok(Arc::new(decimal_array))
            }
            _ => {
                // For non-integer types, try to keep the original array
                Ok(Arc::clone(array))
            }
        }
    }
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

impl AuthArgs {
    pub fn from_env() -> Result<AuthArgs, SnowflakeApiError> {
        let auth_type = if let Ok(password) = std::env::var("SNOWFLAKE_PASSWORD") {
            Ok(AuthType::Password(PasswordArgs { password }))
        } else if let Ok(private_key_pem) = std::env::var("SNOWFLAKE_PRIVATE_KEY") {
            Ok(AuthType::Certificate(CertificateArgs { private_key_pem }))
        } else {
            Err(MissingEnvArgument(
                "SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY".to_owned(),
            ))
        };

        Ok(AuthArgs {
            account_identifier: std::env::var("SNOWFLAKE_ACCOUNT")
                .map_err(|_| MissingEnvArgument("SNOWFLAKE_ACCOUNT".to_owned()))?,
            warehouse: std::env::var("SNOWLFLAKE_WAREHOUSE").ok(),
            database: std::env::var("SNOWFLAKE_DATABASE").ok(),
            schema: std::env::var("SNOWFLAKE_SCHEMA").ok(),
            username: std::env::var("SNOWFLAKE_USER")
                .map_err(|_| MissingEnvArgument("SNOWFLAKE_USER".to_owned()))?,
            role: std::env::var("SNOWFLAKE_ROLE").ok(),
            auth_type: auth_type?,
        })
    }
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
    host: Option<String>,
}

impl SnowflakeApiBuilder {
    pub fn new(auth: AuthArgs) -> Self {
        Self {
            auth,
            client: None,
            host: None,
        }
    }

    pub fn with_client(mut self, client: ClientWithMiddleware) -> Self {
        self.client = Some(client);
        self
    }

    pub fn with_host(mut self, uri: &str) -> Self {
        self.host = Some(uri.to_string());
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
                self.host.as_deref(),
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
                self.host.as_deref(),
            ),
        };

        let account_identifier = self.auth.account_identifier.to_uppercase();

        Ok(
            SnowflakeApi::new(Arc::clone(&connection), session, account_identifier)
                .with_host(self.host),
        )
    }
}

/// Snowflake API, keeps connection pool and manages session for you
pub struct SnowflakeApi {
    connection: Arc<Connection>,
    session: Session,
    account_identifier: String,
    host: Option<String>,
}

impl SnowflakeApi {
    /// Create a new `SnowflakeApi` object with an existing connection and session.
    pub fn new(connection: Arc<Connection>, session: Session, account_identifier: String) -> Self {
        Self {
            connection,
            session,
            account_identifier,
            host: None,
        }
    }

    pub fn with_host(mut self, host: Option<String>) -> Self {
        self.host = host.to_owned();
        self.session = self.session.with_host(host);
        self
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
            None,
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
            None,
        );

        let account_identifier = account_identifier.to_uppercase();
        Ok(Self::new(
            Arc::clone(&connection),
            session,
            account_identifier,
        ))
    }

    pub fn from_env() -> Result<Self, SnowflakeApiError> {
        SnowflakeApiBuilder::new(AuthArgs::from_env()?).build()
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
        let raw = self.exec_raw(sql, false).await?;
        let res = raw.deserialize_arrow()?;
        Ok(res)
    }

    /// Executes a single query against API.
    /// If statement is PUT, then file will be uploaded to the Snowflake-managed storage
    /// Returns raw bytes in the Arrow response
    pub async fn exec_raw(
        &self,
        sql: &str,
        stream: bool,
    ) -> Result<RawQueryResult, SnowflakeApiError> {
        let put_re = Regex::new(r"(?i)^(?:/\*.*\*/\s*)*put\s+").unwrap();

        // put commands go through a different flow and result is side-effect
        if put_re.is_match(sql) {
            log::info!("Detected PUT query");
            self.exec_put(sql).await.map(|()| RawQueryResult::Empty)
        } else {
            self.exec_arrow_raw(sql, stream).await
        }
    }

    async fn exec_put(&self, sql: &str) -> Result<(), SnowflakeApiError> {
        let resp = self
            .run_sql::<ExecResponse>(sql, QueryType::JsonQuery)
            .await?;
        log::debug!("Got PUT response: {:?}", resp);

        match resp {
            ExecResponse::Query(_) => Err(SnowflakeApiError::UnexpectedResponse),
            ExecResponse::PutGet(pg) => put::put(pg).await,
            ExecResponse::Error(e) => Err(SnowflakeApiError::ApiError(
                e.data.error_code,
                e.message.unwrap_or_default(),
            )),
        }
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

    async fn exec_arrow_raw(
        &self,
        sql: &str,
        stream: bool,
    ) -> Result<RawQueryResult, SnowflakeApiError> {
        if stream {
            let bytes_stream = self.run_sql_stream(sql, QueryType::ArrowQuery).await?;
            return Ok(RawQueryResult::Stream(Box::pin(bytes_stream)));
        }

        let resp = self
            .run_sql::<ExecResponse>(sql, QueryType::ArrowQuery)
            .await?;

        self.parse_arrow_raw_response(resp).await
    }

    pub async fn parse_arrow_raw_response(
        &self,
        resp: ExecResponse,
    ) -> Result<RawQueryResult, SnowflakeApiError> {
        let resp = match resp {
            // processable response
            ExecResponse::Query(qr) => Ok(qr),
            ExecResponse::PutGet(_) => Err(SnowflakeApiError::UnexpectedResponse),
            ExecResponse::Error(e) => Err(SnowflakeApiError::ApiError(
                e.data.error_code,
                e.message.unwrap_or_default(),
            )),
        }?;

        // if response was empty, base64 data is empty string
        // todo: still return empty arrow batch with proper schema? (schema always included)
        if resp.data.returned == 0 {
            log::debug!("Got response with 0 rows");
            Ok(RawQueryResult::Empty)
        } else if let Some(value) = resp.data.rowset {
            log::debug!("Got JSON response");
            // NOTE: json response could be chunked too. however, go clients should receive arrow by-default,
            // unless user sets session variable to return json. This case was added for debugging and status
            // information being passed through that fields.
            Ok(RawQueryResult::Json(JsonResult {
                value,
                schema: resp.data.rowtype.into_iter().map(Into::into).collect(),
            }))
        } else if let Some(base64) = resp.data.rowset_base64 {
            // fixme: is it possible to give streaming interface?
            let mut chunks = try_join_all(resp.data.chunks.iter().map(|chunk| {
                self.connection
                    .get_chunk(&chunk.url, &resp.data.chunk_headers)
            }))
            .await?;

            // fixme: should base64 chunk go first?
            // fixme: if response is chunked is it both base64 + chunks or just chunks?
            if !base64.is_empty() {
                log::debug!("Got base64 encoded response");
                let bytes = Bytes::from(base64::engine::general_purpose::STANDARD.decode(base64)?);
                chunks.push(bytes);
            }

            // Include schema information for decimal conversion
            let schema: Vec<FieldSchema> = resp.data.rowtype.into_iter().map(Into::into).collect();
            Ok(RawQueryResult::BytesWithSchema(chunks, schema))
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
                self.host.as_deref(),
            )
            .await?;

        Ok(resp)
    }

    async fn run_sql_stream(
        &self,
        sql_text: &str,
        query_type: QueryType,
    ) -> Result<impl Stream<Item = std::result::Result<Bytes, reqwest::Error>>, SnowflakeApiError>
    {
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
            .send_request(
                query_type,
                &self.account_identifier,
                &[],
                Some(&parts.session_token_auth_header),
                body,
                self.host.as_deref(),
            )
            .await?;

        Ok(resp.bytes_stream())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_convert_int32_to_decimal() {
        let int_array = Int32Array::from(vec![Some(12345), Some(67890), None, Some(-9876)]);
        let array_ref: ArrayRef = Arc::new(int_array);

        let result = RawQueryResult::convert_integer_to_decimal(&array_ref, 10, 2).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(10, 2));

        let decimal_array = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(decimal_array.len(), 4);
        assert_eq!(decimal_array.value(0), 12345);
        assert_eq!(decimal_array.value(1), 67890);
        assert!(decimal_array.is_null(2));
        assert_eq!(decimal_array.value(3), -9876);
        assert_eq!(decimal_array.precision(), 10);
        assert_eq!(decimal_array.scale(), 2);
    }

    #[test]
    fn test_convert_int64_to_decimal() {
        let int_array = Int64Array::from(vec![Some(1234567890), Some(-987654321), None]);
        let array_ref: ArrayRef = Arc::new(int_array);

        let result = RawQueryResult::convert_integer_to_decimal(&array_ref, 15, 4).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(15, 4));

        let decimal_array = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(decimal_array.len(), 3);
        assert_eq!(decimal_array.value(0), 1234567890);
        assert_eq!(decimal_array.value(1), -987654321);
        assert!(decimal_array.is_null(2));
        assert_eq!(decimal_array.precision(), 15);
        assert_eq!(decimal_array.scale(), 4);
    }

    #[test]
    fn test_convert_decimal_columns() {
        let int32_array = Int32Array::from(vec![Some(12345), Some(67890)]);
        let int64_array = Int64Array::from(vec![Some(1000), Some(2000)]);
        let text_array = arrow::array::StringArray::from(vec![Some("test1"), Some("test2")]);
        let schema = ArrowSchema::new(vec![
            Field::new("price", DataType::Int32, true),
            Field::new("quantity", DataType::Int64, true),
            Field::new("description", DataType::Utf8, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(int32_array),
                Arc::new(int64_array),
                Arc::new(text_array),
            ],
        )
        .unwrap();

        let snowflake_schema = vec![
            FieldSchema {
                name: "price".to_string(),
                type_: SnowflakeType::Fixed,
                precision: Some(10),
                scale: Some(2),
                nullable: true,
            },
            FieldSchema {
                name: "quantity".to_string(),
                type_: SnowflakeType::Fixed,
                precision: Some(8),
                scale: Some(0),
                nullable: true,
            },
            FieldSchema {
                name: "description".to_string(),
                type_: SnowflakeType::Text,
                precision: None,
                scale: None,
                nullable: true,
            },
        ];

        let result_batch =
            RawQueryResult::convert_decimal_columns(batch, &snowflake_schema).unwrap();
        let result_schema = result_batch.schema();

        assert_eq!(result_schema.fields().len(), 3);
        assert_eq!(
            result_schema.field(0).data_type(),
            &DataType::Decimal128(10, 2)
        );
        assert_eq!(result_schema.field(0).name(), "price");
        assert_eq!(
            result_schema.field(1).data_type(),
            &DataType::Decimal128(8, 0)
        );
        assert_eq!(result_schema.field(1).name(), "quantity");
        assert_eq!(result_schema.field(2).data_type(), &DataType::Utf8);
        assert_eq!(result_schema.field(2).name(), "description");
        assert_eq!(result_batch.num_rows(), 2);
        assert_eq!(result_batch.num_columns(), 3);

        let price_column = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(price_column.value(0), 12345); // Raw value preserved
        assert_eq!(price_column.value(1), 67890);
        assert_eq!(price_column.precision(), 10);
        assert_eq!(price_column.scale(), 2);

        let quantity_column = result_batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(quantity_column.value(0), 1000);
        assert_eq!(quantity_column.value(1), 2000);
        assert_eq!(quantity_column.precision(), 8);
        assert_eq!(quantity_column.scale(), 0);

        let desc_column = result_batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(desc_column.value(0), "test1");
        assert_eq!(desc_column.value(1), "test2");
    }

    #[test]
    fn test_non_fixed_types_unchanged() {
        let int_array = Int32Array::from(vec![Some(123), Some(456)]);
        let schema = ArrowSchema::new(vec![Field::new("count", DataType::Int32, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(int_array)]).unwrap();
        let snowflake_schema = vec![FieldSchema {
            name: "count".to_string(),
            type_: SnowflakeType::Real, // Not Fixed type
            precision: Some(10),
            scale: Some(2),
            nullable: true,
        }];
        let result_batch =
            RawQueryResult::convert_decimal_columns(batch, &snowflake_schema).unwrap();

        assert_eq!(result_batch.schema().field(0).data_type(), &DataType::Int32);
    }

    #[test]
    fn test_missing_precision_or_scale() {
        let int_array = Int32Array::from(vec![Some(123), Some(456)]);
        let schema = ArrowSchema::new(vec![Field::new("value", DataType::Int32, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(int_array)]).unwrap();
        let snowflake_schema = vec![FieldSchema {
            name: "value".to_string(),
            type_: SnowflakeType::Fixed,
            precision: None, // Missing precision
            scale: Some(2),
            nullable: true,
        }];
        let result_batch =
            RawQueryResult::convert_decimal_columns(batch, &snowflake_schema).unwrap();

        assert_eq!(result_batch.schema().field(0).data_type(), &DataType::Int32);
    }

    #[test]
    fn test_bytes_with_schema_deserialization() {
        let int_array = Int32Array::from(vec![Some(12345), Some(67890)]);
        let schema = ArrowSchema::new(vec![Field::new("price", DataType::Int32, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(int_array)]).unwrap();
        let mut buffer = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        let bytes = Bytes::from(buffer);
        let snowflake_schema = vec![FieldSchema {
            name: "price".to_string(),
            type_: SnowflakeType::Fixed,
            precision: Some(10),
            scale: Some(2),
            nullable: true,
        }];

        let raw_result = RawQueryResult::BytesWithSchema(vec![bytes], snowflake_schema);
        let query_result = raw_result.deserialize_arrow().unwrap();

        match query_result {
            QueryResult::Arrow(batches) => {
                assert_eq!(batches.len(), 1);
                let result_batch = &batches[0];

                assert_eq!(
                    result_batch.schema().field(0).data_type(),
                    &DataType::Decimal128(10, 2)
                );

                let decimal_column = result_batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap();
                assert_eq!(decimal_column.value(0), 12345);
                assert_eq!(decimal_column.value(1), 67890);
                assert_eq!(decimal_column.precision(), 10);
                assert_eq!(decimal_column.scale(), 2);
            }
            _ => panic!("Expected Arrow result"),
        }
    }

    #[test]
    fn test_various_precision_scale_combinations() {
        let test_cases = vec![
            (5, 0),   // Integer-like: 12345
            (5, 2),   // Currency-like: 123.45
            (10, 4),  // High precision: 123456.7890
            (38, 10), // Maximum precision with high scale
        ];

        for (precision, scale) in test_cases {
            let int_array = Int32Array::from(vec![Some(1234567890)]);
            let array_ref: ArrayRef = Arc::new(int_array);
            let result =
                RawQueryResult::convert_integer_to_decimal(&array_ref, precision, scale).unwrap();

            assert_eq!(result.data_type(), &DataType::Decimal128(precision, scale));

            let decimal_array = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(decimal_array.precision(), precision);
            assert_eq!(decimal_array.scale(), scale);
            assert_eq!(decimal_array.value(0), 1234567890);
        }
    }
}
