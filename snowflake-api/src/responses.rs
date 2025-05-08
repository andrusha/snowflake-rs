use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ExecResponse {
    Query(QueryExecResponse),
    PutGet(PutGetExecResponse),
    Error(ExecErrorResponse),
}

impl ExecResponse {
    pub fn clone_as_meta(&self) -> Self {
        match self {
            ExecResponse::Query(q) => {
                ExecResponse::Query(QueryExecResponse {
                    code: q.code.clone(),
                    message: q.message.clone(),
                    success: q.success,
                    data: q.data.clone_as_meta(),
                })
            },
            ExecResponse::PutGet(g) => ExecResponse::PutGet(g.clone()),
            ExecResponse::Error(e) => ExecResponse::Error(e.clone()),
        }
    }
}

// todo: add close session response, which should be just empty?
// FIXME: dead_code
#[allow(clippy::large_enum_variant, dead_code)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AuthResponse {
    Login(LoginResponse),
    Auth(AuthenticatorResponse),
    Renew(RenewSessionResponse),
    Close(CloseSessionResponse),
    Error(AuthErrorResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseRestResponse<D> {
    // null for auth
    pub code: Option<String>,
    pub message: Option<String>,
    pub success: bool,
    pub data: D,
}

pub type PutGetExecResponse = BaseRestResponse<PutGetResponseData>;
pub type QueryExecResponse = BaseRestResponse<QueryExecResponseData>;
pub type ExecErrorResponse = BaseRestResponse<ExecErrorResponseData>;
pub type AuthErrorResponse = BaseRestResponse<AuthErrorResponseData>;
pub type AuthenticatorResponse = BaseRestResponse<AuthenticatorResponseData>;
pub type LoginResponse = BaseRestResponse<LoginResponseData>;
pub type RenewSessionResponse = BaseRestResponse<RenewSessionResponseData>;
// Data should be always `null` on successful close session response
pub type CloseSessionResponse = BaseRestResponse<Option<()>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecErrorResponseData {
    pub age: i64,
    pub error_code: String,
    pub internal_error: bool,

    // come when query is invalid
    pub line: Option<i64>,
    pub pos: Option<i64>,

    // fixme: only valid for exec query response error? present in any exec query response?
    pub query_id: String,
    pub sql_state: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
// FIXME: dead_code
#[allow(dead_code)]
pub struct AuthErrorResponseData {
    pub authn_method: Option<String>,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NameValueParameter {
    pub name: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
// FIXME
#[allow(dead_code)]
pub struct LoginResponseData {
    pub session_id: i64,
    pub token: String,
    pub master_token: String,
    pub server_version: String,
    #[serde(default)]
    pub parameters: Vec<NameValueParameter>,
    pub session_info: SessionInfo,
    pub master_validity_in_seconds: i64,
    pub validity_in_seconds: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
// FIXME: dead_code
#[allow(dead_code)]
pub struct SessionInfo {
    pub database_name: Option<String>,
    pub schema_name: Option<String>,
    pub warehouse_name: Option<String>,
    pub role_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
// FIXME: dead_code
#[allow(dead_code)]
pub struct AuthenticatorResponseData {
    pub token_url: String,
    pub sso_url: String,
    pub proof_key: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
// FIXME: dead_code
#[allow(dead_code)]
pub struct RenewSessionResponseData {
    pub session_token: String,
    pub validity_in_seconds_s_t: i64,
    pub master_token: String,
    pub validity_in_seconds_m_t: i64,
    pub session_id: i64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryExecResponseData {
    pub parameters: Vec<NameValueParameter>,
    pub rowtype: Vec<ExecResponseRowType>,
    // default for non-SELECT queries
    // GET / PUT has their own response format
    #[serde(skip_serializing)]
    pub rowset: Option<serde_json::Value>,
    // only exists when binary response is given, eg Arrow
    // default for all SELECT queries
    // is base64-encoded Arrow IPC payload
    #[serde(skip_serializing)]
    pub rowset_base64: Option<String>,
    pub total: i64,
    pub returned: i64,    // unused in .NET
    pub query_id: String, // unused in .NET
    pub database_provider: Option<String>,
    pub final_database_name: Option<String>, // unused in .NET
    pub final_schema_name: Option<String>,
    pub final_warehouse_name: Option<String>, // unused in .NET
    pub final_role_name: String,              // unused in .NET
    // only present on SELECT queries
    pub number_of_binds: Option<i32>, // unused in .NET
    // todo: deserialize into enum
    pub statement_type_id: i64,
    pub version: i64,
    // if response is chunked
    #[serde(default)]
    #[serde(skip_serializing)]
    // soft-default to empty Vec if not present
    pub chunks: Vec<ExecResponseChunk>,
    // x-amz-server-side-encryption-customer-key, when chunks are present for download
    pub qrmk: Option<String>,
    #[serde(default)] // chunks are present
    pub chunk_headers: HashMap<String, String>,
    // when async query is run (ping pong request?)
    pub get_result_url: Option<String>,
    // multi-statement response, comma-separated
    pub result_ids: Option<String>,
    // `progressDesc`, and `queryAbortAfterSecs` are not used but exist in .NET
    // `sendResultTime`, `queryResultFormat`, `queryContext` also exist
}

impl QueryExecResponseData {
    /// A helper function to make a metadata version that doesn't include underlying data
    /// 
    /// This is used as the `meta` field on [super::QueryResult] and [super::RawQueryResult]
    pub fn clone_as_meta(&self) -> Self {
        Self {
            parameters: self.parameters.clone(),
            rowtype: self.rowtype.clone(),
            // skip actual data
            rowset: None,
            // skip actual data
            rowset_base64: None,
            total: self.total,
            returned: self.returned,
            query_id: self.query_id.clone(),
            database_provider: self.database_provider.clone(),
            final_database_name: self.final_database_name.clone(),
            final_schema_name: self.final_schema_name.clone(),
            final_warehouse_name: self.final_warehouse_name.clone(),
            final_role_name: self.final_role_name.clone(),
            number_of_binds: self.number_of_binds.clone(),
            statement_type_id: self.statement_type_id,
            version: self.version,
            // these are just links, so we'll keep them
            chunks: self.chunks.clone(),
            qrmk: self.qrmk.clone(),
            chunk_headers: self.chunk_headers.clone(),
            get_result_url: self.get_result_url.clone(),
            result_ids: self.result_ids.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecResponseRowType {
    pub name: String,
    #[serde(rename = "byteLength")]
    pub byte_length: Option<i64>,
    // unused in .NET
    pub length: Option<i64>,
    #[serde(rename = "type")]
    pub type_: SnowflakeType,
    pub scale: Option<i64>,
    pub precision: Option<i64>,
    pub nullable: bool,
}

// fixme: is it good idea to keep this as an enum if more types could be added in future?
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnowflakeType {
    Fixed,
    Real,
    Text,
    Date,
    Variant,
    TimestampLtz,
    TimestampNtz,
    TimestampTz,
    Object,
    Binary,
    Time,
    Boolean,
    Array,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecResponseChunk {
    pub url: String,
    pub row_count: i32,
    pub uncompressed_size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutGetResponseData {
    // `kind`, `operation` are present in Go implementation, but not in .NET
    pub command: CommandType,
    pub local_location: Option<String>,
    // inconsistent case naming
    #[serde(rename = "src_locations", default)]
    pub src_locations: Vec<String>,
    // file upload parallelism
    pub parallel: usize, // fixme: originally i32, handle this in parsing somehow?
    // file size threshold, small ones are should be uploaded with given parallelism
    pub threshold: i64,
    // doesn't need compression if source is already compressed
    pub auto_compress: bool,
    pub overwrite: bool,
    // maps to one of the predefined compression algos
    // todo: support different compression formats?
    pub source_compression: String,
    pub stage_info: PutGetStageInfo,
    pub encryption_material: EncryptionMaterialVariant,
    // GCS specific. If you request multiple files?
    #[serde(default)]
    pub presigned_urls: Vec<String>,
    #[serde(default)]
    pub parameters: Vec<NameValueParameter>,
    pub statement_type_id: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum CommandType {
    Upload,
    Download,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PutGetStageInfo {
    Aws(AwsPutGetStageInfo),
    Azure(AzurePutGetStageInfo),
    Gcs(GcsPutGetStageInfo),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AwsPutGetStageInfo {
    pub location_type: String,
    pub location: String,
    pub region: String,
    pub creds: AwsCredentials,
    // FIPS endpoint
    pub end_point: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct AwsCredentials {
    pub aws_key_id: String,
    pub aws_secret_key: String,
    pub aws_token: String,
    pub aws_id: String,
    pub aws_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcsPutGetStageInfo {
    pub location_type: String,
    pub location: String,
    pub storage_account: String,
    pub creds: GcsCredentials,
    pub presigned_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct GcsCredentials {
    pub gcs_access_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AzurePutGetStageInfo {
    pub location_type: String,
    pub location: String,
    pub storage_account: String,
    pub creds: AzureCredentials,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub struct AzureCredentials {
    pub azure_sas_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EncryptionMaterialVariant {
    Single(PutGetEncryptionMaterial),
    Multiple(Vec<PutGetEncryptionMaterial>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutGetEncryptionMaterial {
    // base64 encoded
    pub query_stage_master_key: String,
    pub query_id: String,
    pub smk_id: i64,
}
