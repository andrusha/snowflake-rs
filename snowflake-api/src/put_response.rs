use crate::error_response::ErrorResponse;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum PutResponse {
    S3(S3PutResponse),
    Error(ErrorResponse),
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
    pub parallel: u64,
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
