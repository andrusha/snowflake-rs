use base64::Engine;
use jsonwebtoken::{Algorithm, encode, EncodingKey, Header};
use openssl;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::{Duration, OffsetDateTime};

use crate::JwtError::JwtEncodingError;

#[derive(Error, Debug)]
pub enum JwtError {
    #[error(transparent)]
    OpenSslError(#[from] openssl::error::ErrorStack),

    #[error("unable to encode JWT: `{0}`")]
    JwtEncodingError(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    #[serde(with = "jwt_numeric_date")]
    iat: OffsetDateTime,
    #[serde(with = "jwt_numeric_date")]
    exp: OffsetDateTime,
}

impl Claims {
    /// If a token should always be equal to its representation after serializing and deserializing
    /// again, this function must be used for construction. `OffsetDateTime` contains a microsecond
    /// field but JWT timestamps are defined as UNIX timestamps (seconds). This function normalizes
    /// the timestamps.
    pub fn new(iss: String, sub: String, iat: OffsetDateTime, exp: OffsetDateTime) -> Self {
        // normalize the timestamps by stripping of microseconds
        let iat = iat
            .date()
            .with_hms_milli(iat.hour(), iat.minute(), iat.second(), 0)
            .unwrap()
            .assume_utc();
        let exp = exp
            .date()
            .with_hms_milli(exp.hour(), exp.minute(), exp.second(), 0)
            .unwrap()
            .assume_utc();

        Self { iss, sub, iat, exp }
    }
}

mod jwt_numeric_date {
    //! Custom serialization of OffsetDateTime to conform with the JWT spec (RFC 7519 section 2, "Numeric Date")
    use serde::{self, Deserialize, Deserializer, Serializer};
    use time::OffsetDateTime;

    /// Serializes an OffsetDateTime to a Unix timestamp (milliseconds since 1970/1/1T00:00:00T)
    pub fn serialize<S>(date: &OffsetDateTime, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        let timestamp = date.unix_timestamp();
        serializer.serialize_i64(timestamp)
    }

    /// Attempts to deserialize an i64 and use as a Unix timestamp
    pub fn deserialize<'de, D>(deserializer: D) -> Result<OffsetDateTime, D::Error>
        where
            D: Deserializer<'de>,
    {
        OffsetDateTime::from_unix_timestamp(i64::deserialize(deserializer)?)
            .map_err(|_| serde::de::Error::custom("invalid Unix timestamp value"))
    }
}

fn pubkey_fingerprint(pubkey: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(pubkey);

    base64::engine::general_purpose::STANDARD.encode(&hasher.finalize())
}

pub fn generate_jwt_token<T: AsRef<[u8]>>(
    private_key_pem: T,
    // Snowflake expects uppercase <account identifier>.<username>
    full_identifier: &str,
) -> Result<String, JwtError> {
    // Reading a private key:
    // rsa-2048.p8 -> public key -> der bytes -> hash
    let privk = openssl::rsa::Rsa::private_key_from_pem(private_key_pem.as_ref())?;
    let pubk = privk.public_key_to_der()?;

    let iss = format!(
        "{}.SHA256:{}",
        full_identifier,
        pubkey_fingerprint(&pubk)
    );

    let iat = OffsetDateTime::now_utc();
    let exp = iat + Duration::days(1);

    let claims = Claims::new(iss, full_identifier.to_owned(), iat, exp);
    let ek = EncodingKey::from_rsa_der(&privk.private_key_to_der()?);

    encode(&Header::new(Algorithm::RS256), &claims, &ek).map_err(|e| JwtEncodingError(e.to_string())).into()
}
