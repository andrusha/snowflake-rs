#![doc(
    issue_tracker_base_url = "https://github.com/mycelial/snowflake-rs/issues",
    test(no_crate_inject)
)]
#![doc = include_str ! ("../README.md")]

use base64::Engine;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use time::{Duration, OffsetDateTime};

#[derive(Error, Debug)]
pub enum JwtError {
    #[error(transparent)]
    Rsa(#[from] rsa::Error),

    #[error(transparent)]
    Pkcs8(#[from] rsa::pkcs8::Error),

    #[error(transparent)]
    Spki(#[from] rsa::pkcs8::spki::Error),

    #[error(transparent)]
    Pkcs1(#[from] rsa::pkcs1::Error),

    #[error(transparent)]
    Utf8(#[from] std::string::FromUtf8Error),

    #[error(transparent)]
    Der(#[from] rsa::pkcs1::der::Error),

    #[error(transparent)]
    JwtEncoding(#[from] jsonwebtoken::errors::Error),
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

    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

pub fn generate_jwt_token(
    private_key_pem: &str,
    // Snowflake expects uppercase <account identifier>.<username>
    full_identifier: &str,
) -> Result<String, JwtError> {
    // Reading a private key:
    // rsa-2048.p8 -> public key -> der bytes -> hash
    let pkey = rsa::RsaPrivateKey::from_pkcs8_pem(private_key_pem)?;
    let pubk = pkey.to_public_key().to_public_key_der()?;
    let iss = format!(
        "{}.SHA256:{}",
        full_identifier,
        pubkey_fingerprint(pubk.as_bytes())
    );

    let iat = OffsetDateTime::now_utc();
    let exp = iat + Duration::days(1);

    let claims = Claims::new(iss, full_identifier.to_owned(), iat, exp);
    let ek = EncodingKey::from_rsa_der(pkey.to_pkcs1_der()?.as_bytes());

    let res = encode(&Header::new(Algorithm::RS256), &claims, &ek)?;
    Ok(res)
}
