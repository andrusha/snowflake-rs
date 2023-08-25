use reqwest::header;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest_middleware::ClientWithMiddleware;
use reqwest_retry::policies::ExponentialBackoff;
use reqwest_retry::RetryTransientMiddleware;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error(transparent)]
    RequestError(#[from] reqwest::Error),

    #[error(transparent)]
    RequestMiddlewareError(#[from] reqwest_middleware::Error),

    #[error(transparent)]
    UrlParsing(#[from] url::ParseError),

    #[error(transparent)]
    Deserialization(#[from] serde_json::Error),

    #[error(transparent)]
    InvalidHeader(#[from] header::InvalidHeaderValue),
}

/// Container for query parameters
/// This API has different endpoints and MIME types for different requests
struct QueryContext {
    path: &'static str,
    accept_mime: &'static str,
}

pub enum QueryType {
    Auth,
    JsonQuery,
    ArrowQuery,
}

impl QueryType {
    fn query_context(&self) -> QueryContext {
        match self {
            QueryType::Auth => QueryContext {
                path: "session/v1/login-request",
                accept_mime: "application/json",
            },
            QueryType::JsonQuery => QueryContext {
                path: "queries/v1/query-request",
                accept_mime: "application/json",
            },
            QueryType::ArrowQuery => QueryContext {
                path: "queries/v1/query-request",
                accept_mime: "application/snowflake",
            },
        }
    }
}

/// Connection pool
/// Minimal session will have at least 2 requests - login and query
pub struct Connection {
    // no need for Arc as it's already inside the reqwest client
    client: ClientWithMiddleware,
}

impl Connection {
    pub fn new() -> Result<Self, ConnectionError> {
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);

        // use builder to fail safely, unlike client new
        let client = reqwest::ClientBuilder::new()
            .user_agent("Rust/0.0.1")
            .referer(false);

        #[cfg(debug_assertions)]
        let client = client.connection_verbose(true);

        let client = client.build()?;

        let client = reqwest_middleware::ClientBuilder::new(client)
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        Ok(Connection { client })
    }

    /// Perform request of given query type with extra body or parameters
    // todo: implement soft error handling
    // todo: is there better way to not repeat myself?
    pub async fn request<R: serde::de::DeserializeOwned>(
        &self,
        query_type: QueryType,
        account_identifier: &str,
        extra_get_params: &[(&str, &str)],
        auth: Option<&str>,
        body: impl serde::Serialize,
    ) -> Result<R, ConnectionError> {
        let context = query_type.query_context();

        let request_id = Uuid::new_v4();
        let request_guid = Uuid::new_v4();
        let client_start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let client_start_time = client_start_time.to_string();

        // fixme: update uuid's on the retry
        let request_id = request_id.to_string();
        let request_guid = request_guid.to_string();

        let mut get_params = vec![
            ("clientStartTime", client_start_time.as_str()),
            ("requestId", request_id.as_str()),
            ("request_guid", request_guid.as_str()),
        ];
        get_params.extend_from_slice(extra_get_params);

        let url = format!(
            "https://{}.snowflakecomputing.com/{}",
            &account_identifier, context.path
        );
        let url = Url::parse_with_params(&url, get_params)?;

        let mut headers = HeaderMap::new();

        headers.append(
            header::ACCEPT,
            HeaderValue::from_static(context.accept_mime),
        );
        if let Some(auth) = auth {
            let mut auth_val = HeaderValue::from_str(auth)?;
            auth_val.set_sensitive(true);
            headers.append(header::AUTHORIZATION, auth_val);
        }

        // todo: persist client to use connection polling
        let resp = self
            .client
            .post(url)
            .headers(headers)
            .json(&body)
            .send()
            .await?;

        Ok(resp.json::<R>().await?)
    }
}
