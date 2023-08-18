use std::marker::PhantomData;

use reqwest::{Client, ClientBuilder, header};
use reqwest::header::{HeaderMap, HeaderValue};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

use crate::{AsyncRuntime, DefaultRuntime};

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error(transparent)]
    RequestError(#[from] reqwest::Error),

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
pub struct Connection<R = DefaultRuntime> {
    // no need for Arc as it's already inside the reqwest client
    client: Client,
    _runtime: PhantomData<R>,
}

impl<R> Connection<R>
    where
        R: AsyncRuntime {
    pub fn new() -> Result<Self, ConnectionError> {
        // use builder to fail safely, unlike client new
        let client = ClientBuilder::new()
            .user_agent("Rust/0.0.1")
            .referer(false)
            // fixme: disable later
            .connection_verbose(true)
            .build()?;

        Ok(Connection {
            client,
            _runtime: PhantomData,
        })
    }

    /// Perform request of given query type with extra body or parameters
    // todo: implement retry logic
    // todo: implement soft error handling
    // todo: is there better way to not repeat myself?
    pub async fn request<DS: serde::de::DeserializeOwned>(
        &self,
        query_type: QueryType,
        account_identifier: &str,
        extra_get_params: &[(&str, &str)],
        auth: Option<&str>,
        body: impl serde::Serialize,
    ) -> Result<DS, ConnectionError> {
        let context = query_type.query_context();

        // todo: increment subsequent request ids (on retry?)
        let request_id = Uuid::now_v1(&[0, 0, 0, 0, 0, 0]);
        let request_guid = Uuid::new_v4();
        let (client_start_time, _nanos) = request_id.get_timestamp().unwrap().to_unix();

        let client_start_time = client_start_time.to_string();
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

        Ok(resp.json::<DS>().await?)
    }
}
