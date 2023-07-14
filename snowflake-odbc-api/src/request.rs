use reqwest::header;
use reqwest::header::{HeaderMap, HeaderValue};
use thiserror::Error;
use url::Url;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum RequestError {
    #[error(transparent)]
    RequestError(#[from] reqwest::Error),

    #[error(transparent)]
    UrlParseError(#[from] url::ParseError),

    #[error(transparent)]
    DeserializationError(#[from] serde_json::Error),

    #[error(transparent)]
    InvalidHeaderError(#[from] header::InvalidHeaderValue)
}

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

// todo: implement retry logic
// todo: implement soft error handling
pub async fn request<R: serde::de::DeserializeOwned>(
    query_type: QueryType,
    account_identifier: &str,
    extra_get_params: &[(&str, &str)],
    auth: Option<&str>,
    body: impl serde::Serialize,
) -> Result<R, RequestError> {
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
    headers.append(header::USER_AGENT, HeaderValue::from_static("Rust/0.0.1"));
    headers.append(header::ACCEPT, HeaderValue::from_static(context.accept_mime));
    if let Some(auth) = auth {
        headers.append(header::AUTHORIZATION, HeaderValue::from_str(auth)?);
    }

    // todo: persist client to use connection polling
    let client = reqwest::Client::new();
    let resp = client.post(url)
        .headers(headers)
        .json(&body)
        .send()
        .await?;

    Ok(resp.json::<R>().await?)
}
