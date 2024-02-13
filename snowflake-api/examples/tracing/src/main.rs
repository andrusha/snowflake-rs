use anyhow::Result;
use arrow::util::pretty::pretty_format_batches;
use opentelemetry::global;
use opentelemetry_otlp::WithExportConfig;

use snowflake_api::connection::Connection;
use snowflake_api::{AuthArgs, AuthType, PasswordArgs, QueryResult, SnowflakeApiBuilder};
use tracing_subscriber::layer::SubscriberExt;

use reqwest_middleware::Extension;
use reqwest_tracing::{OtelName, SpanBackendWithUrl};

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("OTEL_SERVICE_NAME", "snowflake-rust-client-demo");

    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint("http://localhost:4319");

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer.clone());
    let subscriber = tracing_subscriber::Registry::default().with(telemetry);
    tracing::subscriber::set_global_default(subscriber)?;

    dotenv::dotenv().ok();

    let auth_args = AuthArgs {
        account_identifier: std::env::var("SNOWFLAKE_ACCOUNT").expect("SNOWFLAKE_ACCOUNT not set"),
        warehouse: std::env::var("SNOWLFLAKE_WAREHOUSE").ok(),
        database: std::env::var("SNOWFLAKE_DATABASE").ok(),
        schema: std::env::var("SNOWFLAKE_SCHEMA").ok(),
        username: std::env::var("SNOWFLAKE_USER").expect("SNOWFLAKE_USER not set"),
        role: std::env::var("SNOWFLAKE_ROLE").ok(),
        auth_type: AuthType::Password(PasswordArgs {
            password: std::env::var("SNOWFLAKE_PASSWORD").expect("SNOWFLAKE_PASSWORD not set"),
        }),
    };

    let mut client = Connection::default_client_builder();
    client = client
        .with_init(Extension(OtelName(std::borrow::Cow::Borrowed(
            "snowflake-api",
        ))))
        .with(reqwest_tracing::TracingMiddleware::<SpanBackendWithUrl>::new());

    let builder = SnowflakeApiBuilder::new(auth_args).with_client(client.build());
    let api = builder.build()?;

    run_in_span(&api).await?;

    global::shutdown_tracer_provider();

    Ok(())
}

#[tracing::instrument(name = "snowflake_api", skip(api))]
async fn run_in_span(api: &snowflake_api::SnowflakeApi) -> anyhow::Result<()> {
    let res = api.exec("select 'hello from snowflake' as col1;").await?;

    match res {
        QueryResult::Arrow(a) => {
            println!("{}", pretty_format_batches(&a).unwrap());
        }
        QueryResult::Json(j) => {
            println!("{}", j);
        }
        QueryResult::Empty => {
            println!("Query finished successfully")
        }
    }

    Ok(())
}
