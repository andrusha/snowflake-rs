use anyhow::Result;
use polars::frame::DataFrame;
use snowflake_api::{AuthArgs, AuthType, PasswordArgs, SnowflakeApiBuilder};

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

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

    let api = SnowflakeApiBuilder::new(auth_args).build()?;

    // run a query that returns a tabular arrow response
    run_and_print(
        &api,
        r"
            select
                count(query_id) as num_queries,
                user_name
            from snowflake.account_usage.query_history
            where
                start_time > current_date - 7
            group by user_name;
    ",
    )
    .await?;

    // run a query that returns a json response
    run_and_print(&api, r"SHOW DATABASES;").await?;

    Ok(())
}

async fn run_and_print(api: &snowflake_api::SnowflakeApi, sql: &str) -> Result<()> {
    let res = api.exec_raw(sql).await?;

    let df = DataFrame::try_from(res)?;
    // alternatively, you can use the `try_into` method on the response
    // let df: DataFrame = res.try_into()?;

    println!("{:?}", df);

    Ok(())
}
