use anyhow::Result;
use snowflake_api::SnowflakeApi;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./tests/sql_migrations");
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

    let mut conn = SnowflakeApi::from_env()?;
    embedded::migrations::runner().run_async(&mut conn).await?;

    Ok(())
}
