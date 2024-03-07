use anyhow::Result;
use snowflake_api::{AuthArgs, SnowflakeApiBuilder};

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./tests/sql_migrations");
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt::init();

    let auth_args = AuthArgs::from_env()?;
    let mut conn = SnowflakeApiBuilder::new(auth_args).build()?;

    embedded::migrations::runner().run_async(&mut conn).await?;

    Ok(())
}
