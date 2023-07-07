use anyhow::Result;
use clap::Parser;
use std::fs;
use snowflake_rest_api::SnowflakeRestApi;

extern crate snowflake_rest_api;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to RSA PEM private key
    #[arg(long)]
    private_key: String,

    /// <account_identifier> in Snowflake format, uppercase
    #[arg(short, long)]
    account_identifier: String,

    /// Database name
    #[arg(short, long)]
    database: String,

    /// Schema name
    #[arg(long)]
    schema: String,

    /// Warehouse
    #[arg(short, long)]
    warehouse: String,

    /// username to whom the private key belongs to
    #[arg(short, long)]
    username: String,

    /// role which user will assume
    #[arg(short, long)]
    role: String,

    /// sql statement to execute and print result from
    #[arg(long)]
    sql: String,
}

fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let sf_api = SnowflakeRestApi::new(
        &fs::read(&args.private_key)?,
        &args.username,
        Some(&args.role),
        &args.account_identifier,
        &args.warehouse,
    )?;
    let result = sf_api.run_query(&args.sql)?;
    println!("{:?}", result);

    Ok(())
}
