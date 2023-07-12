use anyhow::{Context, Error, Result};
use clap::Parser;
use std::fs;
use snowflake_odbc_api::auth::SnowflakeAuth;
use snowflake_odbc_api::SnowflakeOdbcApi;

extern crate snowflake_odbc_api;

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
    let args = Args::parse();
    let pem = fs::read(&args.private_key)?;

    let auth = snowflake_odbc_api::auth::SnowflakeCertAuth::new(
        &pem,
        &args.username,
        &args.role,
        &args.account_identifier,
        &args.warehouse,
        &args.database,
    )?;

    let api = SnowflakeOdbcApi::new(Box::new(auth), &args.account_identifier)?;

    let res = api.exec(&args.sql)?;

    println!("{:?}", res);


    Ok(())
}
