use anyhow::{Context, Error, Result};
use clap::Parser;
use std::fs;
use snowflake_odbc_api::auth::SnowflakeAuth;

extern crate snowflake_odbc_api;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// <account_identifier> in Snowflake format, uppercase
    #[arg(short, long)]
    account_identifier: String,

    /// Warehouse
    #[arg(short, long)]
    warehouse: String,

    /// Database
    #[arg(short, long)]
    database: String,

    /// username to whom the private key belongs to
    #[arg(short, long)]
    username: String,

    /// username to whom the private key belongs to
    #[arg(short, long)]
    password: String,

    /// role which user will assume
    #[arg(short, long)]
    role: String,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let auth = snowflake_odbc_api::auth::SnowflakePasswordAuth::new(
        &args.username,
        &args.password,
        &args.role,
        &args.account_identifier,
        &args.warehouse,
        &args.database
    )?;

    let token = auth.get_master_token()?;

    println!("{:?}", token);

    Ok(())
}
