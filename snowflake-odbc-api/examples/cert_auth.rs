use anyhow::Result;
use clap::Parser;
use snowflake_odbc_api::{Connection, Session};
use std::fs;
use std::sync::Arc;

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

    /// Warehouse
    #[arg(short, long)]
    warehouse: String,

    /// Database
    #[arg(short, long)]
    database: String,

    /// username to whom the private key belongs to
    #[arg(short, long)]
    username: String,

    /// role which user will assume
    #[arg(short, long)]
    role: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    let pem = fs::read(&args.private_key)?;

    let connection = Arc::new(Connection::new()?);

    let mut auth = Session::cert_auth(
        connection,
        &args.account_identifier,
        &args.warehouse,
        Some(&args.database),
        None,
        &args.username,
        Some(&args.role),
        &pem,
    );

    let token = auth.get_token().await?;

    println!("{}", token);

    Ok(())
}
