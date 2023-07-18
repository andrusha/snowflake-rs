use anyhow::Result;
use clap::Parser;
use snowflake_odbc_api::{Connection, Session};
use std::sync::Arc;

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

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();

    let connection = Arc::new(Connection::new()?);

    let mut auth = Session::password_auth(
        connection,
        &args.account_identifier,
        &args.warehouse,
        Some(&args.database),
        None,
        &args.username,
        Some(&args.role),
        &args.password,
    );

    let token = auth.get_token().await?;

    println!("{:?}", token);

    Ok(())
}
