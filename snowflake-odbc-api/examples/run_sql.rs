use anyhow::Result;
use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use snowflake_odbc_api::{SnowflakeCertAuth, SnowflakeOdbcApi};
use std::fs;

extern crate snowflake_odbc_api;

#[derive(clap::ValueEnum, Clone, Debug)]
enum OutputFormat {
    Arrow,
    Json
}

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

    #[arg(long)]
    #[clap(value_enum)]
    output: OutputFormat
}

fn main() -> Result<()> {
    let args = Args::parse();
    let pem = fs::read(&args.private_key)?;

    let auth = SnowflakeCertAuth::new(
        &pem,
        &args.username,
        &args.role,
        &args.account_identifier,
        &args.warehouse,
        &args.database,
    )?;

    let api = SnowflakeOdbcApi::new(Box::new(auth), &args.account_identifier)?;

    match args.output {
        OutputFormat::Arrow => {
            let res = api.exec_arrow(&args.sql)?;
            println!("{}", pretty_format_batches(&res).unwrap());
        }
        OutputFormat::Json => {
            let res = api.exec_json(&args.sql)?;
            println!("{}", res);
        }
    }

    Ok(())
}
