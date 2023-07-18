use anyhow::Result;
use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use snowflake_odbc_api::{Connection, QueryResult, Session, SnowflakeOdbcApi};
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

    #[arg(long)]
    csv_path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    let pem = fs::read(&args.private_key)?;

    let connection = Arc::new(Connection::new()?);

    let session = Session::cert_auth(
        Arc::clone(&connection),
        &args.account_identifier,
        &args.warehouse,
        Some(&args.database),
        Some(&args.schema),
        &args.username,
        Some(&args.role),
        &pem,
    );
    let mut api = SnowflakeOdbcApi::new(
        Arc::clone(&connection),
        session,
        &args.account_identifier,
    )?;

    log::info!("Creating table");
    api.exec(
        "CREATE OR REPLACE TABLE OSCAR_AGE_MALE(Index integer, Year integer, Age integer, Name varchar, Movie varchar);"
    ).await?;

    log::info!("Uploading CSV file");
    api.exec(&format!("PUT file://{} @%OSCAR_AGE_MALE;", &args.csv_path))
        .await?;

    log::info!("Create temporary file format");
    api.exec(
        "CREATE OR REPLACE TEMPORARY FILE FORMAT CUSTOM_CSV_FORMAT TYPE = CSV COMPRESSION = NONE FIELD_DELIMITER = ',' FILE_EXTENSION = 'csv' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"' TRIM_SPACE = TRUE SKIP_BLANK_LINES = TRUE;"
    ).await?;

    log::info!("Copying into table");
    api.exec("COPY INTO OSCAR_AGE_MALE FILE_FORMAT = CUSTOM_CSV_FORMAT;")
        .await?;

    log::info!("Querying for results");
    let res = api.exec("SELECT * FROM OSCAR_AGE_MALE;").await?;

    match res {
        QueryResult::Arrow(a) => {
            println!("{}", pretty_format_batches(&a).unwrap());
        }
        QueryResult::Empty => {
            println!("Nothing was returned");
        }
        QueryResult::Json(j) => {
            println!("{}", j.to_string());
        }
    }

    Ok(())
}
