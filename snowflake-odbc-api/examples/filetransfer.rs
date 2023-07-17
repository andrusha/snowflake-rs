use anyhow::Result;
use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use snowflake_odbc_api::{QueryResult, SnowflakeCertAuth, SnowflakeOdbcApi};
use std::fs;

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

    let auth = SnowflakeCertAuth::new(
        &pem,
        &args.username,
        &args.role,
        &args.account_identifier,
        &args.warehouse,
        &args.database,
    )?;
    let api = SnowflakeOdbcApi::new(Box::new(auth), &args.account_identifier)?;

    let table_name = format!("{}.OSCAR_AGE_MALE", &args.schema);

    log::info!("Creating table");
    api.exec(
        &format!("CREATE OR REPLACE TABLE {}(Index integer, Year integer, Age integer, Name varchar, Movie varchar);", table_name)
    ).await?;

    log::info!("Uploading CSV file");
    api.exec(
        &format!("PUT file://{} @{}.%OSCAR_AGE_MALE;", &args.csv_path, &args.schema)
    ).await?;

    log::info!("Create temporary file format");
    api.exec(
        "CREATE OR REPLACE TEMPORARY FILE FORMAT CUSTOM_CSV_FORMAT TYPE = CSV COMPRESSION = NONE FIELD_DELIMITER = ',' FILE_EXTENSION = 'csv' SKIP_HEADER = 1;"
    ).await?;

    log::info!("Copying into table");
    api.exec(
        &format!("COPY INTO {} FILE_FORMAT = CUSTOM_CSV_FORMAT;", table_name)
    ).await?;

    log::info!("Querying for results");
    let res = api.exec(
        &format!("SELECT * FROM {};", table_name)
    ).await?;

    match res {
        QueryResult::Arrow(a) => {
            println!("{}", pretty_format_batches(&a).unwrap());
        }
        QueryResult::Empty => {
            println!("Nothing was returned")
        }
    }

    Ok(())
}
