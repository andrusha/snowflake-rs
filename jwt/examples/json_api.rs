use anyhow::{Context, Result};
use clap::Parser;
use std::fs;

extern crate snowflake_jwt;

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
    let pem = fs::read_to_string(&args.private_key)?;
    let full_identifier = format!("{}.{}", &args.account_identifier, &args.username);
    let jwt = snowflake_jwt::generate_jwt_token(&pem, &full_identifier)?;

    println!("{}", &args.sql);

    let url = format!(
        "https://{}.snowflakecomputing.com/api/v2/statements",
        &args.account_identifier
    );
    let auth = format!("Bearer {}", &jwt);
    let resp = ureq::post(&url)
        .set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
        .set("Authorization", &auth)
        .set("User-Agent", "myApplicationName/1.0")
        .send_json(ureq::json!({
            "statement": &args.sql,
            "timeout": 60,
            "database": &args.database,
            "schema": &args.schema,
            "warehouse": &args.warehouse,
            "role": &args.role
        }));

    match resp {
        Ok(r) => {
            println!("{}", r.into_string()?);
        }
        Err(ureq::Error::Status(code, r)) => {
            let rstr = r.into_string().context("consuming response")?;
            println!(
                "failed to execute statement, server replied with {}, {}",
                code, rstr
            );
        }
        Err(ureq::Error::Transport(tr)) => {
            println!("transport error: {:?}", tr);
        }
    }

    Ok(())
}
