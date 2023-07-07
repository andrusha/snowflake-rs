use anyhow::{Context, Error, Result};
use clap::Parser;
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;
use uuid::Uuid;

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
    let full_identifier = format!("{}.{}", &args.account_identifier, &args.username);
    let jwt = snowflake_jwt::generate_jwt_token(&pem, &full_identifier)?;

    println!("{}", &args.sql);

    // auth request

    let url = format!("https://{}.snowflakecomputing.com/session/v1/login-request", &args.account_identifier);

    let request_id = Uuid::now_v1(&[0, 0, 0, 0, 0, 0]);
    let (client_start_time, _nanos) = request_id.get_timestamp().unwrap().to_unix();
    let request_guid = Uuid::new_v4();
    let url = Url::parse_with_params(
        &url,
        &[
            ("requestId", request_id.to_string()),
            ("request_guid", request_guid.to_string()),
            ("databaseName", args.database),
            ("roleName", args.role),
            ("schemaName", args.schema),
            ("warehouse", args.warehouse)
        ])?;

    let auth = format!("Bearer {}", &jwt);
    let resp = ureq::request_url("POST", &url)
        .set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
        .set("Authorization", &auth)
        .set("User-Agent", "Rust/0.0.1")
        .set("accept", "application/json")
        .send_json(ureq::json!({
            "data": {
                "CLIENT_APP_ID": "Rust",
                "CLIENT_APP_VERSION": "0.0.1",
                "SVN_REVISION": "",
                "ACCOUNT_NAME": &args.account_identifier,
                "LOGIN_NAME": &args.username,
                "AUTHENTICATOR": "SNOWFLAKE_JWT",
                "TOKEN": &jwt,
                "SESSION_PARAMETERS": {
                    "CLIENT_VALIDATE_DEFAULT_PARAMETERS": true
                },
                "CLIENT_ENVIRONMENT": {
                    "APPLICATION": "Rust",
                    "OS": "darwin",
                    "OS_VERSION": "gc-arm64",
                    "OCSP_MODE": "FAIL_OPEN"
                }
            }
        }));

    match resp {
        Ok(r) => {
            println!("{}", r.into_string()?);
        }
        Err(ureq::Error::Status(code, r)) => {
            let rstr = r.into_string().context("consuming response")?;
            println!("failed to execute statement, server replied with {}, {}", code, rstr);
        }
        Err(ureq::Error::Transport(tr)) => {
            println!("transport error: {:?}", tr);
        }
    }

    // query request

    // let url = format!("https://{}.snowflakecomputing.com/queries/v1/query-request", &args.account_identifier);
    //
    // let request_id = Uuid::now_v1(&[0, 0, 0, 0, 0, 0]);
    // let (client_start_time, _nanos) = request_id.get_timestamp().unwrap().to_unix();
    // let request_guid = Uuid::new_v4();
    // let url = Url::parse_with_params(
    //     &url,
    //     &[
    //         ("client_start_time", client_start_time.to_string()),
    //         ("requestId", request_id.to_string()),
    //         ("request_guid", request_guid.to_string())])?;
    //
    // let auth = format!("Bearer {}", &jwt);
    // let resp = ureq::request_url("POST", &url)
    //     .set("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
    //     .set("Authorization", &auth)
    //     .set("User-Agent", "myApplicationName/1.0")
    //     .set("accept", "application/json")
    //     .send_json(ureq::json!({
    //         "sqlText": &args.sql,
    //         "asyncExec": false,
    //         "sequenceId": 1,
    //         "isInternal": false,
    //         "timeout": 60,
    //         "database": &args.database,
    //         "schema": &args.schema,
    //         "warehouse": &args.warehouse,
    //         "role": &args.role
    //     }));
    //
    // match resp {
    //     Ok(r) => {
    //         println!("{}", r.into_string()?);
    //     }
    //     Err(ureq::Error::Status(code, r)) => {
    //         let rstr = r.into_string().context("consuming response")?;
    //         println!("failed to execute statement, server replied with {}, {}", code, rstr);
    //     }
    //     Err(ureq::Error::Transport(tr)) => {
    //         println!("transport error: {:?}", tr);
    //     }
    // }

    Ok(())
}
