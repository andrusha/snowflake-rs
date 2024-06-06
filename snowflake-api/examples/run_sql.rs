extern crate snowflake_api;

use anyhow::Result;
use arrow::util::pretty::pretty_format_batches;
use clap::Parser;
use std::fs;

use snowflake_api::{
    AuthArgs, CertificateArgs, PasswordArgs, QueryResult, SnowflakeApi, SnowflakeApiBuilder,
};

#[derive(clap::ValueEnum, Clone, Debug)]
enum Output {
    Arrow,
    Json,
    Query,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to RSA PEM private key
    #[arg(long)]
    private_key: Option<String>,

    /// Password if certificate is not present
    #[arg(long)]
    password: Option<String>,

    /// <account_identifier> in Snowflake format, uppercase
    #[arg(short, long)]
    account_identifier: String,

    /// Database name
    #[arg(short, long)]
    database: Option<String>,

    /// Schema name
    #[arg(long)]
    schema: Option<String>,

    /// Warehouse
    #[arg(short, long)]
    warehouse: Option<String>,

    /// username to whom the private key belongs to
    #[arg(short, long)]
    username: String,

    /// role which user will assume
    #[arg(short, long)]
    role: Option<String>,

    /// sql statement to execute and print result from
    #[arg(long)]
    sql: String,

    #[arg(long)]
    #[arg(value_enum, default_value_t = Output::Arrow)]
    output: Output,
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let args = Args::parse();

    let mut api = match (&args.private_key, &args.password) {
        (Some(pkey), None) => {
            let pem = fs::read_to_string(pkey)?;
            SnowflakeApiBuilder::new(AuthArgs {
                account_identifier: args.account_identifier,
                warehouse: args.warehouse,
                database: args.database,
                schema: args.schema,
                username: args.username,
                role: args.role,
                auth_type: snowflake_api::AuthType::Certificate(CertificateArgs {
                    private_key_pem: pem,
                }),
            })
            .build()?
        }
        (None, Some(pwd)) => SnowflakeApiBuilder::new(AuthArgs {
            account_identifier: args.account_identifier,
            warehouse: args.warehouse,
            database: args.database,
            schema: args.schema,
            username: args.username,
            role: args.role,
            auth_type: snowflake_api::AuthType::Password(PasswordArgs {
                password: pwd.to_string(),
            }),
        })
        .build()?,
        _ => {
            panic!("Either private key path or password must be set")
        }
    };

    match args.output {
        Output::Arrow => {
            let res = api.exec(&args.sql).await?;
            match res {
                QueryResult::Arrow(a) => {
                    println!("{}", pretty_format_batches(&a).unwrap());
                }
                QueryResult::Json(j) => {
                    println!("{j}");
                }
                QueryResult::Empty => {
                    println!("Query finished successfully")
                }
            }
        }
        Output::Json => {
            let res = api.exec_json(&args.sql).await?;
            println!("{res}");
        }
        Output::Query => {
            let res = api.exec_response(&args.sql).await?;
            println!("{:?}", res);
        }
    }

    Ok(())
}
