extern crate snowflake_api;

use std::process;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use futures::future::{self, FutureExt};

use snowflake_api::{AsyncRuntime, SnowflakeApi};

pub struct SmolRuntime;

impl AsyncRuntime for SmolRuntime {
    type Delay = future::Map<smol::Timer, fn(Instant)>;

    fn delay_for(duration: Duration) -> Self::Delay {
        FutureExt::map(smol::Timer::after(duration), |_| ())
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    password: String,

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
    pretty_env_logger::init();

    let args = Args::parse();

    let mut api: SnowflakeApi<SmolRuntime> = SnowflakeApi::with_password_auth(
        &args.account_identifier,
        &args.warehouse,
        Some(&args.database),
        Some(&args.schema),
        &args.username,
        Some(&args.role),
        &args.password,
    )?;

    smol::block_on(async {
        let res = api.exec_json(&args.sql).await;
        match res {
            Ok(r) => {
                println!("{}", r.to_string());
            }
            Err(e) => {
                eprintln!("Error querying API: {:?}", e);
                process::exit(1);
            }
        }
    });

    Ok(())
}
