# snowflake-jwt

Generates JWT token in Snowflake-compatible format, see [Using Key Pair Authentication](https://docs.snowflake.com/en/developer-guide/sql-api/authenticating#label-sql-api-authenticating-key-pair).

Can be used in order to run queries against [SQL REST API](https://docs.snowflake.com/developer-guide/sql-api/intro).

## Usage

```toml
[dependencies]
snowflake-jwt = "0.1.0"
```

Check [examples](./examples) for working programs using the library.

```rust
use anyhow;
use fs;
use snowflake_jwt;

fn get_token(private_key_path: &str, account_identifier: &str, username: &str) -> Result<String> {
    let pem = fs::read(private_key_path)?;
    let full_identifier = format!("{}.{}", account_identifier, username);
    let jwt = snowflake_jwt::generate_jwt_token(&pem, &full_identifier)?;

    Ok(jwt)
}
```
