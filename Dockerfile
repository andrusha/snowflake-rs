FROM rust:1.80.1

WORKDIR /app_source/deps/snowflake-rs

COPY . .