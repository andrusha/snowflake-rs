#FROM rust:1.80.1
FROM scratch

WORKDIR /app_source/deps/snowflake-rs

COPY . .