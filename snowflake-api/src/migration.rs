#![allow(clippy::cast_sign_loss)]
use arrow::array::{
    Int32Array, Int64Array, Int8Array, PrimitiveArray, RecordBatch, StringArray, StructArray,
};
use arrow::datatypes::ArrowPrimitiveType;
use async_trait::async_trait;
use refinery_core::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use refinery_core::Migration;

use serde::Deserialize;

use once_cell::sync::Lazy;
use regex::Regex;
use thiserror::Error;

use time::OffsetDateTime;

use crate::{QueryResult, SnowflakeApi, SnowflakeApiError};

static BLOCK_COMMENT_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?s)/\*.*?\*/").unwrap());

/// copied from `refinery_core`
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
enum State {
    Applied,
    Unapplied,
}

/// copied from `refinery_core`
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
enum TypeInner {
    Versioned,
    Unversioned,
}

#[async_trait]
impl AsyncTransaction for SnowflakeApi {
    type Error = SnowflakeApiError;

    async fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        self.exec("SET previous_role = CURRENT_ROLE();").await?;
        self.exec("SET previous_database = CURRENT_DATABASE();")
            .await?;
        self.exec("BEGIN TRANSACTION;").await?;

        let mut modified_queries = Vec::with_capacity(queries.len() + 1);
        // Here we are slotting in the previous database and role
        // assume queries so that the migration insert is run with the orignal
        // database and role that the script is executed with, else we run
        // into issues with the migration table not being found due to permissions
        for &query in &queries[..queries.len() - 1] {
            modified_queries.push(query);
        }
        modified_queries.push("USE DATABASE IDENTIFIER($previous_database);");
        modified_queries.push("USE ROLE IDENTIFIER($previous_role);");
        modified_queries.push(queries[queries.len() - 1]);

        for query in modified_queries {
            log::debug!("Executing migration query: {:?}", query);
            let split_queries = split_query(query);
            log::debug!("Split queries: {:?}", split_queries);

            for statement in split_queries {
                self.exec(&statement).await?;
            }
        }

        self.exec("COMMIT;").await?;

        Ok(queries.len())
    }
}

fn split_query(query: &str) -> Vec<String> {
    let query_without_block_comments = BLOCK_COMMENT_RE.replace_all(query, "").to_string();

    let concatenated = query_without_block_comments
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.starts_with("--"))
        .collect::<Vec<&str>>()
        .join(" ");

    concatenated
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

#[async_trait]
impl AsyncQuery<Vec<Migration>> for SnowflakeApi {
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        let results = match self.exec(query).await {
            Ok(r) => r,
            Err(e) => {
                log::error!("Error: {:?}", e);
                return Err(e);
            }
        };

        let res = match results {
            QueryResult::Arrow(arrow) => result_to_migrations(arrow).map_err(|e| {
                SnowflakeApiError::ArrowError(arrow::error::ArrowError::IpcError(e.to_string()))
            })?,
            QueryResult::Json(_) | QueryResult::Empty => {
                vec![]
            }
        };

        Ok(res)
    }
}

impl AsyncMigrate for SnowflakeApi {
    fn assert_migrations_table_query(migration_table_name: &str) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {migration_table_name} (
                version INT,
                name STRING,
                applied_on TIMESTAMP_LTZ,
                checksum BIGINT
            )",
        )
    }
}

#[derive(Debug, Error)]
pub enum MigrationArrowError {
    #[error("Error parsing migration arrow")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("Unexpected None value")]
    None,

    #[error("Error handling Snowflake timestamp")]
    TimeError(#[from] time::error::ComponentRange),
}

fn get_column_primitive<T: ArrowPrimitiveType>(
    batch: &RecordBatch,
    name: &str,
) -> Result<PrimitiveArray<T>, MigrationArrowError> {
    let array_data = batch
        .column_by_name(name)
        .ok_or(MigrationArrowError::None)?
        .to_data();
    let array = PrimitiveArray::<T>::from(array_data);
    Ok(array)
}

fn get_column_struct(batch: &RecordBatch, name: &str) -> Result<StructArray, MigrationArrowError> {
    let array = batch
        .column_by_name(name)
        .ok_or(MigrationArrowError::None)?
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or(MigrationArrowError::None)?;
    Ok(array.clone())
}

fn get_column_string(batch: &RecordBatch, name: &str) -> Result<StringArray, MigrationArrowError> {
    let array = batch
        .column_by_name(name)
        .ok_or(MigrationArrowError::None)?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(MigrationArrowError::None)?;
    Ok(array.clone())
}

fn result_to_migrations(arrow: Vec<RecordBatch>) -> Result<Vec<Migration>, MigrationArrowError> {
    // convert arrow to Vec<MigrationInner>
    //
    // We have to do this because Snowflake select from will always
    // return arrow record batches instead of allowing us to coerce json
    let mut versions: Vec<i32> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    let mut applied_ons: Vec<OffsetDateTime> = Vec::new();
    let mut checksums: Vec<i64> = Vec::new();

    for batch in arrow {
        let names_array: StringArray = get_column_string(&batch, "NAME")?;
        names.extend(
            names_array
                .iter()
                .map(|x| x.map(std::string::ToString::to_string).unwrap_or_default()),
        );

        let version_array: Int8Array = get_column_primitive(&batch, "VERSION")?;
        versions.extend(
            version_array
                .iter()
                .map(|x| x.map(i32::from).unwrap_or_default()),
        );

        let applied_on_struct = get_column_struct(&batch, "APPLIED_ON")?;
        let e = applied_on_struct
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(MigrationArrowError::None)?;

        let f = applied_on_struct
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or(MigrationArrowError::None)?;

        for (epoch, fraction) in e.iter().zip(f.iter()) {
            if let (Some(epoch), Some(fraction)) = (epoch, fraction) {
                applied_ons.push(
                    OffsetDateTime::from_unix_timestamp(epoch)?
                        .replace_nanosecond(fraction as u32)?,
                );
            }
        }

        let checksum_array: Int64Array = get_column_primitive(&batch, "CHECKSUM")?;
        checksums.extend(
            checksum_array
                .iter()
                .map(std::option::Option::unwrap_or_default),
        );
    }

    // check that all arrays have the same length
    if versions.len() != names.len()
        || versions.len() != applied_ons.len()
        || versions.len() != checksums.len()
    {
        return Err(MigrationArrowError::None);
    }

    // Safety: We are sure that all arrays have the same length
    let res = (0..versions.len())
        .map(|i| {
            Migration::applied(
                versions[i],
                names[i].clone(),
                applied_ons[i],
                checksums[i] as u64,
            )
        })
        .collect::<Vec<Migration>>();

    Ok(res)
}

#[cfg(test)]
mod test_migrate {
    #[test]
    fn test_split_query() {
        let input = "-- SET previous_role = CURRENT_ROLE();\n-- SET previous_database = CURRENT_DATABASE();\n\n\nUSE ROLE SYSADMIN;\nCREATE OR REPLACE DATABASE test_db;\n\n-- Assume Snowflake ACCOUNTADMIN role\nUSE ROLE ACCOUNTADMIN;\n\n-- Create a new role 'test_role'\nCREATE OR REPLACE ROLE test_role;\n\n-- Grant some privileges to 'test_role'\nGRANT USAGE ON DATABASE test_db TO ROLE test_role;\nGRANT USAGE ON SCHEMA test_db.public TO ROLE test_role;\n\n\n-- Create a file format for CSV files\nCREATE OR REPLACE FILE FORMAT my_csv_format\n  TYPE = 'CSV'\n  FIELD_DELIMITER = ','\n  SKIP_HEADER = 1;\n\n/*\nUSE ROLE IDENTIFIER($previous_role);\nUSE DATABASE IDENTIFIER($previous_database);\n*/";
        let expected = vec!["USE ROLE SYSADMIN", "CREATE OR REPLACE DATABASE test_db", "USE ROLE ACCOUNTADMIN", "CREATE OR REPLACE ROLE test_role", "GRANT USAGE ON DATABASE test_db TO ROLE test_role", "GRANT USAGE ON SCHEMA test_db.public TO ROLE test_role", "CREATE OR REPLACE FILE FORMAT my_csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1"];
        assert_eq!(expected, super::split_query(input));
    }
}
