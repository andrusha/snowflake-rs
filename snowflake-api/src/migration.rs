#![allow(clippy::cast_sign_loss)]
use arrow::array::{
    Int32Array, Int64Array, Int8Array, PrimitiveArray, RecordBatch, StringArray, StructArray,
};
use arrow::datatypes::ArrowPrimitiveType;
use async_trait::async_trait;
use refinery_core::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use refinery_core::Migration;

use serde::{de, Deserialize};
use sqlparser::dialect::SnowflakeDialect;

use tap::Tap;
use thiserror::Error;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::{QueryResult, SnowflakeApi, SnowflakeApiError};

const DIALECT: SnowflakeDialect = sqlparser::dialect::SnowflakeDialect {};

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

/// copied from `refinery_core`
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct MigrationInner {
    state: State,
    name: String,
    checksum: u64,
    version: i32,
    prefix: TypeInner,
    sql: Option<String>,
    #[serde(deserialize_with = "deserialize_date")]
    applied_on: Option<OffsetDateTime>,
}

fn deserialize_date<'de, D>(deserializer: D) -> Result<Option<OffsetDateTime>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(s) => Ok(Some(
            OffsetDateTime::parse(&s, &Rfc3339).map_err(de::Error::custom)?,
        )),
        None => Ok(None),
    }
}

impl MigrationInner {
    fn applied(
        version: i32,
        name: String,
        applied_on: OffsetDateTime,
        checksum: u64,
    ) -> MigrationInner {
        MigrationInner {
            state: State::Applied,
            name,
            checksum,
            version,
            // applied migrations are always versioned
            prefix: TypeInner::Versioned,
            sql: None,
            applied_on: Some(applied_on),
        }
    }
}

impl From<MigrationInner> for Migration {
    fn from(inner: MigrationInner) -> Self {
        assert_eq!(
            std::mem::size_of::<Migration>(),
            std::mem::size_of::<MigrationInner>()
        );
        unsafe { std::mem::transmute(inner) }
    }
}

#[async_trait]
impl AsyncTransaction for SnowflakeApi {
    type Error = SnowflakeApiError;

    async fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        self.exec("BEGIN TRANSACTION").await?;

        for query in queries {
            let parsed_query = sqlparser::parser::Parser::parse_sql(&DIALECT, query)
                .map_err(SnowflakeApiError::QueryParserError)?;
            for statement in parsed_query {
                self.exec(&statement.to_string()).await?;
            }
        }

        self.exec("COMMIT").await?;

        Ok(queries.len())
    }
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

        Ok(res
            .into_iter()
            .map(Migration::from)
            .tap(|m| log::debug!("{m:#?}"))
            .collect())
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

fn result_to_migrations(
    arrow: Vec<RecordBatch>,
) -> Result<Vec<MigrationInner>, MigrationArrowError> {
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
            MigrationInner::applied(
                versions[i],
                names[i].clone(),
                applied_ons[i],
                checksums[i] as u64,
            )
        })
        .collect::<Vec<MigrationInner>>();

    Ok(res)
}
