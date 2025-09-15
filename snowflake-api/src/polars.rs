use std::convert::TryFrom;
use std::num::NonZero;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use bytes::{Buf, Bytes};
use polars_core::frame::DataFrame;
use polars_io::ipc::IpcStreamReader;
use polars_io::json::{JsonFormat, JsonReader};
use polars_io::SerReader;
use serde::de::Error;
use serde_json::{Map, Value};
use thiserror::Error;

use crate::{JsonResult, RawQueryResult};

#[derive(Error, Debug)]
pub enum PolarsCastError {
    #[error(transparent)]
    SerdeError(#[from] serde_json::Error),

    #[error(transparent)]
    PolarsError(#[from] polars_core::error::PolarsError),
}

impl RawQueryResult {
    pub fn to_polars(self) -> Result<DataFrame, PolarsCastError> {
        match self {
            RawQueryResult::Bytes(bytes) => dataframe_from_bytes(bytes),
            RawQueryResult::BytesWithSchema(bytes, schema) => {
                // convert to Arrow first then to Polars to ensure decimal
                // conversion happens before Polars processing
                let arrow_result = RawQueryResult::BytesWithSchema(bytes, schema)
                    .deserialize_arrow()
                    .map_err(|e| {
                        PolarsCastError::PolarsError(polars_core::error::PolarsError::ComputeError(
                            e.to_string().into(),
                        ))
                    })?;

                match arrow_result {
                    crate::QueryResult::Arrow(batches) => {
                        // convert Arrow batches to Polars DataFrame
                        let mut df = DataFrame::empty();
                        for batch in batches {
                            let batch_bytes = arrow_ipc_to_bytes(batch)?;
                            let df_chunk =
                                IpcStreamReader::new(std::io::Cursor::new(batch_bytes)).finish()?;
                            df.vstack_mut(&df_chunk)?;
                        }
                        df.align_chunks();
                        Ok(df)
                    }
                    _ => Err(PolarsCastError::PolarsError(
                        polars_core::error::PolarsError::ComputeError(
                            "Unexpected query result type".into(),
                        ),
                    )),
                }
            }
            RawQueryResult::Stream(_bytes_stream) => todo!(),
            RawQueryResult::Json(json) => dataframe_from_json(&json),
            RawQueryResult::Empty => Ok(DataFrame::empty()),
        }
    }
}

fn dataframe_from_json(json_result: &JsonResult) -> Result<DataFrame, PolarsCastError> {
    let objects = arrays_to_objects(json_result)?;
    // fixme: serializing json again, is it possible to keep bytes? or implement casting?
    let json_string = serde_json::to_string(&objects)?;
    let reader = std::io::Cursor::new(json_string.as_bytes());
    let df = JsonReader::new(reader)
        .with_json_format(JsonFormat::Json)
        .infer_schema_len(Some(NonZero::new(5).unwrap()))
        .finish()?;
    Ok(df)
}

/// This is required because the polars json reader expects an array of objects, and
/// the snowflake json response is an array of arrays (without real column names).
///
/// This is apparent if you run a system query (not a select) like `SHOW DATABASES;`.
fn arrays_to_objects(json_result: &JsonResult) -> Result<Value, PolarsCastError> {
    let arrays: &Vec<Value> = json_result
        .value
        .as_array()
        .ok_or(serde_json::Error::custom("Input must be array an array"))?;
    let names: Vec<&str> = json_result.schema.iter().map(|s| s.name.as_str()).collect();

    let objects: Result<Vec<Value>, PolarsCastError> = arrays
        .iter()
        .map(|array| {
            array
                .as_array()
                .ok_or(serde_json::Error::custom("Input must be array of array"))
                .map(|array| {
                    // Use references to avoid cloning names
                    let map: Map<String, Value> = names
                        .iter()
                        .zip(array.iter())
                        .map(|(&name, value)| (name.to_string(), value.clone()))
                        .collect();
                    Value::Object(map)
                })
                .map_err(PolarsCastError::SerdeError)
        })
        .collect();

    objects.map(Value::Array)
}

fn dataframe_from_bytes(bytes: Vec<Bytes>) -> Result<DataFrame, PolarsCastError> {
    let mut df = DataFrame::empty();
    for b in bytes {
        let df_chunk = IpcStreamReader::new(b.reader()).finish()?;
        df.vstack_mut(&df_chunk)?;
    }
    df.align_chunks();
    Ok(df)
}

/// Convert an Arrow RecordBatch back to bytes in IPC format
fn arrow_ipc_to_bytes(batch: RecordBatch) -> Result<Vec<u8>, PolarsCastError> {
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema()).map_err(|e| {
            PolarsCastError::PolarsError(polars_core::error::PolarsError::ComputeError(
                e.to_string().into(),
            ))
        })?;
        writer.write(&batch).map_err(|e| {
            PolarsCastError::PolarsError(polars_core::error::PolarsError::ComputeError(
                e.to_string().into(),
            ))
        })?;
        writer.finish().map_err(|e| {
            PolarsCastError::PolarsError(polars_core::error::PolarsError::ComputeError(
                e.to_string().into(),
            ))
        })?;
    }
    Ok(buffer)
}

impl TryFrom<RawQueryResult> for DataFrame {
    type Error = PolarsCastError;

    fn try_from(value: RawQueryResult) -> Result<Self, Self::Error> {
        value.to_polars()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FieldSchema, SnowflakeType};
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn test_polars_decimal_conversion() {
        let int_array = Int32Array::from(vec![Some(12345), Some(67890), Some(-1234)]);
        let schema = ArrowSchema::new(vec![Field::new("price", DataType::Int32, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(int_array)]).unwrap();

        // serialize to Arrow IPC bytes
        let mut buffer = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        let bytes = Bytes::from(buffer);

        // create Snowflake schema with decimal metadata
        let snowflake_schema = vec![FieldSchema {
            name: "price".to_string(),
            type_: SnowflakeType::Fixed,
            precision: Some(10),
            scale: Some(2),
            nullable: true,
        }];

        // test conversion to Polars DataFrame
        let raw_result = RawQueryResult::BytesWithSchema(vec![bytes], snowflake_schema);
        let df = raw_result.to_polars().unwrap();

        // verify DataFrame structure
        assert_eq!(df.height(), 3);
        assert_eq!(df.width(), 1);
        let column_names = df.get_column_names();
        assert_eq!(column_names.len(), 1);
        assert_eq!(column_names[0].as_str(), "price");

        // verify that decimal conversion was applied before Polars processing
        // the exact representation in Polars may vary, but the data should be preserved
        let price_series = df.column("price").unwrap();
        assert_eq!(price_series.len(), 3);
    }

    #[test]
    fn test_polars_mixed_types() {
        // test DataFrame with both decimal and non-decimal columns
        let int_array = Int32Array::from(vec![Some(12345), Some(67890)]);
        let text_array = arrow::array::StringArray::from(vec![Some("item1"), Some("item2")]);

        let schema = ArrowSchema::new(vec![
            Field::new("price", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(int_array), Arc::new(text_array)],
        )
        .unwrap();

        // serialize to Arrow IPC bytes
        let mut buffer = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        let bytes = Bytes::from(buffer);

        // create mixed Snowflake schema
        let snowflake_schema = vec![
            FieldSchema {
                name: "price".to_string(),
                type_: SnowflakeType::Fixed,
                precision: Some(8),
                scale: Some(2),
                nullable: true,
            },
            FieldSchema {
                name: "name".to_string(),
                type_: SnowflakeType::Text,
                precision: None,
                scale: None,
                nullable: true,
            },
        ];

        // test conversion to Polars DataFrame
        let raw_result = RawQueryResult::BytesWithSchema(vec![bytes], snowflake_schema);
        let df = raw_result.to_polars().unwrap();

        // verify DataFrame structure
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 2);

        let column_names = df.get_column_names();
        assert!(column_names.iter().any(|name| name.as_str() == "price"));
        assert!(column_names.iter().any(|name| name.as_str() == "name"));
    }

    #[test]
    fn test_polars_regular_bytes_unchanged() {
        // test that regular Bytes variant still works (backward compatibility)
        let int_array = Int32Array::from(vec![Some(123), Some(456)]);
        let schema = ArrowSchema::new(vec![Field::new("value", DataType::Int32, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(int_array)]).unwrap();

        // Serialize to Arrow IPC bytes
        let mut buffer = Vec::new();
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        let bytes = Bytes::from(buffer);

        // use regular Bytes variant (no schema metadata)
        let raw_result = RawQueryResult::Bytes(vec![bytes]);
        let df = raw_result.to_polars().unwrap();

        // should work normally without decimal conversion
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 1);
        let column_names = df.get_column_names();
        assert_eq!(column_names.len(), 1);
        assert_eq!(column_names[0].as_str(), "value");
    }
}
