use crate::WorkerError;

use datafusion::{arrow::array::StringViewArray, prelude::*};
use tokio_stream::StreamExt;

/// get file names from df table with links to s3 location to download
pub async fn get_files_names(df: DataFrame) -> Result<Vec<String>, WorkerError> {
    tracing::info!("selecting file names");
    let df = df.select_columns(&["file_path"])?;
    let mut stream = df.execute_stream().await?;
    let mut keys = vec![];
    while let Some(batch) = stream.next().await.transpose()? {
        let file_pathes = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("Expected a StringViewArray for file_pathes");
        for name in file_pathes {
            match name {
                Some(k) => keys.push(k.to_string()),
                None => tracing::error!("found none file path in batch"),
            };
        }
    }
    Ok(keys)
}

pub async fn df_to_json_bytes(df: DataFrame) -> Result<Vec<u8>, WorkerError> {
    let batches = df.collect().await?;
    let buf = vec![];
    let mut writer = arrow_json::ArrayWriter::new(buf);
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.finish()?;
    let data = writer.into_inner();
    Ok(data)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    use color_eyre::Result;
    use datafusion::arrow::{
        array::{Int32Array, RecordBatch, StringArray, StringViewArray},
        datatypes::{DataType, Field, Schema},
    };
    use serde_json::Value;

    #[tokio::test]
    async fn test_df_to_json_bytes() -> Result<()> {
        let ctx = SessionContext::new();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
            ],
        )?;
        let df = ctx.read_batch(batch)?;
        let res = df_to_json_bytes(df).await?;
        let value: Value = serde_json::from_slice(&res)?;
        assert_eq!(
            value.to_string(),
            r#"[{"id":1,"name":"foo"},{"id":2,"name":"bar"},{"id":3,"name":"baz"}]"#
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_get_files_names() -> Result<()> {
        let ctx = SessionContext::new();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("file_path", DataType::Utf8View, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["foo", "bar", "baz"])),
                Arc::new(StringViewArray::from(vec![
                    "foo/path/",
                    "bar/path/",
                    "baz/path/",
                ])),
            ],
        )?;
        let df = ctx.read_batch(batch)?;
        let res = get_files_names(df).await?;
        assert_eq!(res, vec!["foo/path/", "bar/path/", "baz/path/"]);
        Ok(())
    }
}
