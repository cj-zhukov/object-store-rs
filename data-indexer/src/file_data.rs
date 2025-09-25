use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::prelude::*;

pub struct FileData {
    pub file_name: Option<String>,
    pub file_type: Option<String>,
    pub file_path: Option<String>,
    pub file_size: Option<i64>,
    pub file_url: Option<String>,
    pub dt: Option<String>,
}

impl FileData {
    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("file_name", DataType::Utf8, true),
            Field::new("file_type", DataType::Utf8, true),
            Field::new("file_size", DataType::Int64, true),
            Field::new("file_path", DataType::Utf8, true),
            Field::new("file_url", DataType::Utf8, true),
            Field::new("dt", DataType::Utf8, true),
        ])
    }

    fn to_record_batch(records: &[Self]) -> Result<RecordBatch> {
        let schema = FileData::schema();

        let file_names = records.iter().map(|r| r.file_name.as_deref()).collect::<Vec<_>>();
        let file_types = records.iter().map(|r| r.file_type.as_deref()).collect::<Vec<_>>();
        let file_sizes = records.iter().map(|r| r.file_size).collect::<Vec<_>>();
        let file_paths = records.iter().map(|r| r.file_path.as_deref()).collect::<Vec<_>>();
        let file_urls = records.iter().map(|r| r.file_url.as_deref()).collect::<Vec<_>>();
        let dts = records.iter().map(|r| r.dt.as_deref()).collect::<Vec<_>>();

        Ok(RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(StringArray::from(file_names)),
                Arc::new(StringArray::from(file_types)),
                Arc::new(Int64Array::from(file_sizes)),
                Arc::new(StringArray::from(file_paths)),
                Arc::new(StringArray::from(file_urls)),
                Arc::new(StringArray::from(dts)),
            ],
        )?)
    }
}

impl FileData {
    pub async fn to_df(
        ctx: SessionContext, 
        records: &[Self],
    ) -> Result<DataFrame> {
        let batch = Self::to_record_batch(records)?;
        let df = ctx.read_batch(batch)?;
        Ok(df)
    }
}
