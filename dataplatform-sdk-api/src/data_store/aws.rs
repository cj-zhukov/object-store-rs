use awscreds::Credentials;
use datafusion::arrow::array::{Array, Int64Array, StringViewArray};
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

use std::sync::Arc;
use url::Url;

use super::error::DataStoreError;
use crate::utils::datafusion::is_empty;

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub file_name: Option<String>,
    pub file_type: Option<String>,
    pub file_size: Option<i64>,
    pub file_path: Option<String>,
    pub file_url: Option<String>,
    pub dt: Option<String>,
}

impl Table {
    /// deserialize df to struct
    pub async fn df_to_records(df: DataFrame) -> Result<Vec<Self>, DataStoreError> {
        tracing::info!("converting df to struct");
        let mut stream = df.execute_stream().await?;
        let mut records = vec![];
        while let Some(batch) = stream.next().await.transpose()? {
            let file_names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("Expected a StringViewArray for file_names");

            let file_types = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("Expected a StringViewArray for file_types");

            let file_sizes = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Expected a Int64Array for file_sizes");

            let file_paths = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("Expected a StringViewArray for file_paths");

            let file_urls = batch
                .column(4)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("Expected a StringViewArray for file_urls");

            let dts = batch
                .column(5)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("Expected a StringViewArray for dts");

            for i in 0..batch.num_rows() {
                let file_name = if file_names.is_null(i) {
                    None
                } else {
                    Some(file_names.value(i).to_string())
                };

                let file_type = if file_types.is_null(i) {
                    None
                } else {
                    Some(file_types.value(i).to_string())
                };

                let file_size = if file_sizes.is_null(i) {
                    None
                } else {
                    Some(file_sizes.value(i))
                };

                let file_path = if file_paths.is_null(i) {
                    None
                } else {
                    Some(file_paths.value(i).to_string())
                };

                let file_url = if file_urls.is_null(i) {
                    None
                } else {
                    Some(file_urls.value(i).to_string())
                };

                let dt = if dts.is_null(i) {
                    None
                } else {
                    Some(dts.value(i).to_string())
                };

                records.push(Self {
                    file_name,
                    file_type,
                    file_size,
                    file_path,
                    file_url,
                    dt,
                });
            }
        }

        Ok(records)
    }

    async fn read(ctx: &SessionContext, query: &str) -> Result<DataFrame, DataStoreError> {
        let df = ctx.sql(query).await?;
        Ok(df)
    }

    /// query object_store table to dataframe
    pub async fn query(
        ctx: &SessionContext,
        query: &str,
    ) -> Result<Option<DataFrame>, DataStoreError> {
        tracing::info!("quering object_store table");
        let df = Table::read(ctx, query).await?;
        if is_empty(df.clone()).await? {
            return Ok(None);
        }
        Ok(Some(df))
    }
}

pub async fn init_table_ctx(
    ctx: &SessionContext,
    region: &str,
    bucket: &str,
    key: &str,
    table_name: &str,
) -> Result<(), DataStoreError> {
    let creds = Credentials::default()?;
    let aws_access_key_id = creds.access_key.unwrap_or_default();
    let aws_secret_access_key = creds.secret_key.unwrap_or_default();
    let aws_session_token = creds.session_token.unwrap_or_default();

    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(aws_access_key_id)
        .with_secret_access_key(aws_secret_access_key)
        .with_token(aws_session_token)
        .build()
        .map_err(|e| DataStoreError::UnexpectedError(e.into()))?;

    let path = format!("s3://{bucket}");
    let s3_url = Url::parse(&path)?;
    ctx.runtime_env()
        .register_object_store(&s3_url, Arc::new(s3));
    let path = format!("s3://{bucket}/{key}");
    ctx.register_parquet(table_name, &path, ParquetReadOptions::default())
        .await?;
    Ok(())
}
