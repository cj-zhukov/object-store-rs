use datafusion::arrow::array::{Array, Int64Array, StringViewArray};
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;

use super::error::DataStoreError;
use crate::utils::datafusion::is_empty;

#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogTable {
    pub year: Option<String>,
    pub file_type: Option<String>,
    pub cnt_file_type: Option<i64>,
    pub sum_file_size: Option<i64>,
}

impl CatalogTable {
    /// deserialize df to struct
    pub async fn df_to_records(df: DataFrame) -> Result<Vec<Self>, DataStoreError> {
        tracing::info!("converting catalog df to struct");
        let mut stream = df.execute_stream().await?;
        let mut records = vec![];
        while let Some(batch) = stream.next().await.transpose()? {
            let years = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("Expected a StringViewArray for file_sizes");

            let file_types = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("Expected a StringViewArray for file_paths");

            let cnt_file_types = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Expected a Int64Array for file_urls");

            let sum_file_sizes = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Expected a Int64Array for order_ids");

            for i in 0..batch.num_rows() {
                let year = if years.is_null(i) {
                    None
                } else {
                    Some(years.value(i).to_string())
                };

                let file_type = if file_types.is_null(i) {
                    None
                } else {
                    Some(file_types.value(i).to_string())
                };

                let cnt_file_type = if cnt_file_types.is_null(i) {
                    None
                } else {
                    Some(cnt_file_types.value(i))
                };

                let sum_file_size = if sum_file_sizes.is_null(i) {
                    None
                } else {
                    Some(sum_file_sizes.value(i))
                };

                records.push(Self {
                    year,
                    file_type,
                    cnt_file_type,
                    sum_file_size,
                });
            }
        }

        Ok(records)
    }

    async fn read(ctx: &SessionContext, query: &str) -> Result<DataFrame, DataStoreError> {
        let df = ctx.sql(query).await?;
        Ok(df)
    }

    /// query object_store catalog table to dataframe
    pub async fn query(
        ctx: &SessionContext,
        query: &str,
    ) -> Result<Option<DataFrame>, DataStoreError> {
        tracing::info!("quering object_store_catalog table");
        let df = CatalogTable::read(ctx, query).await?;
        if is_empty(df.clone()).await? {
            return Ok(None);
        }
        Ok(Some(df))
    }
}
