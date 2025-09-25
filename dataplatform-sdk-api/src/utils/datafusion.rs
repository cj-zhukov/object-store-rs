use std::sync::Arc;

use datafusion::{
    datasource::MemTable,
    error::DataFusionError,
    prelude::{DataFrame, SessionContext},
};
use tokio_stream::StreamExt;

use super::error::UtilsError;

pub async fn df_to_table(
    ctx: &SessionContext,
    df: DataFrame,
    table_name: &str,
) -> Result<(), UtilsError> {
    let schema = df.clone().schema().as_arrow().clone();
    let batches = df.collect().await?;
    let mem_table = MemTable::try_new(Arc::new(schema), vec![batches])?;
    ctx.register_table(table_name, Arc::new(mem_table))?;
    Ok(())
}

pub async fn is_empty(df: DataFrame) -> Result<bool, DataFusionError> {
    let mut stream = df.execute_stream().await?;
    if let Some(batch) = stream.next().await.transpose()? {
        if batch.num_rows() > 0 {
            return Ok(false);
        }
    }
    Ok(true)
}
