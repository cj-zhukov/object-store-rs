pub mod error;
pub mod utils;
mod worker;

use std::sync::Arc;
use std::time::Instant;

use datafusion::prelude::SessionContext;
pub use error::WorkerError;
use worker::process;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use crate::utils::aws::read_file_to_df;
use crate::utils::datafusion::{df_to_json_bytes, get_files_names};

#[tracing::instrument(level = "info", name = "handler", skip(client))]
pub async fn handler(
    client: Arc<Client>,
    bucket: String,
    prefix: String,
    request_id: String,
) -> Result<(), WorkerError> {
    let start = Instant::now();
    let ctx = SessionContext::new();
    tracing::info!({ request_id }, "starting handler");
    let keys_file = format!("{prefix}{request_id}.parquet");
    let key = format!("{prefix}{request_id}.zip");
    let df = read_file_to_df(client.clone(), &ctx, bucket.clone(), keys_file.clone()).await?;
    let json_data = df_to_json_bytes(df.clone()).await?;
    let keys = get_files_names(df).await?;

    tracing::info!({ file_name = %keys_file }, "processing files");
    let data = process(client.clone(), bucket.clone(), keys, json_data, request_id).await?;

    tracing::info!({ prefix = %key }, "coping data");
    let body = ByteStream::from(data);
    let _resp = client
        .put_object()
        .bucket(bucket)
        .key(&key)
        .body(body)
        .content_type("application/zip") // for browser
        .content_disposition("attachment; filename=\"download.zip\"") // for browser
        .send()
        .await?;

    let exec_time = start.elapsed().as_secs();
    tracing::info!({ duration = %exec_time }, "finishing handler");
    Ok(())
}
