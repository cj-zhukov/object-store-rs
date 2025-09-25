use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use crate::utils::constants::*;
use crate::WorkerError;

use aws_config::retry::RetryConfig;
use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::config::Builder;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::Client;
use bytes::Bytes;
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::prelude::*;
use futures::TryStreamExt;
use tokio::io::AsyncReadExt;
use tokio::sync::Semaphore;

pub async fn get_aws_object(
    client: Arc<Client>,
    bucket: &str,
    key: &str,
) -> Result<GetObjectOutput, WorkerError> {
    let req = client.get_object().bucket(bucket).key(key);
    let res = req.send().await?;
    Ok(res)
}

pub async fn get_aws_client(region: String) -> Client {
    let region = Region::new(region);
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region)
        .load()
        .await;
    let timeout = TimeoutConfig::builder()
        .operation_timeout(Duration::from_secs(60 * 5))
        .operation_attempt_timeout(Duration::from_secs(60 * 5))
        .connect_timeout(Duration::from_secs(60 * 5))
        .build();
    let config_builder = Builder::from(&sdk_config)
        .timeout_config(timeout)
        .retry_config(RetryConfig::standard().with_max_attempts(10));
    let config = config_builder.build();
    Client::from_conf(config)
}

/// Read file from aws s3
pub async fn read_file(
    client: Arc<Client>,
    bucket: String,
    key: String,
) -> Result<Vec<u8>, WorkerError> {
    let object = retry(|| {
        let client = client.clone();
        let bucket = bucket.clone();
        let key = key.clone();
        async move { get_aws_object(client, &bucket, &key).await }
    })
    .await?;
    let size = object.content_length().unwrap_or(0) as u64;
    if size == 0 {
        tracing::error!("Size of file: {key} is 0");
        return Ok(Vec::new());
    }

    if size <= CHUNK_SIZE {
        // Small file: download whole body
        retry(|| {
            let client = client.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            async move {
                let object = get_aws_object(client.clone(), &bucket, &key).await?;
                let mut reader = object.body.into_async_read();
                let mut buf = Vec::with_capacity(size as usize);
                reader.read_to_end(&mut buf).await?;
                Ok(buf)
            }
        })
        .await
    } else if size <= PARALLEL_THRESHOLD {
        // Medium file: sequential chunks
        retry(|| {
            let client = client.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            async move {
                let object = get_aws_object(client.clone(), &bucket, &key).await?;
                let mut buf = Vec::with_capacity(size as usize);
                let mut stream = object.body;
                while let Some(chunk) = stream.try_next().await? {
                    buf.extend_from_slice(&chunk);
                }
                Ok(buf)
            }
        })
        .await
    } else {
        // Large file: parallel download of chunks
        let mut ranges = vec![];
        for start in (0..size).step_by(CHUNK_SIZE as usize) {
            let end = (start + CHUNK_SIZE - 1).min(size - 1);
            ranges.push((start, end));
        }

        let semaphore = Arc::new(Semaphore::new(CHUNKS_WORKERS));
        let mut tasks = vec![];
        let ranges_len = ranges.clone().len();

        for (i, (start, end)) in ranges.into_iter().enumerate() {
            let client = client.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| WorkerError::UnexpectedError(e.into()))?;

            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                retry(|| {
                    let client = client.clone();
                    let bucket = bucket.clone();
                    let key = key.clone();
                    async move {
                        let range = format!("bytes={}-{}", start, end);
                        let out = client
                            .get_object()
                            .bucket(&bucket)
                            .key(&key)
                            .range(range)
                            .send()
                            .await?;
                        let bytes = out.body.collect().await?.into_bytes();
                        Ok::<(usize, Bytes), WorkerError>((i, bytes))
                    }
                })
                .await
            }));
        }

        let mut results = vec![Bytes::new(); ranges_len];
        for task in tasks {
            let (i, chunk) = task
                .await
                .map_err(|e| WorkerError::UnexpectedError(e.into()))?
                .map_err(|e| WorkerError::UnexpectedError(e.into()))?;
            results[i] = chunk;
        }

        let total_size: usize = results.iter().map(|b| b.len()).sum();
        let mut buf = Vec::with_capacity(total_size);
        for chunk in results {
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    }
}

async fn retry<F, Fut, T>(mut operation: F) -> Result<T, WorkerError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, WorkerError>>,
{
    let mut attempts = 0;
    loop {
        match operation().await {
            Ok(val) => return Ok(val),
            Err(e) if attempts < MAX_ATTEMPTS => {
                attempts += 1;
                tracing::warn!("Retry {attempts} after error: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Read parquet file to dataframe
pub async fn read_file_to_df(
    client: Arc<Client>,
    ctx: &SessionContext,
    bucket: String,
    key: String,
) -> Result<DataFrame, WorkerError> {
    let buf = read_file(client, bucket, key).await?;
    let stream = ParquetRecordBatchStreamBuilder::new(Cursor::new(buf))
        .await?
        .build()?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    let res = ctx.read_batches(batches)?;
    Ok(res)
}
