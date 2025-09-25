use std::sync::Arc;

use crate::utils::aws::read_file;
use crate::utils::constants::*;
use crate::WorkerError;

use async_zip::base::write::ZipFileWriter;
use async_zip::{Compression, ZipEntryBuilder};
use aws_sdk_s3::Client;
use futures::AsyncWriteExt;
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

#[tracing::instrument(level = "info", name = "processor", skip(client, other))]
pub async fn process(
    client: Arc<Client>,
    bucket: String,
    keys: Vec<String>,
    other: Vec<u8>,
    request_id: String,
) -> Result<Vec<u8>, WorkerError> {
    tracing::info!("start reading and zipping files");
    let mut zip_writer = ZipFileWriter::new(vec![]);
    let (tx, mut rx) = mpsc::channel::<(String, Vec<u8>)>(MAX_ASYNC_WORKERS * 10);
    let sem = Arc::new(Semaphore::new(MAX_ASYNC_WORKERS));
    let mut tasks = JoinSet::new();

    for key in keys {
        let permit = Arc::clone(&sem)
            .acquire_owned()
            .await
            .map_err(|e| WorkerError::UnexpectedError(e.into()))?;
        let tx = tx.clone();
        let client = client.clone();
        let bucket = bucket.clone();
        let file_name = key.rsplit('/').next().unwrap_or_default().to_string();

        tasks.spawn(async move {
            let _permit = permit;
            match read_file(client, bucket, key.clone()).await {
                Ok(bytes) => {
                    if let Err(e) = tx.send((file_name.clone(), bytes)).await {
                        tracing::error!("Failed to send file: {key} to zip task: {e:?}");
                    }
                }
                Err(e) => tracing::error!("Failed to read file: {key}: {e:?}"),
            }
        });
    }

    drop(tx);

    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            tracing::error!("File read task failed: {e:?}");
        }
    }

    while let Some((file_name, data)) = rx.recv().await {
        let builder = ZipEntryBuilder::new(file_name.into(), Compression::Deflate);
        let mut entry_writer = zip_writer.write_entry_stream(builder).await?;
        entry_writer.write_all(&data).await?;
        entry_writer.close().await?;
    }

    // add json file with query result
    {
        let builder = ZipEntryBuilder::new(
            format!("query-result-{request_id}.json").into(),
            Compression::Deflate,
        );
        let mut entry_writer = zip_writer.write_entry_stream(builder).await?;
        entry_writer.write_all(&other).await?;
        entry_writer.close().await?;
    }

    let buffer = zip_writer.close().await?;
    Ok(buffer)
}
