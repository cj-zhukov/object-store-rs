use std::time::Duration;

use aws_sdk_s3::{presigning::PresigningConfig, Client};
use color_eyre::eyre::Report;
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};

use crate::{
    data_store::aws::Table,
    error::ApiError,
    utils::{
        aws::{get_ecs_client, run_ecs_task, write_df_to_s3},
        constants::*,
        constants::prod::*,
        datafusion::is_empty,
    },
    ApiResponse, ApiResponseKind,
};

#[derive(Deserialize, Serialize, Debug)]
pub struct DownloadResponse {
    pub result: String,
}

#[tracing::instrument(level = "info", name = "download", skip(ctx, client))]
pub async fn post_download(
    client: &Client,
    ctx: &SessionContext,
    query: &str,
    request_id: &str,
) -> Result<ApiResponse, ApiError> {
    let df = Table::query(ctx, query)
        .await
        .map_err(|e| ApiError::UnexpectedError(e.into()))?;

    let response = match df {
        None => ApiResponseKind::NotFound.try_into()?,
        Some(df) => {
            // write parquet file to target s3, ecs then uses this file to get file names to process
            if is_empty(df.clone())
                .await
                .map_err(|e| ApiError::UnexpectedError(e.into()))?
            {
                return Err(ApiError::UnexpectedError(Report::msg(
                    "No file names for download found",
                )));
            }

            let file_list_key = format!("{}{}.parquet", DATA_BUCKET_SECRET.as_str(), request_id); // parquet file that contains query result
            tracing::info!("writing parquet file with query result: {}", file_list_key);
            write_df_to_s3(client, &DATA_BUCKET_SECRET, &file_list_key, df)
                .await
                .map_err(|e| ApiError::UnexpectedError(e.into()))?;

            // prepare zip file and presigned url
            let key = format!("{DATA_PREFIX}{request_id}.zip", ); // zip file that will be used to store all files
            tracing::info!("creating presigned object for key: {}", key);
            let get_object_request = client
                .get_object()
                .bucket(DATA_BUCKET_SECRET.as_str())
                .key(&key)
                .response_content_type("application/zip") // for browser
                .response_content_disposition("attachment; filename=\"download.zip\""); // for browser

            let presigning_config = PresigningConfig::builder()
                .expires_in(Duration::from_secs(PRESIGNED_TIMEOUT))
                .build()
                .map_err(|e| ApiError::UnexpectedError(e.into()))?;

            let presigned_url = get_object_request
                .presigned(presigning_config.clone())
                .await
                .map_err(|e| ApiError::UnexpectedError(e.into()))?;

            let resp = DownloadResponse {
                result: presigned_url.uri().to_string(),
            };
            let body = serde_json::to_string(&resp)?;

            // pass request_id to ecs task and start the task
            let ecs_client = get_ecs_client(REGION.to_string()).await;
            let subnets = SUBNETS_SECRET.as_slice().to_vec();
            let security_groups = SECURITY_GROUPS_SECRET.as_slice().to_vec();
            let _output = run_ecs_task(
                &ecs_client,
                &ECS_CLUSTER_SECRET,
                TASK_NAME,
                CONTAINER_NAME,
                Some(subnets),
                Some(security_groups),
                request_id,
            )
            .await
            .map_err(|e| ApiError::UnexpectedError(e.into()))?; //#TODO process output of ecs
            tracing::info!("starting ecs task");

            ApiResponseKind::Ok(Some(body)).try_into()?
        }
    };
    Ok(response)
}
