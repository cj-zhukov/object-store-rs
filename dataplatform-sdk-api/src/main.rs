use datafusion::prelude::SessionContext;
use lambda_runtime::{run, service_fn, Error};

use dataplatform_sdk_api::data_store::aws::init_table_ctx;
use dataplatform_sdk_api::error::init_error_handler;
use dataplatform_sdk_api::utils::aws::get_aws_client;
use dataplatform_sdk_api::utils::constants::{prod::*, INDEX_BUCKET_SECRET};
use dataplatform_sdk_api::utils::tracing::init_tracing;
use dataplatform_sdk_api::{handler, AppState};

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_error_handler()?;
    init_tracing();

    let client = get_aws_client(REGION.to_string()).await;
    let ctx = SessionContext::new();
    init_table_ctx(&ctx, REGION, &INDEX_BUCKET_SECRET, INDEX_PREFIX, TABLE_NAME) // object_store table init
        .await
        .map_err(|err| {
            tracing::error!(?err, "failed to init context");
            err
        })?;
    init_table_ctx(&ctx, REGION, &INDEX_BUCKET_SECRET, CATALOG_PREFIX, CATALOG_NAME) // object_store table init
        .await
        .map_err(|err| {
            tracing::error!(?err, "failed to init context");
            err
        })?;
    let app_state = AppState::new(client, ctx);

    run(service_fn(|event| async {
        handler(event, app_state.clone()).await.map_err(|err| {
            tracing::error!(?err, "lambda handler failed");
            err
        })
    }))
    .await?;
    Ok(())
}
