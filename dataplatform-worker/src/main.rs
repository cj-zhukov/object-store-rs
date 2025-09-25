use std::sync::Arc;

use color_eyre::Result;

use dataplatform_worker::handler;
use dataplatform_worker::utils::aws::get_aws_client;
use dataplatform_worker::utils::constants::*;
use dataplatform_worker::utils::tracing::init_tracing;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing()?;

    let client = get_aws_client(REGION.to_string()).await;
    let client_ref = Arc::new(client);

    handler(
        client_ref,
        BUCKET.to_string(),
        PREFIX.to_string(),
        REQUEST_ID.to_string(),
    )
    .await?;
    Ok(())
}
