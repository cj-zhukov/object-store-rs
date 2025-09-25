use std::time::Instant;

use data_indexer::config::Config;
use data_indexer::handler;
use data_indexer::utils::aws::get_aws_client;
use data_indexer::utils::constants::*;
use data_indexer::utils::tracing::init_tracing;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    tracing::info!("start processing");
    let now = Instant::now();
    let client = get_aws_client(REGION).await;
    let config = Config::new()?;
    handler(client, config).await?;
    tracing::info!("end processing elapsed: {:.2?}", now.elapsed());
    Ok(())
}
