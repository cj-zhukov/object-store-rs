pub mod config;
pub mod file_data;
pub mod utils;

use config::Config;
use datafusion::prelude::SessionContext;
use file_data::FileData;
use utils::{aws::list_keys_to_map, datafusion::write_df_to_s3};

use std::path::Path;

use anyhow::Result;
use aws_sdk_s3::Client;
use uuid::Uuid;

pub async fn handler(client: Client, config: Config) -> Result<()> {
    tracing::info!("start running handler for data indexer");
    tracing::info!(
        "reading data from: {}{}",
        &config.prefix_source,
        &config.item_name
    );
    let prefix = format!("{}{}", &config.prefix_source, &config.item_name);
    let files = list_keys_to_map(client.clone(), &config.bucket_source, &prefix).await?;

    tracing::info!("start processing data");
    let mut file_data_all = vec![];
    for (file, file_metatada) in files {
        let path = Path::new(&file);
        let file_name = path.file_name().map(|x| x.to_string_lossy().to_string());
        let file_type = path.extension().map(|x| x.to_string_lossy().to_string());
        let file_url = Some(format!("s3://{}/{}", &config.bucket_source, file));
        let file_size = file_metatada.0;
        let dt = file_metatada.1;
        let file_data = FileData {
            file_name,
            file_type,
            file_path: Some(file),
            file_size,
            file_url,
            dt,
        };
        file_data_all.push(file_data);
    }

    tracing::info!("start converting to df");
    let ctx = SessionContext::new();
    let df = FileData::to_df(ctx, &file_data_all).await?;

    let id = Uuid::new_v4();
    let prefix_target = &config.prefix_target;
    let key = format!("{prefix_target}id={id}-table=data_index.parquet");
    tracing::info!("writing file to s3: {}", key);
    write_df_to_s3(client, &config.bucket_target, &key, df).await?;
    Ok(())
}
