use std::collections::HashMap;

use crate::utils::constants::*;

use anyhow::Result;
use aws_config::{retry::RetryConfig, BehaviorVersion, Region};
use aws_sdk_s3::{
    config::Builder,
    operation::get_object::{GetObjectError, GetObjectOutput},
    Client,
};

pub async fn get_aws_client(region: &str) -> Client {
    let region = Region::new(region.to_string());
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region)
        .load()
        .await;
    let config_builder = Builder::from(&sdk_config)
        .retry_config(RetryConfig::standard().with_max_attempts(AWS_MAX_RETRIES));
    let config = config_builder.build();
    Client::from_conf(config)
}

pub async fn read_file(client: Client, bucket: &str, key: &str) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut object = get_file(client, bucket, key).await?;
    while let Some(bytes) = object.body.try_next().await? {
        buf.extend(bytes.to_vec());
    }
    Ok(buf)
}

pub async fn try_get_file(
    client: Client,
    bucket: &str,
    key: &str,
) -> Result<Option<GetObjectOutput>> {
    let resp = client.get_object().bucket(bucket).key(key);

    let res = resp.send().await;

    match res {
        Ok(res) => Ok(Some(res)),
        Err(sdk_err) => match sdk_err.into_service_error() {
            GetObjectError::NoSuchKey(_) => Ok(None),
            err => Err(err.into()),
        },
    }
}

pub async fn get_file(client: Client, bucket: &str, key: &str) -> Result<GetObjectOutput> {
    let resp = client.get_object().bucket(bucket).key(key).send().await?;
    Ok(resp)
}

pub async fn list_keys(client: Client, bucket: &str, prefix: &str) -> Result<Vec<String>> {
    let mut stream = client
        .list_objects_v2()
        .prefix(prefix)
        .bucket(bucket)
        .into_paginator()
        .send();

    let mut files = Vec::new();
    while let Some(objects) = stream.next().await.transpose()? {
        for obj in objects.contents() {
            if let Some(key) = obj.key() {
                if !key.ends_with('/') {
                    files.push(key.to_string());
                }
            }
        }
    }
    Ok(files)
}

pub async fn list_keys_to_map(
    client: Client,
    bucket: &str,
    prefix: &str,
) -> Result<HashMap<String, (Option<i64>, Option<String>)>> {
    let mut stream = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();

    let mut files: HashMap<String, (Option<i64>, Option<String>)> = HashMap::new();
    while let Some(objects) = stream.next().await.transpose()? {
        for obj in objects.contents() {
            if let Some(key) = obj.key() {
                if !key.ends_with('/') {
                    let file_name = key.to_string();
                    let file_size = obj.size();
                    let file_dt = obj.last_modified.map(|x| x.to_string());
                    files.insert(file_name, (file_size, file_dt));
                }
            }
        }
    }
    Ok(files)
}
