use std::io::BufRead;
use std::time::Duration;

use crate::constants::ADDRESS;
use crate::helpers::{unzip_file, TestApp};
use dataplatform_sdk_api::routes::DownloadResponse;
use dataplatform_sdk_api::utils::constants::test::*;

use reqwest::StatusCode;
use tempfile::tempdir;
use tokio::fs::File;
use tokio::io::copy;
use tokio::time::sleep;
use tokio_stream::StreamExt;

const MAX_RETRIES: usize = 30;
const RETRY_DELAY_SECS: u64 = 5;
const TMP_FILE_NAME: &str = "downloaded_file.zip";
const TMP_FILE: &str = "BG_20211124_092859_0.0.9.2672.log.txt";
const TMP_ORDER: &str = "2021-11-24T09-33-38-003YB";

#[tokio::test]
async fn should_return_200_if_valid_input() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("select * from {TABLE_NAME} where order_id = '{TMP_ORDER}' and file_name = '{TMP_FILE}'"),
    });
    let response = app.post_download(&input).await;
    assert_eq!(response.status().as_u16(), 200);

    let response = response
        .json::<DownloadResponse>()
        .await
        .expect("Could not deserialize response body to Response");
    assert!(!response.result.is_empty());

    let url = response.result;
    let tmp_dir = tempdir().expect("Failed to get tmpdir working directory");
    let tmp_file_path = tmp_dir.path().join(TMP_FILE_NAME);
    for _ in 1..MAX_RETRIES {
        let response = reqwest::get(url.clone()).await.unwrap();
        if response.status() == StatusCode::OK {
            let mut dest = File::create(&tmp_file_path).await.unwrap();
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.unwrap();
                copy(&mut chunk.as_ref(), &mut dest).await.unwrap();
            }
        } else {
            sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
        }
    }

    let archive = File::open(&tmp_file_path)
        .await
        .expect("Failed to open zip file");
    unzip_file(archive, tmp_dir.path()).await;
    let input = tmp_dir.path().join(TMP_FILE);
    let input = std::fs::File::open(&input).unwrap();
    let buffered = std::io::BufReader::new(input);
    let mut data = vec![];
    for line in buffered.lines().take(3) {
        data.push(line.unwrap());
    }
    let expected = vec![
        "Line#, Log level, Time stamp, Thread, Source, Message",
        "1, INFO, 2021-11-24 09:28:59.8456, BackgroundMain, Logger Init parameters successfully ",
        "2, INFO, 2021-11-24 09:29:00.1249, BackgroundMain, , BackgroundModeStartup::Startup",
    ];
    assert_eq!(data, expected);
}

#[tokio::test]
async fn should_return_400_if_invalid_input() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("foo bar baz"),
    });
    let response = app.post_download(&input).await;
    assert_eq!(response.status().as_u16(), 400);
}

#[tokio::test]
async fn should_return_404_if_valid_input() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("select * from {TABLE_NAME} where file_name = 'file-that-doesnot-exist'"),
    });
    let response = app.post_download(&input).await;
    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test]
async fn should_return_404_for_order_id_is_null() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("select * from {TABLE_NAME} where order_id is null limit 5"),
    });
    let response = app.post_select(&input).await;
    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test]
async fn should_return_404_for_study_is_null() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("select * from {TABLE_NAME} where study is null limit 5"),
    });
    let response = app.post_select(&input).await;
    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test]
async fn should_return_404_for_scanner_is_null() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("select * from {TABLE_NAME} where scanner_type is null limit 5"),
    });
    let response = app.post_select(&input).await;
    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test]
async fn should_return_404_for_data_type_is_null() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("select * from {TABLE_NAME} where data_type is null limit 5"),
    });
    let response = app.post_select(&input).await;
    assert_eq!(response.status().as_u16(), 404);
}
