use crate::constants::ADDRESS;
use crate::helpers::TestApp;
use dataplatform_sdk_api::utils::constants::test::*;

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
