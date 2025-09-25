use dataplatform_sdk_api::utils::constants::test::*;

use crate::constants::ADDRESS;
use crate::helpers::TestApp;

#[tokio::test]
async fn test_alive() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("select * from {TABLE_NAME} limit 5"),
    });
    let response = app.get_alive(&input).await;
    assert_eq!(response.status().as_u16(), 200);
}
