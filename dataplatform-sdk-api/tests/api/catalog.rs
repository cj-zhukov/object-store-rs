use crate::constants::ADDRESS;
use crate::helpers::TestApp;
use dataplatform_sdk_api::{routes::CatalogResponse, utils::constants::test::*};

#[tokio::test]
async fn should_return_200_if_valid_input() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("select * from {CATALOG_NAME} limit 10"),
    });
    let response = app.post_catalog(&input).await;
    assert_eq!(response.status().as_u16(), 200);

    let response = response
        .json::<CatalogResponse>()
        .await
        .expect("Could not deserialize response body to Response");
    assert!(!response.result.is_empty());
}

#[tokio::test]
async fn should_return_400_if_invalid_input() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("foo bar baz"),
    });
    let response = app.post_catalog(&input).await;
    assert_eq!(response.status().as_u16(), 400);
}

#[tokio::test]
async fn should_return_404_if_valid_input() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("select * from {CATALOG_NAME} where order_id = 'order-id-that-doesnot-exist'"),
    });
    let response = app.post_catalog(&input).await;
    assert_eq!(response.status().as_u16(), 404);
}

#[tokio::test]
async fn should_return_404_for_order_id_is_null() {
    let app = TestApp::new(ADDRESS.to_string());
    let input = serde_json::json!({
        "query": format!("select * from {CATALOG_NAME} where order_id is null limit 5"),
    });
    let response = app.post_catalog(&input).await;
    assert_eq!(response.status().as_u16(), 404);
}
