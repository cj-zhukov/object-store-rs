use std::time::Instant;
use std::{collections::HashMap, sync::Arc};

use aws_sdk_s3::Client;
use datafusion::prelude::*;
use http::Response;
use lambda_runtime::LambdaEvent;
use serde::{Deserialize, Serialize};

pub mod data_store;
pub mod error;
pub mod routes;
pub mod utils;

use error::ApiError;
use routes::{ping, post_download, post_select};
use utils::queryparser::prepare_query;

use crate::routes::{post_catalog, ApiRoute};
use crate::utils::queryparser::QueryKind;

pub enum ApiResponseKind {
    Ok(Option<String>),
    NotFound,
    BadRequest,
}

#[derive(Deserialize, Debug)]
pub struct ApiRequest {
    #[serde(rename = "httpMethod")]
    pub method: String,
    pub path: String,
    pub body: String,
    #[serde(rename = "requestContext")]
    pub request_context: RequestContext,
}

#[derive(Deserialize, Debug)]
pub struct RequestContext {
    pub identity: Identity,
}

#[derive(Deserialize, Debug)]
pub struct Identity {
    #[serde(rename = "sourceIp")]
    pub source_ip: Option<String>,
    #[serde(rename = "userAgent")]
    pub user_agent: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ApiResponse {
    #[serde(rename = "statusCode")]
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Option<String>,
}

impl ApiResponse {
    fn new(response: Response<Option<String>>) -> Self {
        let status = response.status().as_u16();
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers.insert("Access-Control-Allow-Origin".to_string(), "*".to_string());
        headers.insert("Access-Control-Allow-Headers".to_string(), "*".to_string());
        headers.insert(
            "Access-Control-Allow-Methods".to_string(),
            "POST, GET, OPTIONS".to_string(),
        );
        let body = response.body().to_owned();
        Self {
            status,
            headers,
            body,
        }
    }
}

impl TryFrom<ApiResponseKind> for ApiResponse {
    type Error = ApiError;

    fn try_from(kind: ApiResponseKind) -> Result<Self, Self::Error> {
        let response = match kind {
            ApiResponseKind::NotFound => Response::builder().status(404).body(None)?,
            ApiResponseKind::BadRequest => Response::builder().status(400).body(None)?,
            ApiResponseKind::Ok(body) => Response::builder().status(200).body(body)?,
        };
        Ok(ApiResponse::new(response))
    }
}

#[derive(Deserialize, Debug)]
struct Query {
    pub query: String,
}

pub struct AppState {
    pub client: Client,
    pub ctx: SessionContext,
}

impl AppState {
    pub fn new(client: Client, ctx: SessionContext) -> Arc<Self> {
        Arc::new(Self { client, ctx })
    }
}

#[tracing::instrument(level = "info", name = "handler", skip(event, state))]
pub async fn handler(
    event: LambdaEvent<ApiRequest>,
    state: Arc<AppState>,
) -> Result<ApiResponse, ApiError> {
    let start = Instant::now();
    let (request, context) = event.into_parts();
    let method = request.method;
    let path = request.path;
    let body = request.body;
    let request_id = context.request_id;
    let user_ip = request.request_context.identity.source_ip;
    let user_agent = request.request_context.identity.user_agent;
    tracing::info!({ user_ip, user_agent, path, method, query = %body }, "starting handler");

    let route: ApiRoute = match (method.as_str(), path.as_str()).try_into() {
        Ok(route) => route,
        Err(e) => {
            tracing::error!("{e}, query: {body}");
            return ApiResponseKind::BadRequest.try_into();
        }
    };

    let response = match route {
        ApiRoute::AliveGet => ping().await?,

        ApiRoute::SelectPost => {
            handle_query(&body, QueryKind::SelectDownload, |query| async move {
                post_select(&state.ctx, &query).await
            })
            .await?
        }

        ApiRoute::DownloadPost => {
            handle_query(&body, QueryKind::SelectDownload, |query| async move {
                post_download(&state.client, &state.ctx, &query, &request_id).await
            })
            .await?
        }

        ApiRoute::CatalogPost => {
            handle_query(&body, QueryKind::Catalog, |query| async move {
                post_catalog(&state.ctx, &query).await
            })
            .await?
        }
    };

    let exec_time = start.elapsed().as_secs();
    tracing::info!({ duration = %exec_time }, "finishing handler");
    Ok(response)
}

async fn handle_query<F, Fut>(body: &str, kind: QueryKind, f: F) -> Result<ApiResponse, ApiError>
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = Result<ApiResponse, ApiError>>,
{
    let query = match serde_json::from_str::<Query>(body) {
        Ok(q) => q,
        Err(e) => {
            tracing::error!("failed parsing query: {} cause: {}", body, e);
            return ApiResponseKind::BadRequest.try_into();
        }
    };

    let prepared = match prepare_query(&query.query, kind) {
        Ok(q) => {
            tracing::info!({ q }, "preparing query");
            q
        }
        Err(e) => {
            tracing::error!("failed preparing query: {} cause: {}", body, e);
            return ApiResponseKind::BadRequest.try_into();
        }
    };

    f(prepared).await
}
