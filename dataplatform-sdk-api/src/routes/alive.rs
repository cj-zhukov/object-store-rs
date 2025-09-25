use serde::{Deserialize, Serialize};

use crate::{ApiError, ApiResponse, ApiResponseKind};

#[derive(Deserialize, Serialize, Debug)]
pub struct PingResponse {
    pub result: Option<String>,
}

pub async fn ping() -> Result<ApiResponse, ApiError> {
    //#TODO make more compicated check
    let resp = PingResponse {
        result: Some("foo".to_string()),
    };
    let body = serde_json::to_string(&resp)?;
    let response = ApiResponseKind::Ok(Some(body)).try_into()?;
    Ok(response)
}
