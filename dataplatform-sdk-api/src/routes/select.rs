use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};

use crate::data_store::aws::Table;
use crate::{ApiError, ApiResponse, ApiResponseKind};

#[derive(Deserialize, Serialize, Debug)]
pub struct SelectResponse {
    pub result: Vec<Table>,
}

#[tracing::instrument(level = "info", name = "select", skip(ctx))]
pub async fn post_select(ctx: &SessionContext, query: &str) -> Result<ApiResponse, ApiError> {
    let df = Table::query(ctx, query)
        .await
        .map_err(|e| ApiError::UnexpectedError(e.into()))?;

    let response = match df {
        None => ApiResponseKind::NotFound.try_into()?,
        Some(df) => {
            let records = Table::df_to_records(df)
                .await
                .map_err(|e| ApiError::UnexpectedError(e.into()))?;

            let resp = SelectResponse { result: records };
            let body = serde_json::to_string(&resp)?;
            ApiResponseKind::Ok(Some(body)).try_into()?
        }
    };
    Ok(response)
}
