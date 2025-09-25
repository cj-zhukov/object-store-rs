use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};

use crate::{data_store::catalog::CatalogTable, ApiError, ApiResponse, ApiResponseKind};

#[derive(Deserialize, Serialize, Debug)]
pub struct CatalogResponse {
    pub result: Vec<CatalogTable>,
}

#[tracing::instrument(level = "info", name = "catalog", skip(ctx))]
pub async fn post_catalog(ctx: &SessionContext, query: &str) -> Result<ApiResponse, ApiError> {
    let df = CatalogTable::query(ctx, query)
        .await
        .map_err(|e| ApiError::UnexpectedError(e.into()))?;

    let response = match df {
        None => ApiResponseKind::NotFound.try_into()?,
        Some(df) => {
            let records = CatalogTable::df_to_records(df)
                .await
                .map_err(|e| ApiError::UnexpectedError(e.into()))?;

            let resp = CatalogResponse { result: records };
            let body = serde_json::to_string(&resp)?;
            ApiResponseKind::Ok(Some(body)).try_into()?
        }
    };
    Ok(response)
}
