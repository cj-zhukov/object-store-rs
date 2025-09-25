use awscreds::error::CredentialsError as AWSCredentialsError;
use color_eyre::eyre::Report;
use datafusion::error::DataFusionError;
use thiserror::Error;
use tokio::task::JoinError;
use url::ParseError;

#[derive(Debug, Error)]
pub enum DataStoreError {
    #[error("AWSCredentialsError")]
    AWSCredentialsError(#[from] AWSCredentialsError),

    #[error("DataFusion error")]
    DataFusionError(#[from] DataFusionError),

    #[error("URL parse error")]
    ParseError(#[from] ParseError),

    #[error("Tokio error")]
    TokioError(#[from] JoinError),

    #[error("Unexpected error")]
    UnexpectedError(#[source] Report),
}
