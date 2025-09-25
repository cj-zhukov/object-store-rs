use async_zip::error::ZipError;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::put_object::PutObjectError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use aws_smithy_types::byte_stream::error::Error as AWSSmithyError;
use color_eyre::eyre::Report;
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use datafusion::parquet::errors::ParquetError;
use std::io::Error as IoError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("Arrow error")]
    ArrowError(#[from] ArrowError),

    #[error("Io error")]
    IoError(#[from] IoError),

    #[error("DataFusion error")]
    DataFusionError(#[from] DataFusionError),

    #[error("AWS Sdk error")]
    SdkError(#[from] SdkError<GetObjectError>),

    #[error("AWSSmithy error")]
    AWSSmithyError(#[from] AWSSmithyError),

    #[error("AWS PutObjectError error")]
    PutObjectError(#[from] SdkError<PutObjectError>),

    #[error("ParquetError")]
    ParquetError(#[from] ParquetError),

    #[error("AWS UploadPartError error")]
    UploadPartError(#[from] SdkError<UploadPartError>),

    #[error("Zip error")]
    ZipError(#[from] ZipError),

    #[error("Unexpected error")]
    UnexpectedError(#[source] Report),
}
