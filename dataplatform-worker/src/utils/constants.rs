use std::{env as std_env, sync::LazyLock};

use dotenvy::dotenv;

pub const PREFIX: &str = "presigned/";
pub const REGION: &str = "eu-central-1";
pub const MAX_ASYNC_WORKERS: usize = 10; // how many files process concurrently
pub const CHUNK_SIZE: u64 = 10_000_000; // 10 MiB
pub const PARALLEL_THRESHOLD: u64 = 300_000_000; // 300 MiB
pub const CHUNKS_WORKERS: usize = 10; // max workers chunks for file
pub const MAX_ATTEMPTS: usize = 5;

pub mod env {
    pub const REQ_ID_ENV_VAR: &str = "REQUEST_ID"; // request_id is used for zip & json files
    pub const BUCKET_ENV_VAR: &str = "BUCKET";
}

pub static REQUEST_ID: LazyLock<String> = LazyLock::new(|| {
    dotenv().ok();
    let secret = std_env::var(env::REQ_ID_ENV_VAR).expect("REQUEST_ID must be set.");
    if secret.is_empty() {
        panic!("REQUEST_ID must not be empty.");
    }
    secret
});

pub static BUCKET: LazyLock<String> = LazyLock::new(|| {
    dotenv().ok();
    let secret = std_env::var(env::REQ_ID_ENV_VAR).expect("BUCKET must be set.");
    if secret.is_empty() {
        panic!("BUCKET must not be empty.");
    }
    secret
});
