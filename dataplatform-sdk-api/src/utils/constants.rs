use dotenvy::dotenv;
use std::env as std_env;
use std::sync::LazyLock;

pub mod prod {
    pub const REGION: &str = "eu-central-1";
    pub const INDEX_PREFIX: &str = "index/combined/";
    pub const DATA_PREFIX: &str = "presigned/"; 
    pub const CATALOG_PREFIX: &str = "catalog/";
    pub const MAX_ROWS: u64 = 10;
    pub const MAX_ROWS_CATALOG: u64 = 1000;
    pub const TABLE_NAME: &str = "object_store";
    pub const CATALOG_NAME: &str = "object_store_catalog";
    pub const PRESIGNED_TIMEOUT: u64 = 3600;
}

pub mod test {
    pub const REGION: &str = "eu-central-1";
    pub const INDEX_PREFIX: &str = "index/combined/";
    pub const DATA_PREFIX: &str = "presigned/"; 
    pub const CATALOG_PREFIX: &str = "catalog/";
    pub const MAX_ROWS: u64 = 10;
    pub const MAX_ROWS_CATALOG: u64 = 10;
    pub const TABLE_NAME: &str = "object_store";
    pub const CATALOG_NAME: &str = "object_store_catalog";
    pub const PRESIGNED_TIMEOUT: u64 = 1800;
}

pub mod env {
    pub const DATA_BUCKET_ENV_VAR: &str = "DATA_BUCKET";
    pub const INDEX_BUCKET_ENV_VAR: &str = "INDEX_BUCKET";   
    pub const ECS_CLUSTER_ENV_VAR: &str = "ECS_CLUSTER";   
    pub const SUBNETS_ENV_VAR: &str = "SUBNETS";  
    pub const SECURITY_GROUPS_ENV_VAR: &str = "SECURITY_GROUPS";    
}

pub static DATA_BUCKET_SECRET: LazyLock<String> = LazyLock::new(|| {
    dotenv().ok();
    let secret = std_env::var(env::DATA_BUCKET_ENV_VAR)
        .expect("DATA_BUCKET must be set.");
    if secret.is_empty() {
        panic!("DATA_BUCKET must not be empty.");
    }
    secret
});

pub static INDEX_BUCKET_SECRET: LazyLock<String> = LazyLock::new(|| {
    dotenv().ok();
    let secret = std_env::var(env::INDEX_BUCKET_ENV_VAR)
        .expect("INDEX_BUCKET must be set.");
    if secret.is_empty() {
        panic!("INDEX_BUCKET must not be empty.");
    }
    secret
});

pub static ECS_CLUSTER_SECRET: LazyLock<String> = LazyLock::new(|| {
    dotenv().ok();
    let secret = std_env::var(env::ECS_CLUSTER_ENV_VAR)
        .expect("ECS_CLUSTER must be set.");
    if secret.is_empty() {
        panic!("ECS_CLUSTER must not be empty.");
    }
    secret
});

pub static SUBNETS_SECRET: LazyLock<Vec<String>> = LazyLock::new(|| {
    dotenv().ok();
    let secret = std_env::var(env::SUBNETS_ENV_VAR)
        .expect("SUBNETS must be set.");
    if secret.is_empty() {
        panic!("SUBNETS must not be empty.");
    }
    secret.split('.').map(|x| x.trim().to_string()).collect()
});

pub static SECURITY_GROUPS_SECRET: LazyLock<Vec<String>> = LazyLock::new(|| {
    dotenv().ok();
    let secret = std_env::var(env::SECURITY_GROUPS_ENV_VAR)
        .expect("SECURITY_GROUPS must be set.");
    if secret.is_empty() {
        panic!("SECURITY_GROUPS must not be empty.");
    }
    secret.split('.').map(|x| x.trim().to_string()).collect()
});

pub const CONTAINER_NAME: &str = "datalake-worker";
pub const TASK_NAME: &str = "datalake-worker-run-dev";
