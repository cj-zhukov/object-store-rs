use std::env;

use anyhow::Result;
use serde::Deserialize;

struct Input {
    bucket_source: String,
    bucket_target: String,
    prefix_source: String,
    prefix_target: String,
    item_name: String,
    args: String,
}

impl Input {
    fn new() -> Result<Self> {
        let bucket_source = env::var("bucket_source")?;
        let bucket_target = env::var("bucket_target")?;
        let prefix_source = env::var("prefix_source")?;
        let prefix_target = env::var("prefix_target")?;
        let item_name = env::var("item_name")?;
        let args = env::var("args")?;

        Ok(Self {
            bucket_source,
            bucket_target,
            prefix_source,
            prefix_target,
            item_name,
            args,
        })
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub bucket_source: String,
    pub bucket_target: String,
    pub prefix_source: String,
    pub prefix_target: String,
    pub item_name: String,
    pub args: Args,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Args {
    pub region: Option<String>,
}

impl Config {
    pub fn new() -> Result<Self> {
        let input = Input::new()?;
        let args: Args = serde_json::from_str(&input.args)?;

        Ok(Self {
            bucket_source: input.bucket_source,
            bucket_target: input.bucket_target,
            prefix_source: input.prefix_source,
            prefix_target: input.prefix_target,
            item_name: input.item_name,
            args,
        })
    }

    pub fn create(
        bucket_source: &str,
        bucket_target: &str,
        prefix_source: &str,
        prefix_target: &str,
        item_name: &str,
        args: &str,
    ) -> Result<Self> {
        let args: Args = serde_json::from_str(args)?;

        Ok(Self {
            bucket_source: bucket_source.to_string(),
            bucket_target: bucket_target.to_string(),
            prefix_source: prefix_source.to_string(),
            prefix_target: prefix_target.to_string(),
            item_name: item_name.to_string(),
            args,
        })
    }
}

impl std::fmt::Display for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "config: item_name: {} bucket_source: {} bucket_target: {} prefix_source: {} prefix_target: {} args: {:?}",
        self.item_name, 
        self.bucket_source, 
        self.bucket_target, 
        self.prefix_source, 
        self.prefix_target, 
        self.args,
        )
    }
}
