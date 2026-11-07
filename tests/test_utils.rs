#![allow(unused)]
use anyhow::{Context, Result};
use bunshin::protocol::AppendEntryReq;
use std::fs;

pub const TEST_NODE_ID: u64 = 1;
pub const EXTERNAL_NODE_ID: u64 = 2;
pub const TIMEOUT_DURATION_SECS: u64 = 2;

pub fn remove_file(path: &str) -> Result<()> {
    if fs::exists(path)? {
        fs::remove_file(path)
            .with_context(|| format!("could not delete temp_file"))
            .unwrap()
    }

    Ok(())
}

pub fn test_listen_addr() -> String {
    "localhost:9000".to_string()
}

pub fn test_peers() -> Vec<String> {
    vec!["localhost:9001".to_string()]
}

pub fn default_append_entries() -> Vec<AppendEntryReq> {
    vec![
        AppendEntryReq::default(EXTERNAL_NODE_ID),
        AppendEntryReq::default(EXTERNAL_NODE_ID),
        AppendEntryReq::default(EXTERNAL_NODE_ID),
    ]
}
