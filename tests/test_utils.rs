#![allow(unused)]
use anyhow::{Context, Result};
use bunshin::protocol::AppendEntryReq;
use std::fs;

pub const TEST_NODE_ID: u64 = 1;
pub const EXTERNAL_NODE_ID: u64 = 2;
pub const TIMEOUT_SECS: u64 = 2;

pub fn remove_file(path: &str) -> Result<()> {
    if fs::exists(path)? {
        fs::remove_file(path)
            .with_context(|| "could not delete temp_file".to_string())
            .unwrap()
    }

    Ok(())
}
