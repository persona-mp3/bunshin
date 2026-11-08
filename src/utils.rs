use crate::protocol;
use anyhow::{Context, Result, anyhow};
// use crossbeam::channel::{Receiver, Sender, select};
use rand::RngExt;
use serde_json;

use std::{
    io::{BufWriter, Write},
    net::TcpStream,
    time::{self, Duration},
};

/// random_timeout returns a timeout range of 1-5 in secs. The long range
/// is to accomade e2e tests and external test scripts that rely on it
pub fn random_timeout() -> time::Duration {
    let mut rng = rand::rng();
    let timeout = rng.random_range(2..5);
    Duration::from_secs(timeout)
}

pub fn send_and_recv_json<R: Iterator<Item = serde_json::Result<protocol::Rpc>>>(
    req: &protocol::Rpc,
    writer: &mut BufWriter<TcpStream>,
    replies: &mut R,
) -> Result<protocol::Rpc> {
    serde_json::to_writer(&mut *writer, req).context("failed to send request to target node")?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    replies
        .next()
        .ok_or_else(|| anyhow!("target node closed the connection"))?
        .context("failed to parse json from target node")
}
