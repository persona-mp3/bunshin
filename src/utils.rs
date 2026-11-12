use crate::protocol;
use anyhow::{Context, Result, anyhow};
// use crossbeam::channel::{Receiver, Sender, select};
use rand::RngExt;
use serde_json;

use std::{
    error::Error,
    io::{BufWriter, Write},
    net::TcpStream,
    time::{self, Duration},
};

/// random_timeout returns a timeout range of 1-8 in secs. The long range
/// is to accomade e2e tests and external test scripts that rely on it
pub fn random_timeout() -> time::Duration {
    let mut rng = rand::rng();
    let timeout = rng.random_range(1..8);
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

/// Validates all peer addresses as valid IP addresses and the listen address
/// of this node is not included as a peer
pub fn parse_peers(
    listen_addr: &str,
    raw_peer: Option<Vec<String>>,
) -> Result<Vec<String>, Box<dyn Error>> {
    use std::net::ToSocketAddrs;
    let peers = if let Some(peers) = raw_peer {
        peers
    } else {
        return Ok(Vec::new());
    };

    if peers.is_empty() {
        return Ok(peers);
    }

    let mut parsed_addrs = vec![];

    for (idx, port) in peers.as_slice().iter().enumerate() {
        let addr = format!("localhost:{}", port);
        if addr == listen_addr {
            let msg = format!("cannot use self.addr as a peer at index: {}", idx);
            return Err(msg.into());
        }
        addr.as_str()
            .to_socket_addrs()?
            .next()
            .context("invalid address provided for peer")?;

        parsed_addrs.push(addr)
    }

    Ok(parsed_addrs)
}
