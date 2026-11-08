//! Transport crate
use crate::{
    protocol::{AppendEntryReq, Rpc},
    utils::send_and_recv_json,
};
use anyhow::{Context, Result};
use serde_json;
use std::{
    io::{BufReader, BufWriter},
    net::TcpStream,
};

/// MakeRpc traits supports connecting to other computers over the network
pub trait MakeRpc {
    /// Call is used to make an rpc request to an underlying client at the address
    /// `addr`. The `reply` is used to pass values back to the callers under the context
    /// of conccurent usage. For example, callers might want to connect clients asynchronously
    /// since net operations are usually blocking.
    ///
    /// Another reason for a `sender` is due to testing reasons, as network responses
    /// should be able to be mocked. This enables users to be able to write specific handlers
    /// to sepcific requests. See [`JsonRpcDialer`] for real implementation
    fn dial(&self, addr: &str, req: Rpc) -> Result<Rpc>;
}

/// JsonRpcDialer connects with clients over TCP and uses [`utils::send_and_recv_json`]
/// to send requests and parse responses. When the response is received, it uses
/// the `try_send()` method to avoid blocking. Callers are responsible for handling errors
pub struct JsonRpcDialer {}

impl MakeRpc for JsonRpcDialer {
    fn dial(&self, addr: &str, req: Rpc) -> Result<Rpc> {
        let conn =
            TcpStream::connect(addr).with_context(|| format!("failed to connect to {:?}", addr))?;

        println!("dialed {}", addr);
        let mut writer = BufWriter::new(
            conn.try_clone()
                .with_context(|| format!("failed to clone connection for writing"))?,
        );

        let reader = BufReader::new(conn);
        let mut json_reader = serde_json::Deserializer::from_reader(reader).into_iter();
        let response = send_and_recv_json(&req, &mut writer, &mut json_reader);
        response
    }
}

pub struct MockAppendEntryDialer {}

impl MakeRpc for MockAppendEntryDialer {
    #![allow(unused)]
    fn dial(&self, addr: &str, _rpc: Rpc) -> Result<Rpc> {
        todo!();
    }
}

#[allow(unused)]
fn example_use() {
    let mut rpc_threads = vec![];
    for addr in vec!["baby", "goat", "red", "chinese", "japan"] {
        let dialer = JsonRpcDialer {};
        let t = std::thread::Builder::new()
            .name(addr.to_string())
            .spawn(move || {
                dialer.dial(
                    "localhost:9090",
                    Rpc::AppendEntryReq(AppendEntryReq::default(0)),
                )
            });
        rpc_threads.push(t);
    }

    for t in rpc_threads {
        let thread = t.expect("failed to unwrap thread");
        match thread.join() {
            Ok(rpc) => println!("recv rpc from rpc: {:?}", rpc),
            Err(err) => println!("err: {:?}", err),
        };
    }
}
