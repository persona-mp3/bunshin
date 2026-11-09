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
trait MakeRpc {
    fn dial(&self, addr: &str, req: Rpc) -> Result<Rpc>;
}

/// Transport connects to `addr`, sends an rpc request and returns the result of
pub type Transport = fn(addr: &str, req: Rpc) -> Result<Rpc>;

/// json_transport_dialer connects to `addr`, sends an rpc request via [`serde_json`]
pub fn json_transport_dialer(addr: &str, req: Rpc) -> Result<Rpc> {
    let conn =
        TcpStream::connect(addr).with_context(|| format!("failed to connect to {:?}", addr))?;

    println!("dialed {}", addr);
    let mut writer = BufWriter::new(
        conn.try_clone()
            .with_context(|| "failed to clone connection for writing".to_string())?,
    );

    let reader = BufReader::new(conn);
    let mut json_reader = serde_json::Deserializer::from_reader(reader).into_iter();
    
    send_and_recv_json(&req, &mut writer, &mut json_reader)
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
                .with_context(|| "failed to clone connection for writing".to_string())?,
        );

        let reader = BufReader::new(conn);
        let mut json_reader = serde_json::Deserializer::from_reader(reader).into_iter();
        
        send_and_recv_json(&req, &mut writer, &mut json_reader)
    }
}

struct MockAppendEntryDialer {}

impl MakeRpc for MockAppendEntryDialer {
    #![allow(unused)]
    fn dial(&self, addr: &str, _rpc: Rpc) -> Result<Rpc> {
        todo!();
    }
}

#[allow(unused)]
fn example_use() {
    let mut rpc_threads = vec![];
    for addr in ["baby", "goat", "red", "chinese", "japan"] {
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
