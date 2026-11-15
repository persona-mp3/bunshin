//! Transport provides an interface for making external rpc calls by the state machine
//! At the moment, this has be tailored the [Candidate] state. Main reason for
//! abstracting it into a trait is for testablitly as we don't actual tests
//! making arbitrary send_rpcions.
//!
//! The tansport implementations available by default are [TcpTransport] which creates Tcp
//! send_rpcions and uses json and [SimTransport] which stores responses locally within itself
use crate::{protocol::Rpc, utils::send_and_recv_json};

use anyhow::{Context, Result};
use crossbeam::channel::{Receiver, Sender};
use serde_json;
use std::collections::HashMap;
use std::{
    io::{BufReader, BufWriter, Read, Write},
    net::TcpStream,
    sync::Mutex,
};

/// Transport trait for making network rpc calls
pub trait Transport: Send + Sync {
    type Conn: Read + Write;
    fn connect(&self, addr: &str) -> Result<Self::Conn>;

    fn send_rpc(&self, addr: &str, req: Rpc) -> Result<Rpc>;
}

/// TcpTransport send_rpcs to the addr over tcp. It uses `serde_json` for parsing
/// and marshalling the data and response
pub struct TcpTransport;

impl Transport for TcpTransport {
    type Conn = TcpStream;
    /// connect attemps to make a tcp connection to `addr` and returns
    /// that stream
    fn connect(&self, addr: &str) -> Result<Self::Conn> {
        Ok(TcpStream::connect(addr)?)
    }

    /// send_rpc connects to the client, sends a request and expects a single response back.
    /// Afterwards the connection is dropped
    fn send_rpc(&self, addr: &str, req: Rpc) -> Result<Rpc> {
        let conn = TcpStream::connect(addr)
            .with_context(|| format!("failed to send_rpc to {:?}", addr))?;

        let mut writer = BufWriter::new(
            conn.try_clone()
                .with_context(|| "failed to clone send_rpcion for writing".to_string())?,
        );

        let reader = BufReader::new(conn);
        let mut json_reader = serde_json::Deserializer::from_reader(reader).into_iter();

        send_and_recv_json(&req, &mut writer, &mut json_reader)
    }
}

/// This transport is used for testing as it does not make external network
/// requests, but instead holds a map of the address the node wants to contact,
/// and the response it should send. This is also safe for concurrent use as
/// the data is wrapped behind a mutex
#[derive(Default)]
pub struct SimTransport {
    pub conn_res: Mutex<HashMap<String, Rpc>>,
}

pub struct SimConn {}

impl Read for SimConn {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(0)
    }
}

impl Write for SimConn {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        Ok(1)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Transport for SimTransport {
    /// send_rpc panics when a call was made to an unregistered address
    type Conn = SimConn;
    fn connect(&self, _addr: &str) -> Result<Self::Conn> {
        BufReader::new(SimConn{});
        Ok(SimConn {})
    }


    fn send_rpc(&self, addr: &str, _req: Rpc) -> Result<Rpc> {
        let conn = self.conn_res.lock().unwrap();
        let response = conn
            .get(addr)
            .with_context(|| format!("expected rpc call to be made to {addr}"))
            .unwrap();

        Ok(response.clone())
    }
}
