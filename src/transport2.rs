#![allow(unused)]
use crate::protocol::{AppendEntryReq, Rpc};
use anyhow::Result;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;

pub trait Transport: Send + Sync {
    type Conn: Read + Write;
    fn connect(&self, addr: &str) -> Result<Self::Conn>;
    fn dial(&self, addr: &str, req: Rpc) -> Result<Rpc>;
}

struct TcpTransport;

impl Transport for TcpTransport {
    type Conn = TcpStream;
    fn connect(&self, addr: &str) -> Result<Self::Conn> {
        Ok(TcpStream::connect(addr)?)
    }

    fn dial(&self, addr: &str, req: Rpc) -> Result<Rpc> {
        Ok(Rpc::AppendEntryReq(AppendEntryReq::default(0)))
    }
}
