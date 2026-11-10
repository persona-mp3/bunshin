use crate::{protocol::Rpc, utils::send_and_recv_json};

use anyhow::{Context, Result};
use serde_json;
use std::collections::HashMap;
use std::{
    io::{BufReader, BufWriter},
    net::TcpStream,
};

use std::sync::Mutex;

pub trait Transport: Send + Sync {
    fn connect(&self, addr: &str, req: Rpc) -> Result<Rpc>;
}

pub struct TcpTransport;

impl Transport for TcpTransport {
    fn connect(&self, addr: &str, req: Rpc) -> Result<Rpc> {
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

#[derive(Default)]
pub struct SimTransport {
    pub conn_res: Mutex<HashMap<String, Rpc>>,
}

impl Transport for SimTransport {
    fn connect(&self, addr: &str, _req: Rpc) -> Result<Rpc> {
        let conn = self.conn_res.lock().unwrap();
        let response = conn
            .get(addr)
            .with_context(|| format!("expected rpc call to be made to {addr}"))
            .unwrap();

        Ok(response.clone())
    }
}
