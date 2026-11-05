use anyhow::Result;
use clap::Parser;
use crossbeam::channel::{self, Sender};
use serde_json;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use bunshin::{State, protocol, state_machine};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
struct Cli {
    #[arg(short, long)]
    port: u64,

    #[arg(short, long)]
    node_id: u64,

    #[arg(long, value_delimiter = ',')]
    peers: Option<Vec<String>>,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let address = format!("localhost:{}", args.port);
    let (network_sender, network_receiver) = channel::unbounded::<state_machine::Envelope>();
    let mut machine = state_machine::Machine::new(args.node_id, address.clone(), network_receiver);

    std::thread::spawn(move || {
        let state = State::new(args.node_id);
        machine.run(state);
        println!("state-machine has being shutdown");
    });

    start_server(address.clone(), network_sender);

    Ok(())
}

// TODO: Look for how to signal to server to exit
pub fn start_server(addr: String, sender: Sender<state_machine::Envelope>) {
    let listener = TcpListener::bind(&addr).unwrap();
    println!("starting server on {}", addr);
    for stream in listener.incoming() {
        let conn = stream.unwrap();
        handle_conn(conn, sender.clone()).unwrap();
    }
}

fn handle_conn(conn: TcpStream, state_sender: Sender<state_machine::Envelope>) -> Result<()> {
    let reader = BufReader::new(conn.try_clone().unwrap());
    let mut writer = BufWriter::new(conn);

    let parser = serde_json::Deserializer::from_reader(reader);
    for msg in parser.into_iter::<protocol::RPC>() {
        let rpc_msg = match msg {
            Ok(m) => m,
            Err(ref err) => {
                println!("error parsing message {:?}, {:?}", err, &msg);
                continue;
            }
        };

        let (sender, recv) = channel::unbounded::<protocol::RPC>();

        let envelope = state_machine::Envelope {
            rpc: rpc_msg,
            sender: sender,
        };

        state_sender.send(envelope).unwrap();
        let rpc_reply = recv.recv_timeout(Duration::from_secs(5)).unwrap();
        serde_json::to_writer(&mut writer, &rpc_reply).unwrap();
        writer.write_all(b"\n")?;
        writer.flush()?;
    }
    Ok(())
}
