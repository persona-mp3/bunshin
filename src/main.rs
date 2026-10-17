use bunshin::{State, handle_conn};
use clap::{Parser, Subcommand};
use std::net::TcpListener;

struct Node {
    id: u32,
    state: State,
}

impl Node {
    fn new(id: u32) -> Node {
        let state = State::default(id);
        Node { id, state }
    }
}

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[arg(short, long)]
    addr: String,

    #[arg(short, long)]
    node_id: u32,
}

fn main() {
    let args = Cli::parse();

    let addr = args.addr.trim();

    let listener = TcpListener::bind(args.addr.trim()).unwrap();
    let mut node = Node::new(args.node_id);

    println!(
        "initialising node with id {}, listening on {}",
        node.id, addr
    );
    for stream in listener.incoming() {
        match stream {
            Ok(conn) => {
                let _ = handle_conn(args.node_id, conn, &mut node.state);
            }
            Err(err) => {
                println!("error accepting connectoin: {:?}", err)
            }
        }
    }
}

// enum Args {
//     Nodes { count: u32, addrs: Vec<String> },
// }
// }
