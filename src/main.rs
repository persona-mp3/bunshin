use bunshin::{State, handle_conn};
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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: bushin [addr] [node-id]\n Example\n\t bushin localhost:1738 3");
        return;
    }

    let addr = args[1].trim();
    let node_id: u32 = args[2].trim().parse().unwrap();

    let listener = TcpListener::bind(addr).unwrap();
    let mut node = Node::new(node_id);
    println!(
        "initialising node with id {}, listening on {}",
        node.id, addr
    );
    for stream in listener.incoming() {
        match stream {
            Ok(conn) => {
                let _ = handle_conn(node_id, conn, &mut node.state);
            }
            Err(err) => {
                println!("error accepting connectoin: {:?}", err)
            }
        }
    }
}
