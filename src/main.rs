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
    if args.len() < 2 {
        eprintln!("please provide id for node");
        return;
    }

    let node_id: u32 = args[1].trim().parse().unwrap();

    let listener = TcpListener::bind("127.0.0.1:1738").unwrap();
    let mut node = Node::new(node_id);
    println!(
        "initialising node with id {}, and server listening on localhost:1738",
        node.id
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
