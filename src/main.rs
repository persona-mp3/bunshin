use std::net::TcpListener;
use tai::{Entry, State, handle_conn};

struct Node {
    id: u32,
    state: State,
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
        "initialising node with id {}, anad server listening on localhost:1738",
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

impl Node {
    fn new(id: u32) -> Node {
        let previous_log_index = 0;
        let previous_log_term = 0;
        let entries: Vec<Entry> = vec![];
        Node {
            id,
            state: State {
                previous_log_index,
                previous_log_term,
                entries,
            },
        }
    }
}
