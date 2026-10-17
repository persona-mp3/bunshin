use bunshin::{State, handle_conn};
use clap::Parser;
use std::net::TcpListener;
use std::thread;

struct Node {
    id: u32,
    state: State,
    addr: String,
    peers: Option<Vec<String>>,
}

impl Node {
    fn new(id: u32, port: usize) -> Node {
        let state = State::default(id);
        let addr = format!("localhost:{}", port);
        Node {
            id,
            state,
            addr,
            peers: None,
        }
    }

    fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.addr)?;
        println!(
            "initialising node with id {}, listening on {}",
            self.id, self.addr
        );

        if let Some(peers) = &self.peers {
            // update the inner state with peers
            self.state.config = Some(bunshin::Config {
                peers: peers.clone(),
            });
            println!("attempting connection to peers");

            if let Err(err) = self.connect_to_peers() {
                println!(
                    "[warning] failed to connect to peers on initialisation: {}",
                    err
                );
            };
        }

        for stream in listener.incoming() {
            match stream {
                Ok(conn) => {
                    let _ = handle_conn(conn, &mut self.state);
                }
                Err(err) => {
                    println!("error accepting connectoin: {:?}", err)
                }
            }
        }

        Ok(())
    }

    fn connect_to_peers(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let peers = self.peers.clone().unwrap_or_default();
        thread::spawn(move || {
            println!("peers to connect-to {:?}", peers);
        });

        Ok(())
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
struct Cli {
    #[arg(short, long)]
    port: usize,

    #[arg(short, long)]
    node_id: u32,

    #[arg(long, value_delimiter = ',')]
    peers: Option<Vec<String>>,
}

fn main() {
    let args = Cli::parse();
    let mut node = Node::new(args.node_id, args.port);
    node.peers = args.peers;
    if let Err(err) = node.run() {
        eprintln!("could not start node {}. reason: {}", node.id, err)
    };
}
