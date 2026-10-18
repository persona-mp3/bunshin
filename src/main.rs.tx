use bunshin::{State, core::RaftState, handle_conn, start_timer};
use clap::Parser;
use crossbeam::{
    channel::{self, Receiver, Sender},
    select,
};
use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant};

struct Node {
    id: u32,
    state: State,
    addr: String,
    peers: Option<Vec<String>>,
    raft_state: RaftState,
}

const TIMEOUT: u64 = 2;

impl Node {
    fn new(id: u32, port: usize) -> Node {
        let state = State::default(id);
        let addr = format!("localhost:{}", port);
        Node {
            id,
            state,
            addr,
            peers: None,
            raft_state: RaftState::Follower,
        }
    }

    // TODO: visit 26 lines down from here and read the TODO
    fn run_state_machine() {}

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

        // call state.start_timer()
        let (mut send_timeout_ch, mut recv_timeout_ch) = start_timer(Duration::from_secs(TIMEOUT));
        // trigger_state_change
        // TODO: Revisit this later, we need to start deciding on how the state machine needs to be
        // worked out, ie who can change or modify the state, and does the actions, triggers the
        // next state change and what not

        // thread::spawn(|| match recv_timeout_ch.recv() {
        //     Ok(_) => {
        //         println!("timeout reached, updating state to Candidate");
        //         self.raft_state = RaftState::Candidate;
        //     }
        //     Err(err) => println!("step-thread failed to recv_timeout, because closed? {err}"),
        // });

        for stream in listener.incoming() {
            match stream {
                Ok(conn) => {
                    let _ = handle_conn(conn, &mut self.state, &mut send_timeout_ch);
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

fn listener(receiver: Receiver<()>) {}


