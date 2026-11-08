use crate::{RaftState, State, handlers::run_follower, protocol::Packet, utils};
#[allow(unused)]
use crossbeam::channel::{self, Receiver};
use std::time::Duration;

pub struct Machine {
    pub id: u64,
    pub peers: Vec<String>,
    pub timeout: Duration,
}

impl Machine {
    pub fn default(id: u64, peers: Vec<String>) -> Self {
        let timeout = utils::random_timeout();
        Machine { id, peers, timeout }
    }

    pub fn run(&mut self, state: &mut State, network_rx: &mut Receiver<Packet>) {
        let mut next_state = RaftState::Follower;
        loop {
            next_state = match next_state {
                RaftState::Follower => {
                    let timeout = utils::random_timeout();
                    run_follower(network_rx, state, timeout)
                }
                _ => {
                    let _ = network_rx;
                    println!("unhandled state: {:?}", next_state);
                    return;
                }
            }
        }
    }
}
