use crate::{
    RaftState, State, candidate::run_candidate, follower::run_follower, protocol::Packet,
    transport::Transport, utils,
};
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

    pub fn run(
        &mut self,
        mut state: &mut State,
        network_rx: &mut Receiver<Packet>,
        transport: Transport,
    ) {
        let mut next_state = RaftState::Follower;
        // TODO: There's a better way to do this
        loop {
            next_state = match next_state {
                RaftState::Follower => {
                    let timeout = utils::random_timeout();
                    let next_state = run_follower(network_rx, state, timeout);
                    println!("transitioning from Follower -> {:?}", next_state);
                    assert_ne!(
                        next_state,
                        RaftState::Follower,
                        "Follower state should not return to follower state"
                    );
                    assert_ne!(
                        next_state,
                        RaftState::Leader,
                        "Follower state should not return to leader state"
                    );
                    state.rs = next_state.clone();
                    next_state
                }

                RaftState::Candidate => {
                    let timeout = utils::random_timeout();
                    let next_state = run_candidate(
                        network_rx,
                        state,
                        timeout,
                        self.peers.clone(),
                        transport,
                    );
                    state.rs = next_state.clone();
                    // println!("transitioning from Candidate -> {:?}", next_state);
                    assert_ne!(
                        next_state,
                        RaftState::Candidate,
                        "Candidate state should not return to Candidate state"
                    );
                    next_state
                }
                RaftState::Shutdown(msg) => {
                    println!("shutting down: state: {:?}", msg);
                    return;
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
