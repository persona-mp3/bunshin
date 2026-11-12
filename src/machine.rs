use crate::{
    HEARTBEAT_TIMEOUT, RaftState, State, candidate::run_candidate, follower::run_follower,
    leader::run_leader, protocol::Packet, transport::Transport, utils,
};
use crossbeam::channel::Receiver;
#[allow(unused)]
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Debug)]
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

    pub fn run<T: Transport + 'static>(
        &mut self,
        state: &mut State,
        network_rx: &mut Receiver<Packet>,
        transport: T,
    ) {
        let mut next_state = RaftState::Follower;
        let dialer = Arc::new(transport);
        // TODO: There's a better way to do this
        loop {
            next_state = match next_state {
                RaftState::Follower => {
                    let timeout = utils::random_timeout();
                    let next_state = run_follower(network_rx, state, timeout);
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
                    let t = Arc::clone(&dialer);
                    let next_state =
                        run_candidate(network_rx, state, timeout, self.peers.clone(), t);
                    state.rs = next_state.clone();
                    assert_ne!(
                        next_state,
                        RaftState::Candidate,
                        "Candidate state should not return to Candidate state"
                    );
                    next_state
                }
                RaftState::Leader => {
                    let timeout = Duration::from_secs(HEARTBEAT_TIMEOUT);
                    let t = Arc::clone(&dialer);
                    let protected_state = Arc::new(Mutex::new(state.clone()));

                    let (new_state, next_state) =
                        run_leader(network_rx, self.peers.clone(), protected_state, timeout, t);

                    assert_ne!(
                        next_state,
                        RaftState::Candidate,
                        "Leader state should not return to Candidate state"
                    );

                    state.rs = next_state.clone();
                    *state = new_state;
                    next_state
                }
                RaftState::Shutdown(msg) => {
                    println!("shutting down: state: {:?}", msg);
                    return;
                }
            }
        }
    }
}
