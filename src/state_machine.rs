use anyhow::Context;
use crossbeam::channel::{self, Receiver, Sender, select};
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::protocol::RPC;
use crate::utils::{random_timeout, start_timer};
use crate::{ABSENT_LEADER, RaftState, State};

#[derive(Debug, Clone)]
pub struct Envelope {
    pub rpc: RPC,
    pub sender: Sender<RPC>,
}

#[derive(Debug, Clone)]
pub struct Machine {
    pub id: u64,
    pub raft_state: RaftState,
    pub addr: String,
    pub timeout: Duration,
    pub network: Receiver<Envelope>,
    pub peers: Vec<String>,
}

impl Machine {
    pub fn new(
        id: u64,
        listen_addr: String,
        network_receiver: Receiver<Envelope>,
        peers: Vec<String>,
    ) -> Machine {
        Machine {
            id,
            addr: listen_addr,
            network: network_receiver,
            raft_state: RaftState::Follower,
            timeout: random_timeout(),
            peers,
        }
    }

    pub fn run(&mut self, mut state: State) {
        let mut rstate = RaftState::Follower;
        println!("state_machine running");
        loop {
            rstate = match rstate {
                RaftState::Follower => self.run_follower(&mut state),
                RaftState::Candidate => self.run_candidate(&mut state),
                RaftState::Shutdown => {
                    return;
                }
                _ => {
                    println!("{:?} state not yet implemented", rstate);
                    RaftState::Shutdown
                }
            }
        }
    }

    pub fn run_follower(&mut self, state: &mut State) -> RaftState {
        let new_timeout = random_timeout();
        self.timeout = new_timeout;

        let (reset_timer_sender, reset_timer_recv) = channel::unbounded::<()>();
        let (timeout_elapsed_sender, timeout_elapsed_recv) = channel::unbounded::<()>();

        std::thread::spawn(move || {
            start_timer(new_timeout, reset_timer_recv, timeout_elapsed_sender)
        });

        loop {
            select! {
                recv(self.network) -> incoming => {
                    let envelope = incoming.unwrap();

                    // do some checks
                    match envelope.rpc {
                        RPC::AppendEntry(req) => {
                            let reply = req.handle(state).unwrap();

                            if reply.success {
                                 reset_timer_sender.send(()).context("failed to send reset timer from run_follower").unwrap();
                            }

                            match envelope.sender.send(RPC::AppendEntry(reply)) {
                                Ok(_) => {},
                                Err(err) => {println!("failed to send reply. reason:{:?}", err);}

                            };
                        },


                        _ => { panic!("unimplemented for follower: {:?}", envelope.rpc);}

                    }
                },

                recv(timeout_elapsed_recv) -> _sig => {
                    return RaftState::Candidate;
                },

            }
        }
    }

    pub fn run_candidate(&mut self, state: &mut State) -> RaftState {
        println!("resetting leader and updating current_term");
        state.current_leader = ABSENT_LEADER;
        state.term += 1;
        state.vote = self.id;

        println!(
            "term: {} leader: {}\n contacting followers for new election",
            state.term, state.current_leader,
        );

        if self.peers.len() == 0 {
            println!("no peers to contact: {:?}", self.peers);
            return RaftState::Shutdown;
        };

        let won = collect_votes(
            self.id,
            &self.peers,
            state.term,
            state.previous_log_index,
            state.previous_log_term,
        );

        if won {
            RaftState::Leader
        } else {
            RaftState::Follower
        }

        // let (reset_timer_sender, reset_timer_recv) = channel::unbounded::<()>();
        // let (timeout_elapsed_sender, timeout_elapsed_recv) = channel::unbounded::<()>();
        //
        // std::thread::spawn(move || {
        //     start_timer(election_timeout, reset_timer_recv, timeout_elapsed_sender)
        // });

        // loop {
        //     select! {
        //         recv(self.network) -> incoming => {
        //             let envelope = incoming.unwrap();
        //             match envelope.rpc {
        //                 RPC::AppendEntry(req) => {
        //                 },
        //                 _ => {},
        //             }
        //         }
        //     }
        // }

        // RaftState::Shutdown
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VoteRPC {
    id: u64,
    previous_log_index: u64,
    previous_log_term: u64,
}

impl VoteRPC {
    pub fn handle(&self, _state: &mut State) {
        unimplemented!()
    }
}

#[allow(unused)]
fn collect_votes(
    id: u64,
    peers: &Vec<String>,
    term: u64,
    prev_log_index: u32,
    prev_log_term: u32,
) -> bool {
    println!(" collecting votes....");
    false
}

mod tests {
    #[allow(unused)]
    use super::*;

    #[test]
    fn test_candidate_updates_state_for_voting() {
        let node_id: u64 = 1;
        let mut state = State::new(node_id);
        let (_, rx) = channel::unbounded();
        let peers = vec![String::from("localhost:9000")];
        let mut machine = Machine::new(1, String::from(""), rx, peers);
        for _ in 0..3 {
            machine.run_candidate(&mut state);
        }

        let expected_state = State {
            id: node_id,
            term: 3,       // expected term
            vote: node_id, // node should grant vote to itself
            commit_index: 0,
            current_leader: ABSENT_LEADER,
            entries: Vec::new(),
            file_name: String::from("node-1-persist.json"),
            previous_log_index: 0,
            previous_log_term: 0,
        };

        assert_eq!(state, expected_state);
    }
}
