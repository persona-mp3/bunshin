use anyhow::Context;
use crossbeam::channel::{self, Receiver, Sender, select};
use std::time::Duration;

use crate::protocol::RPC;
use crate::utils::{random_timeout, start_timer};
use crate::{RaftState, State};

#[derive(Debug)]
pub struct Envelope {
    pub rpc: RPC,
    pub sender: Sender<RPC>,
}

pub struct Machine {
    pub id: u64,
    pub raft_state: RaftState,
    pub addr: String,
    pub timeout: Duration,
    pub network: Receiver<Envelope>,
}

impl Machine {
    pub fn new(id: u64, listen_addr: String, network_receiver: Receiver<Envelope>) -> Machine {
        Machine {
            id,
            addr: listen_addr,
            network: network_receiver,
            raft_state: RaftState::Follower,
            timeout: random_timeout(),
        }
    }

    pub fn run(&mut self, mut state: State) {
        let mut rstate = RaftState::Follower;
        println!("state-machine running");
        loop {
            rstate = match rstate {
                RaftState::Follower => self.run_follower(&mut state),
                RaftState::Shutdown => {
                    println!("shutting down");
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
                    println!("timeout elapsed, exiting");
                    return RaftState::Candidate;
                },

            }
        }
    }

    pub fn run_candidate(&mut self, _state: &mut State) -> RaftState {
        unimplemented!();
    }
}
