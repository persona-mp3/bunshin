use anyhow::{Context, Result};
use crossbeam::channel::{self, Receiver, Sender, select};
use std::{
    io::{BufReader, BufWriter},
    net::TcpStream,
    thread,
    time::Duration,
};

use serde_json;

use crate::utils::{random_timeout, send_and_recv, start_timer};
use crate::{
    ABSENT_LEADER, RaftState, State,
    protocol::{self, RPC, VoteRPC},
};

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

        let (send_votes, recv_votes) = channel::unbounded::<u64>();

        let req = VoteRPC {
            id: self.id,
            term: state.term,
            previous_log_index: state.previous_log_index as u64,
            previous_log_term: state.previous_log_term as u64,
            granted: false,
        };

        let election_timeout = random_timeout();
        self.timeout = election_timeout;

        collect_votes(
            &self.peers,
            req,
            send_votes,
            connection_dialer,
            election_timeout,
        );

        loop {
            select! {
                recv(recv_votes) -> granted_votes => {
                    let collected_votes = granted_votes.unwrap() as usize;
                    let majority = (self.peers.len() / 2) + 1;
                    println!("granted_votes: {}, majority: {}", collected_votes, majority);
                    if collected_votes >= majority  {
                        return RaftState::Leader;
                    } else {
                        return RaftState::Candidate;
                    }
                },

                recv(self.network) -> incoming_envelope => {
                    let envelope = incoming_envelope.unwrap();
                    println!("candidate recvd envelope: {:?}", envelope);
                    unimplemented!();
                }
            };
        }

        // RaftState::Shutdown
    }
}

type Dialer = fn(addr: &String, req: RPC, sender: Sender<()>) -> Result<()>;

fn connection_dialer(addr: &String, req: RPC, sender: Sender<()>) -> Result<()> {
    let conn = TcpStream::connect(addr)
        .context("could not connect with peer")
        .unwrap();

    let mut writer = BufWriter::new(conn.try_clone().unwrap());
    let reader = BufReader::new(conn);
    let mut reader = serde_json::Deserializer::from_reader(reader).into_iter::<protocol::RPC>();
    let reply = send_and_recv(&req, &mut writer, &mut reader)?;

    match &reply {
        RPC::Vote(rpc_reply) => {
            if rpc_reply.granted {
                let _ = sender.send(()).context("could not send granted vote");
            };
        }
        _ => {
            panic!("unhandled reply: {:?}", reply);
        }
    }

    Ok(())
}

fn collect_votes(
    peers: &[String],
    req: VoteRPC,
    votes: Sender<u64>,
    dialer: Dialer,
    election_timeout: Duration,
) {
    let (tx, rx) = channel::unbounded::<()>();

    println!(" collecting votes....");
    for peer in peers {
        let addr = peer.clone();
        let increment_votes = tx.clone();

        let rpc = RPC::Vote(req.clone());
        thread::spawn(move || {
            match dialer(&addr, rpc, increment_votes) {
                Ok(_) => {}
                Err(err) => println!("dialer failed: {}", err),
            };
        });
    }

    drop(tx);
    let (_, reset_timer_recv) = channel::unbounded::<()>();
    let (timeout_elapsed_sender, timeout_elapsed_recv) = channel::unbounded::<()>();

    std::thread::spawn(move || {
        start_timer(election_timeout, reset_timer_recv, timeout_elapsed_sender)
    });
    let mut granted_votes = 1;

    loop {
        select! {
            recv(rx) -> _new_vote  => {
                granted_votes+=1;
            }

            recv(timeout_elapsed_recv) -> _timeout_elpased => {
                break;
            }
        }
    }
    println!(
        "granted:{}, majority {}",
        granted_votes,
        (peers.len() / 2) + 1
    );

    votes.send(granted_votes).unwrap();
}

mod tests {
    #[allow(unused)]
    use super::*;

    #[test]
    fn test_collect_votes() {
    }

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
