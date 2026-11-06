use anyhow::{Context, Result};
use crossbeam::channel::{self, Receiver, Sender, select};
use std::{
    io::{BufReader, BufWriter},
    net::TcpStream,
    thread,
    time::Duration,
};

use serde_json;

use crate::utils::{random_timeout, send_and_recv};
use crate::{
    ABSENT_LEADER, RaftState, State,
    protocol::{self, RPC, VoteRPC},
};

pub const CHANNEL_SEND_TIMEOUT_SECS: f64 = 1.5;

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
        println!("STATE_MACHINE RUNNING....");
        loop {
            rstate = match rstate {
                RaftState::Follower => self.run_follower(&mut state),
                RaftState::Candidate => {
                    let election_timeout = random_timeout();
                    self.run_candidate(&mut state, connection_dialer, election_timeout)
                }

                RaftState::Leader => self.run_leader(&mut state),

                _ => {
                    println!(
                        "Shutdown:{} \t{:?} state not yet implemented",
                        self.id, rstate
                    );
                    RaftState::Shutdown
                }
            }
        }
    }

    pub fn run_follower(&mut self, state: &mut State) -> RaftState {
        let new_timeout = random_timeout();
        self.timeout = new_timeout;

        let mut timeout = channel::after(new_timeout);
        loop {
            select! {
                recv(self.network) -> incoming => {
                    let envelope = incoming.context("follower: failed to unwrap envelope").unwrap();

                    // do some checks
                    match envelope.rpc {
                        RPC::AppendEntry(req) => {
                            let reply = req.handle(state);

                            if reply.success {
                                timeout = channel::after(new_timeout);
                            }

                            match envelope.sender.send(RPC::AppendEntry(reply)) {
                                Ok(_) => {},
                                Err(err) => {println!("Follower:{} \tfailed to send reply. reason:{:?}", self.id, err);}

                            };
                        },


                        RPC::Vote(req) => {
                            let mut reply = VoteRPC {
                                id: self.id,
                                previous_log_index: state.previous_log_index,
                                previous_log_term: state.previous_log_term,
                                granted: false,
                                term: state.term,
                            };

                            if req.term <= state.term {
                                let _ = envelope.sender.send_timeout(RPC::Vote(reply), Duration::from_secs_f64(CHANNEL_SEND_TIMEOUT_SECS));
                                continue;
                            }

                            let logs_updated = protocol::is_candidate_log_up_to_date(state, req.previous_log_index, req.previous_log_term);

                            if !logs_updated {
                                println!("Follower:{} \toutdated logs from candidate node: {:?}", self.id, req);
                                let _ = envelope.sender.send_timeout(RPC::Vote(reply), Duration::from_secs_f64(CHANNEL_SEND_TIMEOUT_SECS));
                                continue;
                            }

                            state.current_leader = req.id as i32;
                            state.term = req.term;

                            reply.granted = true;
                            reply.term = state.term;
                            println!("Follower:{} \tgranted vote to new cadidate: {:?}", self.id, req);
                            let _ = envelope.sender.send_timeout(RPC::Vote(reply), Duration::from_secs_f64(CHANNEL_SEND_TIMEOUT_SECS));
                        },
                        _ => { panic!("unimplemented for follower: {:?}", envelope.rpc);}

                    }
                },


                recv(timeout) -> _sig => {
                    return RaftState::Candidate;
                },

            }
        }
    }

    pub fn run_candidate(
        &mut self,
        state: &mut State,
        dialer: Dialer,
        election_timeout: Duration,
    ) -> RaftState {
        state.current_leader = ABSENT_LEADER;
        state.term += 1;
        state.vote = self.id as i64;

        println!(
            "Candidate:{} \tNew election for: term: {}",
            self.id, state.term
        );

        if self.peers.is_empty() {
            // TODO: For testing, we'll leave it as this
            println!(
                "Candidate:{} \tno peers to contact: {:?}",
                self.id, self.peers
            );
            return RaftState::Follower;
        };

        let (send_votes, recv_votes) = channel::unbounded::<u64>();

        let req = VoteRPC {
            id: self.id,
            term: state.term,
            previous_log_index: state.previous_log_index,
            previous_log_term: state.previous_log_term,
            granted: false,
        };

        // let election_timeout = random_timeout();
        self.timeout = election_timeout;
        let peers = self.peers.clone();

        thread::spawn(move || {
            collect_votes(&peers, req, send_votes, dialer, election_timeout);
        });

        let cluster_size = self.peers.len() + 1; // peers + self
        let majority = cluster_size / 2 + 1;

        loop {
            select! {
                recv(recv_votes) -> granted_votes => {
                    let collected_votes = granted_votes.unwrap() as usize;
                    if collected_votes >= majority  {
                    println!("Candidate:{} \tgot majority votes: {}, majority: {}", self.id, collected_votes, majority);
                        return RaftState::Leader;
                    } else {
                        println!("Candidate:{} \tlost election: {}, majority: {}", self.id, collected_votes, majority);
                        return RaftState::Follower;
                    }
                },

                recv(self.network) -> incoming_envelope => {
                    let envelope = match incoming_envelope {
                        Ok(env) => env,
                        Err(err) => {println!("err: {}", err); continue;},
                    };
                    println!("Candidate:{} \tincoming_rpc: {:?}", self.id,  envelope.rpc);

                    match envelope.rpc {
                        RPC::Vote(ref req) => {

                            let mut reply = VoteRPC {
                                id: self.id,
                                term: state.term,
                                previous_log_index: state.previous_log_index,
                                previous_log_term: state.previous_log_term,
                                granted: false,

                            };

                            if req.term > state.term {
                                state.term = req.term;
                                reply.granted = true;
                                reply.term = state.term;

                                let _ = envelope.sender.send_timeout(RPC::Vote(reply), Duration::from_secs_f64(CHANNEL_SEND_TIMEOUT_SECS));
                                state.vote = ABSENT_LEADER as i64;
                                state.current_leader = req.id as i32;
                                println!("Candidate:{} \tgranted vote: to req and assigned as new leader: {:?}", self.id, envelope.rpc);
                                return RaftState::Follower;
                            }


                            // since we've already voted ourself for this campagin we can't give out votes
                            println!("Candidate:{} \tdenying vote for request: {:?}", self.id,envelope.rpc);
                            let _ = envelope.sender.send_timeout(RPC::Vote(reply), Duration::from_secs_f64(CHANNEL_SEND_TIMEOUT_SECS));
                        }

                        _ => {
                            println!("not wanted request: {:?}", envelope);
                        }

                    }
                }
            };
        }
    }

    pub fn run_leader(&mut self, _state: &mut State) -> RaftState {
        println!("Leader:{} \tSimulation 2s of leader work", self.id);
        thread::sleep(Duration::from_secs(2));
        RaftState::Follower
    }
}

type Dialer = fn(addr: &String, req: RPC, sender: Sender<()>) -> Result<()>;

fn connection_dialer(addr: &String, req: RPC, sender: Sender<()>) -> Result<()> {
    let conn = TcpStream::connect(addr).context("could not connect with peer")?;

    let mut writer = BufWriter::new(
        conn.try_clone()
            .with_context(|| format!("faied to clone connections' writer"))?,
    );

    let reader = BufReader::new(conn);
    let mut reader = serde_json::Deserializer::from_reader(reader).into_iter::<protocol::RPC>();
    let reply = send_and_recv(&req, &mut writer, &mut reader)
        .with_context(|| format!("failed to send_and_recv"))?;

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
    let (vote_tx, vote_rx) = channel::unbounded::<()>();

    for peer in peers {
        let addr = peer.clone();
        let tx = vote_tx.clone();
        let rpc = RPC::Vote(req.clone());
        thread::spawn(move || {
            if let Err(err) = dialer(&addr, rpc, tx) {
                println!("dialer failed: {}", err);
            }
        });
    }
    drop(vote_tx);

    let cluster_size = peers.len() + 1; // peers + self
    let majority = cluster_size / 2 + 1;
    let mut granted: u64 = 1; // vote for self
    let timeout = channel::after(election_timeout);

    while (granted as usize) < majority {
        select! {
            recv(vote_rx) -> msg => match msg {
                Ok(()) => granted += 1,
                Err(_) => break, // all peers responded
            },
            recv(timeout) -> _ => {
                break;
            }
        }
    }

    let _ = votes.send(granted);
}

#[cfg(test)]
fn test_dialer_granted_votes(_addr: &String, _req: RPC, sender: Sender<()>) -> Result<()> {
    sender.send(())?;
    Ok(())
}

#[cfg(test)]
fn test_dialer_denies_votes(_addr: &String, _req: RPC, _sender: Sender<()>) -> Result<()> {
    Ok(())
}

mod tests {
    #![allow(unused)]
    use super::*;
    const TIMEOUT_SECS: u64 = 2;

    #[test]
    fn test_collect_votes() {
        let node_id: u64 = 1;
        let state = State::new(node_id);
        let (send_votes, recv_votes) = channel::unbounded();

        // needs to be more than 2 peers
        let peers = vec![
            String::from("localhost:9000"),
            String::from("localhost:1738"),
        ];

        let req = VoteRPC {
            id: state.id,
            previous_log_index: state.previous_log_index,
            previous_log_term: state.previous_log_term,
            term: state.term,
            granted: false,
        };

        thread::spawn(move || {
            collect_votes(
                &peers,
                req,
                send_votes,
                test_dialer_granted_votes,
                Duration::from_secs(TIMEOUT_SECS),
            );
        });

        let votes = match recv_votes.recv() {
            Ok(v) => v,
            Err(err) => {
                println!(
                    "could not collect votes for test_collect_votes with: {}",
                    err
                );
                return;
            }
        };
        assert_eq!(votes, 2);
    }

    #[test]
    fn test_candidate_transitons_to_follower_after_loosing_electoin() {
        let node_id: u64 = 1;
        let mut state = State::new(node_id);
        let (_, rx) = channel::unbounded();
        let peers = vec![String::from("localhost:9000")];
        let mut machine = Machine::new(1, String::from(""), rx, peers);
        let rs = machine.run_candidate(
            &mut state,
            test_dialer_denies_votes,
            Duration::from_secs(TIMEOUT_SECS),
        );

        assert_eq!(rs, RaftState::Follower);
    }

    #[test]
    fn test_candidate_transitons_to_leader_after_winning_election() {
        let node_id: u64 = 1;
        let mut state = State::new(node_id);
        let (_, rx) = channel::unbounded();

        // needs to be more than 2 peers
        let peers = vec![
            String::from("localhost:9000"),
            String::from("localhost:9001"),
        ];
        let mut machine = Machine::new(1, String::from(""), rx, peers);
        let rs = machine.run_candidate(
            &mut state,
            test_dialer_granted_votes,
            Duration::from_secs(TIMEOUT_SECS),
        );

        assert_eq!(rs, RaftState::Leader)
    }

    #[test]
    fn test_candidate_updates_state_for_voting() {
        let node_id: u64 = 1;
        let mut state = State::new(node_id);
        let (_, rx) = channel::unbounded();
        let peers = vec![String::from("localhost:9000")];
        let mut machine = Machine::new(1, String::from(""), rx, peers);
        for _ in 0..3 {
            machine.run_candidate(
                &mut state,
                test_dialer_granted_votes,
                Duration::from_secs(TIMEOUT_SECS),
            );
        }

        let expected_state = State {
            id: node_id,
            term: 3,              // expected term
            vote: node_id as i64, // node should grant vote to itself
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
