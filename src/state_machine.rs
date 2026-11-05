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
                RaftState::Candidate => {
                    let election_timeout = random_timeout();
                    self.run_candidate(&mut state, connection_dialer, election_timeout)
                }
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

        let mut timeout = channel::after(new_timeout);
        loop {
            select! {
                recv(self.network) -> incoming => {
                    let envelope = incoming.unwrap();

                    // do some checks
                    match envelope.rpc {
                        RPC::AppendEntry(req) => {
                            let reply = req.handle(state).unwrap();

                            if reply.success {
                                timeout = channel::after(new_timeout);
                            }

                            match envelope.sender.send(RPC::AppendEntry(reply)) {
                                Ok(_) => {},
                                Err(err) => {println!("failed to send reply. reason:{:?}", err);}

                            };
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
        println!("resetting leader and updating current_term");
        state.current_leader = ABSENT_LEADER;
        state.term += 1;
        state.vote = self.id;

        println!(
            "term: {} leader: {}\n contacting followers for new election",
            state.term, state.current_leader,
        );

        if self.peers.len() == 0 {
            // TODO: For testing, we'll leave it as this
            println!("no peers to contact: {:?}", self.peers);
            return RaftState::Follower;
        };

        let (send_votes, recv_votes) = channel::unbounded::<u64>();

        let req = VoteRPC {
            id: self.id,
            term: state.term,
            previous_log_index: state.previous_log_index as u64,
            previous_log_term: state.previous_log_term as u64,
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
                    println!("granted_votes: {}, majority: {}", collected_votes, majority);
                    if collected_votes >= majority  {
                        return RaftState::Leader;
                    } else {
                        return RaftState::Follower;
                    }
                },

                recv(self.network) -> incoming_envelope => {
                    let envelope = match incoming_envelope {
                        Ok(env) => env,
                        Err(err) => {println!("err: {}", err); continue;},
                    };
                    println!("candidate recvd envelope: {:?}", envelope);
                    // unimplemented!();
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
    #[allow(unused)]
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
