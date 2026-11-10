#![allow(unused)]
use anyhow::{Context, Result};
use bunshin::{
    LogEntry, RaftState, ShutdownErrors, State,
    candidate::run_candidate,
    protocol::{Packet, Rpc, VoteReply},
    transport::SimTransport,
};

use crossbeam::channel::{self, Receiver};
use rand::Rng;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

mod test_utils;
use test_utils::{EXTERNAL_NODE_ID, TEST_NODE_ID, TIMEOUT_SECS};

#[test]
fn test_candidate_updates_term_for_election() {
    let lower_term = 3;
    let election_term = 4;
    let mut state = State::new(TEST_NODE_ID);
    state.term = lower_term;

    let sim_transport = Arc::new(SimTransport::default());
    let (_, mut network_rx) = channel::unbounded::<Packet>();
    let transition = run_candidate(
        &mut network_rx,
        &mut state,
        Duration::from_secs(TIMEOUT_SECS),
        vec!["".to_string()],
        sim_transport,
    );

    assert_eq!(state.term, election_term);
    assert_eq!(state.voted_for, Some(TEST_NODE_ID));
    assert_eq!(transition, RaftState::Shutdown(ShutdownErrors::NetworkDrop));
}

#[test]
fn test_candidate_transitions_to_follower_with_majority_votes() {
    let mut state = State::new(TEST_NODE_ID);
    let mut sim_transport = SimTransport::default();

    // arbitrary higher term the node should parade for a 10th election
    let election_term = 10;
    let current_term = 9;
    state.term = current_term;

    let mut responses: HashMap<String, Rpc> = HashMap::new();
    let mut peers = vec!["9000".to_string(), "9001".to_string(), "9002".to_string()];
    for (idx, peer) in peers.iter().enumerate() {
        let s = responses.insert(
            peer.clone(),
            Rpc::VoteReply(VoteReply {
                id: EXTERNAL_NODE_ID,
                term: rand::random_range(1..9),
                granted: true,
            }),
        );
    }

    let sim_transport = Arc::new(SimTransport {
        conn_res: Mutex::new(responses),
    });

    let (_tx, mut network_rx) = channel::unbounded::<Packet>();
    // allow it to be blocking, we should set a timer for cargo to fail if anylonger
    let transition = run_candidate(
        &mut network_rx,
        &mut state,
        Duration::from_secs(TIMEOUT_SECS),
        peers,
        sim_transport,
    );

    assert_eq!(state.term, 10);
    assert_eq!(state.current_leader, Some(TEST_NODE_ID));
    assert_eq!(transition, RaftState::Leader);
}

#[test]
fn test_candidate_looses_election_without_majority() {
    let mut state = State::new(TEST_NODE_ID);
    let mut sim_transport = SimTransport::default();

    // arbitrary higher term the node should parade for a 10th election
    let election_term = 10;
    let current_term = 9;
    state.term = current_term;

    let mut responses: HashMap<String, Rpc> = HashMap::new();
    let mut peers = vec!["9000".to_string(), "9001".to_string(), "9002".to_string()];
    for (idx, peer) in peers.iter().enumerate() {
        let s = responses.insert(
            peer.clone(),
            Rpc::VoteReply(VoteReply {
                id: EXTERNAL_NODE_ID,
                term: rand::random_range(1..9),
                granted: false,
            }),
        );
    }

    let sim_transport = Arc::new(SimTransport {
        conn_res: Mutex::new(responses),
    });

    let (_, mut network_rx) = channel::unbounded::<Packet>();

    let transition = run_candidate(
        &mut network_rx,
        &mut state,
        Duration::from_secs(TIMEOUT_SECS),
        peers,
        sim_transport,
    );

    assert_eq!(state.term, 10);
    assert_eq!(state.current_leader, None);
    assert_eq!(state.voted_for, Some(TEST_NODE_ID));
    assert_eq!(transition, RaftState::Shutdown(ShutdownErrors::NetworkDrop));
}
