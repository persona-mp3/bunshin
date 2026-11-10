#![allow(unused)]
use anyhow::{Context, Result};
use bunshin::{
    LogEntry, RaftState, ShutdownErrors, State,
    candidate::run_candidate,
    protocol::{AppendEntryReply, AppendEntryReq, Packet, Rpc, VoteReply},
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
    state.rs = RaftState::Candidate;
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
    state.rs = RaftState::Candidate;

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
    state.rs = RaftState::Candidate;
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

#[test]
fn test_candidate_steps_down_from_append_entry_with_higher_term() {
    let higher_term = 6;

    let mut state = State::new(TEST_NODE_ID);
    state.rs = RaftState::Candidate;

    let mut state = Arc::new(Mutex::new(state));

    let sim_transport = Arc::new(SimTransport::default());

    let (network_tx, mut network_rx) = channel::unbounded::<Packet>();

    let c = Arc::clone(&state);
    let transition = thread::spawn(move || {
        let mut state = c.lock().unwrap();
        let transition = run_candidate(
            &mut network_rx,
            &mut state,
            Duration::from_secs(TIMEOUT_SECS),
            vec!["".to_string()],
            sim_transport,
        );
        transition
    });

    let mut higher_append_entry = AppendEntryReq::default(EXTERNAL_NODE_ID);
    higher_append_entry.term = higher_term;
    higher_append_entry.info = Some("from test_node".to_string());

    let (packet_tx, packet_rx) = channel::unbounded::<Rpc>();

    let packet = Packet {
        rpc: Rpc::AppendEntryReq(higher_append_entry),
        sender: packet_tx,
    };

    network_tx
        .send_timeout(packet, Duration::from_secs(1))
        .expect("failed to send packet to run_candidate");

    let res = packet_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("failed to recv packet from run_candidate");

    drop(network_tx);
    let state = state.lock().unwrap();
    assert_eq!(
        state.current_leader,
        Some(EXTERNAL_NODE_ID),
        "candidate node should update it's leader from the higher_append_entry request"
    );
    assert_eq!(state.term, higher_term);
    assert_eq!(state.voted_for, Some(EXTERNAL_NODE_ID));
    assert_eq!(transition.join().unwrap(), RaftState::Follower)
}


#[test]
fn test_candidate_steps_down_from_append_entry_with_same_term() {
    let higher_term = 7;

    let mut state = State::new(TEST_NODE_ID);
    state.rs = RaftState::Candidate;
    state.term = 6;

    let mut state = Arc::new(Mutex::new(state));

    let sim_transport = Arc::new(SimTransport::default());

    let (network_tx, mut network_rx) = channel::unbounded::<Packet>();

    let c = Arc::clone(&state);
    let transition = thread::spawn(move || {
        let mut state = c.lock().unwrap();
        let transition = run_candidate(
            &mut network_rx,
            &mut state,
            Duration::from_secs(TIMEOUT_SECS),
            vec!["".to_string()],
            sim_transport,
        );
        transition
    });

    let mut higher_append_entry = AppendEntryReq::default(EXTERNAL_NODE_ID);
    higher_append_entry.term = higher_term;
    higher_append_entry.info = Some("from test_node".to_string());

    let (packet_tx, packet_rx) = channel::unbounded::<Rpc>();

    let packet = Packet {
        rpc: Rpc::AppendEntryReq(higher_append_entry),
        sender: packet_tx,
    };

    network_tx
        .send_timeout(packet, Duration::from_secs(1))
        .expect("failed to send packet to run_candidate");

    let res = packet_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("failed to recv packet from run_candidate");

    drop(network_tx);
    let state = state.lock().unwrap();
    assert_eq!(
        state.current_leader,
        Some(EXTERNAL_NODE_ID),
        "candidate node should update it's leader from the higher_append_entry request"
    );
    assert_eq!(state.term, higher_term);
    assert_eq!(state.voted_for, Some(EXTERNAL_NODE_ID));
    assert_eq!(transition.join().unwrap(), RaftState::Follower)
}
