#![allow(unused)]
use anyhow::{Context, Result};
use bunshin::{LogEntry, RaftState, ShutdownErrors, State, candidate::run_candidate, protocol::*};
use crossbeam::channel::{self, Receiver};
use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
mod test_utils;

fn mock_dialer_grants_votes(addr: &str, req: Rpc) -> Result<Rpc> {}
fn mock_dialer_denies_votes() {}

#[test]
fn test_candidate_updates_term_for_election() {
    let mut state = State::new(test_utils::TEST_NODE_ID);
    run_candidate();
}
