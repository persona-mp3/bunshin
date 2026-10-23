use anyhow::Result;
use crossbeam::channel::{Receiver, select};
use std::time::{Duration, Instant};

use crate::{RaftState, State};

pub fn run_follower(timeout: Duration, receive_reset_timer: Receiver<()>) -> Result<RaftState> {
    let mut deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        select! {
        recv(receive_reset_timer) -> _sig => {
            deadline = Instant::now() + timeout;
            println!("resetting timer");
        },
        default(remaining) => {
            println!("timeout reached after: {:?}", timeout);
            break
        }
        }
    }

    Ok(RaftState::Candidate)
}

pub fn run_candidate(state: &mut State) -> Result<RaftState> {
    state.term = state.term + 1;
    println!(
        "running candidate:  term: {:?}, prev_log_index: {}, prev_log_term: {}",
        state.term, state.previous_log_index, state.previous_log_term
    );
    Ok(RaftState::Leader)
}

pub fn run_leader(state: &mut State) -> Result<RaftState> {
    println!(
        "running leader:     term: {:?}, prev_log_index: {}, prev_log_term: {}",
        state.term, state.previous_log_index, state.previous_log_term
    );
    Ok(RaftState::Follower)
}
