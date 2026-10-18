use crate::State;

pub fn check_log_consistency(state: &State, prev_log_index: u32, prev_log_term: u32) -> bool {
    if state.previous_log_index == prev_log_index && state.previous_log_term == prev_log_term {
        return true;
    }

    if state.previous_log_index < prev_log_index && state.previous_log_term < prev_log_term {
        println!(
            "logs are not up to date prev_log_indexes {}, {}, prev_log_terms {} {}",
            state.previous_log_index, prev_log_index, state.previous_log_term, prev_log_term,
        )
    } else if state.previous_log_index > prev_log_index && state.previous_log_term > prev_log_term {
        println!(
            "request came with outdated logs. prev_log_indexes {}, {}, prev_log_terms {} {}",
            state.previous_log_index, prev_log_index, state.previous_log_term, prev_log_term,
        )
    }

    false
}

pub fn verify_leader(current_leader: i32, req_id: i32) -> bool {
    if current_leader == -1 {
        true
    } else {
        current_leader == req_id
    }
}

pub enum RaftState {
    Leader,
    Follower,
    Candidate,
}
