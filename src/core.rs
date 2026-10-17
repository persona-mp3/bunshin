// use bunshin::{};
// fn check_log_consistency(
//     state: &State
//     ) -> bool {
//     true
// }

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
