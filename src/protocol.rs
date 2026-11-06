use crate::{Command, Entry, State};
use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq)]
/// CommandReply is sent to external clients
pub struct CommandReply {
    pub value: String,
    pub success: bool,
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Clone)]
pub struct AppendEntry {
    pub id: u64,
    pub previous_log_index: u64,
    pub previous_log_term: u64,
    #[serde(default = "default_commit_index")]
    pub commit_index: u64,
    pub entry: Option<Entry>,
    pub success: bool,
    pub info: Option<String>,
    pub term: u64,
}

impl AppendEntry {
    pub fn new(id: u64) -> AppendEntry {
        AppendEntry {
            id,
            previous_log_index: 0,
            previous_log_term: 0,
            commit_index: 0,
            entry: None,
            success: false,
            info: None,
            term: 0,
        }
    }

    pub fn handle(&self, state: &mut State) -> AppendEntry {
        let req_id = self.id as i32;
        let mut default_reply = AppendEntry {
            id: state.id,
            commit_index: state.commit_index,
            previous_log_index: state.previous_log_index,
            previous_log_term: state.previous_log_term,
            entry: None,
            success: false,
            info: None,
            term: state.term,
        };

        // TODO: Might be worth doing this outside this handler, as all rpcs
        // will need to do this check, which avoids duplication
        if state.term > self.term {
            default_reply.info = Some(String::from("you have an outdated term"));
            return default_reply;
        } else if self.term > state.term {
            state.term = self.term;
            state.current_leader = req_id;

            default_reply.term = self.term;
            println!("request came from node with higher term, assigning as leader");
        } else {
            let legit_leader = verify_leader(state.current_leader, req_id);
            if !legit_leader {
                default_reply.info = Some("unrecognized as leader".to_string());
                println!(
                    "did not recognize {req_id} as leader. Already have leader {}",
                    state.current_leader
                );
                return default_reply;
            }

            state.current_leader = req_id;
        }

        let logs_match =
            check_log_consistency(state, self.previous_log_index, self.previous_log_term);

        if !logs_match {
            default_reply.info = Some("logs don't match".to_string());
            return default_reply;
        };

        println!("logs match....");
        // check if a log exists for us to append
        if let Some(new_entry) = &self.entry {
            state.append_entry(new_entry.clone());
            println!("updated_logs with {:?}", new_entry);
        } else {
            println!("empty append_entry received from leader - {}", self.id)
        }

        if state.commit_index < self.commit_index {
            let stop_index = state
                .persist_till(self.commit_index)
                .context("failed to persist logs")
                .unwrap();
            state.commit_index = stop_index;
        }

        default_reply.previous_log_index = state.previous_log_index;
        default_reply.previous_log_term = state.previous_log_term;
        default_reply.success = true;
        default_reply.commit_index = state.commit_index;

        default_reply
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct VoteRPC {
    pub id: u64,
    pub previous_log_index: u64,
    pub previous_log_term: u64,
    pub term: u64,
    pub granted: bool,
}

impl VoteRPC {
    pub fn default(id: u64) -> Self {
        VoteRPC {
            id,
            previous_log_index: 0,
            previous_log_term: 0,
            term: 0,
            granted: false,
        }
    }
    pub fn handle(&self, _state: &mut State) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RPC {
    AppendEntry(AppendEntry),

    Command(Command),

    CommandReply(CommandReply),

    Vote(VoteRPC),
}

fn default_commit_index() -> u64 {
    0
}

pub fn is_candidate_log_up_to_date(
    state: &mut State,
    candidate_last_log_index: u64,
    candidate_last_log_term: u64,
) -> bool {
    let last_log = state.entries.pop().unwrap_or_default();
    if candidate_last_log_index != last_log.index {
        candidate_last_log_term > last_log.term
    } else {
        candidate_last_log_index >= last_log.index
    }
}

// TODO: update this to do a look up on the logs instead
pub fn check_log_consistency(state: &State, prev_log_index: u64, prev_log_term: u64) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;

    #[test]
    fn test_append_entry_accepts_higher_term_as_leader() -> Result<()> {
        let req_id = 1;
        let node_id = 2;

        let higher_term = 10;
        let mut node_state = State::new(node_id);
        let mut append_entry = AppendEntry::new(req_id);
        append_entry.term = higher_term;

        let node_response = append_entry.handle(&mut node_state);

        let expected_response = AppendEntry {
            id: node_id,
            commit_index: 0,
            previous_log_index: 0,
            previous_log_term: 0,
            info: None,
            success: true,
            term: higher_term,
            entry: None,
        };

        assert_eq!(
            expected_response, node_response,
            "expected response for append_entry_handler don't match when it receives a higher term"
        );

        let expected_state = State {
            id: node_id,
            commit_index: 0,
            previous_log_index: 0,
            previous_log_term: 0,
            current_leader: req_id as i32,
            file_name: String::from("node-2-persist.json"),
            term: higher_term,
            entries: Vec::new(),
            vote: 0,
        };
        assert_eq!(
            expected_state, node_state,
            "expected the state for the node to have been updated to the new leader"
        );
        Ok(())
    }

    #[test]
    fn test_append_entry_rejects_lower_term() -> Result<()> {
        let req_id = 1;
        let node_id = 2;

        let lower_term = 1;
        let higher_term = 10;

        let mut node_state = State::new(node_id);
        node_state.term = higher_term;

        // to avoid edgecases
        node_state.current_leader = 999_999;

        let mut append_entry = AppendEntry::new(req_id);
        append_entry.term = lower_term;

        let node_response = append_entry.handle(&mut node_state);

        let expected_response = AppendEntry {
            id: node_id,
            commit_index: 0,
            previous_log_index: 0,
            previous_log_term: 0,
            info: Some(String::from("you have an outdated term")),
            success: false,
            term: higher_term,
            entry: None,
        };

        assert_eq!(
            expected_response, node_response,
            "expected response for append_entry_handler don't match when it receives a higher term"
        );

        let expected_state = State {
            id: node_id,
            commit_index: 0,
            previous_log_index: 0,
            previous_log_term: 0,
            current_leader: 999_999,
            file_name: String::from("node-2-persist.json"),
            term: higher_term,
            entries: Vec::new(),
            vote: 0,
        };
        assert_eq!(
            expected_state, node_state,
            "expected the state for the node to have remained the same after request from lower term"
        );
        Ok(())
    }
}
