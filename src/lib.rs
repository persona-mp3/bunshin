use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Write};

pub mod candidate;
pub mod follower;
pub mod handlers;
pub mod machine;
pub mod protocol;
pub mod transport;
pub mod utils;

pub const TIMEOUT: u64 = 10;

/// Sentinel value to represent that the node has just being initialised and
/// has no leader, or is in Candidate state
pub const ABSENT_LEADER: i32 = -1;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ShutdownErrors {
    NetworkDrop,
    Unexpected,
    Custom(String),
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RaftState {
    Leader,
    Follower,
    Candidate,
    Shutdown(ShutdownErrors),
}

#[derive(Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
pub struct LogEntry {
    pub index: usize,
    pub term: u64,
    pub command: Command,
}

impl LogEntry {
    /// Returns an entry with index and term initialised as 0
    /// operation as "set", key as "init" and value as "init_value"
    pub fn default() -> Self {
        LogEntry {
            index: 0,
            term: 0,
            command: Command {
                key: "init".to_string(),
                value: "init_value".to_string(),
                operation: "set".to_string(),
            },
        }
    }
}

/// Commands are sent by external clients
#[derive(Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
pub struct Command {
    pub key: String,
    pub value: String,
    pub operation: String,
}

impl Command {
    pub fn default() -> Self {
        Command {
            key: "init".to_string(),
            value: "init_value".to_string(),
            operation: "set".to_string(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct State {
    pub id: u64,
    pub logs: Vec<LogEntry>,
    pub commit_index: usize,
    /// log entries and state of the node is stored
    pub file_name: String,
    /// when current-leader is -1, no leader has being selected
    pub current_leader: Option<u64>,

    pub term: u64,
    /// vote is granted to a new candidate for a new term
    pub voted_for: Option<u64>,
    pub rs: RaftState,
}

impl State {
    pub fn new(id: u64) -> State {
        let file_name = format!("node-{}-persist.json", id);

        State {
            id,
            commit_index: 0,
            // initiate with sentinel value at 0
            logs: vec![LogEntry::default()],
            file_name,
            current_leader: None,
            term: 0,
            voted_for: None,
            rs: RaftState::Follower,
        }
    }

    /// persist_till flushes logs to `self.file` starting from `self.commit` up
    /// to `self.stop`. If no logs exists within such range, it returns the current
    /// value of `self.commit`. Otherwise it returns the index of the latest log
    /// that has been persisted
    pub fn persist_till(&self, stop: u64) -> Result<usize> {
        let mut actual_stop_index = 0;
        let file = File::options()
            .create(true)
            .append(true)
            .open(&self.file_name)?;

        let mut writer = BufWriter::new(file);

        if let Some(volatile_logs) = self.logs.get(self.commit_index..stop as usize + 1) {
            for log in volatile_logs.iter() {
                serde_json::to_writer(&mut writer, &log)
                    .with_context(|| "could not persist entry with serde".to_string())?;
                writer.write_all(b"\n")?;
                writer.flush()?;
                actual_stop_index = log.index;
            }
        } else {
            println!("nothing to update");
            return Ok(self.commit_index);
        }

        Ok(actual_stop_index)
    }

    pub fn has_key(&self, key: &String) -> Option<Command> {
        for entry in self.logs.iter().rev() {
            if entry.command.key == *key {
                if entry.command.operation == "rm" {
                    return None;
                }
                return Some(entry.command.clone());
            };
        }
        None
    }

    pub fn append_entry(&mut self, entry: LogEntry) {
        // current_logs = [0, 1, 2, 3, 4]
        // the next log should be entry.index should be 5
        assert_eq!(
            self.logs.len() + 1,
            entry.index,
            "
            it is required that a new log getting appended 
            must match the next index position of the logs.

            all_logs: {:?}
            new_index: {:?}

            ",
            self.logs,
            entry
        );
        self.logs.push(entry);
    }

    /// get_leader must be called after has_leader to check if a leader
    /// currently exists for this node, otherwise it panics
    pub fn get_leader(&self) -> u64 {
        self.current_leader.unwrap()
    }
    pub fn has_leader(&mut self) -> bool {
        self.current_leader.is_some()
    }

    pub fn assign_leader(&mut self, id: u64) {
        self.current_leader = Some(id);
    }

    pub fn get_log_at_index(&self, idx: usize) -> Option<LogEntry> {
        self.logs.get(idx).cloned()
    }

    pub fn get_last_log(&self) -> LogEntry {
        if self.logs.is_empty() {
            // this node's logs is completely empty
            LogEntry::default()
        } else {
            self.logs.last().unwrap().clone()
        }
    }

    pub fn truncate_from(&mut self, start_index: usize) {
        // [1, 2, 3, 4, 5] truncate form idx 4, means split at 4
        // let (truncated_logs, _) = self.logs.split_at(start_index);
        // self.logs = truncated_logs.to_vec()
        self.logs.truncate(start_index);
    }
    pub fn append_log(&mut self, log_entry: LogEntry) {
        self.logs.push(log_entry)
    }

    /// Raft uses the voting process to prevent a candidate from winning an election
    /// unless its log contains all committed entries. A candidate must contact a
    /// majority of the cluster in order to be elected, which means that every
    /// committed entry must be present in at least one of those servers. If the
    /// candidate's log is at least as up-to-date as any other log in that majority
    /// (where "up-to-date" is defined precisely below), then it will hold all the
    /// committed entries. The RequestVote RPC implements this restriction: the RPC
    /// includes information about the candidate's log, and the voter denies its vote
    /// if its own log is more up-to-date than that of the candidate.
    ///
    /// Raft determines which of two logs is more up-to-date by comparing the index
    /// and term of the last entries in the logs. If the logs have last entries with
    /// different terms, then the log with the later term is more up-to-date. If the
    /// logs end with the same term, then whichever log is longer is more up-to-date.
    pub fn candidate_log_is_up_to_date(&self, log_index: usize, log_term: u64) -> bool {
        if log_index < self.logs.len() {
            return false;
        } else {
            log_term >= self.get_last_log().term
        }
    }
}
