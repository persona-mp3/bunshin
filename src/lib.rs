use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Write};

pub mod protocol;
pub mod handlers;
pub mod machine;
pub mod utils;
pub const TIMEOUT: u64 = 10;

/// Sentinel value to represent that the node has just being initialised and
/// has no leader, or is in Candidate state
pub const ABSENT_LEADER: i32 = -1;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum RaftState {
    Leader,
    Follower,
    Candidate,
    Shutdown,
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

        if let Some(volatile_logs) = self.logs.get(self.commit_index as usize..stop as usize + 1) {
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
        self.logs.get(idx).map(|entry| entry.clone())
    }

    pub fn get_last_log(&self) -> LogEntry {
        if self.logs.len() == 0 {
            // this node's logs is completely empty
            LogEntry::default()
        } else {
            self.logs.get(self.logs.len() - 1).unwrap().clone()
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
}
