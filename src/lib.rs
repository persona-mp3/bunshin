use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Write};

pub mod protocol;
pub mod state_machine;
pub const TIMEOUT: u64 = 10;
pub mod utils;

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
pub struct Entry {
    pub index: u64,
    pub term: u64,
    pub command: Command,
}

// impl Entry {
// }

impl Default for Entry {
    /// Returns an entry with index and term initialised as 0
    /// operation as "set", key as "init" and value as "init_value"
    fn default() -> Self {
        Entry {
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
    pub previous_log_index: u64,
    pub previous_log_term: u64,
    pub entries: Vec<Entry>,
    pub commit_index: u64,
    /// log entries and state of the node is stored
    pub file_name: String,
    /// when current-leader is -1, no leader has being selected
    pub current_leader: i32,
    pub term: u64,
    /// vote is granted to a new candidate for a new term
    pub vote: i64,
}

impl State {
    pub fn new(id: u64) -> State {
        let previous_log_index = 0;
        let previous_log_term = 0;
        let entries: Vec<Entry> = vec![];
        let file_name = format!("node-{}-persist.json", id);

        State {
            id,
            commit_index: 0,
            previous_log_index,
            previous_log_term,
            entries,
            file_name,
            current_leader: -1,
            term: 0,
            vote: 0,
        }
    }

    /// persist_till flushes logs to `self.file` starting from `self.commit` up
    /// to `self.stop`. If no logs exists within such range, it returns the current
    /// value of `self.commit`. Otherwise it returns the index of the latest log
    /// that has been persisted
    pub fn persist_till(&self, stop: u64) -> Result<u64> {
        let mut actual_stop_index = 0;
        let file = File::options()
            .create(true)
            .append(true)
            .open(&self.file_name)?;

        let mut writer = BufWriter::new(file);

        if let Some(volatile_logs) = self
            .entries
            .get(self.commit_index as usize..stop as usize + 1)
        {
            for log in volatile_logs.iter() {
                serde_json::to_writer(&mut writer, &log)
                    .with_context(|| format!("could not persist entry with serde"))?;
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
        for entry in self.entries.iter().rev() {
            if entry.command.key == *key {
                if entry.command.operation == "rm" {
                    return None;
                }
                return Some(entry.command.clone());
            };
        }
        None
    }

    pub fn append_entry(&mut self, entry: Entry) {
        self.previous_log_index = entry.index;
        self.previous_log_term = entry.term;
        self.entries.push(entry);
    }

    pub fn append_new_command(&mut self, cmd: &Command) -> Entry {
        self.previous_log_index += 1;

        let entry = Entry {
            index: self.previous_log_index,
            term: self.previous_log_term,
            command: cmd.clone(),
        };

        self.entries.push(entry.clone());
        entry
    }
}
