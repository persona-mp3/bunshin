use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;

mod core;

use core::check_log_consistency;

#[derive(Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
pub struct Entry {
    pub index: u32,
    pub term: u32,
    pub command: Command,
}

/// Commands are sent by external clients
#[derive(Debug, Deserialize, Serialize, Clone, Eq, PartialEq)]
pub struct Command {
    pub key: String,
    pub value: String,
    pub operation: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CommandReply {
    value: String,
    success: bool,
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Clone)]
pub struct AppendEntry {
    pub id: u32,
    pub previous_log_index: u32,
    pub previous_log_term: u32,
    #[serde(default = "default_commit_index")]
    pub commit_index: u32,
    pub entry: Option<Entry>,
    pub success: bool,
    pub info: Option<String>,
}

impl AppendEntry {
    pub fn new(id: u32) -> AppendEntry {
        AppendEntry {
            id,
            previous_log_index: 0,
            previous_log_term: 0,
            commit_index: 0,
            entry: None,
            success: false,
            info: None,
        }
    }
    pub fn handle(&self, state: &mut State) -> Result<AppendEntry, Box<dyn std::error::Error>> {
        let req_id = self.id as i32;
        let mut default_reply = AppendEntry {
            id: state.id,
            commit_index: state.commit_index,
            previous_log_index: state.previous_log_index,
            previous_log_term: state.previous_log_term,
            entry: None,
            success: false,
            info: None,
        };

        let legit_leader = core::verify_leader(state.current_leader, req_id);
        if !legit_leader {
            default_reply.info = Some("unrecognized as leader".to_string());
            return Ok(default_reply);
        }

        state.current_leader = req_id;

        let logs_match =
            check_log_consistency(state, self.previous_log_index, self.previous_log_term);

        if !logs_match {
            default_reply.info = Some("logs don't match".to_string());
            return Ok(default_reply);
        };

        // check if a log exists for us to append
        if let Some(new_entry) = &self.entry {
            state.append_entry(new_entry.clone());
            println!("updated_logs with {:?}", new_entry);
        } else {
            println!("empty append_entry received from leader - {}", self.id)
        }

        if state.commit_index < self.commit_index {
            let stop_index = state.persist_till(self.commit_index)?;
            state.commit_index = stop_index;
        }

        default_reply.previous_log_index = state.previous_log_index;
        default_reply.previous_log_term = state.previous_log_term;
        default_reply.success = true;
        default_reply.commit_index = state.commit_index;

        Ok(default_reply)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RPC {
    AppendEntry(AppendEntry),

    Command(Command),

    CommandReply(CommandReply),
}

#[derive(Debug, Clone)]
pub struct Config {
    pub peers: Vec<String>,
}

#[derive(Debug)]
pub struct State {
    pub id: u32,
    pub previous_log_index: u32,
    pub previous_log_term: u32,
    pub entries: Vec<Entry>,
    pub config: Option<Config>,

    commit_index: u32,
    file_name: String,
    // when current-leader is -1, no leader has being selected
    current_leader: i32,
}

impl State {
    pub fn default(id: u32) -> State {
        let previous_log_index = 0;
        let previous_log_term = 0;
        let entries: Vec<Entry> = vec![];
        let file_name = format!("node-{}-persist.json", id);

        State {
            id,
            commit_index: 0,
            previous_log_index,
            previous_log_term,
            config: None,
            entries,
            file_name,
            current_leader: -1,
        }
    }

    /// persist_till flushes logs to `self.file` starting from `self.commit` up
    /// to `self.stop`. If no logs exists within such range, it returns the current
    /// value of `self.commit`. Otherwise it returns the index of the latest log
    /// that has been persisted
    fn persist_till(&self, stop: u32) -> Result<u32, Box<dyn std::error::Error>> {
        let mut actual_stop_index = 0;
        let file = File::options()
            .create(true)
            .append(true)
            .open(&self.file_name)?;

        let mut writer = BufWriter::new(file);

        if let Some(volatile_logs) = self
            .entries
            .get(self.commit_index as usize..stop as usize + 1)
        // to make it inclusive we add 1
        {
            for log in volatile_logs.iter() {
                let json = serde_json::to_string(log);
                if let Ok(value) = json {
                    if let Err(err) = serde_json::to_writer_pretty(&mut writer, &value) {
                        eprintln!("failed to persist: {:?}, {:?}, json: {:?}", err, log, value);
                        continue;
                    }

                    writer.write_all(b"\n")?;
                } else {
                    eprintln!("could not marshall: {:?}, {:?}", json.unwrap(), log);
                    continue;
                }

                actual_stop_index = log.index;
            }
        } else {
            println!("nothing to update");
            return Ok(self.commit_index);
        }

        Ok(actual_stop_index)
    }

    fn has_key(&self, key: &String) -> Option<Command> {
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

    fn append_entry(&mut self, entry: Entry) {
        self.previous_log_index = entry.index;
        self.previous_log_term = entry.term;
        self.entries.push(entry.clone());
    }

    fn append_new_command(&mut self, cmd: &Command) -> Entry {
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

pub fn handle_conn(conn: TcpStream, state: &mut State) -> Result<(), Box<dyn std::error::Error>> {
    println!("accepted new connection from {:?}", conn.peer_addr()?);

    let reader = BufReader::new(conn.try_clone()?);
    let mut writer = BufWriter::new(conn);

    let parser = serde_json::Deserializer::from_reader(reader);
    for msg in parser.into_iter::<RPC>() {
        let rpc_msg = match msg {
            Ok(m) => m,
            Err(ref err) => {
                println!("error parsing message {:?}, {:?}", err, &msg);
                continue;
            }
        };

        match rpc_msg {
            RPC::AppendEntry(append_entry) => {
                let reply = match append_entry.handle(state) {
                    Ok(rep) => rep,
                    Err(err) => {
                        println!("append_entry reply failed: {}", err);
                        continue;
                    }
                };

                let rpc_reply = RPC::AppendEntry(reply);
                serde_json::to_writer(&mut writer, &rpc_reply)?;
                writer.write_all(b"\n")?;
                writer.flush()?;
            }
            RPC::Command(command) => {
                let reply = handle_command_rpc(state, &command);
                let rpc_reply = RPC::CommandReply(reply);
                serde_json::to_writer(&mut writer, &rpc_reply)?;
                writer.write_all(b"\n")?;
                writer.flush()?;
            }

            _ => {
                todo!("unexpected rpc recevied: {:?}", rpc_msg);
            }
        }
    }

    Ok(())
}

fn handle_command_rpc(state: &mut State, command: &Command) -> CommandReply {
    let entry_exists = state.has_key(&command.key);

    match command.operation.as_str() {
        // we just place it in there regardless
        "set" => {
            state.append_new_command(command);
            println!("recvd set request for {:?}", command);
            return CommandReply {
                success: true,
                value: command.value.to_string(),
            };
        }

        "rm" => {
            // check if we have that value
            match entry_exists {
                None => (),
                Some(entry) => {
                    state.append_new_command(command);
                    return CommandReply {
                        success: true,
                        value: entry.value,
                    };
                }
            }
        }
        "get" => {
            // check
            match entry_exists {
                None => (),
                Some(entry) => {
                    return CommandReply {
                        success: true,
                        value: entry.value,
                    };
                }
            }
        }
        &_ => {
            panic!("unexpected operation from command: {:?}", command)
        }
    }
    CommandReply {
        value: "not_found".to_string(),
        success: false,
    }
}

fn default_commit_index() -> u32 {
    0
}
