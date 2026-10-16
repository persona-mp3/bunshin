use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Entry {
    pub index: u32,
    pub term: u32,
    pub command: Command,
}

/// Commands are sent by external clients
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Command {
    pub key: String,
    pub value: String,
    // TODO: why
    #[serde(default = "default_operation")]
    pub operation: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CommandReply {
    value: String,
    success: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RPC {
    AppendEntry {
        id: u32,
        previous_log_index: u32,
        previous_log_term: u32,
        #[serde(default = "default_commit_index")]
        commit_index: u32,
        entry: Option<Entry>,
        success: bool,
    },

    Command(Command),
}

#[derive(Debug)]
pub struct State {
    pub id: u32,
    pub previous_log_index: u32,
    pub previous_log_term: u32,
    pub entries: Vec<Entry>,
    pub file: Option<File>,
    commit_index: u32,
    file_name: String,
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
            entries,
            file: None,
            file_name,
        }
    }

    /// persist_till flushes logs to `self.file` starting from `self.commit` up
    /// to `self.stop`. If no logs exists within such range, it returns the current
    /// value of `self.commit`. Otherwise it returns the index of the latest log
    /// that has been persisted
    fn persist_till(&mut self, stop: u32) -> Result<u32, Box<dyn std::error::Error>> {
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
                return Some(entry.command.clone());
            };
        }
        None
    }

    fn append_new_entry(&mut self, cmd: &Command) -> Entry {
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

pub fn handle_conn(
    node_id: u32,
    conn: TcpStream,
    state: &mut State,
) -> Result<(), Box<dyn std::error::Error>> {
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
            RPC::AppendEntry {
                id: _,
                previous_log_index,
                previous_log_term,
                entry,
                commit_index,
                success: _,
            } => {
                if previous_log_index < state.previous_log_index
                    || previous_log_term < state.previous_log_term
                {
                    serde_json::to_writer(
                        &mut writer,
                        &RPC::AppendEntry {
                            id: node_id,
                            previous_log_index: state.previous_log_index,
                            previous_log_term: state.previous_log_term,
                            commit_index: state.commit_index,
                            entry: None,
                            success: false,
                        },
                    )?;

                    writer.write_all(b"\n")?;
                    writer.flush()?;
                    continue;
                }

                if previous_log_index > state.previous_log_index
                    || previous_log_term > state.previous_log_term
                {
                    println!(
                        "prev_log_index and prev_log_term outdated {node_id} idx: {:?}, term: {:?}, {:?} ",
                        previous_log_index, previous_log_term, state,
                    );

                    serde_json::to_writer(
                        &mut writer,
                        &RPC::AppendEntry {
                            id: node_id,
                            previous_log_index: state.previous_log_index,
                            previous_log_term: state.previous_log_term,
                            entry: None,
                            commit_index: state.commit_index,
                            success: false,
                        },
                    )?;
                    writer.write_all(b"\n")?;
                    writer.flush()?;

                    continue;
                }

                if let Some(entry) = entry {
                    state.previous_log_index = entry.index;
                    state.previous_log_term = entry.term;
                    state.entries.push(entry);
                } else {
                    println!("no provided entry from {node_id}");
                }

                // check here if we need to persist
                if state.commit_index < commit_index {
                    let latest_commit = state.persist_till(commit_index)?;
                    println!("stopped commit at {latest_commit}");
                    state.commit_index = latest_commit;
                }

                serde_json::to_writer(
                    &mut writer,
                    &RPC::AppendEntry {
                        id: node_id,
                        previous_log_index: state.previous_log_index,
                        previous_log_term: state.previous_log_term,
                        entry: None,
                        commit_index: state.commit_index,
                        success: true,
                    },
                )?;
                writer.write_all(b"\n")?;
                writer.flush()?;
            }

            RPC::Command(command) => {
                let mut reply = CommandReply {
                    success: true,
                    value: "".to_string(),
                };

                // since get is 'read', the last operation must not be a 'rm', otherwise it doesn't exist
                match command.operation.as_str() {
                    "get" => match state.has_key(&command.key) {
                        Some(stored_entry) => {
                            if stored_entry.operation == "rm" {
                                reply.success = false;
                                reply.value = "not_found".to_string();
                            } else if stored_entry.operation == "set " {
                                reply.success = true;
                                reply.value = stored_entry.value;
                            } else {
                                panic!(
                                    "last_operation was neither set or rm
                                        {:?},",
                                    &stored_entry
                                )
                            }
                        }
                        None => {
                            reply.success = false;
                            reply.value = "not_found".to_string();
                        }
                    },

                    "rm" => match state.has_key(&command.key) {
                        Some(stored_entry) => {
                            if stored_entry.operation != "rm" {
                                state.append_new_entry(&command);
                                reply.value = stored_entry.value;
                                println!(
                                    "[rm_request] last_operation: {:?} for key: {:?}",
                                    stored_entry.operation, stored_entry.key,
                                );
                            } else if stored_entry.operation == "rm" {
                                reply.success = false;
                                reply.value = "not_found".to_string();
                            }
                        }
                        None => {
                            reply.success = false;
                            reply.value = "not_found".to_string();
                        }
                    },

                    "set" => {
                        let new_entry = state.append_new_entry(&command);
                        reply.value = command.value;
                        println!("new entry: {:?}", new_entry)
                    }

                    &_ => {
                        reply.success = false;
                        reply.value = format!("unknown operation '{:?}'", command.operation);
                        println!("invalid request receieved: {:?}", command)
                    }
                }

                serde_json::to_writer(&mut writer, &reply)?;
                writer.write_all(b"\n")?;
                writer.flush()?;
            }
        }
    }

    Ok(())
}

fn default_commit_index() -> u32 {
    0
}

fn default_operation() -> String {
    "error_invalid".to_string()
}
