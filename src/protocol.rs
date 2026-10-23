use crate::{Command, Entry, Node, State};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

#[derive(Debug, Deserialize, Serialize)]
pub struct CommandReply {
    pub value: String,
    pub success: bool,
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Clone)]
pub struct AppendEntry {
    pub id: u64,
    pub previous_log_index: u32,
    pub previous_log_term: u32,
    #[serde(default = "default_commit_index")]
    pub commit_index: u32,
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

    pub fn handle(&self, state: &mut State) -> Result<AppendEntry> {
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
            return Ok(default_reply);
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
                return Ok(default_reply);
            }

            state.current_leader = req_id;
        }

        let logs_match =
            check_log_consistency(state, self.previous_log_index, self.previous_log_term);

        if !logs_match {
            default_reply.info = Some("logs don't match".to_string());
            return Ok(default_reply);
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

pub fn handle_conn(conn: TcpStream, node: Arc<Mutex<Node>>) -> Result<()> {
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
                let mut current_node = node.lock().unwrap();
                let reply = match append_entry.handle(&mut current_node.state) {
                    Ok(rep) => rep,
                    Err(err) => {
                        println!("append_entry reply failed: {}", err);
                        continue;
                    }
                };

                if reply.success {
                    current_node
                        .reset_timer
                        .send(())
                        .expect("could not send to reset timer");
                }

                let rpc_reply = RPC::AppendEntry(reply);
                serde_json::to_writer(&mut writer, &rpc_reply)?;
                writer.write_all(b"\n")?;
                writer.flush()?;
            }

            RPC::Command(command) => {
                let mut current_node = node.lock().unwrap();
                let reply = handle_command_rpc(&mut current_node.state, &command);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_append_entry_accepts_higher_term_as_leader() -> Result<()> {
        let req_id = 1;
        let node_id = 2;

        let higher_term = 10;
        let mut node_state = State::new(node_id);
        let mut append_entry = AppendEntry::new(req_id);
        append_entry.term = higher_term;

        let node_response = append_entry.handle(&mut node_state)?;

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

        let node_response = append_entry.handle(&mut node_state)?;

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
        };
        assert_eq!(
            expected_state, node_state,
            "expected the state for the node to have remained the same after request from lower term"
        );
        Ok(())
    }
}
