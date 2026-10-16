use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;

#[derive(Debug, Deserialize, Serialize)]
pub struct Entry {
    pub index: u32,
    pub term: u32,
    pub command: Command,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Command {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RPC {
    AppendEntry {
        id: u32,
        previous_log_index: u32,
        previous_log_term: u32,
        #[serde(default = "default_commit_index")]
        commit_index: u32,
        entry: Option<Entry>,
        success: bool,
    },
}

#[derive(Debug)]
pub struct State {
    pub previous_log_index: u32,
    pub previous_log_term: u32,
    pub entries: Vec<Entry>,
    pub file: Option<File>,
    commit_index: u32,
}

impl State {
    pub fn default() -> State {
        let previous_log_index = 0;
        let previous_log_term = 0;
        let entries: Vec<Entry> = vec![];
        State {
            commit_index: 0,
            previous_log_index,
            previous_log_term,
            entries,
            file: None,
        }
    }

    /// persist_till flushes logs to `self.file` starting from `self.commit` up
    /// to `self.stop`. If no logs exists within such range, it returns the current
    /// value of `self.commit`. Otherwise it returns the index of the latest log
    /// that has been persisted
    fn persist_till(&mut self, stop: u32) -> u32 {
        let mut actual_stop_index = 0;
        if let Some((_, volatile_logs)) = self.entries.split_at_checked(self.commit_index as usize)
        {
            for log in volatile_logs.iter() {
                if log.index > stop {
                    break;
                }

                actual_stop_index = log.index;
                println!("{:?}", log);
            }
        } else {
            println!("nothing to update");
            self.commit_index;
        }

        actual_stop_index
    }
}

pub fn handle_conn(
    node_id: u32,
    conn: TcpStream,
    state: &mut State,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("accepted new connection {:?}", conn.peer_addr()?);

    let reader = BufReader::new(conn.try_clone()?);
    let mut writer = BufWriter::new(conn);

    let parser = serde_json::Deserializer::from_reader(reader);
    for msg in parser.into_iter::<RPC>() {
        let rpc_msg = match msg {
            Ok(m) => m,
            Err(err) => {
                println!("error parsing message {:?}", err);
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
                        "prev_log_index and prev_log_term are not up to date with {node_id} idx: {:?}, term: {:?}, {:?} ",
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
                    println!("updated entries {:?}\n", state.entries)
                } else {
                    println!("no provided entry from {node_id}");
                }

                // check here if we need to persist
                if state.commit_index < commit_index {
                    let latest_commit = state.persist_till(commit_index);
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
        }
    }

    Ok(())
}

fn default_commit_index() -> u32 {
    return 0;
}
