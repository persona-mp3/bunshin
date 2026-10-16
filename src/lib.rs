use serde::{Deserialize, Serialize};
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;

#[derive(Debug)]
pub struct State {
    pub previous_log_index: u32,
    pub previous_log_term: u32,
    pub entries: Vec<Entry>,
}

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
        entry: Option<Entry>,
        success: bool,
    },
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
                        "
                prev_log_index and prev_log_term are not up to date with {node_id}
                prev_log_index: {:?}, prev_log_term: {:?}, raft_state: {:?}
              ",
                        previous_log_index, previous_log_term, state,
                    );

                    serde_json::to_writer(
                        &mut writer,
                        &RPC::AppendEntry {
                            id: node_id,
                            previous_log_index: state.previous_log_index,
                            previous_log_term: state.previous_log_term,
                            entry: None,
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
                    println!("updated entries succesfully: {:?}", state.entries)
                } else {
                    println!("no provided entry from {node_id}");
                }

                serde_json::to_writer(
                    &mut writer,
                    &RPC::AppendEntry {
                        id: node_id,
                        previous_log_index: state.previous_log_index,
                        previous_log_term: state.previous_log_term,
                        entry: None,
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
