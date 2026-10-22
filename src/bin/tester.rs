use anyhow::{Context, Result, anyhow};
use bunshin::{self, protocol};

use clap::Parser;
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpStream;

const TEST_ID: u64 = 90_90_90;
const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const RESET: &str = "\x1b[0m";

#[derive(Parser, Debug, Clone)]
#[command(author, version, about)]
struct Cli {
    #[arg(short, long)]
    port: usize,

    #[arg(short, long)]
    node_id: u64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    let addr = format!("localhost:{}", args.port);
    let conn = match TcpStream::connect(addr) {
        Ok(stream) => stream,
        Err(err) => {
            return Err(format!(
                "could not connect to {}. please make sure it is running. reason: {}",
                args.port, err
            )
            .into());
        }
    };

    let mut reader = BufReader::new(conn.try_clone()?);
    let mut writer = BufWriter::new(conn);

    test_leader_recognition(args.node_id, &mut reader, &mut writer)?;
    println!("{GREEN}leader_recognition passed{RESET}");
    test_node_appends_logs(args.node_id, &mut reader, &mut writer)?;
    Ok(())
}

/// test_leader_recognition sends two rpcs. If the target node has just started
/// it is expected to acknowledge the first this node as a leader, hence the first
/// request must return `protocol::AppendEntry.success` as true. The second rpc
/// is contains another id and the node should reject it sending `protocol::AppendEntry.success`
/// as false. No data is sent across
fn test_leader_recognition(
    target_node_id: u64,
    reader: &mut BufReader<TcpStream>,
    writer: &mut BufWriter<TcpStream>,
) -> Result<(), Box<dyn std::error::Error>> {
    let request = protocol::AppendEntry::new(TEST_ID);
    let rpc_request = protocol::RPC::AppendEntry(request.clone());
    // QUESTION: we simply want to reborrow the underlying write stream??
    serde_json::to_writer(&mut *writer, &rpc_request)?;
    writer.write_all(b"\n")?;
    writer.flush()?;

    let mut json_reader =
        serde_json::Deserializer::from_reader(reader).into_iter::<protocol::RPC>();
    let inital_response = match json_reader.next().unwrap() {
        Ok(res) => res,
        Err(err) => return Err(format!("failed to read response: {}", err).into()),
    };

    let mut expected_initial_response = protocol::AppendEntry::new(target_node_id);
    expected_initial_response.success = true;
    match inital_response {
        protocol::RPC::AppendEntry(reply) => {
            assert_eq!(expected_initial_response, reply);
        }

        _ => {
            return Err(format!(
                "{RED}expected append_entry, got: {:?}{RESET}",
                inital_response
            )
            .into());
        }
    }

    println!("{GREEN}initial response success{RESET}");

    let invalid_id = 8902;
    let invalid_request = protocol::AppendEntry::new(invalid_id);

    let invalid_rpc = protocol::RPC::AppendEntry(invalid_request.clone());

    serde_json::to_writer(&mut *writer, &invalid_rpc)?;
    writer.write_all(b"\n")?;
    writer.flush()?;

    let mut expected_invalid_reply = protocol::AppendEntry::new(target_node_id);
    expected_invalid_reply.success = false;
    expected_invalid_reply.info = Some("unrecognized as leader".to_string());

    let new_response = json_reader.next().unwrap()?;
    match new_response {
        protocol::RPC::AppendEntry(reply) => {
            assert_eq!(
                expected_invalid_reply, reply,
                "{RED}node is supposed to reject request from an unrecognized leader
                the acknowledged leader sent in a previous request: {}, invalid request: {}{RESET}
                ",
                TEST_ID, invalid_id,
            );
        }

        _ => {
            return Err(
                format!("{RED}expected append_entry, got: {:?}{RESET}", new_response).into(),
            );
        }
    }
    println!("{GREEN}invalid response passed{RESET}");
    Ok(())
}

fn test_node_appends_logs(
    _target_node_id: u64,
    reader: &mut BufReader<TcpStream>,
    writer: &mut BufWriter<TcpStream>,
) -> Result<()> {
    // send 3 logs, and test against previous_log_indexes
    let mut append_entry_requests = Vec::new();
    let base_append_entry = protocol::AppendEntry {
        id: TEST_ID,
        previous_log_index: 0,
        previous_log_term: 0,
        commit_index: 0,
        entry: Some(bunshin::Entry {
            index: 1,
            term: 1,
            command: bunshin::Command {
                key: String::from("init"),
                value: String::from("init_value"),
                operation: String::from("set"),
            },
        }),
        term: 1,
        success: false,
        info: None,
        
    };

    append_entry_requests.push(base_append_entry.clone());
    let mut second_append_entry = base_append_entry.clone();
    second_append_entry.previous_log_index = 1;
    second_append_entry.previous_log_term = 1;
    second_append_entry.entry = Some(bunshin::Entry {
        index: 2,
        term: 1,
        command: bunshin::Command {
            key: String::from("second_log_entry"),
            value: String::from("This is the second log entry"),
            operation: String::from("set"),
        },
    });

    append_entry_requests.push(second_append_entry);

    let mut json_reader =
        serde_json::Deserializer::from_reader(reader).into_iter::<protocol::RPC>();

    let mut failed_count = 0;

    for req in append_entry_requests {
        let reply = send_and_recv(
            &protocol::RPC::AppendEntry(req.clone()),
            writer,
            &mut json_reader,
        )?;
        match reply {
            protocol::RPC::AppendEntry(rpc_reply) => {
                // check if log indexes match
                if req.entry.as_ref().unwrap().index != rpc_reply.previous_log_index
                    || req.entry.as_ref().unwrap().term != rpc_reply.previous_log_term
                {
                    let err_msg = format!(
                        "{RED} log_consistency broken. (log_index, log_term)
                            expected ({}, {}), got ({}, {}) {RESET}
                            ",
                        req.entry.as_ref().unwrap().index,
                        req.entry.as_ref().unwrap().term,
                        rpc_reply.previous_log_index,
                        rpc_reply.previous_log_term
                    );

                    failed_count += 1;
                    println!("{}", err_msg)
                };
            }
            _ => {
                return Err(anyhow!(format!(
                    "{RED} [test_node_appends_logs] expected AppendEntryRPC got {:?} {RESET}",
                    reply
                )));
            }
        }
    }

    if failed_count == 0 {
        println!("{GREEN}test_node_appends passed{RESET}");
    };
    Ok(())
}

fn send_and_recv<R: Iterator<Item = serde_json::Result<protocol::RPC>>>(
    req: &protocol::RPC,
    writer: &mut BufWriter<TcpStream>,
    replies: &mut R,
) -> Result<protocol::RPC> {
    serde_json::to_writer(&mut *writer, req).context("failed to send request to target node")?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    replies
        .next()
        .ok_or_else(|| anyhow!("target node closed the connection"))?
        .context("failed to parse json from target node")
}
