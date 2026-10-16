#![allow(dead_code)]
use serde::{Deserialize, Serialize};
use std::io::{BufReader, BufWriter, Write};
use std::net::TcpListener;
use std::net::TcpStream;

#[derive(Debug, Deserialize, Serialize)]
struct Command {
  key: String,
  value: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Entry {
  index: u32,
  term: u32,
  command: Command,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RPC {
  AppendEntry {
    id: u32,
    previous_log_index: u32,
    previous_log_term: u32,
    entry: Option<Entry>,
    success: bool,
  },
}

#[derive(Debug)]
struct State {
  previous_log_index: u32,
  previous_log_term: u32,
  entries: Vec<Entry>,
}

fn main() {
  let args: Vec<String> = std::env::args().collect();
  if args.len() < 2 {
    eprintln!("please provide id for node");
    return;
  }

  let node_id: u32 = args[1].trim().parse().unwrap();

  let listener = TcpListener::bind("127.0.0.1:1738").unwrap();
  println!("server listening on localhost:1738");
  for stream in listener.incoming() {
    let mut node = Node::new(node_id);
    match stream {
      Ok(conn) => {
        let _ = node.handle_conn(conn);
      }
      Err(err) => {
        println!("error accepting connectoin: {:?}", err)
      }
    }
  }
}

struct Node {
  id: u32,
  state: State,
}

impl Node {
  fn new(id: u32) -> Node {
    let previous_log_index = 0;
    let previous_log_term = 0;
    let entries: Vec<Entry> = vec![];
    Node {
      id,
      state: State {
        previous_log_index,
        previous_log_term,
        entries,
      },
    }
  }
  fn handle_conn(&mut self, conn: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
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
          id,
          previous_log_index,
          previous_log_term,
          entry,
          success: _,
        } => {
          if previous_log_index < self.state.previous_log_index
            || previous_log_term < self.state.previous_log_term
          {
            serde_json::to_writer(
              &mut writer,
              &RPC::AppendEntry {
                id: self.id,
                previous_log_index: self.state.previous_log_index,
                previous_log_term: self.state.previous_log_term,
                entry: None,
                success: false,
              },
            )?;

            writer.write_all(b"\n")?;
            writer.flush()?;
            continue;
          }

          if previous_log_index > self.state.previous_log_index
            || previous_log_term > self.state.previous_log_term
          {
            println!(
              "
                prev_log_index and prev_log_term are not up to date with {id}
                prev_log_index: {:?}, prev_log_term: {:?}, raft_state: {:?}
              ",
              previous_log_index, previous_log_term, self.state,
            );

            serde_json::to_writer(
              &mut writer,
              &RPC::AppendEntry {
                id: self.id,
                previous_log_index: self.state.previous_log_index,
                previous_log_term: self.state.previous_log_term,
                entry: None,
                success: false,
              },
            )?;
            writer.write_all(b"\n")?;
            writer.flush()?;

            continue;
          }

          if let Some(entry) = entry {
            self.state.previous_log_index = entry.index;
            self.state.previous_log_term = entry.term;
            self.state.entries.push(entry);
            println!("updated entries succesfully: {:?}", self.state.entries)
          } else {
            println!("no provided entry from {id}");
          }

          serde_json::to_writer(
            &mut writer,
            &RPC::AppendEntry {
              id: self.id,
              previous_log_index: self.state.previous_log_index,
              previous_log_term: self.state.previous_log_term,
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
}
