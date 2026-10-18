use anyhow::Result;
use crossbeam::channel::{self, Receiver, Sender, select};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub mod protocol;
pub const TIMEOUT: u64 = 10;

#[derive(Debug, Clone)]
pub enum RaftState {
    Leader,
    Follower,
    Candidate,
}

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

#[derive(Debug, Clone)]
pub struct Node {
    pub id: u64,
    pub raft_state: RaftState,
    pub state: State,
    pub addr: String,
    pub reset_timer: Sender<()>,
}

impl Node {
    pub fn new(id: u64, listen_addr: String) -> Node {
        let (tx, _) = channel::unbounded();
        Node {
            id,
            addr: listen_addr,
            state: State::new(id),
            raft_state: RaftState::Follower,
            reset_timer: tx,
        }
    }

    // pub fn run(&mut self) -> Result<()> {
    //     // let self_node = self.clone();
    //     // let node = Arc::new(Mutex::new(self));
    //     let node = Arc::new(Mutex::new(Node::new(self.id,  self.addr.clone())));
    //     // let node = Arc::new(Mutex::new(self.clone()));
    //
    //     let addr = self.addr.clone();
    //     let clone = Arc::clone(&node);
    //
    //     thread::spawn(move || {
    //         // always want to panic if the server stops
    //         start_server(addr, clone).unwrap();
    //     });
    //
    //     let mut current_raft_state = RaftState::Follower;
    //     let mut current_term = 0;
    //
    //     loop {
    //         println!("current_term: {}", current_term);
    //         current_raft_state = match current_raft_state {
    //             RaftState::Leader => {
    //                 let mut protected_node = node.lock().unwrap();
    //                 println!("{:?}", protected_node.state);
    //                 let next_state = run_leader(&mut protected_node.state)?;
    //
    //                 next_state
    //             }
    //
    //             RaftState::Follower => {
    //                 let (send_reset_timer, receiver_reset_timer) = channel::unbounded();
    //
    //                 {
    //                     let mut protected_node = node.lock().unwrap();
    //                     println!("follower:: {:?}", protected_node.state);
    //                     protected_node.reset_timer = send_reset_timer;
    //                 }
    //
    //                 let timeout = Duration::from_secs(TIMEOUT);
    //                 let next_state = run_follower(timeout, receiver_reset_timer).unwrap();
    //
    //                 next_state
    //             }
    //
    //             RaftState::Candidate => {
    //                 let mut protected_node = node.lock().unwrap();
    //                 let next_state = run_candidate(&mut protected_node.state)?;
    //                 next_state
    //             }
    //         };
    //         current_term += 1;
    //     }
    // }
}

pub fn run_follower(timeout: Duration, receive_reset_timer: Receiver<()>) -> Result<RaftState> {
    let mut deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        select! {
        recv(receive_reset_timer) -> _sig => {
            deadline = Instant::now() + timeout;
            println!("resetting timer");
        },
        default(remaining) => {
            println!("timeout reached, descension to candidate");
            break
        }
        }
    }

    Ok(RaftState::Candidate)
}

pub fn run_candidate(state: &mut State) -> Result<RaftState> {
    println!("running candidate, current_state: {:?}", state);
    Ok(RaftState::Leader)
}

pub fn run_leader(state: &mut State) -> Result<RaftState> {
    println!("running leader, current_state: {:?}", state);
    Ok(RaftState::Follower)
}

#[derive(Debug, Clone)]
pub struct State {
    pub id: u64,
    pub previous_log_index: u32,
    pub previous_log_term: u32,
    pub entries: Vec<Entry>,
    commit_index: u32,
    file_name: String,
    // when current-leader is -1, no leader has being selected
    current_leader: i32,
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
            // config: None,
            entries,
            file_name,
            current_leader: -1,
            // reset_timer,
        }
    }

    /// persist_till flushes logs to `self.file` starting from `self.commit` up
    /// to `self.stop`. If no logs exists within such range, it returns the current
    /// value of `self.commit`. Otherwise it returns the index of the latest log
    /// that has been persisted
    fn persist_till(&self, stop: u32) -> Result<u32> {
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

fn start_server(addr: String, node: Arc<Mutex<Node>>) -> Result<()> {
    let listener = TcpListener::bind(&addr)?;
    println!("successfully connected to {:?}", addr);
    for stream in listener.incoming() {
        if let Ok(conn) = stream {
            let conn_node = Arc::clone(&node);
            thread::spawn(|| {
                if let Err(err) = protocol::handle_conn(conn, conn_node) {
                    println!("error handling connection: {err}");
                };
            });
        } else {
            println!("error accepting connection: {:?}", stream.err().unwrap());
            continue;
        };
    }
    Ok(())
}

pub fn run(out: Node) -> Result<()> {
    let addr = out.addr.clone();
    let node = Arc::new(Mutex::new(out));

    let server_clone = Arc::clone(&node);

    thread::spawn(move || {
        // always want to panic if the server stops
        start_server(addr, server_clone).unwrap();
    });

    let mut current_raft_state = RaftState::Follower;

    loop {
        current_raft_state = match current_raft_state {
            RaftState::Leader => {
                let mut n = node.lock().unwrap();
                println!("{:?}", n.state);
                

                run_leader(&mut n.state)?
            }

            RaftState::Follower => {
                let (send_reset_timer, receiver_reset_timer) = channel::unbounded();

                {
                    let mut n = node.lock().unwrap();
                    n.reset_timer = send_reset_timer;
                }

                let timeout = Duration::from_secs(TIMEOUT);
                

                run_follower(timeout, receiver_reset_timer).unwrap()
            }

            RaftState::Candidate => {
                let mut n = node.lock().unwrap();
                
                run_candidate(&mut n.state)?
            }
        };
    }
}
