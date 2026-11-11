// The way a leader works is that it sends rpcs to all other nodes in the
// network before they timeout
#![allow(unused)]
use crate::{
    RaftState, ShutdownErrors, State,
    handlers::{handle_append_entry_request, handle_vote_request},
    protocol::{AppendEntryReq, Packet, Rpc},
    transport::Transport,
    utils::send_and_recv_json,
    worker::WorkerThread,
};
use anyhow::{Context, Result};
use crossbeam::channel::{self, Receiver, select};
use serde_json;
use std::{
    io::{BufReader, BufWriter},
    net::TcpStream,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

// TODO(daniel)
// For the workers, the only ways they exit are the following:
// - higher term from a node
// - unhandled on unexpected states: panic
// - failed requests. Not sure but we might need more robust ways to
// determine when this function returns, those threads get cleaned up too,
// Another mechanism is that this leader panics with the lock, but if
// the leader returns succesfully?  for an unexpected reason not related to the
// protocol?
pub fn run_leader<T: Transport + 'static>(
    network_rx: &mut Receiver<Packet>,
    peers: Vec<String>,
    state: Arc<Mutex<State>>,
    hb: Duration,
    dialer: Arc<T>,
) -> (State, RaftState) {
    let mut timer = channel::after(hb);

    let mut node_id: u64;
    let mut ruling_term: u64;

    {
        let s = state.lock().unwrap();
        node_id = s.id;
        ruling_term = s.term;
    }

    let clone = Arc::clone(&state);

    setup_leader(peers, state, hb);

    loop {
        select! {
            recv(network_rx) -> signal => {
                let packet = match signal {
                    Ok(packet) => packet,
                    Err(err) => {
                        eprintln!("network_rx dropped while a leader:{}, sending shutdown error: {:?}", node_id, err);
                        let mut final_state = clone.lock().unwrap();
                        return (final_state.clone(), RaftState::Shutdown(ShutdownErrors::NetworkDrop));
                    },
                };

                // match against the rpc we recvd
                match packet.rpc {
                    Rpc::AppendEntryReq(req) => {
                        let mut s = clone.lock().unwrap();
                        let (reply, next) = handle_append_entry_request(&req, &mut s);

                        // non-blocking and fails  after first try to send
                        let _ = packet.sender.try_send(Rpc::AppendEntryReply(reply.clone()));
                        if reply.term > ruling_term {
                            println!("dropping from leader->follower");
                            return (s.clone(), RaftState::Follower)
                        } else if req.term == ruling_term {
                            println!("!! invariant found, two leaders for current term !!");
                            println!("!! i_am:{:?}, my_term: {:?}, append_entry_req_from:{:?}", node_id, ruling_term, req);
                            return (s.clone(), RaftState::Shutdown(ShutdownErrors::Unexpected));
                        };

                        println!("recvd lower_term append append_entry::{:?}", req);

                    },
                    Rpc::VoteReq(req) => {
                        let mut s = clone.lock().unwrap();
                        let (reply, next) = handle_vote_request(&req, &mut s);

                        // non-blocking and fails  after first try to send
                        let _ = packet.sender.try_send(Rpc::VoteReply(reply.clone()));


                        if next.is_some() {
                            println!("stepping down from leader in vote_handler");
                            return (s.clone(), next.unwrap());
                        }


                    },
                    _ => {
                        eprintln!("unhandled rpc for leader: {:?}", packet.rpc);
                            let mut final_state = clone.lock().unwrap();
                            return (final_state.clone(), RaftState::Shutdown(ShutdownErrors::Unexpected));
                    },
                }
            }

        }
    }
}

fn setup_leader(peers: Vec<String>, s: Arc<Mutex<State>>, heartbeat: Duration) {
    for addr in peers {
        let c = Arc::clone(&s);
        thread::spawn(move || {
            let mut worker = WorkerThread::new(addr, c);
            worker.run(heartbeat);
        });
    }
}
