// The way a leader works is that it sends rpcs to all other nodes in the
// network before they timeout
use crate::{
    RaftState, ShutdownErrors, State,
    handlers::{append_handler, vote_handler},
    protocol::{AppendEntryReq, Packet, Rpc},
    utils::send_and_recv_json,
};
use anyhow::Result;
use bus;
use crossbeam::channel::{self, Receiver, select};
use std::{
    io::{BufReader, BufWriter},
    net::TcpStream,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

pub fn run_leader(
    network_rx: &mut Receiver<Packet>,
    peers: Vec<String>,
    state: Arc<Mutex<State>>,
    hb: Duration,
) -> (State, RaftState) {
    let node_id: u64;
    let ruling_term: u64;

    {
        let s = state.lock().unwrap();
        node_id = s.id;
        ruling_term = s.term;
    }

    let clone = Arc::clone(&state);

    // TODO: Not sure how large this needs to be
    let mut bus = bus::Bus::<()>::new(peers.len() + 2);
    let mut worker_threads = vec![];

    let vc = peers.clone();
    for (idx, addr) in vc.into_iter().enumerate() {
        let rx = bus.add_rx();
        let a = addr.clone();
        let worker_clone = Arc::clone(&clone);
        let t = thread::spawn(move || {
            let res = begin_replication(idx, a.as_str(), worker_clone, hb, rx);
            println!("\t\tworker:{:?} returned: {:?}", addr, res)
        });

        worker_threads.push(t);
    }

    loop {
        select! {
            recv(network_rx) -> signal => {
                let packet = match signal {
                    Ok(packet) => packet,
                    Err(err) => {
                        eprintln!("network_rx dropped while a leader:{}, sending shutdown error: {:?}", node_id, err);
                        let _ = bus.try_broadcast(());
                        let  final_state = clone.lock().unwrap();
                        return (final_state.clone(), RaftState::Shutdown(ShutdownErrors::NetworkDrop));
                    },
                };

                // match against the rpc we recvd
                match packet.rpc {
                    Rpc::AppendEntryReq(req) => {
                        let mut s = clone.lock().unwrap();
                        let reply = append_handler(&req, &mut s);
                        let should_reset_timer = reply.reset_timer;

                        let _ = packet.sender.try_send(Rpc::AppendEntryReply(reply.clone()));
                        if should_reset_timer {
                            // while a candidate, we set ourself as the current leader for this term
                            // so the append_handler will always check that if we have a leader
                            // (which we already set in candiadte) is the same as the id from the req
                            // if they don't match it rejects the request and reset_timer is false,
                            // although this should not happen ( two leaders for a term ) so a
                            // leader who recvs an append_entry will only drop if the
                            // req.term > ruling_term
                            let _ = bus.try_broadcast(());
                            assert!(
                                req.term > ruling_term,
                                "[leader:{}] should not step down from a stale term request or similar term request. my_term:{:?}, req:{:?}",
                                s.id, ruling_term, req
                            );

                            return (s.clone(), RaftState::Follower);
                        };

                    },

                    Rpc::VoteReq(req) => {
                        let mut s = clone.lock().unwrap();
                        let reply = vote_handler(&req, &mut s);
                        let granted_vote = reply.granted;

                        let _ = packet.sender.try_send(Rpc::VoteReply(reply.clone()));
                        if granted_vote {
                            let _ = bus.try_broadcast(());
                            assert!(
                                req.term > ruling_term,
                                "[leader:{}] should not step down from a stale term vote_request. my_term:{:?}, req:{:?}",
                                s.id, ruling_term, req
                            );
                            return (s.clone(), RaftState::Follower);
                        }

                    },
                    _ => {
                        eprintln!("unhandled rpc for leader: {:?}", packet.rpc);
                            let final_state = clone.lock().unwrap();
                            return (final_state.clone(), RaftState::Shutdown(ShutdownErrors::Unexpected));
                    },
                }
            }

        }
    }
}

fn begin_replication(
    _id: usize,
    addr: &str,
    state: Arc<Mutex<State>>,
    heartbeat: Duration,
    mut done: bus::BusReader<()>,
) -> Result<()> {
    let conn = TcpStream::connect(addr)?;
    let mut writer = BufWriter::new(conn.try_clone()?);
    let buf_reader = BufReader::new(conn);
    let mut reader = serde_json::Deserializer::from_reader(buf_reader).into_iter();

    let mut timer = channel::after(heartbeat);

    #[allow(unused)]
    let mut ruling_term: u64 = 0;
    #[allow(unused)]
    let mut node_id: u64 = 0;

    let mut append_req = {
        let mut s = state.lock().unwrap();
        // #[allow()]
        ruling_term = s.term;
        node_id = s.id;

        let prev_log = s.get_previous_log();

        #[allow(unused)]
        AppendEntryReq {
            id: node_id,
            term: ruling_term,
            previous_log_index: prev_log.index,
            previous_log_term: prev_log.term,
            commit_index: s.commit_index as u64,
            entry: None,
            info: None,
        }
    };

    loop {
        select! {
            recv(timer) -> _sig => {
                // check if we need to exit
                if let Ok(_exit) = done.try_recv() {
                    println!("[worker:{}] signal recvd from leader to exit", node_id);
                    break
                }

                append_req = {
                    let mut s = state.lock().unwrap();
                    let prev_log = s.get_previous_log();

                    AppendEntryReq {
                        id: node_id,
                        term: ruling_term,
                        previous_log_index: prev_log.index,
                        previous_log_term: prev_log.term,
                        commit_index: s.commit_index as u64,
                        entry: None,
                        info: None,
                    }

                };

                let rpc_req = Rpc::AppendEntryReq(append_req);
                let rpc_reply =  send_and_recv_json(&rpc_req, &mut writer, &mut reader);
                let append_reply = if let Ok(reply) = rpc_reply {
                    reply
                } else {
                    println!("[worker:{}] failed to make contact with {:?}. {:?}", node_id, addr, rpc_reply.err());
                    continue
                };

                match append_reply {
                    Rpc::AppendEntryReply(reply) =>{
                        if reply.term > ruling_term {
                            println!("[worker:{node_id}] exiting due to higher term from {:?}", reply);
                            break
                        } else if reply.term < ruling_term {
                            panic!("[worker:{node_id}] followers must send a term match. ruling_term: {ruling_term} , reply: {:?}", reply);
                        }

                        if !reply.success {
                            println!("[worker:{node_id}] follower did not replicate,  stepping back log {:?}", reply);
                        }

                    }
                    _ => println!("[worker:{node_id}] expected append_reply from {addr}, got: {:?}", append_reply),
                }

                timer = channel::after(heartbeat);
            }
        }
    }
    Ok(())
}
