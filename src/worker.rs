use crate::{
    State,
    protocol::{AppendEntryReq, Rpc},
    utils::send_and_recv_json,
};
use anyhow::{Context, Result};
use crossbeam::channel::{self, select};
use serde_json;
use std::{
    io::{BufReader, BufWriter},
    net::TcpStream,
    sync::{Arc, Mutex},
    time::Duration,
};

pub struct WorkerThread {
    pub addr: String,
    pub state: Arc<Mutex<State>>,
}

impl WorkerThread {
    pub fn new(addr: String, state: Arc<Mutex<State>>) -> Self {
        Self { addr, state }
    }

    pub fn run(&mut self, hb: Duration) -> Result<()> {
        let conn = TcpStream::connect(&self.addr)
            .with_context(|| format!("worker failed to contact peer: {:?}", self.addr))?;
        let mut writer = BufWriter::new(
            conn.try_clone()
                .with_context(|| format!("worker:{} failed to clone connection", self.addr))?,
        );

        let reader = BufReader::new(conn);
        let mut json_reader = serde_json::Deserializer::from_reader(reader).into_iter();

        let mut timer = channel::after(hb);
        // let c = Arc::clone(&self.state);
        let mut initial_req = {
            let s = self.state.lock().unwrap();
            let last_log = s.get_last_log();
            AppendEntryReq {
                id: s.id,
                term: s.term,
                commit_index: s.commit_index as u64,
                info: None,
                entry: None,
                previous_log_index: last_log.index,
                previous_log_term: last_log.term,
            }
        };

        #[allow(unused)]
        let mut rpc_req = Rpc::AppendEntryReq(initial_req);
        println!("worker loop started...");

        loop {
            select! {
                recv(timer) -> signal => {
                    signal.expect(format!("worker:{} unexpected failure as signal for timer broke", self.addr).as_str());
                    initial_req = {
                        let current_state = self.state.lock().unwrap();
                        let  last_log = current_state.get_last_log();
                        AppendEntryReq {
                            id: current_state.id,
                            term: current_state.term,
                            commit_index: current_state.commit_index as u64,
                            info: None,
                            entry: None,
                            previous_log_index: last_log.index,
                            previous_log_term: last_log.term,
                        }
                    };


                    rpc_req = Rpc::AppendEntryReq(initial_req.clone());
                    let reply = send_and_recv_json(&rpc_req, &mut writer, &mut json_reader);
                    // 1. if failed log the error
                    let reply = match reply {
                        Ok(r) =>  r,
                        Err(err) => {
                            println!("worker:{} failed to send hb. {}",self.addr, err ); return Ok(())
                        ;},
                    };

                    // 2. ignore other type of replies
                    // TODO: since we want this to stop the machine as a whole, we want
                    // to delibrately hold the lock and panic, this will collapse the
                    // entire node, hopefullyy to the server level
                    let append_entry_reply = match reply {
                        Rpc::AppendEntryReply(reply) => reply,
                        _ => {

                            self.state.lock().unwrap();
                            panic!("worker:{}!! Expected AppendEntryReply got rpc: {:?}. Held the lock and pancked, Node should shutdown", self.addr, reply)
                        }
                    };

                    if append_entry_reply.term > initial_req.term {
                        println!("worker:{} stepping down our term:{} is outdated from reply: {:?}", self.addr, initial_req.term, append_entry_reply);
                        break
                    }

                    assert_eq!(
                        append_entry_reply.term,
                        initial_req.term,
                        "worker:{} expected AppendEntryReply term to match our term:{}. Followers should send a term match. Reply: {:?}",
                        initial_req.id, initial_req.term, append_entry_reply
                    );

                    if !append_entry_reply.success  {
                        println!("heartbeat unsuccess, stepping back log: {:?}", append_entry_reply);
                    }

                    timer = channel::after(hb);
                }
            }
        }
        Ok(())
    }
}
