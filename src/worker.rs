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

        let mut timer = channel::after(std::time::Duration::from_millis(1));

        // TODO: another reason why we need the bus
        // if the main thread handlers update the term, it means the node should
        // step down, but because there's no mechanism for shutdown, the main
        // leader loop will exit, and the worker threads will use that updated value
        // Hence, causing two leaders for a new term
        #[allow(unused)]
        let mut node_id: u64 = 0;
        #[allow(unused)]
        let mut ruling_term: u64 = 0;

        let mut initial_req = {
            let s = self.state.lock().unwrap();
            let last_log = s.get_last_log();
            node_id = s.id;
            ruling_term = s.term;

            AppendEntryReq {
                id: node_id,
                term: ruling_term,
                commit_index: s.commit_index as u64,
                info: None,
                entry: None,
                previous_log_index: last_log.index,
                previous_log_term: last_log.term,
            }
        };

        #[allow(unused)]
        let mut rpc_req = Rpc::AppendEntryReq(initial_req);

        loop {
            select! {
                recv(timer) -> signal => {
                    signal.unwrap_or_else(|_| panic!("[worker:{}] unexpected failure as signal for timer broke", self.addr));
                    initial_req = {
                        let current_state = self.state.lock().unwrap();
                        let  last_log = current_state.get_last_log();
                        AppendEntryReq {
                            id: node_id,
                            term: ruling_term,
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
                        // TODO(daniel): This is why we might need to use the bus crate [https://github.com/jonhoo/bus]
                        // We need to be able to tell each worker when to shutdown. With our current
                        // solution:
                        // - cannot contact peer, then return
                        // - if it was a temporary failure, the peer node will continue to increment
                        //   it's term until it's granted a vote.
                        //      - Even though they have a higher term, another legit Candidate for a
                        //        higher term will have to step down and only Followers can reject,
                        //        but that was an oversight and will need to be implemented.
                        // - outer leader loop leaves these threads behind
                        // TASK: Integrate bus crate to allow for pub-sub or multiple
                        // consumer single-sender task to let the leader loop close
                        // all worker threads
                        Err(err) => {
                            println!("[worker:{}] failed to send hb. {}", node_id, err ); return Ok(())
                        ;},
                    };

                    // 2. ignore other type of replies
                    // TODO: since we want this to stop the machine as a whole, we want
                    // to delibrately hold the lock and panic, this will collapse the
                    // entire node, hopefullyy to the server level
                    let append_entry_reply = match reply {
                        Rpc::AppendEntryReply(reply) => reply,
                        _ => {

                            let _drop_lock = self.state.lock().unwrap();

                            panic!(
                                "[worker:{}] !! Expected AppendEntryReply got rpc: {:?}. Held the lock and panicked, Node should shutdown",
                                node_id, reply
                            )
                        }
                    };

                    if append_entry_reply.term > ruling_term {
                        println!(
                            "[worker:{}] stepping down our term:{} is outdated from reply: {:?}",
                            node_id, initial_req.term, append_entry_reply
                        );
                        break
                    }

                    assert_eq!(
                        append_entry_reply.term,
                        initial_req.term,
                        "[worker:{}] expected AppendEntryReply term to match our term:{}. Followers should send a term match. Reply: {:?}",
                        node_id, initial_req.term, append_entry_reply
                    );

                    if !append_entry_reply.success  {
                        println!("[worker:{}] heartbeat unsuccess {:?}, last message sent {:?}", node_id, append_entry_reply, initial_req);
                    }

                    timer = channel::after(hb);
                }
            }
        }
        Ok(())
    }
}
