use crate::protocol::{AppendEntryReply, AppendEntryReq, Packet, Rpc};
use crate::{RaftState, State};
use crossbeam::channel::{self, Receiver, select};
use std::time::Duration;

/// run_follower runs a single follower loop,listens over a network and handles incoming
/// network connections via `network_rx`. It returns with a state of [RaftState::Candidate]
/// when it's the given `timeout` has elpased or [RaftState::Shutdown] when the network
/// channel has being dropped.
///
/// It panics when it receives a state to transition to from child functions
pub fn run_follower(
    network_rx: &mut Receiver<Packet>,
    state: &mut State,
    timeout: Duration,
) -> RaftState {
    let mut timeout_ch = channel::after(timeout);
    loop {
        select! {
            recv(network_rx) -> signal => {
                let envelope = match signal {
                    Ok(msg) => msg,
                    Err(err) => {
                        println!("network channel has being dropped, shutting down: {}",  err);
                        return RaftState::Shutdown;
                    }

                };
               match envelope.rpc {
                   Rpc::AppendEntryReq(req) => {
                       let (reply, transition) = handle_append_entry_request(req, state);
                       if reply.reset_timer {
                           println!("resetting timer");
                           timeout_ch = channel::after(timeout)
                       }

                       // ignore send fialures
                       let _ = envelope.sender.try_send(Rpc::AppendEntryReply(reply));

                       debug_assert!(
                           transition.is_none(),
                           // A follower can only move to a Candidate and that is handled within this
                           // loop
                           "follower should never recv a transition state except Shutdown, got: {:?}",
                           transition
                           );

                   },

                   _ => {}

               }
            }

            recv(timeout_ch) -> signal => {
                println!("timeout has elapsed, moving to candidate");
                 match signal {
                    Ok(_) => {},
                    Err(err) => {
                        println!("timer channel has being dropped, shutting down: {}",  err);
                        return RaftState::Shutdown;
                    }
                }
                return RaftState::Candidate;
            }
        }
    }
}

pub fn handle_append_entry_request(
    req: AppendEntryReq,
    state: &mut State,
) -> (AppendEntryReply, Option<RaftState>) {
    if req.term < state.term {
        let mut reply = AppendEntryReply::default(state.id);
        reply.term = state.term;
        return (reply, None);
    }

    match state.rs {
        RaftState::Follower => {
            if req.term > state.term {
                state.assign_leader(req.id);
                state.term = req.term;

                let reply = check_log_consistency(req, state);
                return (reply, None);
            };

            // now that both terms are equal if we don't have a leader for current term
            // it could happend on the following occasions:
            // - we lost a previous election due to a timeout or
            // - no votes were granted
            //      -  node was partitioned from the cluster for a moment and had no one to contact
            if !state.has_leader() {
                println!("no leader for current term, accepting {:?}", req);
                state.assign_leader(req.id);
                let reply = check_log_consistency(req, state);
                return (reply, None);
            }

            let current_leader = state.get_leader();
            if current_leader != req.id {
                println!(
                    "duplicate leaders for term:{:?}, leader: {}, req: {:?}",
                    state.term, current_leader, req
                );

                return (AppendEntryReply::default(state.id), None);
            }

            // now terms are equal, and we have a valid leader that matches our stored one
            let reply = check_log_consistency(req, state);
            return (reply, None);
        }

        RaftState::Candidate => {
            if req.term > state.term {
                state.term = req.term;
                state.voted_for = Some(req.id);
                let reply = check_log_consistency(req, state);
                return (reply, Some(RaftState::Follower));
            }

            // just reject lower or equal terms
            return (
                AppendEntryReply {
                    id: state.id,
                    term: state.term,
                    conflict_index: 0,
                    conflict_term: None,
                    reset_timer: false,
                    success: false,
                },
                None,
            );
        }
        _ => {}
    }

    panic!("unhandled case")
}

pub fn check_log_consistency(req: AppendEntryReq, state: &mut State) -> AppendEntryReply {
    let mut reply = AppendEntryReply::default(state.id);
    reply.reset_timer = true;
    reply.term = state.term;

    let target_log = state.get_log_at_index(req.previous_log_index as usize);
    let last_log = state.get_last_log();

    // we first check if we have any log at the previous_log_index
    // if it doesn't exists, we need to reject the reject the request, and send
    // a hint to the sender/leader what our last_log.index and last_log.term lies at.
    // Hence the leader can simply skip closer to the hint we supply under.
    // If we do have an log at such index and the terms differ, we set both conflict_index
    // and conflict_term
    let log_entry = match target_log {
        None => {
            println!("no previous_log at {} from leader", req.previous_log_index);
            reply.conflict_index = last_log.index;
            reply.conflict_term = None;
            reply.success = false;
            return reply;
        }

        Some(log_entry) => log_entry,
    };

    // we have a conflict at this log position, so we need to reject it and hint to the
    // leader/sender that we have an index at the position, but the terms don't match
    // so we send it again. This time the leader could decide to skip that particular
    // term as whole
    if log_entry.term != req.previous_log_term {
        reply.conflict_index = last_log.index;
        reply.conflict_term = Some(log_entry.term);
        return reply;
    }

    // our logs match with the leaders'
    reply.success = true;

    // since we have a matching log at previous_log_index and previous_log_term, we need to check
    // if this was just a heartbeat or a new entry. If it's a new entry, we check
    // if we already have it to avoid duplicates, and then append ,otherwise we just
    // ack it
    match req.entry {
        None => println!("recvd heartbeat from leader"),
        Some(entry) => {
            let log_exists = state.get_log_at_index(entry.index);
            match log_exists {
                None => {
                    println!("no log exists at {} in mine", entry.index);
                    state.append_log(entry);
                }
                Some(conflict) => {
                    if conflict.term != entry.term {
                        state.truncate_from(entry.index);
                        state.append_log(entry);
                    } else {
                        println!("duplicate log recvd prev:{:?}, req:{:?}", conflict, entry)
                    }
                }
            }
        }
    }
    reply
}
