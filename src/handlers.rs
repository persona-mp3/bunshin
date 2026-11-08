use crate::{RaftState, ShutdownErrors, State};
use crate::{
    protocol::{AppendEntryReply, AppendEntryReq, Packet, Rpc, VoteReply, VoteReq},
    transport::MakeRpc,
};
use crossbeam::channel::{self, Receiver, select};
use std::{thread, time::Duration};

/// run_follower runs a single follower loop,listens over a network and handles incoming
/// network connections via `network_rx`. It returns with a state of [`RaftState::Candidate`]
/// when it's the given `timeout` has elpased or [`RaftState::Shutdown`] when the network
/// channel has being dropped.
///
/// NOTE: When it returns, the term is not incremented, instead this is handled by the
/// the [`run_candidate`] function
///
/// It panics when it receives a state to transition to from children functions
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
                        println!("network closed while follower: {:?} current_state: {:?}, ", err, state);
                        return RaftState::Shutdown(ShutdownErrors::NetworkDrop);
                    }

                };
               if let Rpc::AppendEntryReq(req) = envelope.rpc {
                   let (reply, transition) = handle_append_entry_request(req, state);
                   if reply.reset_timer {
                       println!("resetting timer");
                       timeout_ch = channel::after(timeout)
                   }

                   // ignore send fialures
                   let _ = envelope.sender.try_send(Rpc::AppendEntryReply(reply));

                    // A follower can only move to a Candidate and that is handled within this
                    // loop, so other children functions should not for a follower
                   if transition.is_some() {
                       let next = transition.unwrap();
                       assert_eq!(next, RaftState::Follower, "Follower -> Shutdown only allowed in run_follower");
                       assert_eq!(next, RaftState::Candidate, "Follower -> Shutdown only allowed in run_follower");
                       return next;
                   }

                   // debug_assert!(
                   //     transition.is_none(),
                   //     "follower should never recv a transition state except Shutdown, got: {:?}",
                   //     transition
                   //     );

               }
            }

            recv(timeout_ch) -> signal => {
                 match signal {
                    Ok(_) => {},
                    Err(err) => {
                        println!( "the timer channel for follower has being shutdown which is odd: {:?}", err);
                        return RaftState::Shutdown(ShutdownErrors::Unexpected);
                    }
                }
                return RaftState::Candidate;
            }
        }
    }
}

/// Section: 5.4.1
/// Raft determines which of two logs is more up-to-date
/// by comparing the index and term of the last entries in the
/// logs. If the logs have last entries with different terms, then
/// the log with the later term is more up-to-date. If the logs
/// end with the same term, then whichever log is longer is
/// more up-to-date.
/// But then what about commited entries fromm previouis leaders?
/// But we've not even implemented that yet?? So
///
///
/// run_candidate increments the term it's electing for
pub fn run_candidate(
    network_rx: &mut Receiver<Packet>,
    state: &mut State,
    election_timeout: Duration,
    peers: Vec<String>,
    dialer: &(impl MakeRpc + std::marker::Sync),
) -> RaftState {
    // clear voted_for and leader and increment term
    state.voted_for = None;
    state.current_leader = None;
    state.term += 1;

    let last_log = state.get_last_log();

    let (votes_tx, votes_rx) = channel::unbounded();
    if peers.len() == 0 {
        // TODO: we could make this an opt, but for now, lets enforce it
        println!(
            "cannot start Candidate without peers, this is a hard rule at the moment use --peers"
        );
        return RaftState::Shutdown(ShutdownErrors::Unexpected);
    }
    for peer in &peers {
        let votes_tx = votes_tx.clone();
        let vote_req = VoteReq {
            id: state.id,
            term: state.term,
            last_log_index: last_log.index,
            last_log_term: last_log.term,
        };

        thread::scope(|s| {
            s.spawn(|| {
                let resp = match dialer.dial(peer, Rpc::VoteReq(vote_req)) {
                    Ok(rpc) => rpc,
                    Err(err) => {
                        println!("rpc_dial_err: {:?}", err);
                        return;
                    }
                };
                let _ = votes_tx.send(resp);
            });
        });
    }
    let timeout = channel::after(election_timeout);
    drop(votes_tx);

    let mut recvd_votes = 0;
    let majority = (peers.len() / 2) + 1;

    loop {
        select! {
            recv(network_rx) -> packet => {
                unimplemented!("unhandled network_recv in run_candidate: {:?}", packet);

            }
            recv(timeout) -> _signal => {
                break
            }
            recv(votes_rx) -> signal => {
                let rpc_reply = match signal {
                    Ok(result) =>  result,
                    Err(_err) => {
                        continue;
                    },
                };

                match rpc_reply {
                    Rpc::VoteReply(reply) => {
                        if reply.granted {
                            recvd_votes += 1;
                            println!("granted vote from {:?}, checking majority", reply);
                            // now check if we've acheived majority
                            if recvd_votes >= majority {
                                println!("won election, recvd: {:?} votes against {:?}", recvd_votes, majority);
                                return RaftState::Leader;
                            }
                        } else {
                            // we don't necessarily need to check the term here because if that node
                            // is higher, it could be the only possible node whose term in the whole
                            // cluster is higher, it hasn't reached other nodes, and this node still
                            // has a chance
                            println!("denied vote from {:?}", reply);
                        }
                    },
                    _ => {
                        println!("Candidate:{:?} \trecvd unexpected reply from vote_reply {:?} ", state.id, rpc_reply);
                    }
                }


            }
        }
    }

    println!(
        "Candidate:{:?} \ttimeout election fired first, checking results",
        state.id
    );

    if recvd_votes >= majority {
        println!(
            "Candidate:{:?} \twon election, recvd: {:?} votes against {:?} for term: {:?}",
            state.id, recvd_votes, majority, state.term,
        );
        // and then we actually want to update our stats
        state.voted_for = Some(state.id);
        state.current_leader = Some(state.id);
        RaftState::Leader
    } else {
        println!(
            "Candidate:{:?} \tlost election recvd {:?} votes, against {:?} majority for term: {:?}",
            state.id, recvd_votes, majority, state.term,
        );
        RaftState::Follower
    }
}

pub fn handle_vote_request(req: VoteReq, state: &mut State) -> (VoteReply, Option<RaftState>) {
    let mut reply = VoteReply {
        id: state.id,
        granted: false,
        term: state.term,
    };
    match state.rs {
        RaftState::Follower => unimplemented!("follower not handling vote_requests"),
        RaftState::Candidate => {
            // SAFETY : So there's no explicit mention or I haven't seen it yet, that
            // a Candidate doesn't need to check another candidates log. It simply
            // rejects it's VoteRpc since it's already granted vote to itself.
            // If the VoteRpc came from a higher term, it should update it's term
            // and step down
            // The election restriction was that, before voting a candidate, you need
            // to check if their logs are up to date, doesn't that apply here? Since
            // the node is a Candidate, it will only accept higher terms, so in effect
            // it's voting for a higher term, but what if IT's own logs are more than
            // that of the sender? We should come back to this.
            //

            // if term is higher, then drop down to follower
            if req.term > state.term {
                state.term = req.term;
                state.voted_for = Some(req.id);
                state.current_leader = Some(req.id);

                println!(
                    "Candidate:{} \trecvd vote request from a higher_term: {:?}, {:?}",
                    state.id, req.term, req
                );

                reply.granted = true;
                reply.term = state.term;
                return (reply, Some(RaftState::Follower));
            };

            // ignore similar or lower terms
            return (reply, None);
        }
        _ => {
            println!(
                "unhanlded case in handle_vote_request: state:{:?}, vote_req:{:?}",
                state, req
            );
            return (reply, Some(RaftState::Shutdown(ShutdownErrors::Unexpected)));
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
        _ => {
            println!(
                "unahndled case in handle_append_entry_request: state: {:?}, request: {:?}",
                state, req
            );
            return (
                AppendEntryReply::default(state.id),
                Some(RaftState::Shutdown(ShutdownErrors::Unexpected)),
            );
        }
    }
}

pub fn check_log_consistency(req: AppendEntryReq, state: &mut State) -> AppendEntryReply {
    let mut reply = AppendEntryReply::default(state.id);
    reply.reset_timer = true;
    reply.term = state.term;

    let target_log = state.get_log_at_index(req.previous_log_index);
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
