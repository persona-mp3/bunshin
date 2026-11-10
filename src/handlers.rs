use crate::protocol::{AppendEntryReply, AppendEntryReq, VoteReply, VoteReq};
use crate::{RaftState, ShutdownErrors, State};

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
            //  -  node was partitioned from the cluster and couldn't reach anyone
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
            (reply, None)
        }

        RaftState::Candidate => {
            if req.term >= state.term {
                state.term = req.term;
                state.voted_for = Some(req.id);
                state.current_leader = Some(req.id);
                let mut reply = check_log_consistency(req, state);
                reply.reset_timer = true;
                return (reply, Some(RaftState::Follower));
            }

            // just reject lower or equal terms
            (
                AppendEntryReply {
                    id: state.id,
                    term: state.term,
                    conflict_index: 0,
                    conflict_term: None,
                    reset_timer: false,
                    success: false,
                },
                None,
            )
        }
        _ => {
            println!(
                "unahndled case in handle_append_entry_request: state: {:?}, request: {:?}",
                state, req
            );
            (
                AppendEntryReply::default(state.id),
                Some(RaftState::Shutdown(ShutdownErrors::Unexpected)),
            )
        }
    }
}

/// check_log_consistency takes the `previous_log_index` and `previous_log_term`
/// from a request and checks if it has any any log at that index.
/// - If it doesn't have any log at that index, it rejects the request.
///   But it also provides a hint to the leader via the [`AppendEntryReply.conflict_index`]
///   which, the index of last log that it has.
///
/// - If it does have an entry at that index and the terms do not match, the
///   [`AppendEntryReply.conflict_term`] is updated with that log's term.
///
/// If the above checks have are succesfully, it then checks if the request came
/// with a new entry [`AppendEntryReq.entry`]. If it doesn't it's considered a heartbeat
///
/// If it does contain a new entry, it checks before inserting into it's own
/// logs that nothing there exists. If there exists one and it's different from
/// the request's, the log is truncated before appending the new one. If the logs
/// are different, it simply ignores it
///
/// It also sets the [`AppendEntryReply.reset_timer`] field to true and [`State.term`] to
/// match the current term of the request
pub fn check_log_consistency(req: AppendEntryReq, state: &mut State) -> AppendEntryReply {
    let mut reply = AppendEntryReply::default(state.id);
    reply.reset_timer = true;
    reply.term = state.term;

    let target_log = state.get_log_at_index(req.previous_log_index);
    let last_log = state.get_last_log();

    // we first check if we have any log at the previous_log_index
    // if it doesn't exist, we need to reject the reject the request, and send
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

pub fn handle_vote_request(req: VoteReq, state: &mut State) -> (VoteReply, Option<RaftState>) {
    let mut reply = VoteReply {
        id: state.id,
        granted: false,
        term: state.term,
    };
    match state.rs {
        RaftState::Follower => {
            if req.term < state.term {
                println!(
                    "Follower:{} \tdenying recvd vote_request for current/older term: {:?}, vote_req {:?}",
                    state.id, state.term, req
                );
                (reply, None)
            } else if state.term == req.term {
                // this is possible as a node might have been partitioned from the cluster
                // and hasn't seen new term changes. for example, it can send requests
                // but cannot receive responses
                println!(
                    "Follower:{} \trecvd a vote request for current term: {:?}, current_state: {:?}, vote_req: {:?}",
                    state.id, state.term, state, req
                );
                (reply, None)
            } else {
                state.term = req.term;
                if state.candidate_log_is_up_to_date(req.last_log_index, req.last_log_term) {
                    reply.granted = true;
                    reply.term = state.term;
                    println!(
                        "Follower:{:?} \trecvd vote_req from higher term: {:?}",
                        state.id, req
                    );
                    return (reply, None);
                }
                (reply, None)
            }
        }
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
            (reply, None)
        }
        _ => {
            println!(
                "Candidate:{:?} \tunhanlded case in handle_vote_request: state:{:?}, vote_req:{:?}",
                state.id, state, req
            );
            (reply, Some(RaftState::Shutdown(ShutdownErrors::Unexpected)))
        }
    }
}
