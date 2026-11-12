use crate::State;
use crate::protocol::{AppendEntryReply, AppendEntryReq, VoteReply, VoteReq};

/// append_handler rejects all terms lower than it's self. If the append entry has a greater term
/// it updates it's `current_leader` to the id and thereby clearing any previous leader.
/// If the terms are equal, it checks against it's current leader and rejects the request if they
/// don't match.
///
/// When the sender of the Rpc is recognised by the node, it checks using [`check_log_consistency`]
pub fn append_handler(req: &AppendEntryReq, state: &mut State) -> AppendEntryReply {
    let reject_reply = AppendEntryReply {
        id: state.id,
        term: state.term,
        success: false,
        conflict_index: 0,
        conflict_term: None,
        reset_timer: false,
    };

    if req.term < state.term {
        return reject_reply;
    }

    let mut accept_reply = AppendEntryReply {
        id: state.id,
        term: state.term,
        success: false,
        conflict_index: 0,
        conflict_term: None,
        reset_timer: true,
    };

    let current_leader = state.current_leader;
    if req.term > state.term {
        // update the leader for current term
        state.current_leader = Some(req.id);
        state.voted_for = Some(req.id);
        state.term = req.term;

        let stats = check_log_consistency(req, state);
        accept_reply.conflict_index = stats.conflict_index;
        accept_reply.conflict_term = stats.conflict_term;
        accept_reply.success = stats.success;
        accept_reply.term = req.term;
        accept_reply
    } else {
        // terms are equal
        // - check if it's a valid/new leader
        //  - new leader if current_leader is None
        //  - valid leader if current_leader.Some == req.id
        if let Some(leader) = current_leader {
            // invalid leader
            if leader != req.id {
                return reject_reply;
            }
            // valid leader

            let stats = check_log_consistency(req, state);
            accept_reply.conflict_index = stats.conflict_index;
            accept_reply.conflict_term = stats.conflict_term;
            accept_reply.success = stats.success;
            accept_reply.term = req.term;
            accept_reply
        } else {
            // new leader for new term

            state.current_leader = Some(req.id);
            state.voted_for = Some(req.id);
            state.term = req.term;

            let stats = check_log_consistency(req, state);
            accept_reply.conflict_index = stats.conflict_index;
            accept_reply.conflict_term = stats.conflict_term;
            accept_reply.success = stats.success;
            accept_reply.term = req.term;
            accept_reply
        }
    }
}

/// VoteRequest rpcs that have a term similar or lower than this node's current term are rejected.
/// The requirements that a [RaftState::Candidate] is granted a vote when the - term from the
/// Candidate is higher and the candidate's log is up to date. A log is considered up to date according
/// to the Raft paper under section 5.4.1. Check [`State::candidate_log_is_up_to_date`]
///
/// According to the Raft paper:
///    The RequestVote RPC implements this restriction: the RPC includes information about the
///    candidate's log, and the voter denies its vote if its own log is more up-to-date than that
///    of the candidate.
///
/// If the request meets the above requirements as described, the node adopts this higher term and
/// clears it's `current_leader`. This handler does not set the `current_leader` to the sender id
/// as the Candidate who sent the request might not win the election. The [`append_handler`] is
/// responsible for detecting that.
pub fn vote_handler(req: &VoteReq, state: &mut State) -> VoteReply {
    let reject_reply = VoteReply {
        id: state.id,
        term: state.term,
        granted: false,
    };

    if req.term <= state.term {
        return reject_reply;
    }

    // a vote request is valid if the term is higher, and the
    // candidate's logs are up to date
    let accept_reply = VoteReply {
        id: state.id,
        term: req.term,
        granted: true,
    };

    let logs_are_up_to_date =
        state.candidate_log_is_up_to_date(req.last_log_index, req.last_log_term);
    if !logs_are_up_to_date {
        reject_reply
    } else {
        println!(
            "node:{} \tgranting vote to: {:?}, my_term:{}",
            state.id, req, state.term
        );

        state.term = req.term;
        state.voted_for = Some(req.id);
        state.current_leader = None;

        accept_reply
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
pub fn check_log_consistency(req: &AppendEntryReq, state: &mut State) -> AppendEntryReply {
    let mut reply = AppendEntryReply::default(state.id);
    reply.reset_timer = true;
    reply.term = req.term;

    let target_log = state.get_log_at_index(req.previous_log_index);

    // we first check if we have any log at the previous_log_index
    // if it doesn't exist, we need to reject the reject the request, and send
    // a hint to the sender/leader what our last_log.index and last_log.term lies at.
    // Hence the leader can simply skip closer to the hint we supply under.
    // If we do have an log at such index and the terms differ, we set both conflict_index
    // and conflict_term
    let log_entry = match target_log {
        None => {
            let last_log = state.get_last_log();
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
        println!("term_conflicts? _ => {:?}, mine: {:?}", req, log_entry);
        reply.conflict_index = log_entry.index;
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
        None => println!("[follower:{}] heartbeat from leader: {:?}", state.id, req),
        Some(ref entry) => {
            let log_exists = state.get_log_at_index(entry.index);
            match log_exists {
                None => {
                    println!(
                        "[follower:{}] no log exists at {} in mine",
                        state.id, entry.index
                    );
                    state.append_entry(entry.clone());
                }
                Some(conflict) => {
                    if conflict.term != entry.term {
                        state.truncate_from(entry.index);
                        state.append_entry(entry.clone());
                    } else {
                        println!(
                            "[follower:{}] duplicate log recvd prev:{:?}, req:{:?}",
                            state.id, conflict, entry
                        )
                    }
                }
            }
        }
    }
    reply
}

// pub fn handle_append_entry_request(
//     req: &AppendEntryReq,
//     state: &mut State,
// ) -> (AppendEntryReply, Option<RaftState>) {
//     if req.term < state.term {
//         let mut reply = AppendEntryReply::default(state.id);
//         reply.term = state.term;
//         return (reply, None);
//     }
//
//     match state.rs {
//         RaftState::Follower => {
//             if req.term > state.term {
//                 state.assign_leader(req.id);
//                 state.term = req.term;
//
//                 let reply = check_log_consistency(req, state);
//                 return (reply, None);
//             };
//
//             // now that both terms are equal if we don't have a leader for current term
//             // it could happend on the following occasions:
//             // - we lost a previous election due to a timeout or
//             // - no votes were granted
//             //  -  node was partitioned from the cluster and couldn't reach anyone
//             if !state.has_leader() {
//                 println!(
//                     "[follower:{}] no leader for current term, accepting {:?}",
//                     state.id, req
//                 );
//                 state.assign_leader(req.id);
//                 let reply = check_log_consistency(req, state);
//                 return (reply, None);
//             }
//
//             let current_leader = state.get_leader();
//             if current_leader != req.id {
//                 if Some(current_leader) == state.voted_for {
//                     println!(
//                         "ahh, voted_for {} but not yet assigned as leader. voted_for: {:?}",
//                         current_leader, state.voted_for
//                     );
//                 }
//                 println!(
//                     "[follower:{}] \tduplicate leaders for term:{:?}, leader: {}, req: {:?}",
//                     state.id, state.term, current_leader, req
//                 );
//
//                 let reply = AppendEntryReply {
//                     id: state.id,
//                     term: state.term,
//                     conflict_index: 0,
//                     conflict_term: None,
//                     reset_timer: false,
//                     success: false,
//                 };
//                 return (reply, None);
//             }
//
//             // now terms are equal, and we have a valid leader that matches our stored one
//             let reply = check_log_consistency(req, state);
//             (reply, None)
//         }
//
//         RaftState::Candidate => {
//             if req.term >= state.term {
//                 state.term = req.term;
//                 state.voted_for = Some(req.id);
//                 state.current_leader = Some(req.id);
//                 let mut reply = check_log_consistency(req, state);
//                 reply.reset_timer = true;
//                 return (reply, Some(RaftState::Follower));
//             }
//
//             // just reject lower or equal terms
//             (
//                 AppendEntryReply {
//                     id: state.id,
//                     term: state.term,
//                     conflict_index: 0,
//                     conflict_term: None,
//                     reset_timer: false,
//                     success: false,
//                 },
//                 None,
//             )
//         }
//
//         RaftState::Leader => {
//             if req.term > state.term {
//                 state.assign_leader(req.id);
//                 state.term = req.term;
//
//                 let reply = check_log_consistency(req, state);
//                 return (reply, None);
//             };
//
//             (
//                 AppendEntryReply {
//                     id: state.id,
//                     term: state.term,
//                     conflict_index: 0,
//                     conflict_term: None,
//                     reset_timer: false,
//                     success: false,
//                 },
//                 None,
//             )
//         }
//         _ => {
//             println!(
//                 "unhandled case in handle_append_entry_request: state: {:?}, request: {:?}",
//                 state, req
//             );
//             (
//                 AppendEntryReply::default(state.id),
//                 Some(RaftState::Shutdown(ShutdownErrors::Unexpected)),
//             )
//         }
//     }
// }

// pub fn handle_vote_request(req: &VoteReq, state: &mut State) -> (VoteReply, Option<RaftState>) {
//     let mut reply = VoteReply {
//         id: state.id,
//         granted: false,
//         term: state.term,
//     };
//     match state.rs {
//         RaftState::Follower => {
//             if req.term < state.term {
//                 println!(
//                     "Follower:{} \tdenying recvd vote_request for current/older term: {:?}, vote_req {:?}",
//                     state.id, state.term, req
//                 );
//                 (reply, None)
//             } else if state.term == req.term {
//                 // this is possible as a node might have been partitioned from the cluster
//                 // and hasn't seen new term changes. for example, it can send requests
//                 // but cannot receive responses
//                 println!(
//                     "Follower:{} \trecvd a vote request for current term: {:?}, current_state: {:?}, vote_req: {:?}",
//                     state.id, state.term, state, req
//                 );
//                 (reply, None)
//             } else {
//                 state.term = req.term;
//                 if state.candidate_log_is_up_to_date(req.last_log_index, req.last_log_term) {
//                     reply.granted = true;
//                     reply.term = state.term;
//                     println!(
//                         "Follower:{:?} \trecvd vote_req from higher term: {:?}",
//                         state.id, req
//                     );
//                     return (reply, None);
//                 }
//                 (reply, None)
//             }
//         }
//         RaftState::Candidate => {
//             // SAFETY : So there's no explicit mention or I haven't seen it yet, that
//             // a Candidate doesn't need to check another candidates log. It simply
//             // rejects it's VoteRpc since it's already granted vote to itself.
//             // If the VoteRpc came from a higher term, it should update it's term
//             // and step down
//             // The election restriction was that, before voting a candidate, you need
//             // to check if their logs are up to date, doesn't that apply here? Since
//             // the node is a Candidate, it will only accept higher terms, so in effect
//             // it's voting for a higher term, but what if IT's own logs are more than
//             // that of the sender? We should come back to this.
//
//             // if term is higher, then drop down to follower
//             if req.term > state.term {
//                 state.term = req.term;
//                 state.voted_for = Some(req.id);
//                 state.current_leader = Some(req.id);
//
//                 println!(
//                     "Candidate:{} \trecvd vote request from a higher_term: {:?}, {:?}",
//                     state.id, req.term, req
//                 );
//
//                 reply.granted = true;
//                 reply.term = state.term;
//                 return (reply, Some(RaftState::Follower));
//             };
//
//             // ignore similar or lower terms
//             (reply, None)
//         }
//
//         RaftState::Leader => {
//             if req.term > state.term {
//                 state.term = req.term;
//                 state.voted_for = Some(req.id);
//                 state.current_leader = Some(req.id);
//                 // then check
//
//                 println!(
//                     "Leader:{} \trecvd vote request from a higher_term: {:?}, {:?}",
//                     state.id, req.term, req
//                 );
//
//                 reply.granted = true;
//                 reply.term = state.term;
//                 return (reply, Some(RaftState::Follower));
//             };
//
//             (reply, None)
//         }
//         _ => {
//             println!(
//                 "Shutdown:{:?} \tstate case in handle_vote_request: state:{:?}, vote_req:{:?}",
//                 state.id, state, req
//             );
//             (reply, Some(RaftState::Shutdown(ShutdownErrors::Unexpected)))
//         }
//     }
// }
//
//

// pub fn handle_vote_request(req: &VoteReq, state: &mut State) -> (VoteReply, Option<RaftState>) {
//     let reject_reply = VoteReply {
//         id: state.id,
//         term: state.term,
//         granted: false,
//     };
//
//     let mut accept_reply = VoteReply {
//         id: state.id,
//         term: state.term,
//         granted: true,
//     };
//
//     if req.term <= state.term {
//         (reject_reply, None)
//     } else {
//         // check if candidates log is up to date
//         let logs_are_up_to_date =
//             state.candidate_log_is_up_to_date(req.last_log_index, req.last_log_term);
//
//         if !logs_are_up_to_date {
//             println!(
//                 "node:{} \tdenying vote as candidate_logs are not up to date: {:?}",
//                 state.id, req
//             );
//             (reject_reply, None)
//         } else {
//             println!(
//                 "node:{} \tgranting vote to: {:?}, my_term:{}",
//                 state.id, req, state.term
//             );
//             state.term = req.term;
//             accept_reply.term = req.term;
//             state.voted_for = Some(req.id);
//
//             // NOTE that we don't update the state.current_leader. this is because
//             // the node we gave our vote might not have won the election, instead
//             // we  leave that to the follower state to update.
//             // clear the old leader as it belongs to an older term
//             // the follower state will then accept whoever won the election
//             // for this new term
//             state.current_leader = None;
//             match state.rs {
//                 RaftState::Candidate | RaftState::Leader => {
//                     (accept_reply, Some(RaftState::Follower))
//                 }
//                 RaftState::Follower => (accept_reply, None),
//                 _ => (
//                     accept_reply,
//                     Some(RaftState::Shutdown(ShutdownErrors::Unexpected)),
//                 ),
//             }
//         }
//     }
// }
