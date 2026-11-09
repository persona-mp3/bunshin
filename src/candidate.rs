use crate::{RaftState, ShutdownErrors, State};
use crate::{
    handlers::{handle_append_entry_request, handle_vote_request},
    protocol::{Packet, Rpc, VoteReq},
    transport::Transport,
};
use crossbeam::channel::{self, Receiver, select};
use std::{thread, time::Duration};

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
    dialer: Transport,
) -> RaftState {
    // clear voted_for and leader and increment term
    state.voted_for = None;
    state.current_leader = None;
    state.term += 1;
    // println!("[candidtate:{}] peers to connect: {:?}", state.id, peers);

    let last_log = state.get_last_log();

    let (votes_tx, votes_rx) = channel::unbounded();
    if peers.len() == 0 {
        // TODO: we could make this an opt, but for now, lets enforce it
        println!(
            "cannot start Candidate without peers, this is a hard rule at the moment use --peers"
        );
        return RaftState::Shutdown(ShutdownErrors::Unexpected);
    }
    // thread::scope(|s| {
    // });

    for peer in peers.clone() {
        let votes_tx = votes_tx.clone();
        let vote_req = VoteReq {
            id: state.id,
            term: state.term,
            last_log_index: last_log.index,
            last_log_term: last_log.term,
        };

        thread::spawn(move || {
            let resp = match dialer(&peer, Rpc::VoteReq(vote_req)) {
                Ok(rpc) => rpc,
                Err(err) => {
                    println!("rpc_dial_err: {:?}", err);
                    return;
                }
            };
            let _ = votes_tx.send(resp);
        });
    }
    let timeout = channel::after(election_timeout);
    drop(votes_tx);

    let mut recvd_votes = 0;
    let majority = (peers.len() / 2) + 1;

    loop {
        select! {
            recv(network_rx) -> signal => {
                let packet = match signal {
                    Ok(msg) => msg,
                    Err(err) => {
                        println!("network closed while candidate: {:?} current_state: {:?}, ", err, state);
                        return RaftState::Shutdown(ShutdownErrors::NetworkDrop);
                    }

                };

                println!("candidate:{} new packet: {:?}", state.id, packet.rpc);
                match packet.rpc {
                    Rpc::AppendEntryReq(req) => {
                       let (reply, transition) = handle_append_entry_request(req, state);

                       let r = packet.sender.try_send(Rpc::AppendEntryReply(reply));
                       match r {
                           Ok(_) => {},
                           Err(err) => println!("candidate{} | failed to send to network_layer:: {err}", state.id),
                       };

                       if transition.is_some() {
                           let next = transition.unwrap();
                           assert_eq!(next, RaftState::Follower, "Candidate must only recevive Follower state from handler");
                           return RaftState::Follower;
                       }

                    }

                    Rpc::VoteReq(req) => {
                        let (reply, transition) = handle_vote_request(req, state);
                        let _ = packet.sender.try_send(Rpc::VoteReply(reply));

                        if transition.is_some() {
                            let next = transition.unwrap();
                            assert_eq!(next, RaftState::Follower, "Candidate must only recevive Follower state from handler");
                            return RaftState::Follower;
                        }
                    }
                    _ => {
                        println!("unimplemented request for run_candidate: {:?}", packet.rpc);
                        return RaftState::Shutdown(ShutdownErrors::Unexpected)
                    }
                }

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
