use crate::{RaftState, ShutdownErrors, State};
use crate::{
    handlers::{append_handler, vote_handler},
    protocol::{Packet, Rpc},
};
use crossbeam::channel::{self, Receiver, select};
use std::time::Duration;

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
    println!(
        "follower:{} \t timeout:{:?}, term:{}, leader:{:?}, voted_for:{:?}",
        state.id, timeout, state.term, state.current_leader, state.voted_for
    );
    let mut timer = channel::after(timeout);
    loop {
        select! {
            recv(network_rx) -> signal => {
                let packet = match signal {
                    Ok(msg) => msg,
                    Err(err) => {
                        println!("network closed while follower: {:?} current_state: {:?}, ", err, state);
                        return RaftState::Shutdown(ShutdownErrors::NetworkDrop);
                    }

                };
                match packet.rpc {
                    Rpc::AppendEntryReq(req) => {
                        let reply = append_handler(&req, state);

                        let should_reset_timer = reply.reset_timer;

                        let  _ = packet.sender.try_send(Rpc::AppendEntryReply(reply.clone()));

                        // not sure if we should assert here, but we should most likely
                        // only assert if this leader was recognised as valid so that
                        // means state.leader == req.id
                        if should_reset_timer {
                            timer  = channel::after(timeout);
                            assert_eq!(
                                state.current_leader,
                                Some(req.id),
                                "[follower:{}] should only reset it's timer when from a valid leader
                                current_leader: {:?}, req_from: {}, reply_sent: {:?}",
                                state.id, state.current_leader, req.id, reply)
                        }

                    }

                    Rpc::VoteReq(req) => {
                        let reply = vote_handler(&req, state);
                        let vote_granted = reply.granted;
                        let _ = packet.sender.try_send(Rpc::VoteReply(reply));
                        if vote_granted {
                           timer = channel::after(timeout);
                           assert!(
                               state.current_leader.is_none(),
                               "[follower:{}] should grant vote AND set current_leader to none",
                               state.id
                            );
                        } else {
                            println!(
                                "[follower:{}] denied vote to: {:?} for term:{}. i voted_for:{:?} or have_leader:{:?}",
                                state.id, req,  state.term, state.voted_for, state.current_leader
                            );
                        }


                    },
                    _ => {
                        eprintln!("follower cannot handle {:?} rpc because it's unimplemented\n", packet.rpc);
                        return RaftState::Shutdown(ShutdownErrors::Unexpected)
                    },
                }


               }
            recv(timer) -> signal => {
                 match signal {
                    Ok(_) => {},
                    Err(err) => {
                        println!( "the timer channel for follower has being shutdown which is odd: {:?}", err);
                        return RaftState::Shutdown(ShutdownErrors::Unexpected);
                    }
                }
                println!("[follower:{}] timedout after: {:?}. moving to candidate", state.id, timeout);
                return RaftState::Candidate;
            }
        }
    }
}
