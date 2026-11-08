use crate::{RaftState, ShutdownErrors, State};
use crate::{
    handlers::handle_append_entry_request,
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
