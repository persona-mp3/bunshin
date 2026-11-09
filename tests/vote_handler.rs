mod test_utils;

use bunshin::{
    Command, LogEntry, State,
    handlers::handle_vote_request,
    protocol::{VoteReply, VoteReq},
};
use test_utils::*;

#[test]
fn test_vote_req_handler_rejects_outdated_logs_when_follower() {
    let vote_req = VoteReq {
        id: EXTERNAL_NODE_ID,
        last_log_index: 0,
        last_log_term: 0,
        term: 1,
    };

    let mut state = State::new(TEST_NODE_ID);
    // make followers log updated
    state.append_log(LogEntry {
        index: 1,
        term: 1,
        command: Command::default(),
    });

    let (reply, transition) = handle_vote_request(vote_req, &mut state);
    assert_eq!(
        reply.granted, false,
        "vote_request handler is to reject voterpcs with outdated logs"
    );
    assert_eq!(reply.id, TEST_NODE_ID);

    assert_eq!(
        transition.is_none(),
        true,
        "handler should not transition when rejecting a vote_req"
    )
}

#[test]
fn test_vote_req_handler_accepts_higer_term_with_complete_logs_when_follower() {
    let mut vote_req = VoteReq {
        id: EXTERNAL_NODE_ID,
        last_log_index: 1,
        last_log_term: 1,
        term: 1,
    };

    let mut state = State::new(TEST_NODE_ID);
    // make followers log updated
    state.append_log(LogEntry {
        index: 1,
        term: 1,
        command: Command::default(),
    });

    let (reply, transition) = handle_vote_request(vote_req.clone(), &mut state);
    assert_eq!(
        reply.granted, true,
        "vote_request handler is to accept voterpcs with similar or up to date logs: {:?}",
        reply
    );
    assert_eq!(reply.id, TEST_NODE_ID);

    assert_eq!(
        transition.is_none(),
        true,
        "handler should not transition when accepting a vote_req: {:?}",
        transition
    );

    // should also accept when the last log term is higher
    vote_req.last_log_term = 4;
    // also reset term
    state.term = 0;

    let (reply, transition) = handle_vote_request(vote_req.clone(), &mut state);
    assert_eq!(
        reply.granted, true,
        "vote_request handler is to accept voterpcs with similar or up to date logs: {:?}",
        reply
    );
    assert_eq!(reply.id, TEST_NODE_ID);

    assert_eq!(
        transition.is_none(),
        true,
        "handler should not transition when accepting a vote_req: {:?}",
        transition
    );
}

#[test]
fn test_vote_handler_rejects_lower_or_similar_term_votes() {
    let vote_req = VoteReq {
        id: EXTERNAL_NODE_ID,
        last_log_index: 1,
        last_log_term: 1,
        term: 0,
    };

    let mut state = State::new(TEST_NODE_ID);
    // make followers log updated

    let (reply, transition) = handle_vote_request(vote_req.clone(), &mut state);
    assert_eq!(
        reply.granted, false,
        "handle_vote_request should not accept lower term votes. reply: {:?}",
        reply
    );

    assert_eq!(reply.id, TEST_NODE_ID);
    assert_eq!(
        transition.is_none(),
        true,
        "handler should not transition when reject a vote_req: {:?}",
        transition
    );

    // should also reject when the vote_reqis term is lower and it's own term is higher
    state.term = 4;

    let (reply, transition) = handle_vote_request(vote_req.clone(), &mut state);
    assert_eq!(
        reply.granted, false,
        "handler should not transition when reject a vote_req: {:?}",
        reply
    );
    assert_eq!(reply.id, TEST_NODE_ID);

    assert_eq!(
        transition.is_none(),
        true,
        "handler should not transition when accepting a vote_req: {:?}",
        transition
    );
}
