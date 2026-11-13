mod test_utils;

use bunshin::{Command, LogEntry, State, handlers::vote_handler, protocol::VoteReq};
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
    state.append_entry(LogEntry {
        index: 1,
        term: 1,
        command: Command::default(),
    });

    let reply = vote_handler(&vote_req, &mut state);
    assert!(
        !reply.granted,
        "vote_request handler is to reject voterpcs with outdated logs"
    );
    assert_eq!(reply.id, TEST_NODE_ID);
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
    state.append_entry(LogEntry {
        index: 1,
        term: 1,
        command: Command::default(),
    });

    let reply = vote_handler(&vote_req, &mut state);
    assert!(
        reply.granted,
        "vote_request handler is to accept voterpcs with similar or up to date logs: {:?}",
        reply
    );
    assert_eq!(reply.id, TEST_NODE_ID);

    // should also accept when the last log term is higher
    vote_req.last_log_term = 4;
    // also reset term
    state.term = 0;

    let reply = vote_handler(&vote_req, &mut state);
    assert!(
        reply.granted,
        "vote_request handler is to accept voterpcs with similar or up to date logs: {:?}",
        reply
    );
    assert_eq!(reply.id, TEST_NODE_ID);
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

    let reply = vote_handler(&vote_req, &mut state);
    assert!(
        !reply.granted,
        "vote_handler should not accept lower term votes. reply: {:?}",
        reply
    );

    assert_eq!(reply.id, TEST_NODE_ID);

    // should also reject when the vote_reqis term is lower and it's own term is higher
    state.term = 4;

    let reply = vote_handler(&vote_req, &mut state);
    assert!(
        !reply.granted,
        "handler should not transition when reject a vote_req: {:?}",
        reply
    );
    assert_eq!(reply.id, TEST_NODE_ID);
}

#[test]
fn test_handler_rejcts_higher_terms_with_incomplete_logs() {
    let higher_term = 9;
    let base_term = 8;

    let vote_req = VoteReq {
        id: EXTERNAL_NODE_ID,
        last_log_index: 0,
        last_log_term: 0,
        term: higher_term,
    };

    let mut state = State::new(TEST_NODE_ID);
    state.term = base_term;
    // update it's logs using 1based index
    let e = LogEntry {
        index: state.logs.len(),
        ..Default::default()
    };
    state.append_entry(e);

    let reply = vote_handler(&vote_req, &mut state);
    assert!(
        !reply.granted,
        "vote_request handler should not grant votes with higher terms but incomplete logs: {:?}",
        reply
    );

    assert_eq!(
        state.term, base_term,
        "vote_request handler should not update it's term after a rejected vote",
    );
}
