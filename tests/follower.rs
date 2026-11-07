use bunshin::{LogEntry, State, handlers::run_follower, protocol::*};
use crossbeam::channel;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
mod test_utils;

/// Follower in a default state should acknowledge request from a higer term. It
/// should also have the sentinel value of [LogEntry::default()] upon startup. Since
/// the term sent is from a higher term, the follower should also acknowledge the
/// sender as a valid leader for current term
#[test]
fn test_follower_behaviour() {
    let (network_tx, network_rx) = channel::unbounded::<Envelope>();
    let state = State::new(test_utils::TEST_NODE_ID);

    let locked = Arc::new(Mutex::new(state));
    let clone = Arc::clone(&locked);
    std::thread::spawn(move || {
        let mut s = clone.lock().expect("failed to acquire lock for state");
        let timeout = Duration::from_secs(test_utils::TIMEOUT_DURATION_SECS);

        run_follower(network_rx, &mut *s, timeout);
    });

    // acks higher term
    let higher_term = 3;
    let mut req = AppendEntryReq::default(test_utils::EXTERNAL_NODE_ID);
    req.term = higher_term;

    let (packet_tx, packet_rx) = channel::unbounded();

    let packet = Envelope {
        rpc: Rpc::AppendEntryReq(req),
        sender: packet_tx.clone(),
    };

    network_tx
        .try_send(packet)
        .expect("failed to send packet from follower state machine");

    let reply = packet_rx
        .recv()
        .expect("failed to recv packet from follower state machine");

    let mut expected_reply = AppendEntryReply::default(test_utils::TEST_NODE_ID);
    expected_reply.term = higher_term;
    expected_reply.success = true;
    expected_reply.reset_timer = true;

    assert_eq!(reply, Rpc::AppendEntryReply(expected_reply));

    // force follower loop to shutdown
    drop(network_tx);

    let mut final_state = locked.lock().expect("could not get lock for state");
    assert_eq!(
        final_state.has_leader(),
        true,
        "node should have assigned {} as new leader",
        test_utils::TEST_NODE_ID
    );

    assert_eq!(
        final_state.current_leader.unwrap(),
        test_utils::EXTERNAL_NODE_ID,
        "node should have assigned {} as new leader",
        test_utils::EXTERNAL_NODE_ID
    );

    assert_eq!(final_state.logs, [LogEntry::default()]);
}

#[test]
fn ff() {
    let (network_tx, network_rx) = channel::unbounded::<Envelope>();
    let mut state = State::new(test_utils::TEST_NODE_ID);
    // assign state with higher term
    let higher_term = 3;
    state.term = higher_term;

    let locked = Arc::new(Mutex::new(state));
    let clone = Arc::clone(&locked);
    std::thread::spawn(move || {
        let mut s = clone.lock().expect("failed to acquire lock for state");
        let timeout = Duration::from_secs(test_utils::TIMEOUT_DURATION_SECS);

        run_follower(network_rx, &mut *s, timeout);
    });

    let outdated_req = AppendEntryReq::default(test_utils::TEST_NODE_ID); // starts at 0

    let (packet_tx, packet_rx) = channel::unbounded();

    let packet = Envelope {
        rpc: Rpc::AppendEntryReq(outdated_req),
        sender: packet_tx.clone(),
    };

    network_tx
        .try_send(packet)
        .expect("failed to send packet from follower state machine");

    let reply = packet_rx
        .recv()
        .expect("failed to recv packet from follower state machine");

    // force follower loop to shutdown
    drop(network_tx);

    // assert it ignores requests from a lower term
    let mut expected_reply = AppendEntryReply::default(test_utils::TEST_NODE_ID);
    expected_reply.term = higher_term;
    expected_reply.success = false;
    expected_reply.reset_timer = false;

    assert_eq!(reply, Rpc::AppendEntryReply(expected_reply));

    // check initial state remains
    let mut final_state = locked.lock().expect("could not get lock for state");
    assert_eq!(
        final_state.has_leader(),
        false,
        "node should not have assigned {} as new leader",
        test_utils::TEST_NODE_ID
    );

    assert_eq!(
        final_state.current_leader.is_none(),
        true,
        "node should have assigned {} as new leader",
        test_utils::EXTERNAL_NODE_ID
    );

    assert_eq!(final_state.logs, [LogEntry::default()]);
}
