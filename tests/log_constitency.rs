use bunshin::{Command, LogEntry, State, handlers::check_log_consistency, protocol::*};
mod test_utils;
use test_utils::*;

#[test]
fn test_appends_new_logs_when_previous_logs_match() {
    let mut state = State::new(TEST_NODE_ID);
    state.term = 1;

    let new_log = LogEntry {
        index: 1,
        term: 1,
        command: Command {
            key: "first_key".to_string(),
            value: "first_value".to_string(),
            operation: "set".to_string(),
        },
    };

    let prev_log = LogEntry::default();
    let req = AppendEntryReq {
        id: EXTERNAL_NODE_ID,
        commit_index: 0,
        entry: Some(new_log),
        info: None,
        previous_log_index: prev_log.index,
        previous_log_term: prev_log.term,
        term: state.term,
    };

    let reply = check_log_consistency(&req, &mut state);
    let expected_reply = AppendEntryReply {
        id: state.id,
        term: state.term,
        conflict_index: 0,
        conflict_term: None,
        success: true,
        reset_timer: true,
    };

    assert_eq!(reply, expected_reply);
}

#[test]
fn test_correct_hints_are_sent_with_updated_logs() {
    let mut state = State::new(TEST_NODE_ID);
    state.term = 1;
    // TODO: if the index is larger than ours, we basically need to truncate the
    // log of the follower and detecting corrupted logs ie chnage the below index
    // to 3
    state.append_entry(LogEntry {
        index: 1,
        term: 1,
        command: Command::default(),
    });

    let mut updated_logs = vec![];
    for i in 1..4 {
        updated_logs.push(LogEntry {
            index: i,
            term: 1,
            command: Command::default(),
        });
    }

    // since we have 3 logs ahead of the follower, we send the
    // second-to-last one first
    let mut req = AppendEntryReq::default(EXTERNAL_NODE_ID);
    req.previous_log_index = updated_logs[updated_logs.len() - 1].index;
    req.previous_log_term = updated_logs[updated_logs.len() - 1].term;

    let reply = check_log_consistency(&req, &mut state);
    let mut hint_index = reply.conflict_index;
    let hint_term = reply.conflict_term;

    // check that the follower hinted to it's most recent log
    assert_eq!(hint_index, state.logs[state.logs.len() - 1].index);
    assert!(hint_term.is_none());
    assert!(!reply.success);

    let log_size = updated_logs.len();
    let mut prev_log: LogEntry;
    loop {
        if hint_index == 0 || hint_index == log_size {
            break;
        }
        // get the hint index as the next log to send
        let next_log = updated_logs[hint_index].clone();
        if hint_index == 0 {
            prev_log = LogEntry::default();
        } else {
            prev_log = updated_logs[hint_index - 1].clone();
        }

        let mut req = AppendEntryReq::default(EXTERNAL_NODE_ID);
        req.entry = Some(next_log);
        req.previous_log_index = prev_log.index;
        req.previous_log_term = prev_log.term;
        let reply = check_log_consistency(&req, &mut state);
        if !reply.success {
            hint_index -= 1;
            continue;
        }

        assert!(reply.success, "yooo");
        hint_index += 1;
    }

    updated_logs.insert(0, LogEntry::default());
    assert_eq!(state.logs, updated_logs);
}
