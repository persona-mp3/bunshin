use crate::LogEntry;
use crossbeam::channel::Sender;
use serde::{Deserialize, Serialize};

/// Packet is used to communicate between the Network and Machine layer.
/// The network layer sends incoming rpc requests through a [`Sender<Packet>`]
/// which contains the rpc payload and a channel that the Machine layer should
/// give the reply through
#[derive(Debug, Clone)]
pub struct Packet {
    pub rpc: Rpc,
    pub sender: Sender<Rpc>,
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Clone)]
pub enum Rpc {
    AppendEntryReq(AppendEntryReq),
    AppendEntryReply(AppendEntryReply),
    VoteReq(VoteReq),
    VoteReply(VoteReply),
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Clone)]
pub struct AppendEntryReq {
    /// the id of the node sending the request
    pub id: u64,

    /// current term of the node sending
    pub term: u64,

    /// the previous_log_index of the node
    pub previous_log_index: usize,

    /// the previous_log_term of the node
    pub previous_log_term: u64,

    #[serde(default = "default_commit_index")]
    /// commit_index that tracks which logs are safe to be written to disk
    pub commit_index: u64,

    /// holds a new entry that the leader has received. If
    /// empty the request is a heartbeat
    pub entry: Option<LogEntry>,

    /// additional debugging information
    pub info: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Clone)]
pub struct AppendEntryReply {
    /// id of the node sending the reply
    pub id: u64,

    pub term: u64,
    /// conflict_index is not directly referenced in the protocol. The follower
    /// uses it to hint to the leader on what next log index it might need to take
    /// instead of going through the whole log bit by bit.
    ///
    /// Example:
    ///
    /// index:    [1, 2, 3, 4, 5]
    /// Leader:   [1, 2, 3, 4, 5]
    /// Follower: [1, 2]
    ///
    /// the leader starts of by sending the log in the 4th position, i4, and that log
    /// has a term of 4, hence i4:t4
    ///
    /// the follower checks if it has anything at i4. since it doesn't it responds
    /// by sending the last index and term it has: i2:t2. and rejects the append entry
    ///
    /// The leader then does a lookup against `conflict_index`, i2 instead of sending i3 and
    /// waiting for confirmation from the follower
    ///
    /// Summary
    /// L -> i4:t4, next = i5
    /// F -> i2, conflict_index = i2
    /// L -> i2:t2, next: i3:t3
    pub conflict_index: usize,

    /// conflict_term is not directly referenced in the protocol. Like the `conflict_index`
    /// if the Follower does have a log at the `AppendEntryReq.previous_log_index` but the
    /// terms differ, it sends a conflicting term, hence the leader could skip the whole
    /// term
    /// Example:
    ///
    /// index:   [1, 2, 3, 4, 5]
    /// Leader:   [1, 1, 4, 4, 3]
    /// Follower: [1, 2, 7]
    ///
    /// the leader starts of by sending the log in the 4th position, i4, and that log
    /// has a term of 4, hence i4:t4
    ///
    /// the follower checks if it has anything at i4, retrieves nothing,
    /// rejects the entry and sends the i3 as conflict_index
    ///
    /// Leader checks i3 decrements i3 and checks to send i2:t2 and sends with i3:t4 as the new entry
    ///
    /// Follower checks i3 but has t7 then sends it in `conflict_term`
    ///
    /// Leader sees follower has a conflict at i3 with the term t7, so it skips to the
    /// next previous term it has and starts the index from there, which i2:t1
    ///
    ///
    /// Same thing till they meet at t1 and they can begin to sync
    pub conflict_term: Option<u64>,

    /// true if the previous log indexes match, recognized as leader and the log
    /// entry, if any was appended successfully
    pub success: bool,

    /// reset_timer is not part of the protocol but is used within the state machine. It
    /// signifies that the node can reset it's timer
    #[serde(skip)]
    pub reset_timer: bool,
}

impl AppendEntryReply {
    pub fn default(id: u64) -> Self {
        Self {
            id,
            term: 0,
            conflict_index: 0,
            conflict_term: None,
            success: false,
            reset_timer: false,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Clone)]
pub struct VoteReq {
    pub id: u64,
    pub term: u64,
    pub last_log_index: usize,
    pub last_log_term: u64,
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Clone)]
pub struct VoteReply {
    pub id: u64,
    pub term: u64,
    pub granted: bool,
}

impl AppendEntryReq {
    pub fn default(id: u64) -> Self {
        Self {
            id,
            term: 0,
            previous_log_index: 0,
            previous_log_term: 0,
            entry: None,
            commit_index: 0,
            info: None,
        }
    }
}

fn default_commit_index() -> u64 {
    0
}
