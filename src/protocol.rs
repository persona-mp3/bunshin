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
    Vote(VoteReq),
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Clone)]
pub struct AppendEntryReq {
    /// the id of the node sending the request
    pub id: u64,

    /// current term of the node sending or receiving
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

    /// signifies if the receipient node recognises this node's AppendEntryReq
    pub success: bool,

    /// additional debugging information
    pub info: Option<String>,
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
            success: false,
            info: None,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Eq, PartialEq, Clone)]
pub struct AppendEntryReply {
    pub id: u64,
    pub term: u64,
    pub conflict_index: usize,
    pub conflict_term: Option<u64>,
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
    pub previous_log_index: usize,
    pub previous_log_term: u64,
    pub granted: bool,
}

fn default_commit_index() -> u64 {
    0
}
