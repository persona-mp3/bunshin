#![allow(unused)]
use anyhow::Result;
use crossbeam::channel::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use bunshin::{Node, RaftState, State, run_candidate, run_follower, run_leader};

fn main() -> Result<()> {
    let mut node = Node::new(3, String::from("localhost:8989"));
    node.run()
}
