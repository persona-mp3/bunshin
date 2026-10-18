## TODO: 
We might need to refactor or modify the code such that 
the machine:: code get's to inspect the payload, and decide 
on what to do.

For example::

```rust
fn run_follower(
    &mut State,
    rpc_request: Receiver<protocol::RPC>, 
    rpc_reply: Sender<protocol::RPC>
) -> Result<RaftState>{
    Ok(RaftState::Candidate)
}
```

note that in this new one, there might be no need for channel signalling for timers, all that can be 
handled inside one function now.

The `handle_conn` still remains the same, but it forwards RPC's through channels
and receives replies through channels.  The RPC channels should be on `Node` the struct

And then others will follow-as

```rust
fn run_candidate(
    &mut State,
    rpc_request: Receiver<protocol::RPC>, 
    rpc_reply: Sender<protocol::RPC>
) -> Result<RaftState>{
    Ok(RaftState::Follower) || Ok(RaftState::Leader)
}
```


```rust
fn run_leader(
    &mut State,
    rpc_request: Receiver<protocol::RPC>, 
    rpc_reply: Sender<protocol::RPC>
) -> Result<RaftState>{
    Ok(RaftState::Follower)
}
```

Each reply type has it's own handle function, so you need not worry about 
that ie Init has `fn handle(&mut self, &mut State) -> Init`. 
