this is tai, my final attempt at a raft impl
we're going to develop this in a different way, we're going 
to start with the 
- log replication: 
 a node should append an entry to it's logs if it doesn't have it
 but before it does that, it's previous log must match with the leader

- we're not going to have timers now, but I want it to be like the 
flyio thing where the application just listens for an init

so we send some data to this connection ie appendJSON, the appendJSON 
should also contain the previousLogIndex & prevLogTerm. If what the connection's
owns are behind it fails to append, otherwise it appends and sends them back
to the leader

