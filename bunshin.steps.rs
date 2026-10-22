// -------------------------------------------
// Task 1
// -------------------------------------------
// 1. The program should accept AppendEntry requests.
// The requests contain the following fields:
// - id: u32 who sent the  request
// - type: append_entry
// - previous_log_term: u64
// - previous_log_index: u64
// - command: Command
// If the program doesn't have it's prevLogTerm and prevLogIndex up to date
// it rejects the request. It should send an AppendEntry back with the field
// - success: false
// and it's own prevLogTerm, prevLogIndex, and it's id
//
// if they do match, it should simply send a response with success: true without
// the command field

// Now that the program rejects AppendEntry requests where the prev_log_index and
// and prev_log_term don't match. we can move to the next step.
//
// -------------------------------------------
// Task 2
// -------------------------------------------
// This step involve a new field called <commit_index>
// The commit_index represents what log_index the node can persist to disk.
// for example, if the node has 5logs, and it receives an AppendEntry request
// with <commit_index> as 3 and the log indexes match, it should write those logs
// up to the provided commit_index to a `node-id-log-file.json`
//
// The node still needs to keep track of it's own commit_index and include it in
// it's  reply. Success should also be regarded as true in this case, otherwise false
//
// Why?
// We currently store all data in RAM, while the program is running. When we quit
// it, all data is lost. But we might not need all the values to be saved yet,
// all to a certain extent. or the whole log
//
// Good
// Now, we need to make our test harness a little bit better, and work on the interface
// of the project. The first thing we need is to be able to invoke your project with
// - address it should listen on
// - id
//
// we need the id because we're going to eventually run more nodes and they'll
// need to communicate with each other. Not only that our test harness needs to be
// able to run our program and connect with them too
//
//
//
// -------------------------------------------
// Task 3
// -------------------------------------------
// Next thing we need to do is to add another RPC method, Command
// For the moment, a command request should look like this
// operation: get|set|rm
// key: string
// value: string?
//
// When your node receives this request, it should return the latest value of 'key'
// For example, if the operation is a 'set', the node should store the value to it's log
// if the operation is a rm it should store the value to it's logs
// if it's a get, it should return the latest value of the key
// note that in your logs, whether living in ram or on disk encounter a rm for
// a key as the last operation, you are to return nothing.
// If you dont have the  key for requested value, return nothing
// If the key already exists and a set was sent, dont overwrite but append
//
// your node should respond with:
// value: ""|"some_value_for_key"
// success: true
//
// -------------------------------------------
// Task 4
// -------------------------------------------
// We're doing some infra stuff now
// program-infra --nodes 3 --addrs 3001, 3002, 3003
// program-infra --stop 1 // will stop the node at 3002
//
// -------------------------------------------
// Task 5
// -------------------------------------------
// And  should be able to know who it's peers are. You can provided this
// via cli args too
// program --addr 3001 --node-id 3 --peers 3002,3003
// program --addr 3002 --node-id 3 --peers 3001,3003
// program --addr 3003 --node-id 3 --peers 3001,3002
//
// For now, each node should print who their peers are to stdout
//
//
//
// -------------------------------------------
// Task 6
// -------------------------------------------
// The next thing to do is become more indepth with the protocol, assuming
// your test harness still runs.
// Right now, your node accepts requests from any node, and since this is esp for
// a database, data will be corrupted. We need to verify that the request came
// from a trusted person. Right now, your node should have a place to store
// a 'leader'. This leader will come from within the network(peers).
// If the node doesn't have a leader yet, it acknowledges the first person to
// make an AppendEntry{} RPC as the leader. If it already has a leader and the AppendEntry
// RPC came from another node in the network it should reject it.
//
// Addittionaly, update the AppendEntry to have a "reason" field, as it can help
// identify or pinpoint reasons for a failure
// ie logs are out of sync, or invalid_leader
//
//
//
// -------------------------------------------
// Task 7
// -------------------------------------------
// Timers. Now, we're going to spike things up a bit.
// Now, your node simply waits for a leader. What if a leader never shows up?
// It just stays there. If you've never heard of state machines, this is were
// you're going to first encounter them.  State machines represent the state
// of anything. For example the states of a programmer might be confusion, pain
// and pleasing-the-borrow-checker. We're going to take those states from the
// raft paper and start applying them.
//
// Right now, when your node starts, it is the `Follower` state.
// The first node in a network to send it a request, (and the logs are up to date, check Task 1)
// the sender node is in a `Leader` state.  and your node, `Follower` has recognized it
//
// You'll have to encode those variants inside your code somehow
// Now if the Leader current goes offline, your node is stuckk there. Modify your code
// such that, after a set timeout of 2s. It goes to a `Candidate` state. And then
// print `currently in a candidate-state`
//
// -------------------------------------------
// Task 8
// -------------------------------------------
// Concurrency. Right now, your application probably only accepts one connection 
// at a time. As we progress more, that would become a problem, as your application 
// needs to be able to accept multiple connections at once. Depending on what 
// language you're implementing this in, check out the concurrency primitives it 
// offers and how to use them like channels, events, async-await, mutexes and so on

