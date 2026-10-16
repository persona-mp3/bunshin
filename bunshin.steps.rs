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
// This step involve a new field called <commit_index>
// The commit_index represents what log_index the node can persist to disk.
// for example, if the node has 5logs, and it receives an AppendEntry request
// with <commit_index> as 3 and the log indexes match, it should write those logs
// up to the provided commit_index to a `node-id-log-file.json`
//
// The node still needes to keep track of it's own commit_index and include it in
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
