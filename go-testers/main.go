package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
)

const (
	green = "\033[32m"
	red   = "\033[31m"
	reset = "\033[0m"
)

type Command struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Operation string `json:"operation"`
}

type Entry struct {
	Index   uint32  `json:"index"`
	Term    uint32  `json:"term"`
	Command Command `json:"command"`
}
type AppendEntry struct {
	Id               uint32 `json:"id"`
	PreviousLogIndex uint32 `json:"previous_log_index"`
	PreviousLogTerm  uint32 `json:"previous_log_term"`
	Entry            *Entry `json:"entry,omitzero"`
	CommitIndex      uint32 `json:"commit_index"`
	Success          bool   `json:"success"`
}

type RPC struct {
	Type string `json:"type"`
	AppendEntry
}

// TODO: we need to be able to pass flags here to specify what kind of
// tests we want to run for example
//
// tester 1738 3 --type commands -> will send out bunch of commands to the node
// tester 1738 56 --type append_entry -> will send append_entry
// tester 6677 67 --type all  will be the default

func main() {
	if len(os.Args) < 3 {
		log.Fatal("please provide port project is listening on and it's id")
	}

	port := os.Args[1]
	nodeId, err := strconv.ParseUint(os.Args[2], 10, 32)
	if err != nil {
		log.Fatal(err)
	}

	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%s", port))
	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)
	if err != nil {
		log.Fatalf("failed to dial localhost:%s. reason: %s\n", os.Args[1], err)
	}

	runInitTest(uint32(nodeId), encoder, decoder)
	sendEntries(uint32(nodeId), encoder, decoder)
	sendCommands(uint32(nodeId), encoder, decoder)
}

func runInitTest(nodeId uint32, encoder *json.Encoder, decoder *json.Decoder) {
	// first send high logIndexes to test it starts correctly
	initReq := AppendEntry{
		Id:               10_000,
		PreviousLogIndex: 10_000,
		PreviousLogTerm:  10_000,
	}

	initRPC := RPC{
		"append_entry",
		initReq,
	}

	initReply := AppendEntry{}

	expectedInitReply := RPC{
		Type: "append_entry",
		AppendEntry: AppendEntry{
			Id:      nodeId,
			Success: false,
		},
	}

	_ = expectedInitReply

	if err := encoder.Encode(&initRPC); err != nil {
		log.Fatal("runInitTest failed while encoding: reason", err)
	}

	if err := decoder.Decode(&initReply); err != nil {
		log.Fatal("runInitTest failed while decoding: reason", err)
	}

	if initReply.Success || initReply.PreviousLogIndex != 0 || initReply.PreviousLogTerm != 0 {
		fmt.Printf("expected initReply to be false, got %t, %+v\n", initReply.Success, initReply)
		os.Exit(1)
	}

}

// now we can now can now send more entries
func sendEntries(nodeId uint32, encoder *json.Encoder, decoder *json.Decoder) {
	e := AppendEntry{
		Id:               1000,
		PreviousLogIndex: 0,
		PreviousLogTerm:  0,
		Entry:            &Entry{Index: 0, Term: 0, Command: Command{Key: "init", Value: "init", Operation: "set"}},
	}

	rpcReq := RPC{
		"append_entry",
		e,
	}

	// send init request to initialise 0th position
	encoder.Encode(rpcReq)
	decoder.Decode(AppendEntry{})

	outgoingEntries := []AppendEntry{
		{
			Id:               1000,
			PreviousLogIndex: 0,
			PreviousLogTerm:  0,
			Entry:            &Entry{Index: 1, Term: 1, Command: Command{Key: "first_entry", Value: "second_value", Operation: "set"}},
		},

		{
			Id:               1000,
			PreviousLogIndex: 1,
			PreviousLogTerm:  1,
			Entry:            &Entry{Index: 2, Term: 1, Command: Command{Key: "second_entry_key", Value: "second_entry_value", Operation: "set"}},
		},

		{
			Id:               1000,
			PreviousLogIndex: 2,
			PreviousLogTerm:  1,
			Entry:            nil,
		},
		{
			Id:               1000,
			PreviousLogIndex: 2,
			PreviousLogTerm:  1,
			Entry:            &Entry{Index: 3, Term: 1, Command: Command{Key: "third_entry_key", Value: "third_entry_value", Operation: "set"}},
		},
	}

	incomingReplies := []AppendEntry{}
	for _, req := range outgoingEntries {
		rpcReq.AppendEntry = req
		if err := encoder.Encode(rpcReq); err != nil {
			log.Fatalf("failed while sending: %+v\n reason: %s\n", rpcReq, err)
		}

		reply := AppendEntry{}
		if err := decoder.Decode(&reply); err != nil {
			log.Fatalf("failed while decoding: %+v\n reason: %s\n", rpcReq, err)
		}
		incomingReplies = append(incomingReplies, reply)
	}

	expectedReplies := []AppendEntry{
		{
			Id:               nodeId,
			PreviousLogIndex: 1,
			PreviousLogTerm:  1,
			Success:          true,
		},
		{
			Id:               nodeId,
			PreviousLogIndex: 2,
			PreviousLogTerm:  1,
			Success:          true,
		},
		{
			Id:               nodeId,
			PreviousLogIndex: 2,
			PreviousLogTerm:  1,
			Success:          true,
		},
		{
			Id:               nodeId,
			PreviousLogIndex: 3,
			PreviousLogTerm:  1,
			Success:          true,
		},
	}

	failedCount := 0
	passCount := 0
	for idx, expectedReply := range expectedReplies {
		reply := incomingReplies[idx]
		if reply != expectedReply {
			fmt.Printf(
				"expected reply and reply don't match\n reply: %+v\n expectedReply: %+v\n",
				reply, expectedReply)
			failedCount += 1
			continue
		}

		passCount += 1
	}

	fmt.Printf("\n%ssendCommitEntries [passed]: %d%s  %sfailed: %d%s\n", green, passCount, reset, red, failedCount, reset)

	prevLog := expectedReplies[len(expectedReplies)-1]
	sendCommitIndex(nodeId, prevLog.PreviousLogIndex, prevLog.PreviousLogTerm, encoder, decoder)

}

func sendCommitIndex(nodeId uint32, prevLogIndex, prevLogTerm uint32, encoder *json.Encoder, decoder *json.Decoder) {
	expectedCommitIndex := prevLogIndex - 1
	commitReq := AppendEntry{
		Id:               1000,
		PreviousLogIndex: prevLogIndex,
		PreviousLogTerm:  prevLogTerm,
		CommitIndex:      expectedCommitIndex,
		Entry:            nil,
	}

	rpcReq := RPC{"append_entry", commitReq}

	if err := encoder.Encode(rpcReq); err != nil {
		log.Fatal("failed to encode commitRequest", err)
	}

	reply := AppendEntry{}
	if err := decoder.Decode(&reply); err != nil {
		log.Fatal("failed to decode commitReply", err)
	}
	if reply.CommitIndex != expectedCommitIndex {
		fmt.Printf("\n\n%s sendCommitIndex  [failed] expected commitIndex to match %d%s. Got %+v\n",
			red, expectedCommitIndex, reset, reply)
		os.Exit(1)
	}

	fmt.Printf("\n%ssendCommitIndex [passed]  %s\n", green, reset)
}
