package main

import (
	"encoding/json"
	"fmt"
	"log"
)

type CommandRPC struct {
	Type      string `json:"type"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Operation string `json:"operation"`
}

type CommandReply struct {
	Value   string `json:"value"`
	Success bool   `json:"success"`
}

func sendCommands(nodeId uint32, encoder *json.Encoder, decoder *json.Decoder) {
	// Refactored from map to a slice to guarantee execution order
	commands := []struct {
		req           CommandRPC
		expectedReply CommandReply
	}{
		{
			req:           CommandRPC{Key: "project_name", Value: "bushino", Operation: "set"},
			expectedReply: CommandReply{Value: "bushino", Success: true},
		},
		{
			req:           CommandRPC{Key: "naruto_arc", Value: "filler_episodes::netflix", Operation: "set"},
			expectedReply: CommandReply{Value: "filler_episodes::netflix", Success: true},
		},
		{
			// Since we haven't created this kind of value before, we should get a not found
			req:           CommandRPC{Key: "unknown", Operation: "get", Value: ""},
			expectedReply: CommandReply{Value: "not_found", Success: false},
		},
		{
			req:           CommandRPC{Key: "set_then_remove", Operation: "set", Value: "temp_value"},
			expectedReply: CommandReply{Value: "temp_value", Success: true},
		},
		{
			req:           CommandRPC{Key: "set_then_remove", Operation: "rm", Value: ""},
			expectedReply: CommandReply{Value: "temp_value", Success: true},
		},
		{
			req:           CommandRPC{Key: "set_then_remove", Operation: "get", Value: ""},
			expectedReply: CommandReply{Value: "not_found", Success: false},
		},
		{
			req:           CommandRPC{Key: "set_then_remove", Operation: "rm", Value: ""},
			expectedReply: CommandReply{Value: "not_found", Success: false},
		},
		{
			req:           CommandRPC{Key: "project_name", Operation: "get", Value: "bushino"},
			expectedReply: CommandReply{Value: "bushino", Success: true},
		},
		{
			req:           CommandRPC{Key: "naruto_arc", Operation: "get", Value: "filler_episodes::netflix"},
			expectedReply: CommandReply{Value: "filler_episodes::netflix", Success: true},
		},
	}

	for _, tc := range commands {
		req := tc.req
		req.Type = "command"

		if err := encoder.Encode(req); err != nil {
			log.Fatalf("could not encode commandReq. reason: %s, req: %+v\n", err, req)
		}

		var reply CommandReply
		if err := decoder.Decode(&reply); err != nil {
			log.Fatalf("could not decode into commandReply. reason: %s, req: %+v\n", err, req)
		}

		if tc.expectedReply != reply {
			fmt.Printf("\n%s sendCommands [failed], req: %+v, expectedReply: %+v, got: %+v%s\n", red, req, tc.expectedReply, reply, reset)
		}
	}
}
