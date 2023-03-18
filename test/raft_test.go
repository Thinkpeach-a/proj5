package SurfTest

import (
	//"cse224/proj5/pkg/surfstore"
	"fmt"
	"testing"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	//debug
	state, _ := test.Clients[0].GetInternalState(test.Context, &emptypb.Empty{})
	if state == nil {
		t.Fatalf("Could not get state")
	} else {
		t.Log(state.IsLeader)
	}
	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(1) {
			t.Fatalf("Server %d should be in term %d", idx, 1)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	//debug
	//printState(test)
	// heartbeat
	for _, server := range test.Clients {
		printState(test)
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}
	printState(test)
	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(2) {
			t.Fatalf("Server should be in term %d", 2)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}
}

func printState(test TestInfo) {
	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		fmt.Print("id: ", idx)
		fmt.Println("leader: ", state.IsLeader, "Term: ", state.Term, "length: ", len(state.Log))
		//fmt.Println("nextIndex:", state.nextIndex)
	}
}
