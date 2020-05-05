package raft

import "time"

type RaftState interface {
	// when the transfer happend, the function of target state will be called
	InitTransfer(context RaftContext)

	// handle append entries
	HandleAE(context RaftContext, args *AppendEntriesArgs, reply *AppendEntriesReply)

	// handle request votes
	HandleRV(context RaftContext, args *RequestVoteArgs, reply *RequestVoteReply)

	// handle command
	HandleCommand(context RaftContext, command interface{}) (int, int, bool)

	// handle heartbeat timeout signal
	TimeoutHeartbeat(context RaftContext, t time.Time)

	// handle election timeout signal
	TimeoutElection(context RaftContext, t time.Time)
}
