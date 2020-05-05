package raft

import "time"

func NewRaftLeader() RaftState {
	return new(RaftLeader)
}

type RaftLeader struct {
	preHeartBeat int64
}

func (l *RaftLeader) InitTransfer(context RaftContext) {
	l.preHeartBeat = time.Now().UnixNano()
	sendOneHeartbeat(context)
}

func (l *RaftLeader) HandleAE(context RaftContext, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// this request cannot count as heartbeat

	// check term if need to transfer to follower
	persistData := context.GetPersistData()
	persistData.Lock()
	defer persistData.Unlock()
	defer persistData.Persist()
	if args.Term > persistData.CurrentTerm {
		persistData.CurrentTerm = args.Term
		context.TransferToFollower()
		// go: prevent dead lock
		go generalAppendEntries(context, args, reply)
	}
	// TODO: apply
}

func (*RaftLeader) HandleRV(context RaftContext, args *RequestVoteArgs, reply *RequestVoteReply) {
	// this request cannot count as heartbeat
	generalRequestVote(context, args, reply)
	// if leader grant vote to peer
	// then he should change to follower
	if reply.VoteGranted {
		context.TransferToFollower()
	}
}

func (*RaftLeader) HandleCommand(context RaftContext, command interface{}) (int, int, bool) {

}

func (*RaftLeader) TimeoutHeartbeat(context RaftContext, t time.Time) {
	sendOneHeartbeat(context)
}

func (*RaftLeader) TimeoutElection(context RaftContext, t time.Time) {
	// leader do nothing to election timeout signal
}

func sendOneHeartbeat(context RaftContext) {

}
