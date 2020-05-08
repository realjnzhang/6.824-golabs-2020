package raft

import (
	"sync"
	"time"
)

func NewRaftLeader() RaftState {
	return new(RaftLeader)
}

type RaftLeader struct {
	preHeartBeat int64

	nextIndex  []int
	matchIndex []int

	leaderStateLock *sync.Mutex
}

func (l *RaftLeader) InitTransfer(context RaftContext) {
	l.preHeartBeat = time.Now().UnixNano()

	// init leader volatile
	l.nextIndex = make([]int, context.Peers())
	l.matchIndex = make([]int, context.Peers())
	persistData := context.GetPersistData()
	persistData.RLock()
	// nextIndex
	for i := range l.nextIndex {
		l.nextIndex[i] = len(persistData.Log)
	}
	// matchIndex default to be 0

	persistData.RUnlock()

	l.sendOneHeartbeat(context)
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
		persistData.VotedFor = nil
		context.TransferToFollower()
		// go: prevent dead lock
		go generalAppendEntries(context, args, reply)
		go applyCommitLog(context)
	}
	// else: leader can't receive AE request where Term = CurrentTerm?
	reply.Success = false
	reply.Term = persistData.CurrentTerm
	return
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
	return 0, 0, false
}

func (l *RaftLeader) TimeoutHeartbeat(context RaftContext, t time.Time) {
	l.sendOneHeartbeat(context)
}

func (*RaftLeader) TimeoutElection(context RaftContext, t time.Time) {
	// leader do nothing to election timeout signal
}

func (l *RaftLeader) sendOneHeartbeat(context RaftContext) {
	// should follow the lock priority strictly
	persistData := context.GetPersistData()
	persistData.Lock()
	defer persistData.Unlock()
	defer persistData.Persist()
	volatileData := context.GetVolatileData()
	volatileData.Lock()
	defer volatileData.Unlock()
	l.leaderStateLock.Lock()
	defer l.leaderStateLock.Unlock()

	for i := 0; i < context.Peers(); i++ {
		if i == context.Me() {
			continue
		}
		args := new(AppendEntriesArgs)
		reply := new(AppendEntriesReply)

		args.Term = persistData.CurrentTerm
		args.LeaderId = context.Me()
		args.PrevLogIndex = l.nextIndex[i] - 1
		args.PrevLogTerm = persistData.Log[args.PrevLogIndex].Term
		endLogIndx := len(persistData.Log)
		args.Entries = persistData.Log[args.PrevLogIndex:endLogIndx]
		args.LeaderCommit = volatileData.CommitIndex
		// TODO: parallel each peer routine?
		res := context.SendAppendEntries(i, args, reply)
		if !res {
			continue
		}
		if reply.Term > persistData.CurrentTerm {
			persistData.CurrentTerm = reply.Term
			persistData.VotedFor = nil
			context.TransferToFollower()
			return
		}
		if reply.Success {
			l.nextIndex[i] = endLogIndx + 1
			// Question: why we need to maintain the matchIndex?
			l.matchIndex[i] = endLogIndx
		} else {
			// decrement nextIndex
			l.nextIndex[i] = l.nextIndex[i] - 1
			// TODO: Retry immediately?
		}
		//TODO: update commitIndex

	}
}
