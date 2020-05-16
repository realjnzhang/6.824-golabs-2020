package raft

import (
	"math/rand"
	"time"
)

func NewRaftFollower() RaftState {
	return new(RaftFollower)
}

type RaftFollower struct {
	preHeartBeat int64
}

func (f *RaftFollower) InitTransfer(context RaftContext) {
	f.preHeartBeat = time.Now().UnixNano()
}

func (f *RaftFollower) HandleAE(context RaftContext, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// record heartbeat
	f.preHeartBeat = time.Now().UnixNano()

	// append
	generalAppendEntries(context, args, reply)
	// Apply commit
	applyCommitLog(context)
	DPrintf("peer[%v] reply appendAntries[leader=%v term=%v] {%v}", context.Me(), args.LeaderId, args.Term, *reply)
}

func (*RaftFollower) HandleRV(context RaftContext, args *RequestVoteArgs, reply *RequestVoteReply) {
	// this request cannot count as heartbeat
	generalRequestVote(context, args, reply)
}

func (*RaftFollower) HandleCommand(context RaftContext, command interface{}) (int, int, bool) {
	return 0, 0, false
}

func (f *RaftFollower) TimeoutHeartbeat(context RaftContext, t time.Time) {
	// follower do nothing to heartbeat signal
}

func (f *RaftFollower) TimeoutElection(context RaftContext, t time.Time) {
	ts := t.UnixNano()
	elaps := time.Duration(ts-f.preHeartBeat) * time.Nanosecond
	if elaps < electionTimeout*time.Millisecond {
		// nothing happend
		return
	}
	// change to candidate
	DPrintf("peer[%v] transfer to CANDIDATE from FOLLOWER", context.Me())
	context.TransferToCandidate()
}

func generalRequestVote(context RaftContext, args *RequestVoteArgs, reply *RequestVoteReply) {
	persistData := context.GetPersistData()
	lockRound := rand.Int()
	persistData.Lock(context.Me(), lockRound)
	defer persistData.Unlock(context.Me(), lockRound)
	defer persistData.Persist()
	if args.Term < persistData.CurrentTerm {
		reply.Term = persistData.CurrentTerm
		reply.VoteGranted = false
		return
	}
	// TODO: when to update voteFor? when currentTerm are updated?
	if persistData.VotedFor == nil || *persistData.VotedFor == args.CandidateId {
		// checklog
		lastEntry := persistData.Log[len(persistData.Log)-1]
		if lastEntry.Term > args.LastLogTerm {
			reply.Term = persistData.CurrentTerm
			reply.VoteGranted = false
			return
		}
		if lastEntry.Term == args.LastLogTerm && len(persistData.Log)-1 > args.LastLogIndex {
			reply.Term = persistData.CurrentTerm
			reply.VoteGranted = false
			return
		}
		reply.Term = persistData.CurrentTerm
		reply.VoteGranted = true
		// persist vote for
		persistData.VotedFor = &args.CandidateId
		return
	}
	reply.Term = persistData.CurrentTerm
	reply.VoteGranted = false
	return
}

func generalAppendEntries(context RaftContext, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("Peer[%v] got AE from LEADER[%v] args[term=%v prevLogIdx=%v prevLogTerm=%v leaderCommit=%v]", context.Me(), args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	persistData := context.GetPersistData()
	lockRound := rand.Int()
	persistData.RLock(context.Me(), lockRound)
	if args.Term < persistData.CurrentTerm {
		reply.Success = false
		reply.Term = persistData.CurrentTerm
		DPrintf("Peer[%v] reject AE from LEADER[%v] because of argsTerm[%v] < currentTerm[%v]", context.Me(), args.LeaderId, args.Term, persistData.CurrentTerm)
		persistData.RUnlock(context.Me(), lockRound)
		return
	}

	if args.PrevLogIndex >= len(persistData.Log) {
		reply.Success = false
		reply.Term = persistData.CurrentTerm
		DPrintf("Peer[%v] reject AE from LEADER[%v] because of argsPrevIndex[%v] >= lenLog[%v]", context.Me(), args.LeaderId, args.PrevLogIndex, len(persistData.Log))
		persistData.RUnlock(context.Me(), lockRound)
		return
	}

	perLogEntry := persistData.Log[args.PrevLogIndex]
	if perLogEntry.Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = persistData.CurrentTerm
		DPrintf("Peer[%v] reject AE from LEADER[%v] because of argsPrevTerm[%v] != logPrevTerm[%v]", context.Me(), args.LeaderId, args.PrevLogTerm, perLogEntry.Term)
		persistData.RUnlock(context.Me(), lockRound)
		return
	}
	persistData.RUnlock(context.Me(), lockRound)

	lockRound = rand.Int()
	persistData.Lock(context.Me(), lockRound)
	defer persistData.Unlock(context.Me(), lockRound)
	defer persistData.Persist()
	// append log
	log := persistData.Log[:args.PrevLogIndex+1]
	persistData.Log = append(log, args.Entries...)

	// update commit Index
	volatileData := context.GetVolatileData()
	volatileData.Lock()
	defer volatileData.Unlock()
	if args.LeaderCommit > volatileData.CommitIndex {
		volatileData.CommitIndex = args.LeaderCommit
		if len(persistData.Log)-1 < volatileData.CommitIndex {
			volatileData.CommitIndex = len(persistData.Log) - 1
		}
	}
	DPrintf("Peer[%v] accept AE from LEADER[%v] args[term=%v prevLogIdx=%v prevLogTerm=%v leaderCommit=%v]", context.Me(), args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit)
	reply.Success = true
	reply.Term = persistData.CurrentTerm
	return
}

// if commitIndex > lastApplied  apply log
func applyCommitLog(context RaftContext) {

}
