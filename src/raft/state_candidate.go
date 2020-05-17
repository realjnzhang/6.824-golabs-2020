package raft

import (
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"
)

func NewRaftCandidate() RaftState {
	return new(RaftCandidate)
}

type RaftCandidate struct {
	preHeartBeat int64
}

func (c *RaftCandidate) InitTransfer(context RaftContext) {
	c.preHeartBeat = time.Now().UnixNano()
	persistData := context.GetPersistData()
	lockRound := rand.Int()
	persistData.RLock(context.Me(), lockRound)
	termSnapshot := persistData.CurrentTerm
	DPrintf("CANDIDATE[%v] init transfer [term=%v]", context.Me(), termSnapshot)
	persistData.RUnlock(context.Me(), lockRound)
	// when first comes to candidate, start an election
	// r := time.Duration(rand.Float64() * electionTimeout)
	r := time.Duration(context.Me() * electionTimeout)
	time.Sleep(r * time.Millisecond)
	// res := make(chan bool)
	go c.startElection(context, termSnapshot, lockRound)
}

func (*RaftCandidate) HandleAE(context RaftContext, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// check term if need to transfer to follower
	persistData := context.GetPersistData()
	lockRound := rand.Int()
	persistData.Lock(context.Me(), lockRound)

	if args.Term > persistData.CurrentTerm {
		DPrintf("CANDIDATE[%v] receive AE[term=%v] > currentTerm=%v, transfer to Follower", context.Me(), args.Term, persistData.CurrentTerm)
		persistData.VotedFor = nil
		persistData.CurrentTerm = args.Term
		context.TransferToFollower()
	}
	persistData.Persist()
	persistData.Unlock(context.Me(), lockRound)
	generalAppendEntries(context, args, reply)
	// TODO: apply
	applyCommitLog(context)
	DPrintf("peer[%v] reply appendAntries[leader=%v term=%v] {%v}", context.Me(), args.LeaderId, args.Term, *reply)
}

func (*RaftCandidate) HandleRV(context RaftContext, args *RequestVoteArgs, reply *RequestVoteReply) {
	// this request cannot count as heartbeat
	generalRequestVote(context, args, reply)
}

func (*RaftCandidate) HandleCommand(context RaftContext, command interface{}) (int, int, bool) {
	return 0, 0, false
}

func (*RaftCandidate) TimeoutHeartbeat(context RaftContext, t time.Time) {
	// candidate do nothing to heartbeat signal
}

func (c *RaftCandidate) TimeoutElection(context RaftContext, t time.Time) {
	persistData := context.GetPersistData()
	lockRound := rand.Int()
	persistData.RLock(context.Me(), lockRound)
	termSnapshot := persistData.CurrentTerm
	persistData.RUnlock(context.Me(), lockRound)
	r := time.Duration(rand.Float64() * electionTimeout)
	time.Sleep(r * time.Millisecond)
	// new round election
	DPrintf("CANDIDATE[%v] start new round election", context.Me())
	// res := make(chan bool)
	go c.startElection(context, termSnapshot, lockRound)
}

// private function
func (c *RaftCandidate) startElection(context RaftContext, termSnapshot, round int) {
	persistData := context.GetPersistData()
	var args RequestVoteArgs
	lockRound := rand.Int()
	persistData.Lock(context.Me(), lockRound)
	_, file, line, _ := runtime.Caller(1)
	DPrintf("CANDIDATE[%v] start election [currTerm=%v termSnapshot=%v] caller=[%v:%v] at round[%v]", context.Me(), persistData.CurrentTerm, termSnapshot, file, line, round)
	// update persist
	if persistData.CurrentTerm > termSnapshot { // already finished one round election
		persistData.Unlock(context.Me(), lockRound)
		return
	}
	persistData.CurrentTerm = persistData.CurrentTerm + 1
	me := context.Me()
	persistData.VotedFor = &me

	// build args
	args.Term = persistData.CurrentTerm
	args.CandidateId = me
	args.LastLogIndex = len(persistData.Log) - 1
	args.LastLogTerm = persistData.Log[args.LastLogIndex].Term
	persistData.Persist()
	persistData.Unlock(context.Me(), lockRound)

	voteCount := int32(1)
	finished := int32(0)

	for peer := 0; peer < context.Peers(); peer++ {
		if peer == context.Me() {
			continue
		}
		go func(p int) {
			var reply RequestVoteReply
			DPrintf("CANDIDATE[%v] send RV arg[term=%v candidateId=%v lastLogIndex=%v lastLogTerm=%v] to peer[%v]", context.Me(), args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm, p)
			if context.SendRequestVote(p, &args, &reply) {
				DPrintf("CANDIDATE[%v] got RV reply[term=%v grant=%v] from pees[%v]", context.Me(), reply.Term, reply.VoteGranted, p)
				if reply.Term > persistData.CurrentTerm {
					lockRound = rand.Int()
					persistData.Lock(context.Me(), lockRound)
					persistData.CurrentTerm = reply.Term
					persistData.VotedFor = nil
					persistData.Persist()
					persistData.Unlock(context.Me(), lockRound)
					context.TransferToFollower()
					atomic.AddInt32(&voteCount, -1*int32(context.Peers())) // impossible to be elected, not necessary
					return
				}
				if reply.VoteGranted {
					atomic.AddInt32(&voteCount, 1)
				}
			}
			atomic.AddInt32(&finished, 1)
		}(peer)
	}
	// vote counter
	go func() {
		voteCounterTicker := time.NewTicker(2 * time.Millisecond)
		for {
			select {
			case <-voteCounterTicker.C:
				if context.Killed() {
					return
				}
				vc := atomic.LoadInt32(&voteCount)
				fin := atomic.LoadInt32(&finished)
				if int(vc)*2 > context.Peers() {
					DPrintf("CANDIDATE[%v] is elected and transfer to LEADER", context.Me())
					go context.TransferToLeader()
					return
				}
				if int(fin) >= context.Peers() {
					DPrintf("CANDIDATE[%v] is not elected", context.Me())
					// else do nothing, still in candidate
					return
				}
			}
		}
	}()
	return
}
