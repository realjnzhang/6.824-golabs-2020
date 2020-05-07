package raft

import (
	"math/rand"
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
	// when first comes to candidate, start an election
	r := time.Duration(rand.Float64() * electionTimeout)
	time.Sleep(r * time.Millisecond)
	res := make(chan bool)
	c.startElection(context, res)
	select {
	case elected := <-res:
		if elected {
			context.TransferToLeader()
		}
		// else do nothing, still in candidate
	}
}

func (*RaftCandidate) HandleAE(context RaftContext, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// check term if need to transfer to follower
	persistData := context.GetPersistData()
	persistData.Lock()
	defer persistData.Unlock()
	defer persistData.Persist()

	if args.Term > persistData.CurrentTerm {
		persistData.VotedFor = nil
		persistData.CurrentTerm = args.Term
		context.TransferToFollower()
	}
	go generalAppendEntries(context, args, reply)
	// TODO: apply
	go applyCommitLog(context)
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
	r := time.Duration(rand.Float64() * electionTimeout)
	time.Sleep(r * time.Millisecond)
	// new round election
	res := make(chan bool)
	c.startElection(context, res)
	select {
	case elected := <-res:
		if elected {
			context.TransferToLeader()
		}
		// else do nothing, still in candidate
	}
}

// private function
func (c *RaftCandidate) startElection(context RaftContext, ret chan<- bool) {
	persistData := context.GetPersistData()
	var args RequestVoteArgs

	persistData.Lock()
	defer persistData.Unlock()
	defer persistData.Persist()
	// update persist
	persistData.CurrentTerm = persistData.CurrentTerm + 1
	me := context.Me()
	persistData.VotedFor = &me

	// build args
	args.Term = persistData.CurrentTerm
	args.CandidateId = me
	args.LastLogIndex = len(persistData.Log) - 1
	args.LastLogTerm = persistData.Log[args.LastLogIndex].Term

	voteCount := int32(1)
	finished := int32(0)

	for peer := 0; peer < context.Peers(); peer++ {
		if peer == context.Me() {
			continue
		}
		go func(p int) {
			var reply RequestVoteReply
			if context.SendRequestVote(p, &args, &reply) {
				if reply.Term > persistData.CurrentTerm {
					persistData.CurrentTerm = reply.Term
					persistData.VotedFor = nil
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
				vc := atomic.LoadInt32(&voteCount)
				fin := atomic.LoadInt32(&finished)
				if int(vc)*2 > context.Peers() {
					ret <- true
					return
				}
				if int(fin) >= context.Peers() {
					ret <- false
					return
				}
			case k := <-context.Killed():
				if k { // killed
					voteCounterTicker.Stop()
					context.Kill()
					ret <- false
					return
				}
			}
		}
	}()
	return
}
