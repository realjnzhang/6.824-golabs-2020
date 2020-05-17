package raft

import (
	"math/rand"
	"sync"
	"time"
)

func NewRaftLeader() RaftState {
	ret := new(RaftLeader)
	ret.leaderStateLock = new(sync.Mutex)
	return ret
}

type RaftLeader struct {
	preHeartBeat int64

	nextIndex  []int
	matchIndex []int

	leaderStateLock *sync.Mutex
}

func (l *RaftLeader) InitTransfer(context RaftContext) {
	l.preHeartBeat = time.Now().UnixNano()
	persistData := context.GetPersistData()
	lockRound := rand.Int()
	persistData.RLock(context.Me(), lockRound)
	l.leaderStateLock.Lock()
	// init leader volatile
	l.nextIndex = make([]int, context.Peers())
	l.matchIndex = make([]int, context.Peers())
	// nextIndex
	for i := range l.nextIndex {
		l.nextIndex[i] = len(persistData.Log)
	}
	// matchIndex default to be 0
	l.leaderStateLock.Unlock()
	persistData.RUnlock(context.Me(), lockRound)

	l.sendOneHeartbeat(context)
}

func (l *RaftLeader) HandleAE(context RaftContext, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// this request cannot count as heartbeat

	// check term if need to transfer to follower
	persistData := context.GetPersistData()
	lockRound := rand.Int()
	persistData.Lock(context.Me(), lockRound)
	if args.Term > persistData.CurrentTerm {
		persistData.CurrentTerm = args.Term
		persistData.VotedFor = nil
		persistData.Persist()
		persistData.Unlock(context.Me(), lockRound)
		context.TransferToFollower()
		replyChan := make(chan *AppendEntriesReply)
		go func() {
			generalAppendEntries(context, args, reply)
			applyCommitLog(context)
			replyChan <- reply
		}()
		*reply = *<-replyChan
		return
	}
	// else: leader can't receive AE request where Term = CurrentTerm?
	reply.Success = false
	reply.Term = persistData.CurrentTerm
	persistData.Persist()
	persistData.Unlock(context.Me(), lockRound)
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
	volatileData := context.GetVolatileData()
	for i := 0; i < context.Peers(); i++ {
		if i == context.Me() {
			continue
		}
		args := new(AppendEntriesArgs)
		reply := new(AppendEntriesReply)
		lockRound := rand.Int()
		persistData.RLock(context.Me(), lockRound)
		volatileData.RLock()
		l.leaderStateLock.Lock()
		DPrintf("LEADER[%v] nextIndex%v\n", context.Me(), l.nextIndex)
		args.Term = persistData.CurrentTerm
		args.LeaderId = context.Me()
		args.PrevLogIndex = l.nextIndex[i] - 1
		args.PrevLogTerm = persistData.Log[args.PrevLogIndex].Term
		endLogIndx := len(persistData.Log)
		args.Entries = persistData.Log[args.PrevLogIndex+1 : endLogIndx]
		args.LeaderCommit = volatileData.CommitIndex
		l.leaderStateLock.Unlock()
		volatileData.RUnlock()
		persistData.RUnlock(context.Me(), lockRound)
		// TODO: parallel each peer routine?
		go func(peer int, a *AppendEntriesArgs) {
			DPrintf("LEADER[%v] send AE arg[term=%v leaderId=%v prevLogIdx=%v prevLogTerm=%v leaderCommit=%v] to peer[%v]", context.Me(), args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, peer)
			res := context.SendAppendEntries(peer, args, reply)
			if !res {
				return
			}
			DPrintf("LEADER[%v] receive AE reply[term=%v succ=%v] from peer[%v]", context.Me(), reply.Term, reply.Success, peer)
			lockRound = rand.Int()
			persistData.Lock(context.Me(), lockRound)
			defer persistData.Unlock(context.Me(), lockRound)
			defer persistData.Persist()
			volatileData.Lock()
			defer volatileData.Unlock()
			l.leaderStateLock.Lock()
			defer l.leaderStateLock.Unlock()
			if reply.Term > persistData.CurrentTerm {
				DPrintf("LEADER[%v] got replyTerm=%v > currTerm=%v no long leader", context.Me(), reply.Term, persistData.CurrentTerm)
				persistData.CurrentTerm = reply.Term
				persistData.VotedFor = nil
				go context.TransferToFollower()
				return
			}
			if reply.Success {
				l.nextIndex[peer] = endLogIndx
				// Question: why we need to maintain the matchIndex?
				l.matchIndex[peer] = endLogIndx
			} else {
				// decrement nextIndex
				l.nextIndex[peer] = l.nextIndex[peer] - 1
				// TODO: Retry immediately?
			}
			// matchIdx := make([]int, len(l.matchIndex))
			// copy(matchIdx, l.matchIndex)
			// sort.Sort(sort.Reverse(sort.IntSlice(matchIdx)))
			// DPrintf("debug matchIdx=%v\n", matchIdx)
			// maxN := matchIdx[int(len(matchIdx)/2)]
			// for j := maxN; j > volatileData.CommitIndex; j-- {
			// 	if persistData.Log[j].Term == persistData.CurrentTerm {
			// 		volatileData.CommitIndex = j
			// 		break
			// 	}
			// }
			// TODO: apply entry
		}(i, args)
	}
}
