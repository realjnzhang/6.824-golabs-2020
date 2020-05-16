package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"golabs/labrpc"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "golabs/labgob"

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// kill signal
	kill int32

	electionTicker *time.Ticker
	hearbeatTicker *time.Ticker

	// StateHandlers
	stateHandlers  []RaftState
	stateFollower  int32
	stateCandidate int32
	stateLeader    int32
	currentState   int32
	stateLock      *sync.Mutex

	// Persistent state
	persistData *PersistData

	// volatile
	volatileData *VolatileData
}

const (
	electionTimeout   = 300 // 300ms // half
	heartbeatInterval = 100 // 100ms
)

func NewRaft(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.electionTicker = time.NewTicker(electionTimeout * time.Millisecond)
	rf.hearbeatTicker = time.NewTicker(heartbeatInterval * time.Millisecond)

	rf.stateHandlers = []RaftState{NewRaftFollower(), NewRaftCandidate(), NewRaftLeader()}
	rf.stateFollower = 0
	rf.stateCandidate = 1
	rf.stateLeader = 2

	// Raft init to be follower
	rf.currentState = 0
	rf.stateLock = new(sync.Mutex)

	// initialize from state persisted before a crash
	rf.persistData = NewPersistData(persister)

	rf.volatileData = NewVolatileData()

	// rf.nextIndex = make([]int, len(rf.peers))
	// for i := range rf.nextIndex {
	// 	rf.nextIndex[i] = len(rf.log)
	// }
	// rf.matchIndex = make([]int, len(rf.peers))

	go rf.electionSignalBridge()
	go rf.heartbeatSignalBridge()

	return rf
}

func (rf *Raft) currentStateHandler() RaftState {
	return rf.stateHandlers[atomic.LoadInt32(&rf.currentState)]
}

func (rf *Raft) heartbeatSignalBridge() {
	for {
		select {
		case t := <-rf.hearbeatTicker.C:
			if rf.Killed() {
				return
			}
			DPrintf("\tsignal heartbeat to [%v] at time [%v]", rf.Me(), t)
			rf.currentStateHandler().TimeoutHeartbeat(rf, t)
		}
	}
}

func (rf *Raft) electionSignalBridge() {
	for {
		select {
		case t := <-rf.electionTicker.C:
			if rf.Killed() {
				return
			}
			DPrintf("\tsignal election to [%v] at time [%v]", rf.Me(), t)
			rf.currentStateHandler().TimeoutElection(rf, t)
		}
	}
}

// RPCs

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.Killed() {
		return
	}
	DPrintf("peer[%v] receive requestVote[candidate=%v term=%v]", rf.Me(), args.CandidateId, args.Term)
	rf.currentStateHandler().HandleRV(rf, args, reply)
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.Killed() {
		return
	}
	DPrintf("peer[%v] receive appendAntries[leader=%v term=%v]", rf.Me(), args.LeaderId, args.Term)
	rf.currentStateHandler().HandleAE(rf, args, reply)
	DPrintf("peer[%v] reply appendAntries[leader=%v term=%v] {%v}", rf.Me(), args.LeaderId, args.Term, *reply)
}
