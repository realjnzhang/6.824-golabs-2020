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
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent
	CurrentTerm int
	VoteFor     *int
	Logs        []LogEntry

	// volatile
	commitIndex int
	lastApplied int

	// volatile leader
	nextIndex  []int
	matchIndex []int

	// election routine
	// 0: follower 1: candidate 2: leader
	state           int
	electionTimeout *time.Timer

	leaderHeartBeat *time.Timer

	// service handler
	applyChan chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	DPrintf("server[%v] state[currentTerm=%v isLeader=%v]\n", rf.me, rf.CurrentTerm, rf.state == 2)
	return rf.CurrentTerm, rf.state == 2
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	voteFor := -1
	if rf.VoteFor != nil {
		voteFor = *(rf.VoteFor)
	}
	e.Encode(voteFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	// DPrintf("server[%v] try persist data=[%v]\n", rf.me, data)
	rf.persister.SaveRaftState(data)
	return
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	DPrintf("server[%v] try read persist data=[%v]\n", rf.me, data)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	if e := d.Decode(&currentTerm); e == nil {
		rf.CurrentTerm = currentTerm
		DPrintf("server[%v] read term[%v] from persist\n", rf.me, currentTerm)
	} else {
		DPrintf("server[%v] read term from persist err=%v\n", rf.me, e)
	}
	if e := d.Decode(&voteFor); e == nil {
		if voteFor == -1 {
			rf.VoteFor = nil
		} else {
			rf.VoteFor = &voteFor
		}
		DPrintf("server[%v] read voteFor[%v] from persist\n", rf.me, voteFor)
	} else {
		DPrintf("server[%v] read voteFor from persist err=%v\n", rf.me, e)
	}
	if e := d.Decode(&logs); e == nil {
		rf.Logs = logs
		DPrintf("server[%v] read logs[%v] from persist\n", rf.me, logs)
	} else {
		DPrintf("server[%v] read logs from persist err=%v\n", rf.me, e)
	}
	return
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
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
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.dead != 0 {
		return
	}
	DPrintf("peer[%v] get vote request from server[%v]-outlock\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	if args.Term < rf.CurrentTerm {
		DPrintf("peer[%v] reject vote to server[%v], reason: args.Term[%v] < currentTerm[%v]\n", rf.me, args.CandidateId, args.Term, rf.CurrentTerm)
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}
	if args.Term == rf.CurrentTerm && rf.VoteFor != nil && *(rf.VoteFor) != args.CandidateId {
		DPrintf("peer[%v] reject vote to server[%v], reason: args.Term[%v] == currentTerm[%v] and already vote to server[%v]\n", rf.me, args.CandidateId, args.Term, rf.CurrentTerm, *rf.VoteFor)
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}
	// log up-to-date
	var lastLogEntry LogEntry
	var lastLogIndex int
	lastLogIndex = len(rf.Logs) - 1
	lastLogEntry = rf.Logs[lastLogIndex]
	if lastLogEntry.Term > args.Term {
		DPrintf("peer[%v] reject vote to server[%v], reason: lastTerm[%v] > args.Term[%v]\n", rf.me, args.CandidateId, lastLogEntry.Term, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}
	if lastLogIndex > args.LastLogIndex {
		DPrintf("peer[%v] reject vote to server[%v], reason: lastIndex[%v] > args.LastLogIndex[%v]\n", rf.me, args.CandidateId, lastLogIndex, args.LastLogIndex)
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm
		return
	}

	// grant vote
	defer rf.resetTimers()
	rf.state = 0
	rf.VoteFor = &args.CandidateId
	rf.CurrentTerm = args.Term
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = true
	DPrintf("server[%v] get vote from peer[%v]\n", args.CandidateId, rf.me)
	rf.persist()
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	if rf.dead != 0 {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if rf.dead != 0 {
		return -1, -1, false
	}
	index := -1
	term := -1

	// Your code here (2B).
	if rf.state != 2 { // not leader
		return index, term, false
	}

	rf.mu.Lock()
	index = len(rf.Logs)
	term = rf.CurrentTerm
	entry := LogEntry{
		Term:    term,
		Command: command,
	}
	rf.Logs = append(rf.Logs, entry)
	rf.persist()
	rf.mu.Unlock()

	go rf.heartbeat()

	return index, term, true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	DPrintf("kill server[%v]\n", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// create empty first log
	if len(rf.Logs) == 0 {
		rf.Logs = append(rf.Logs, LogEntry{})
	}

	rf.resetTimers()

	return rf
}

func (r *Raft) resetTimers() {
	if r.dead != 0 {
		return
	}
	if r.state == 2 { // leader
		if r.leaderHeartBeat == nil {
			r.leaderHeartBeat = time.AfterFunc(cHeartBeatInterval*time.Millisecond, r.heartbeat)
		} else {
			// if !r.leaderHeartBeat.Stop() {
			// 	<-r.leaderHeartBeat.C
			// }
			r.leaderHeartBeat.Reset(cHeartBeatInterval * time.Millisecond)
		}
	} else { // not leader
		if r.electionTimeout == nil {
			DPrintf("peer[%v] new electionTimeout after %v milli\n", r.me, cElectionTimeout)
			r.electionTimeout = time.AfterFunc(cElectionTimeout*time.Millisecond, r.startElection)
		} else {
			DPrintf("peer[%v] reset electionTimeout after %v milli\n", r.me, cElectionTimeout)
			// if !r.electionTimeout.Stop() {
			// 	<-r.electionTimeout.C
			// }
			r.electionTimeout.Reset(cElectionTimeout * time.Millisecond)
		}
	}
}

const cElectionSleep = 100
const cElectionTimeout = 600
const cHeartBeatInterval = 200

func (rf *Raft) startElection() {
	if rf.dead != 0 {
		return
	}
	if rf.state == 2 { // is leader no need to election
		return
	}
	defer rf.resetTimers()
	rf.state = 1
	curTerm := rf.CurrentTerm
	// random sleep
	r := int(rand.Float64() * cElectionSleep)
	time.Sleep(time.Duration(r) * time.Millisecond)
	if rf.CurrentTerm > curTerm { // already in a new term
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != 1 {
		return
	}
	rf.CurrentTerm = rf.CurrentTerm + 1
	rf.VoteFor = &rf.me
	rf.persist()
	DPrintf("server[%v] start election routine at term [%v] 2\n", rf.me, rf.CurrentTerm)

	voteCounter := int32(1)
	for p := range rf.peers {
		if p == rf.me {
			continue
		}
		go func(peer int) {
			var args RequestVoteArgs
			var reply RequestVoteReply
			args.Term = rf.CurrentTerm
			args.CandidateId = rf.me
			args.LastLogIndex = len(rf.Logs) - 1
			args.LastLogTerm = rf.Logs[args.LastLogIndex].Term
			DPrintf("server[%v] send args[term=%v candidate=%v lastIndex=%v lastTerm=%v] to peer[%v]\n", rf.me, args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm, peer)
			rf.sendRequestVote(peer, &args, &reply)
			DPrintf("server[%v] receive reply[term=%v grant=%v] from peer[%v]\n", rf.me, reply.Term, reply.VoteGranted, peer)
			// reply.Term
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.VoteFor = nil
				rf.state = 0
				return
			}
			if reply.VoteGranted {
				atomic.AddInt32(&voteCounter, 1)
			}
		}(p)
	}
	go func() {
		for {
			if rf.state != 1 {
				break
			}
			if 2*int(voteCounter) > len(rf.peers) {
				rf.state = 2
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIndex[i] = len(rf.Logs)
					rf.matchIndex[i] = 0
				}
				rf.persist()
				DPrintf("server[%v] becomes leader{next=%v, match=%v}\n", rf.me, rf.nextIndex, rf.matchIndex)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()
	return
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if r.dead != 0 {
		return
	}
	defer r.resetTimers()
	DPrintf("AE: peer[%v, %v] receive appendEntries from server[%v], args[term=%v preIndex=%v preTerm=%v commit=%v entries=%v]\n", r.me, r.CurrentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
	if args.Term < r.CurrentTerm {
		DPrintf("AE: peer[%v, %v] reject appendEntries from server[%v], reason: args.Term[%v] < curTerm[%v]\n", r.me, r.CurrentTerm, args.LeaderId, args.Term, r.CurrentTerm)
		reply.Success = false
		reply.Term = r.CurrentTerm
		return
	}
	if len(r.Logs) <= args.PrevLogIndex {
		DPrintf("AE: peer[%v, %v] reject appendEntries from server[%v], reason: lenLogs[%v] < args.PreIndex[%v]\n", r.me, r.CurrentTerm, args.LeaderId, len(r.Logs), args.PrevLogIndex)
		reply.Success = false
		reply.Term = r.CurrentTerm
		return
	}
	entry := r.Logs[args.PrevLogIndex]
	if entry.Term != args.PrevLogTerm {
		DPrintf("AE: peer[%v, %v] reject appendEntries from server[%v], reason: enterTerm[%v] != args.PreTerm[%v]\n", r.me, r.CurrentTerm, args.LeaderId, entry.Term, args.PrevLogTerm)
		reply.Success = false
		reply.Term = r.CurrentTerm
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.CurrentTerm = args.Term
	r.state = 0
	r.Logs = r.Logs[:args.PrevLogIndex+1]
	r.Logs = append(r.Logs, args.Entries...)
	if args.LeaderCommit > r.commitIndex {
		r.commitIndex = args.LeaderCommit
		if len(r.Logs)-1 < r.commitIndex {
			r.commitIndex = len(r.Logs) - 1
		}
	}
	reply.Success = true
	reply.Term = r.CurrentTerm
	r.persist()
	return
}

func (r *Raft) sendAppendEntries(server int) bool {
	if r.dead != 0 {
		return false
	}
	var args AppendEntriesArgs
	var reply AppendEntriesReply

	if r.state != 2 {
		return false
	}

	r.mu.Lock()
	args.PrevLogIndex = r.nextIndex[server] - 1
	args.Term = r.CurrentTerm
	args.PrevLogTerm = r.Logs[args.PrevLogIndex].Term
	args.LeaderId = r.me
	args.LeaderCommit = r.commitIndex
	next := len(r.Logs)
	args.Entries = r.Logs[r.nextIndex[server]:next]
	r.mu.Unlock()

	DPrintf("AE: server[%v] sendAppendEntries args[term=%v preIndex=%v preTerm=%v leaderId=%v leaderCommit=%v entries=%v]\n", r.me, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderId, args.LeaderCommit, args.Entries)
	ret := r.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ret {
		return ret
	}

	if reply.Success {
		r.mu.Lock()
		r.nextIndex[server] = next
		r.matchIndex[server] = next - 1
		// // update commitIndex
		// var matchIdx []int
		// matchIdx = append(matchIdx, r.matchIndex...)
		// sort.Ints(matchIdx)
		// _commitIndex := matchIdx[len(r.peers) / 2]
		// if _commitIndex > r.commitIndex && r.Logs[_commitIndex].Term = r.CurrentTerm {
		// 	r.commitIndex = _commitIndex
		// }
		r.mu.Unlock()
		return true
	} else if reply.Term > r.CurrentTerm {
		r.CurrentTerm = reply.Term
		r.VoteFor = nil
		r.state = 0
		r.persist()
		return false
	} else {
		r.mu.Lock()
		r.nextIndex[server]--
		r.mu.Unlock()
		// retry immediately
		// r.sendAppendEntries(server)
		return false
	}
	return false
}

func (r *Raft) heartbeat() {
	if r.dead != 0 {
		return
	}
	if r.state != 2 { // no longer leader
		DPrintf("AE: server[%v] no longer leader\n", r.me)
		return
	}
	DPrintf("AE: server[%v] sending heart beat\n", r.me)
	for i := range r.peers {
		if i == r.me {
			continue
		}
		go r.sendAppendEntries(i)
	}

	// update commit index
	r.updateCommitIndex()

	// reset timer
	r.resetTimers()
}

func (r *Raft) updateCommitIndex() {
	// update commitIndex
	DPrintf("AE: server[%v] update commit index", r.me)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != 2 {
		return
	}
	var matchIdx []int
	matchIdx = append(matchIdx, r.matchIndex...)
	sort.Ints(matchIdx)
	_commitIndex := matchIdx[len(r.peers)/2]
	if _commitIndex > r.commitIndex && r.Logs[_commitIndex].Term == r.CurrentTerm {
		r.commitIndex = _commitIndex
	}
	return
}
