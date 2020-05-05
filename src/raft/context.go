package raft

// Raft States's Context

type RaftContext interface {
	TransferToFollower()
	TransferToCandidate()
	TransferToLeader()
	GetPersistData() *PersistData
	Peers() int
	Me() int
	SendRequestVote(int, *RequestVoteArgs, *RequestVoteReply) bool
	Kill()
	Killed() <-chan bool
	GetVolatileData() *VolatileData
}

func (rf *Raft) TransferToFollower() {
	rf.currentState = rf.stateFollower
	rf.currentState.InitTransfer(rf)
}

func (rf *Raft) TransferToCandidate() {
	rf.currentState = rf.stateCandidate
	rf.currentState.InitTransfer(rf)
}

func (rf *Raft) TransferToLeader() {
	rf.currentState = rf.stateLeader
	rf.currentState.InitTransfer(rf)
}

func (rf *Raft) Peers() int {
	return len(rf.peers)
}

func (rf *Raft) Me() int {
	return rf.me
}

func (rf *Raft) GetPersistData() *PersistData {
	return rf.persistData
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
// look at the comments in golabs/labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) Killed() <-chan bool {
	return rf.kill
}

func (rf *Raft) GetVolatileData() *VolatileData {
	return rf.volatileData
}
