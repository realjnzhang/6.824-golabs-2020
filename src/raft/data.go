package raft

import (
	"bytes"
	"golabs/labgob"
	"runtime"
	"sync"
)

// Lock priority: persist > all volatile > leader volatile

func NewPersistData(p *Persister) *PersistData {
	ret := new(PersistData)
	ret.Log = append(ret.Log, LogEntry{})
	ret.persister = p
	ret.persistLock = new(sync.RWMutex)
	data := p.ReadRaftState()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return ret
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []LogEntry
	if e := d.Decode(&currentTerm); e != nil {
		ret.CurrentTerm = currentTerm
	} else {
		panic(e)
	}

	if e := d.Decode(&voteFor); e != nil {
		if voteFor == -1 {
			ret.VotedFor = nil
		} else {
			ret.VotedFor = &voteFor
		}
	} else {
		panic(e)
	}

	if e := d.Decode(&log); e != nil {
		ret.Log = log
	} else {
		panic(e)
	}
	return ret
}

type PersistData struct {
	persister   *Persister
	persistLock *sync.RWMutex
	CurrentTerm int
	VotedFor    *int
	Log         []LogEntry
}

func (p *PersistData) Lock(who int, round int) {
	_, file, line, _ := runtime.Caller(1)
	DPrintf("\t\t--[%v] try lock persistData in round [%v] at [%v:%v]--", who, round, file, line)
	p.persistLock.Lock()
	DPrintf("\t\t--[%v] lock persistData succ in round [%v] at [%v:%v]--", who, round, file, line)
}

func (p *PersistData) Unlock(who int, round int) {
	_, file, line, _ := runtime.Caller(1)
	p.persistLock.Unlock()
	DPrintf("\t\t--[%v] unlock persistData in round [%v] at [%v:%v]--", who, round, file, line)
}

func (p *PersistData) RLock(who int, round int) {
	_, file, line, _ := runtime.Caller(1)
	DPrintf("\t\t--[%v] try rlock persistData in round [%v] at [%v:%v]--", who, round, file, line)
	p.persistLock.RLock()
	DPrintf("\t\t--[%v] rlock persistData succ in round [%v] at [%v:%v]--", who, round, file, line)
}

func (p *PersistData) RUnlock(who int, round int) {
	_, file, line, _ := runtime.Caller(1)
	p.persistLock.RUnlock()
	DPrintf("\t\t--[%v] runlock persistData in round [%v] at [%v:%v]--", who, round, file, line)
}

func (p *PersistData) RealPersist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(p.CurrentTerm)
	voteFor := -1
	if p.VotedFor != nil {
		voteFor = *p.VotedFor
	}
	e.Encode(voteFor)
	e.Encode(p.Log)
	data := w.Bytes()
	p.persister.SaveRaftState(data)
}

func (p *PersistData) Persist() {

}

func NewVolatileData() *VolatileData {
	ret := new(VolatileData)
	ret.volatileLock = new(sync.RWMutex)
	return ret
}

type VolatileData struct {
	volatileLock *sync.RWMutex
	CommitIndex  int
	LastApplied  int
}

func (v *VolatileData) Lock() {
	v.volatileLock.Lock()
}

func (v *VolatileData) Unlock() {
	v.volatileLock.Unlock()
}

func (v *VolatileData) RLock() {
	v.volatileLock.RLock()
}

func (v *VolatileData) RUnlock() {
	v.volatileLock.RUnlock()
}
