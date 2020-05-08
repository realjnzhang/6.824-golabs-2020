package raft

import (
	"bytes"
	"golabs/labgob"
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

func (p *PersistData) Lock() {
	p.persistLock.Lock()
}

func (p *PersistData) Unlock() {
	p.persistLock.Unlock()
}

func (p *PersistData) RLock() {
	p.persistLock.RLock()
}

func (p *PersistData) RUnlock() {
	p.persistLock.RUnlock()
}

func (p *PersistData) Persist() {
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
