package raft

import (
	"log"
	"sync"
	"time"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ConcurrentQueue struct {
	q []time.Time
	sync.Mutex
}

func (cq *ConcurrentQueue) tryPush() bool {
	cq.Lock()
	defer cq.Unlock()

	for len(cq.q) > 0 {
		if time.Now().Sub(cq.q[0]) > time.Second {
			cq.q = cq.q[1:]
		}
	}

	if len(cq.q) < 10 {
		cq.q = append(cq.q, time.Now())
		return true
	}
	return false
}

// isMoreUpToDate returns true,
// if peer 'a' has a (strictly) more up-to-date log than peer 'b'.
// termA and indexA are from the last log entry of 'a'.
// termB and indexB are from the last log entry of 'b'.
func isMoreUpToDate(termA, indexA, termB, indexB int) bool {
	if termA != termB {
		return termA > termB
	}
	return indexA > indexB
}

func (rf *Raft) resetTimer() {
	rf.randomizeTimeout()
	rf.timer.Reset(rf.electionTimeout)
}
