package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
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
	// randomize electionTimeout
	// electionTimeout is in [150, 300] ms
	et := rand.Int63n(150) + 150
	rf.electionTimeout = time.Duration(et) * time.Millisecond

	rf.timerStart = time.Now()
}

func (rf *Raft) timeout() bool {
	return time.Now().Sub(rf.timerStart) > rf.electionTimeout
}
