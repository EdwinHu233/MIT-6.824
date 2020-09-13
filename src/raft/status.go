package raft

import (
	"time"
)

func (rf *Raft) convertToFollower(newTerm int32) {
	DPrintf("%d convertToFollower\n", rf.me)

	rf.status = follower

	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm
		rf.votedFor = -1
	}

	rf.resetTimer()

	go rf.electionLoop()
}

// convertToCandidate converts this peer to candidate,
// and starts a new election.
// It assumes the caller has required the mutex.
func (rf *Raft) convertToCandidate() {
	DPrintf("%d convertToCandidate\n", rf.me)

	rf.status = candidate

	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.numGrantedVotes = 1

	rf.resetTimer()
}

// convertToLeader converts this peer to leader,
// and sends heartbeats to other peers.
func (rf *Raft) convertToLeader() {
	DPrintf("%d convertToLeader\n", rf.me)

	rf.status = leader

	go rf.pingLoop()
}

func (rf *Raft) electionLoop() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.status == leader {
			rf.mu.Unlock()
			return
		}

		// start new election,
		// collect votes from other peers
		if rf.timeout() {
			rf.resetTimer()
			rf.convertToCandidate()

			args := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(server int) {
					reply := &RequestVoteReply{}
					ok := rf.sendRequestVote(server, args, reply)
					if ok {
						rf.handleRequestVoteReply(args, reply)
					}
				}(i)
			}
		}

		rf.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

func (rf *Raft) pingLoop() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.status != leader {
			rf.mu.Unlock()
			return
		}

		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderID: rf.me,
		}

		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(server int) {
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, args, reply)
				if ok {
					rf.handleAppendEntriesReply(args, reply)
				}
			}(i)
		}

		rf.mu.Unlock()
		time.Sleep(rf.heartbeatInterval)
	}
}
