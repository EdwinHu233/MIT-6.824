package raft

import (
	"sync"
	"time"
)

func (rf *Raft) convertToFollower(newTerm int32) {
	// DPrintf("%d convertToFollower\n", rf.me)

	rf.status = follower

	if newTerm > rf.CurrentTerm {
		rf.CurrentTerm = newTerm
		rf.VotedFor = -1
		rf.persist()
	}

	rf.resetTimer()

	go rf.electionLoop()
}

// convertToCandidate converts this peer to candidate,
// and starts a new election.
// It assumes the caller has required the mutex.
func (rf *Raft) convertToCandidate() {
	// DPrintf("%d convertToCandidate\n", rf.me)

	rf.status = candidate

	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.numGrantedVotes = 1

	rf.persist()

	rf.resetTimer()
}

// convertToLeader converts this peer to leader,
// and sends heartbeats to other peers.
func (rf *Raft) convertToLeader() {
	DPrintf("%d convertToLeader\n", rf.me)

	rf.status = leader
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.Log)
		rf.matchIndex[i] = 0
	}

	go rf.heartbeatLoop()
	go rf.logReplicationLoop()
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
			rf.convertToCandidate()

			args := &RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidateID:  rf.me,
				LastLogIndex: len(rf.Log) - 1,
				LastLogTerm:  rf.Log[len(rf.Log)-1].Term,
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
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) heartbeatLoop() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.status != leader {
			rf.mu.Unlock()
			return
		}

		// prevIndex := len(rf.Log) - 1
		// args := &AppendEntriesArgs{
		// 	Term:         rf.CurrentTerm,
		// 	LeaderID:     rf.me,
		// 	PrevLogIndex: prevIndex,
		// 	PrevLogTerm:  rf.Log[prevIndex].Term,
		// 	Entries:      nil,
		// 	LeaderCommit: rf.commitIndex,
		// }

		var waitSending sync.WaitGroup

		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			waitSending.Add(1)

			go func(server int) {
				prevIndex := rf.nextIndex[server] - 1
				args := &AppendEntriesArgs{
					Term:         rf.CurrentTerm,
					LeaderID:     rf.me,
					PrevLogIndex: prevIndex,
					PrevLogTerm:  rf.Log[prevIndex].Term,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				reply := &AppendEntriesReply{}

				waitSending.Done()

				ok := rf.sendAppendEntries(server, args, reply)
				if ok {
					rf.handleAppendEntriesReply(server, args, reply)
				}
			}(i)
		}

		waitSending.Wait()
		rf.mu.Unlock()
		time.Sleep(rf.heartbeatInterval)
	}
}

func (rf *Raft) logReplicationLoop() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.status != leader {
			rf.mu.Unlock()
			return
		}

		var waitSending sync.WaitGroup

		// log.Printf("leader's nextIndex:\n%v\n", rf.nextIndex)

		lastLogIndex := len(rf.Log) - 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if lastLogIndex >= rf.nextIndex[i] {

				waitSending.Add(1)

				go func(server int) {
					nextIndex := rf.nextIndex[server]
					prevIndex := nextIndex - 1
					args := &AppendEntriesArgs{
						Term:         rf.CurrentTerm,
						LeaderID:     rf.me,
						PrevLogIndex: prevIndex,
						PrevLogTerm:  rf.Log[prevIndex].Term,
						Entries:      rf.Log[nextIndex:],
						LeaderCommit: rf.commitIndex,
					}

					waitSending.Done()

					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, args, reply)
					if ok {
						rf.handleAppendEntriesReply(server, args, reply)
					}
				}(i)
			}
		}

		waitSending.Wait()

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) applyLoop(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()

		for rf.lastApplied+1 <= rf.commitIndex &&
			rf.lastApplied+1 < len(rf.Log) {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			applyCh <- msg
		}

		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}
