package raft

import (
	"log"
	"time"
)

func (rf *Raft) convertToFollower(newTerm int32, resetTimer bool) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	if newTerm < rf.currentTerm {
		log.Fatal("convertToFollower: outdated newTerm")
	}

	rf.status = follower

	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm
		rf.votedFor = -1
	}

	if resetTimer {
		rf.resetTimer()
	}
}

// convertToLeader converts this peer to leader,
// and sends heartbeats to other peers.
func (rf *Raft) convertToLeader() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	if rf.status != candidate {
		log.Fatalf("convertToLeader: not a candidate")
	}

	rf.status = leader

	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderID: rf.me,
	}

	go func() {
		const maxRetries int = 3
		const retryDelay time.Duration = 200 * time.Microsecond

		for {
			rf.mu.RLock()
			checkStatus := rf.status == leader && rf.currentTerm == args.Term
			rf.mu.RUnlock()
			if !checkStatus {
				return
			}

			// send heartbeats to other peers in one interval
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(server int) {
					reply := &AppendEntriesReply{}
					for i := 0; i < maxRetries; i++ {
						ok := rf.sendAppendEntries(server, args, reply)
						if ok {
							rf.appendEntriesReplies <- *reply
							return
						}
						time.Sleep(retryDelay)
					}
				}(i)
			}

			time.Sleep(rf.heartbeatInterval)
		}
	}()
}

// convertToCandidate converts this peer to candidate,
// and starts a new election.
// It assumes the caller has required the mutex.
func (rf *Raft) convertToCandidate() {
	// DPrintf("%d starts convertToCandidate\n", rf.me)
	// defer DPrintf("%d stops convertToCandidate\n", rf.me)

	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	rf.status = candidate

	rf.currentTerm += 1

	rf.votedFor = rf.me
	rf.numGrantedVotes = 1

	rf.resetTimer()

	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}

	const maxRetries int = 100
	const retryDelay time.Duration = 10 * time.Microsecond

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			for i := 0; i < maxRetries; i++ {
				ok := rf.sendRequestVote(server, args, reply)
				if ok {
					rf.requestVoteReplies <- *reply
					return
				}
				time.Sleep(retryDelay)
			}
		}(i)
	}
}

// actAsFollower follows the rules that are for followers,
// as specified in figure 2 of the original paper.
// It assumes the caller has required the mutex.
func (rf *Raft) actAsFollower(term int32) {
	DPrintf("%d starts actAsFollower\n", rf.me)
	defer DPrintf("%d stops actAsFollower\n", rf.me)

	for {
		rf.mu.Lock()
		if rf.status != follower || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		select {
		case <-rf.timer.C:
			rf.convertToCandidate()
			rf.mu.Unlock()
			return
		default:
		}
		rf.mu.Unlock()
		takeNap()
	}
}

// actAsCandidate follows rules that are for candidates,
// as specified in figure 2 of the original paper.
// It assumes the caller has required the mutex.
func (rf *Raft) actAsCandidate(term int32) {
	// DPrintf("%d starts actAsCandidate\n", rf.me)
	// defer DPrintf("%d stops actAsCandidate\n", rf.me)

	for {
		rf.mu.Lock()
		if rf.status != candidate || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		select {
		case reply := <-rf.requestVoteReplies:
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term, true)
				rf.mu.Unlock()
				return
			}
			if reply.Term == rf.getTerm() && reply.VoteGranted {
				rf.numGrantedVotes++
				if 2*rf.numGrantedVotes > len(rf.peers) {
					rf.convertToLeader()
					rf.mu.Unlock()
					return
				}
			}
		case <-rf.timer.C:
			rf.convertToCandidate()
			rf.mu.Unlock()
			return
		default:
		}
		rf.mu.Unlock()
		takeNap()
	}
}

// actAsLeader follows rules that are for leader,
// as specified in figure 2 of the original paper.
// It assumes the caller has required the mutex.
func (rf *Raft) actAsLeader(term int32) {
	DPrintf("%d starts actAsLeader\n", rf.me)
	defer DPrintf("%d stops actAsLeader\n", rf.me)

	for {
		rf.mu.Lock()
		if rf.status != leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		select {
		case reply := <-rf.appendEntriesReplies:
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term, true)
				rf.mu.Unlock()
				return
			}
			if reply.Term == rf.getTerm() {
				//TODO
				// handle replies from other peers
			}
		default:
			if !rf.checkStatus(leader) {
				rf.mu.Unlock()
				return
			}
		}
		rf.mu.Unlock()
		takeNap()
	}
}

func takeNap() {
	time.Sleep(5 * time.Microsecond)
}
