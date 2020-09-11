package raft

import (
	"log"
	"time"
)

func (rf *Raft) convertToFollower(newTerm int32, resetTimer bool) {
	if newTerm < rf.currentTerm {
		log.Fatal("convertToFollower: outdated newTerm")
	}

	rf.status = follower

	if newTerm > rf.currentTerm {
		rf.currentTerm = newTerm
		rf.votedFor = -1
	}

	// TODO
	// Should the peer reset timer when converts to follower?
	if resetTimer {
		rf.resetTimer()
	}
}

// convertToLeader converts this peer to leader,
// and sends heartbeats to other peers.
func (rf *Raft) convertToLeader() {
	if rf.status != candidate {
		log.Fatalf("convertToLeader: not a candidate")
	}
}

// convertToCandidate converts this peer to candidate,
// and starts a new election.
// It assumes the caller has required the mutex.
func (rf *Raft) convertToCandidate() {
	// DPrintf("%d starts convertToCandidate\n", rf.me)
	// defer DPrintf("%d stops convertToCandidate\n", rf.me)

	rf.status = candidate

	rf.currentTerm += 1

	rf.votedFor = rf.me
	rf.numGrantedVotes = 1

	rf.resetTimer()

	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				args := RequestVoteArgs{
					Term:         rf.getTerm(),
					CandidateID:  rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
				}
				reply := RequestVoteReply{}
				ok := rf.sendRequestVoteWithRetries(server, &args, &reply)
				if !ok {
					reply.VoteGranted = false
					reply.Term = -1
				}
				rf.requestVoteReplies <- reply
			}(i)
		}
	}
}

// actAsFollower follows the rules that are for followers,
// as specified in figure 2 of the original paper.
// It assumes the caller has required the mutex.
func (rf *Raft) actAsFollower() {
	// DPrintf("%d starts actAsFollower\n", rf.me)
	// defer DPrintf("%d stops actAsFollower\n", rf.me)

	for {
		select {
		case <-rf.timer.C:
			rf.convertToCandidate()
			return
		default:
			if !rf.checkStatus(follower) {
				return
			}
			takeNap()
		}
	}
}

// actAsCandidate follows rules that are for candidates,
// as specified in figure 2 of the original paper.
// It assumes the caller has required the mutex.
func (rf *Raft) actAsCandidate() {
	// DPrintf("%d starts actAsCandidate\n", rf.me)
	// defer DPrintf("%d stops actAsCandidate\n", rf.me)

	for {
		select {
		case reply := <-rf.requestVoteReplies:
			if reply.Term > rf.getTerm() {
				rf.convertToFollower(reply.Term, true)
			}
			if reply.Term == rf.getTerm() && reply.VoteGranted {
				rf.numGrantedVotes++
				if 2*rf.numGrantedVotes > len(rf.peers) {
					rf.status = leader
					rf.sendAllHeartbeatsWithRetries()
					return
				}
			}
		case <-rf.timer.C:
			rf.convertToCandidate()
		default:
			if !rf.checkStatus(candidate) {
				return
			}
			takeNap()
		}
	}
}

// actAsLeader follows rules that are for leader,
// as specified in figure 2 of the original paper.
// It assumes the caller has required the mutex.
func (rf *Raft) actAsLeader() {
	// DPrintf("%d starts actAsLeader\n", rf.me)
	// defer DPrintf("%d stops actAsLeader\n", rf.me)

	go func() {
		for rf.checkStatus(leader) {
			rf.sendAllHeartbeatsWithRetries()
			time.Sleep(rf.heartbeatInterval)
		}
	}()

	for {
		select {
		case reply := <-rf.appendEntriesReplies:
			if reply.Term > rf.getTerm() {
				rf.convertToFollower(reply.Term, true)
			}
			if reply.Term == rf.getTerm() {
				//TODO
				// handle replies from other peers
			}
		default:
			if !rf.checkStatus(leader) {
				return
			}
			takeNap()
		}
	}
}

func takeNap() {
	time.Sleep(5 * time.Microsecond)
}
