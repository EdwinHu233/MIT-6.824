package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int32
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int32
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int32
	Success bool
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
	// DPrintf("%d starts sendRequestVote to %d\n", rf.me, server)
	// defer DPrintf("%d stops sendRequestVote to %d\n", rf.me, server)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// DPrintf("%d starts sendAppendEntries\n", rf.me)
	// defer DPrintf("%d stops sendAppendEntries\n", rf.me)

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// // sendAppendEntriesWithRetries send AppendEntries RPC to one peer,
// // possibly with a few retries.
// // It doesn't assume the caller has required mutex.
// func (rf *Raft) sendRequestVoteWithRetries(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	// DPrintf("%d starts sendRequestVoteWithRetries\n", rf.me)
// 	// defer DPrintf("%d stops sendRequestVoteWithRetries\n", rf.me)
// 	for i := 0; i < maxNumRetry; i++ {

// 		rf.mu.Lock()
// 		if rf.status != candidate ||
// 			rf.currentTerm != args.Term {
// 			// This peer's states have changed. No need to send.
// 			rf.mu.Unlock()
// 			return false
// 		}
// 		ok := rf.sendRequestVote(server, args, reply)
// 		rf.mu.Unlock()

// 		if ok {
// 			DPrintf("%d starts to send on rf.requestVoteReplies\n", rf.me)
// 			rf.requestVoteReplies <- *reply
// 			DPrintf("%d stops to send on rf.requestVoteReplies\n", rf.me)
// 			return true
// 		}
// 		time.Sleep(retryDelay)
// 	}
// 	return false
// }

// // sendAppendEntriesWithRetries send AppendEntries RPC to one peer,
// // possibly with a few retries.
// // It doesn't assume the caller has required mutex.
// func (rf *Raft) sendAppendEntriesWithRetries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
// 	for i := 0; i < maxNumRetry; i++ {

// 		rf.mu.Lock()
// 		if rf.status != leader ||
// 			rf.currentTerm != args.Term {
// 			// This peer's states have changed. No need to send.
// 			rf.mu.Unlock()
// 			return false
// 		}
// 		ok := rf.sendAppendEntries(server, args, reply)
// 		rf.mu.Unlock()

// 		if ok {
// 			rf.appendEntriesReplies <- *reply
// 			return true
// 		}
// 		time.Sleep(retryDelay)
// 	}
// 	return false
// }

// // sendHeartbeatWithRetries send heartbeat to one peer,
// // possibly with a few retries.
// // It doesn't assume the caller has required mutex.
// func (rf *Raft) sendHeartbeatWithRetries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
// 	for i := 0; i < maxNumRetry; i++ {

// 		// This peer's states have changed. No need to send.
// 		if rf.status != leader ||
// 			rf.currentTerm != args.Term {
// 			return false
// 		}

// 		if rf.sendAppendEntries(server, args, reply) {
// 			rf.appendEntriesReplies <- *reply
// 			return true
// 		}

// 		time.Sleep(retryDelay)
// 	}
// 	return false
// }

// // sendAllHeartbeatsWithRetries sends heartbeats to other peers,
// // and starts a new heartbeat interval.
// // It assumes the caller has required the mutex.
// func (rf *Raft) sendAllHeartbeatsWithRetries() {
// 	if rf.status != leader {
// 		log.Fatal("sendAllHeartbeat: not a leader")
// 	}
// 	for i := range rf.peers {
// 		if i != rf.me {
// 			args := &AppendEntriesArgs{
// 				Term:     rf.currentTerm,
// 				LeaderID: rf.me,
// 			}
// 			reply := &AppendEntriesReply{}
// 			go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
// 				rf.sendHeartbeatWithRetries(server, args, reply)
// 			}(i, args, reply)
// 		}
// 	}
// }

// // collectRequestVoteReplies drains rf.requestVoteReplies,
// // add granted votes to rf.numGrantedVotes,
// // and can possibly convert this peer back to follower.
// func (rf *Raft) collectRequestVoteReplies() {
// 	for {
// 		select {
// 		case reply := <-rf.requestVoteReplies:
// 			if reply.Term > rf.currentTerm {
// 				rf.convertToFollower(reply.Term, true)
// 			}
// 			if reply.Term == rf.currentTerm {
// 				if reply.VoteGranted {
// 					rf.numGrantedVotes++
// 				}
// 			}
// 		default:
// 			return
// 		}
// 	}
// }

// // collectAppendEntriesReplies drains rf.appendEntriesReplies,
// // and can possibly convert this peer back to follower.
// func (rf *Raft) collectAppendEntriesReplies() {
// 	for {
// 		select {
// 		case reply := <-rf.appendEntriesReplies:
// 			if reply.Term > rf.currentTerm {
// 				rf.convertToFollower(reply.Term, true)
// 			}
// 			if reply.Term == rf.currentTerm &&
// 				rf.status == leader {
// 				// TODO
// 				// leader should do something when recieving replies from other peers
// 			}
// 		default:
// 			return
// 		}
// 	}
// }

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// DPrintf("%d starts handling RequestVote\n", rf.me)
	// defer DPrintf("%d stops handling RequestVote\n", rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// recieving from outdated leader,
	// return immediately.
	term := rf.currentTerm
	if args.Term < term {
		reply.Term = term
		reply.VoteGranted = false
		return
	}

	// recieving from a newer term
	if args.Term > term {
		rf.convertToFollower(args.Term, rf.status != follower)
	}

	// check if this peer should grant vote.
	// (computation are local to this function,
	// doesn't change state yet)
	selfLastIndex := len(rf.log) - 1
	selfLastTerm := rf.log[selfLastIndex].Term

	var voteGranted bool
	if rf.votedFor >= 0 && rf.votedFor != args.CandidateID {
		voteGranted = false
	} else if isMoreUpToDate(selfLastTerm, selfLastIndex, args.LastLogTerm, args.LastLogIndex) {
		voteGranted = false
	} else {
		voteGranted = true
	}

	if voteGranted {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.resetTimer()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// DPrintf("%d starts handling AppendEntries\n", rf.me)
	// defer DPrintf("%d stops handling AppendEntries\n", rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// recieving from outdated leader,
	// return immediately
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	switch rf.status {
	case follower:
		rf.convertToFollower(args.Term, true)
	case candidate:
		rf.convertToFollower(args.Term, true)
	case leader:
		if args.Term > rf.currentTerm {
			rf.convertToFollower(args.Term, true)
		}
	}

	// TODO
	// return false if log doesn't contain an entry at PrevLogIndex whose term matches PrevLogTerm

	// TODO
	// if an existing entry conflicts with a new one
	// (same index but different terms),
	// delete the the existing entry and all that follow it

	// TODO
	// append any new entries not already in the log

	// TODO
	// if leaderCommit > commitIndex,
	// set commitIndex = min(LeaderCommit, index of the last new entry)
}
