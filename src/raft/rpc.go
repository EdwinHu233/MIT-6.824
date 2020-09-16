package raft

import "sort"

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int32
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int32
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
	PrevLogTerm  int32
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// DPrintf("%d starts handling RequestVote\n", rf.me)
	// defer DPrintf("%d stops handling RequestVote\n", rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// recieves from outdated candidate,
	// reject and return immediately.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// recieving from a newer term,
	// convert back to follower.
	if args.Term > rf.currentTerm &&
		rf.status != follower {
		rf.convertToFollower(args.Term)
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

	reply.Term = rf.currentTerm

	// recieving from outdated leader,
	// return immediately
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// recieving from current leader,
	// convert back to follower.
	rf.convertToFollower(args.Term)

	// TODO
	// return false if log doesn't contain an entry at PrevLogIndex whose term matches PrevLogTerm
	if len(rf.log) <= args.PrevLogIndex ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	reply.Success = true

	// TODO
	// 1. if an existing entry conflicts with a new one
	// (same index but different terms),
	// delete the the existing entry and all that follow it
	// 2. append any new entries not already in the log
	i := 0
	for ; i < len(args.Entries); i++ {
		j := args.PrevLogIndex + 1 + i
		if j >= len(rf.log) ||
			rf.log[j].Term != args.Entries[i].Term {
			break
		}
	}
	j := args.PrevLogIndex + 1 + i
	rf.log = append(rf.log[:j], args.Entries[i:]...)

	// TODO
	// if leaderCommit > commitIndex,
	// set commitIndex = min(LeaderCommit, index of the last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastLogIndex := len(rf.log) - 1
		if args.LeaderCommit < lastLogIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastLogIndex
		}
	}
}

func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reply is outdated
	// (this peer's states have changed after sending request)
	// discard it
	if rf.status != leader ||
		rf.currentTerm != args.Term {
		return
	}

	// reply is not outdated
	// handle it the right way

	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		return
	}

	// TODO rules of log replication

	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		rf.matchIndex[server] = match
		rf.nextIndex[server] = match + 1
		matches := make([]int, len(rf.matchIndex))
		for i := range matches {
			matches[i] = rf.matchIndex[i]
		}
		sort.Ints(matches)
		for n := rf.commitIndex + 1; n <= matches[len(matches)/2]; n++ {
			if rf.log[n].Term == rf.currentTerm {
				rf.commitIndex = n
			}
		}
	} else {
		rf.nextIndex[server] = args.PrevLogIndex
	}
}

func (rf *Raft) handleRequestVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reply is outdated
	// (this peer's states have changed after sending request)
	// discard it
	if rf.status != candidate ||
		rf.currentTerm != args.Term {
		return
	}

	// reply is not outdated
	// handle it the right way

	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		return
	}

	if reply.VoteGranted {
		rf.numGrantedVotes++
		if rf.numGrantedVotes*2 > len(rf.peers) {
			rf.convertToLeader()
		}
	}
}
