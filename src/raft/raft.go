package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"lab6824/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "lab6824/labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Command interface{}
	Term    int32
}

const (
	follower int32 = iota
	candidate
	leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	// mu        sync.Mutex          // Lock to protect shared access to this peer's state

	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	status int32

	// recieved RequestVoteReply
	requestVoteReplies chan RequestVoteReply
	// recieved AppendEntriesReply
	appendEntriesReplies chan AppendEntriesReply

	// ----------------------------------------
	// some information needed by the heartbeat mechanism
	// (only valid for leader)
	// ----------------------------------------

	// stores when precedent heartbeats in one second were sent
	// (the earliest is at head of the queue)
	// heartbeatQueue ConcurrentQueue
	// how often should the leader send heartbeats to all other peers
	heartbeatInterval time.Duration
	// when did the leader send heartbeats to other peers
	// lastHeartbeatTime time.Time

	// ----------------------------------------
	// some information of timing
	// ----------------------------------------

	// election timeout as described in the paper
	electionTimeout time.Duration

	// when the timer starts
	timerStart time.Time

	// ----------------------------------------
	// some information needed when leader election
	// (only valid for candidate)
	// ----------------------------------------

	// number of granted votes
	numGrantedVotes int

	// ----------------------------------------
	// persistent state on all servers
	// ----------------------------------------

	// latest term it has seen
	currentTerm int32

	// index of a peer recieved vote from this peer
	votedFor int

	// log entries (index starts at 1)
	log []LogEntry

	// ----------------------------------------
	// volatile state on all servers
	// ----------------------------------------

	// index of highest entry known to be commited
	// (initialized to 0)
	commitIndex int

	// index of highest entry applied to local state machine
	// (initialized to 0)
	lastApplied int

	// ----------------------------------------
	// volatile state on leader
	// ----------------------------------------

	// for each server, index of the next log entry to send to that server
	// (initialized to leader_last_log_index + 1)
	nextIndex []int

	// for each server, index of the highest log entry known to be replicated
	// (initialized to 0, increases monotonically)
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	status := rf.status
	return int(term), status == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// DPrintf("%v: recieve from client\n", rf.me)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.status != leader {
		return -1, -1, false
	}

	index := len(rf.log)
	term := rf.currentTerm

	DPrintf("leader %v: recieve from client, index=%v, term=%v\n", rf.me, index, term)

	rf.log = append(rf.log, LogEntry{
		Command: command,
		Term:    term,
	})
	rf.matchIndex[rf.me] = index

	return index, int(term), true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rand.Seed(time.Now().UnixNano())

	rf.status = follower
	rf.requestVoteReplies = make(chan RequestVoteReply)
	rf.appendEntriesReplies = make(chan AppendEntriesReply)

	rf.heartbeatInterval = 50 * time.Millisecond

	rf.resetTimer()

	rf.numGrantedVotes = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	// create an "empty" log (index starts at 1)
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	go rf.convertToFollower(0)
	go rf.applyLoop(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
