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
	Term    int
}

type PeerStatus int

const (
	FOLLOWER PeerStatus = iota
	CANDIDATE
	LEADER
)

type VoteStatus int

const (
	GRANTED VoteStatus = iota
	REFUSED
	UNKNOWN
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	status                PeerStatus    // current status this peer thinks itself is
	lastRecievedHeartbeat time.Time     // the last time when this peer recieved a heartbeat
	heartbeatInterval     time.Duration // how often the leader sends heartbeat (only meaningful to leader)
	nextHeartbeatTarget   int           // index of the next target of heartbeat (only meaningful to leader)
	electionTimeout       time.Duration // election timeout as described in the paper
	recivedVotes          []VoteStatus  // recieved votes from every peer (only meaningful for candidate)
	nextRequestVoteTarget int           // index of the next target to send RequestVote
	numGrantedVotes       int           // number of granted votes
	electionStartTime     time.Time     // when this current election started (only meaningful for candidate)

	// --- persistent state on all servers ---

	currentTerm int        // latest term it has seen
	votedFor    int        // index of a peer recieved vote from this peer
	log         []LogEntry // log entries (index starts at 1)

	// --- volatile state on all servers ---

	commitIndex int // index of highest entry known to be commited (initialized to 0)
	lastApplied int // index of highest entry applied to local state machine

	// --- volatile state on leader ---

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
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.status == LEADER
	return term, isleader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
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
	Term        int
	voteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	vote := func() bool {
		if args.Term < rf.currentTerm {
			return false
		}
		if rf.votedFor >= 0 && rf.votedFor != args.CandidateID {
			return false
		}
		var lastLogTerm, lastLogIndex int
		if len(rf.log) > 1 {
			lastLogTerm = rf.log[len(rf.log)-1].Term
			lastLogIndex = len(rf.log)
		}
		if lastLogTerm != args.LastLogTerm {
			return args.LastLogTerm > lastLogTerm
		}
		return args.LastLogIndex >= lastLogIndex
	}

	reply.Term = rf.currentTerm
	if vote() {
		reply.voteGranted = true
		rf.votedFor = args.CandidateID
	} else {
		reply.voteGranted = false
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

// startNewElection starts a new election, assuming all operations have already been protected by lock
func (rf *Raft) startNewElection() {
	rf.status = CANDIDATE
	for i := range rf.recivedVotes {
		rf.recivedVotes[i] = UNKNOWN
	}
	rf.recivedVotes[rf.me] = GRANTED
	rf.nextRequestVoteTarget = 0
	rf.numGrantedVotes = 1

	rf.currentTerm += 1
	rf.votedFor = rf.me
}

// assuming all operations have already been protected by lock
func (rf *Raft) actAsFollower() {
	if time.Now().After(rf.lastRecievedHeartbeat.Add(rf.electionTimeout)) {
		rf.startNewElection()
	}
}

// assuming all operations have already been protected by lock
func (rf *Raft) actAsCandidate() {
	if time.Now().After(rf.electionStartTime.Add(rf.electionTimeout)) {
		rf.startNewElection()
	} else {
		if rf.numGrantedVotes*2 > len(rf.peers) {
			// candidate->leader
			rf.status = LEADER
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(i int) {
					// need further change for 2-b
					// more specicially, the following code only
					// does heartbeat to another peer,
					// instead of relicating leader's log
					args := AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderID: rf.me,
					}
					reply := AppendEntriesReply{}
					if rf.sendAppendEntries(i, &args, &reply) {
					}
				}(i)
			}
		} else {
			target := -1
			for i := 0; i < len(rf.peers); i += 1 {
				j := (i + rf.nextRequestVoteTarget) % len(rf.peers)
				if rf.recivedVotes[j] == UNKNOWN {
					target = j
					break
				}
			}
			if target == -1 {
				rf.status = FOLLOWER
				rf.lastRecievedHeartbeat = time.Now()
			} else {
				args := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateID:  rf.me,
					LastLogIndex: len(rf.log) - 1,
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
				}
				var reply RequestVoteReply
				if rf.sendRequestVote(target, &args, &reply) {
					if reply.voteGranted {
						rf.recivedVotes[target] = GRANTED
						rf.numGrantedVotes += 1
					} else {
						rf.recivedVotes[target] = REFUSED
					}
					rf.nextRequestVoteTarget = (target + 1) % len(rf.peers)
				} else {
					rf.nextRequestVoteTarget = target
				}
			}
		}
	}
}

// assuming all operations have already been protected by lock
func (rf *Raft) actAsLeader() {

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

	rf.status = FOLLOWER
	rf.lastRecievedHeartbeat = time.Now()
	// cannot send more than 10 heartbeats, as limited by the tester
	rf.heartbeatInterval = time.Microsecond * 110
	rf.nextHeartbeatTarget = 0
	// timeout = (3 ~ 6) * interval
	magnification := 3 * (rand.Float32() + 1)
	rf.electionTimeout = time.Duration(magnification) * rf.heartbeatInterval
	rf.recivedVotes = make([]VoteStatus, len(peers))

	rf.currentTerm = 0
	rf.votedFor = -1
	// create an empty log (index starts at 1)
	rf.log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			switch rf.status {
			case FOLLOWER:
				rf.actAsFollower()
			case CANDIDATE:
				rf.actAsCandidate()
			case LEADER:
				rf.actAsLeader()
			}
			rf.mu.Unlock()
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
